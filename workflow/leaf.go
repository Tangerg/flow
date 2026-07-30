package workflow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Tangerg/flow"
)

// BindFunc reads a typed input from a Store. Create one with [From], or write
// one inline when a step needs to read several references.
type BindFunc[I any] func(Store) (I, error)

// From returns a BindFunc that reads a value of type I from ref.
func From[I any](ref Ref) BindFunc[I] {
	return func(store Store) (I, error) { return Get[I](store, ref) }
}

// FirstOf returns a BindFunc that reads the first reference that resolves.
// Missing references are skipped in argument order. Once a reference resolves,
// its conversion error is returned rather than trying a later reference. If no
// reference resolves, the result wraps [ErrNotFound].
//
// FirstOf is useful at a merge point whose mutually exclusive upstream paths
// write the same logical input under different step IDs.
func FirstOf[I any](refs ...Ref) BindFunc[I] {
	refs = slices.Clone(refs)
	return func(store Store) (I, error) {
		for _, ref := range refs {
			value, err := Get[I](store, ref)
			if err == nil {
				return value, nil
			}
			if !errors.Is(err, ErrNotFound) {
				var zero I
				return zero, err
			}
		}
		var zero I
		return zero, fmt.Errorf("%w: none of %d references resolve", ErrNotFound, len(refs))
	}
}

// Leaf turns a statically typed node into a [Step]. On each run it binds the
// node's input from the Store, runs it, and writes the result under
// [Output]. Errors are tagged with the step id, lifecycle events are emitted, and
// a run's [Journal] can replay the step instead of repeating it (see [RunConfig]).
//
// This is the prep/exec/post split: bind reads the pool, node computes, the Step
// writes back — the node itself stays free of any Store knowledge and is unit
// testable on its own. id must be non-empty and unique among steps that can run
// in the same execution scope. A policy that may invoke work more than once,
// such as retry or hedging, must wrap node before it is passed to Leaf; invoking
// the returned Step twice in one scope fails with [ErrDuplicateStep].
func Leaf[I, O any](id string, bind BindFunc[I], node flow.Node[I, O]) Step {
	return leafStep[I, O]{
		id:     id,
		bind:   bind,
		runner: nodeRunner[I, O]{node: node},
	}
}

// LeafFunc lifts an ordinary function into a [Step] that reads its input from
// ref. It is the concise form of combining [Leaf], [From], and [flow.NodeFunc].
func LeafFunc[I, O any](
	id string,
	ref Ref,
	fn func(context.Context, I) (O, error),
) Step {
	return Leaf(id, From[I](ref), flow.NodeFunc[I, O](fn))
}

// leafStep is the [Step] produced by [Leaf].
type leafStep[I, O any] struct {
	id     string
	bind   BindFunc[I]
	runner leafRunner[I, O]
}

// leafRunner is the typed computation inside a leaf boundary. Both ordinary
// and streaming leaves use it, which keeps binding, replay, events, suspension,
// journaling, and output publication in one execution path.
type leafRunner[I, O any] interface {
	validate() error
	run(ctx context.Context, input I, invocation leafInvocation) (O, error)
}

type nodeRunner[I, O any] struct {
	node flow.Node[I, O]
}

func (n nodeRunner[I, O]) validate() error {
	if n.node == nil {
		return flow.ErrNilNode
	}
	if function, ok := n.node.(flow.NodeFunc[I, O]); ok && function == nil {
		return flow.ErrNilNode
	}
	return nil
}

func (n nodeRunner[I, O]) run(
	ctx context.Context,
	input I,
	_ leafInvocation,
) (O, error) {
	return n.node.Run(ctx, input)
}

// leafInvocation identifies one execution of a leaf. Runners receive only the
// state that belongs to the invocation; the immutable step definition remains
// safe to reuse concurrently.
type leafInvocation struct {
	id   string
	path []string
	run  *runState
}

func (l leafStep[I, O]) Run(ctx context.Context, store Store) (Store, error) {
	execution := leafExecution[I, O]{
		leaf:  l,
		store: store,
		run:   runFrom(ctx),
	}
	return execution.execute(ctx)
}

func (l leafStep[I, O]) Describe() Description {
	return Description{ID: l.id, Kind: "leaf"}
}

func (l leafStep[I, O]) workflowDefinition() stepDefinition {
	return stepDefinition{kind: definitionNamed, id: l.id}
}

// leafExecution owns the mutable state of one leaf invocation. The leaf
// definition remains immutable and safe for concurrent use.
type leafExecution[I, O any] struct {
	leaf    leafStep[I, O]
	store   Store
	run     *runState
	started time.Time
}

func (l *leafExecution[I, O]) execute(ctx context.Context) (Store, error) {
	if err := l.validate(ctx); err != nil {
		l.run.emit(ctx, Event{
			Kind: EventFailed,
			ID:   l.leaf.id,
			Err:  err,
		})
		return l.store, err
	}
	if replayed, ok := l.replay(ctx); ok {
		return replayed, nil
	}

	l.start(ctx)
	input, err := l.leaf.bind(l.store)
	if err != nil {
		return l.fail(ctx, OpBind, err)
	}
	output, err := l.leaf.runner.run(ctx, input, leafInvocation{
		id:   l.leaf.id,
		path: scope(ctx),
		run:  l.run,
	})
	if err != nil {
		return l.fail(ctx, OpRun, err)
	}
	return l.complete(ctx, output)
}

// validate runs before replay so stale Journal data cannot hide an invalid
// workflow definition. [Leaf] and [StreamLeaf] always install a runner, so only
// the computation it holds can be nil, which runner.validate reports.
func (l *leafExecution[I, O]) validate(ctx context.Context) error {
	switch {
	case l.leaf.id == "":
		return &StepError{ID: l.leaf.id, Op: OpValidate, Err: ErrInvalidStepID}
	case l.leaf.bind == nil:
		return &StepError{ID: l.leaf.id, Op: OpBind, Err: flow.ErrNilFunc}
	default:
		if err := l.leaf.runner.validate(); err != nil {
			return &StepError{ID: l.leaf.id, Op: OpRun, Err: err}
		}
		if err := l.run.claim(scope(ctx), l.leaf.id); err != nil {
			return &StepError{ID: l.leaf.id, Op: OpValidate, Err: err}
		}
		return nil
	}
}

func (l *leafExecution[I, O]) replay(ctx context.Context) (Store, bool) {
	value, ok := l.run.replay(scope(ctx), l.leaf.id)
	if !ok {
		return Store{}, false
	}
	next := l.store.WithOutput(l.leaf.id, value)
	l.run.emit(ctx, Event{
		Kind:  EventSkipped,
		ID:    l.leaf.id,
		Store: next,
	})
	return next, true
}

func (l *leafExecution[I, O]) start(ctx context.Context) {
	if !l.run.observing() {
		return
	}
	l.started = time.Now()
	l.run.emit(ctx, Event{
		Kind: EventStarted,
		ID:   l.leaf.id,
	})
}

func (l *leafExecution[I, O]) fail(
	ctx context.Context,
	op StepOp,
	err error,
) (Store, error) {
	if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
		return l.suspend(ctx, suspensions)
	}

	stepErr := &StepError{ID: l.leaf.id, Op: op, Err: err}
	if l.run.observing() {
		l.run.emit(ctx, Event{
			Kind:    EventFailed,
			ID:      l.leaf.id,
			Elapsed: time.Since(l.started),
			Err:     stepErr,
		})
	}
	return l.store, stepErr
}

func (l *leafExecution[I, O]) suspend(
	ctx context.Context,
	suspensions suspensionList,
) (Store, error) {
	err := suspensions.identify(l.leaf.id, scope(ctx)).err()
	if l.run.observing() {
		l.run.emit(ctx, Event{
			Kind:    EventSuspended,
			ID:      l.leaf.id,
			Elapsed: time.Since(l.started),
			Err:     err,
		})
	}
	return l.store, err
}

func (l *leafExecution[I, O]) complete(ctx context.Context, output O) (Store, error) {
	next := l.store.WithOutput(l.leaf.id, output)
	if err := l.run.journal().record(scope(ctx), l.leaf.id, output); err != nil {
		return l.fail(ctx, OpRun, err)
	}
	if l.run.observing() {
		l.run.emit(ctx, Event{
			Kind:    EventCompleted,
			ID:      l.leaf.id,
			Elapsed: time.Since(l.started),
			Store:   next,
		})
	}
	return next, nil
}
