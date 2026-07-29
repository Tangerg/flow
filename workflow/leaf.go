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
	run(context.Context, I, leafInvocation) (O, error)
}

type nodeRunner[I, O any] struct {
	node flow.Node[I, O]
}

func (runner nodeRunner[I, O]) validate() error {
	if runner.node == nil {
		return flow.ErrNilNode
	}
	if function, ok := runner.node.(flow.NodeFunc[I, O]); ok && function == nil {
		return flow.ErrNilNode
	}
	return nil
}

func (runner nodeRunner[I, O]) run(
	ctx context.Context,
	input I,
	_ leafInvocation,
) (O, error) {
	return runner.node.Run(ctx, input)
}

// leafInvocation identifies one execution of a leaf. Runners receive only the
// state that belongs to the invocation; the immutable step definition remains
// safe to reuse concurrently.
type leafInvocation struct {
	id   string
	path []string
	run  *runState
}

func (leaf leafStep[I, O]) Run(ctx context.Context, store Store) (Store, error) {
	execution := leafExecution[I, O]{
		leaf:  leaf,
		store: store,
		run:   runFrom(ctx),
	}
	return execution.execute(ctx)
}

func (leaf leafStep[I, O]) Describe() Description {
	return Description{ID: leaf.id, Kind: "leaf"}
}

func (leaf leafStep[I, O]) workflowDefinition() stepDefinition {
	return stepDefinition{kind: definitionNamed, id: leaf.id}
}

// leafExecution owns the mutable state of one leaf invocation. The leaf
// definition remains immutable and safe for concurrent use.
type leafExecution[I, O any] struct {
	leaf    leafStep[I, O]
	store   Store
	run     *runState
	started time.Time
}

func (execution *leafExecution[I, O]) execute(ctx context.Context) (Store, error) {
	if err := execution.validate(ctx); err != nil {
		execution.run.emit(ctx, Event{
			Kind: EventFailed,
			ID:   execution.leaf.id,
			Err:  err,
		})
		return execution.store, err
	}
	if replayed, ok := execution.replay(ctx); ok {
		return replayed, nil
	}

	execution.start(ctx)
	input, err := execution.leaf.bind(execution.store)
	if err != nil {
		return execution.fail(ctx, OpBind, err)
	}
	output, err := execution.leaf.runner.run(ctx, input, leafInvocation{
		id:   execution.leaf.id,
		path: scope(ctx),
		run:  execution.run,
	})
	if err != nil {
		return execution.fail(ctx, OpRun, err)
	}
	return execution.complete(ctx, output)
}

// validate runs before replay so stale Journal data cannot hide an invalid
// workflow definition. [Leaf] and [StreamLeaf] always install a runner, so only
// the computation it holds can be nil, which runner.validate reports.
func (execution *leafExecution[I, O]) validate(ctx context.Context) error {
	switch {
	case execution.leaf.id == "":
		return &StepError{ID: execution.leaf.id, Op: OpValidate, Err: ErrInvalidStepID}
	case execution.leaf.bind == nil:
		return &StepError{ID: execution.leaf.id, Op: OpBind, Err: flow.ErrNilFunc}
	default:
		if err := execution.leaf.runner.validate(); err != nil {
			return &StepError{ID: execution.leaf.id, Op: OpRun, Err: err}
		}
		if err := execution.run.claim(scope(ctx), execution.leaf.id); err != nil {
			return &StepError{ID: execution.leaf.id, Op: OpValidate, Err: err}
		}
		return nil
	}
}

func (execution *leafExecution[I, O]) replay(ctx context.Context) (Store, bool) {
	value, ok := execution.run.replay(scope(ctx), execution.leaf.id)
	if !ok {
		return Store{}, false
	}
	next := execution.store.WithOutput(execution.leaf.id, value)
	execution.run.emit(ctx, Event{
		Kind:  EventSkipped,
		ID:    execution.leaf.id,
		Store: next,
	})
	return next, true
}

func (execution *leafExecution[I, O]) start(ctx context.Context) {
	if !execution.run.observing() {
		return
	}
	execution.started = time.Now()
	execution.run.emit(ctx, Event{
		Kind: EventStarted,
		ID:   execution.leaf.id,
	})
}

func (execution *leafExecution[I, O]) fail(
	ctx context.Context,
	op StepOp,
	err error,
) (Store, error) {
	if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
		return execution.suspend(ctx, suspensions)
	}

	stepErr := &StepError{ID: execution.leaf.id, Op: op, Err: err}
	if execution.run.observing() {
		execution.run.emit(ctx, Event{
			Kind:    EventFailed,
			ID:      execution.leaf.id,
			Elapsed: time.Since(execution.started),
			Err:     stepErr,
		})
	}
	return execution.store, stepErr
}

func (execution *leafExecution[I, O]) suspend(
	ctx context.Context,
	suspensions suspensionList,
) (Store, error) {
	err := suspensions.identify(execution.leaf.id, scope(ctx)).err()
	if execution.run.observing() {
		execution.run.emit(ctx, Event{
			Kind:    EventSuspended,
			ID:      execution.leaf.id,
			Elapsed: time.Since(execution.started),
			Err:     err,
		})
	}
	return execution.store, err
}

func (execution *leafExecution[I, O]) complete(ctx context.Context, output O) (Store, error) {
	next := execution.store.WithOutput(execution.leaf.id, output)
	if err := execution.run.journal().record(scope(ctx), execution.leaf.id, output); err != nil {
		return execution.fail(ctx, OpRun, err)
	}
	if execution.run.observing() {
		execution.run.emit(ctx, Event{
			Kind:    EventCompleted,
			ID:      execution.leaf.id,
			Elapsed: time.Since(execution.started),
			Store:   next,
		})
	}
	return next, nil
}
