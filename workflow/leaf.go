package workflow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Tangerg/flow"
)

// Binder prepares a typed input for a [Leaf] from its current Store. It is the
// input-side counterpart to [flow.Node]: Bind prepares data, then Node.Run
// performs the computation. Implementations must be safe for concurrent use
// when the enclosing Leaf is run concurrently. A nil Binder or nil BinderFunc
// is invalid; a nil pointer remains an ordinary Binder value because its Bind
// method may intentionally define nil-receiver behavior. Bind has no context
// because it should only derive an input from the supplied Store; blocking I/O
// and other cancellable work belong in Node.Run.
//
// A Binder with immutable definition state may implement a side-effect-free,
// concurrency-safe Validate() error method. Leaf calls it before Journal replay,
// using the same optional validation convention as [flow.Validate].
type Binder[I any] interface {
	Bind(store Store) (I, error)
}

type binderValidator interface {
	Validate() error
}

// BinderFunc adapts an ordinary function into a [Binder].
type BinderFunc[I any] func(Store) (I, error)

// BinderFunc satisfies Binder.
var _ Binder[any] = BinderFunc[any](nil)

// Bind calls f. A nil BinderFunc returns [flow.ErrNilFunc].
func (b BinderFunc[I]) Bind(store Store) (I, error) {
	if b == nil {
		var zero I
		return zero, flow.ErrNilFunc
	}
	return b(store)
}

// isNilBinder recognizes this package's nil function adapter hidden in an
// interface. Caller-defined concrete types own their nil-receiver behavior.
func isNilBinder[I any](binder Binder[I]) bool {
	if binder == nil {
		return true
	}
	function, ok := binder.(BinderFunc[I])
	return ok && function == nil
}

// From returns a Binder that reads a value of type I from ref. Leaf validates
// ref before Journal replay, so a completed record cannot hide a malformed
// built-in binding definition.
func From[I any](ref Ref) Binder[I] {
	return refBinder[I]{ref: ref}
}

type refBinder[I any] struct {
	ref Ref
}

func (r refBinder[I]) Bind(store Store) (I, error) { return Get[I](store, r.ref) }

func (r refBinder[I]) Validate() error { return r.ref.Validate() }

// FirstOf returns a Binder that reads the first reference that resolves.
// Missing references are skipped in argument order. Once a reference resolves,
// its conversion error is returned rather than trying a later reference. If no
// reference resolves, the result wraps [ErrNotFound].
//
// FirstOf is useful at a merge point whose mutually exclusive upstream paths
// write the same logical input under different step IDs. Leaf validates every
// ref before Journal replay, including references later than the one that would
// resolve at run time. At least one reference is required.
func FirstOf[I any](refs ...Ref) Binder[I] {
	return firstBinder[I]{refs: slices.Clone(refs)}
}

type firstBinder[I any] struct {
	refs []Ref
}

func (f firstBinder[I]) Bind(store Store) (I, error) {
	for _, ref := range f.refs {
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
	return zero, fmt.Errorf("%w: none of %d references resolve", ErrNotFound, len(f.refs))
}

func (f firstBinder[I]) Validate() error {
	if len(f.refs) == 0 {
		return errors.New("first-of binding requires at least one reference")
	}
	for index, ref := range f.refs {
		if err := ref.Validate(); err != nil {
			return fmt.Errorf("reference %d: %w", index, err)
		}
	}
	return nil
}

// Leaf turns a statically typed node into a [Step]. On each run it binds the
// node's input from the Store, runs it, and writes the result under
// [Output]. Errors are tagged with the step id, lifecycle events are emitted, and
// a run's [Journal] can replay the step instead of repeating it (see [RunConfig]).
//
// This is the prep/exec/post split: bind reads the Store, node computes, and the
// Step writes back. The node itself stays free of Store knowledge and remains
// independently testable. Use [From] and [FirstOf] for bindings the workflow
// can validate before replay; adapt application-defined binding functions with
// [BinderFunc]. id must be non-empty, valid UTF-8, and unique among steps
// that can run in the same execution scope. A policy that may invoke work more
// than once, such as retry or hedging, must wrap node before it is passed to
// Leaf; invoking the returned Step twice in one scope fails with
// [ErrDuplicateStep].
// Definition errors, including an invalid Binder or Node, are reported at
// [OpValidate]. An error returned while preparing the input uses [OpBind]; the
// admitted execution and output boundary uses [OpRun].
func Leaf[I, O any](id string, bind Binder[I], node flow.Node[I, O]) Step {
	return leafStep[I, O]{
		id:   id,
		bind: bind,
		node: node,
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
	id   string
	bind Binder[I]
	node flow.Node[I, O]
}

func (l leafStep[I, O]) Run(ctx context.Context, store Store) (Store, error) {
	execution := leafExecution[I, O]{
		leaf:  l,
		store: store,
		run:   runFrom(ctx),
	}
	return execution.execute(ctx)
}

func (l leafStep[I, O]) validate() error {
	if err := validateStepID(l.id); err != nil {
		return newValidationError(l.id, err)
	}
	if isNilBinder(l.bind) {
		return newValidationError(
			l.id,
			fmt.Errorf("binder: %w", flow.ErrNilFunc))
	}
	if validator, ok := l.bind.(binderValidator); ok {
		if err := validator.Validate(); err != nil {
			return newValidationError(
				l.id,
				fmt.Errorf("%w: binder: %w", flow.ErrInvalidConfig, err))
		}
	}
	if err := validateNode(l.node); err != nil {
		return newValidationError(
			l.id,
			fmt.Errorf("node: %w", err))
	}
	return nil
}

func (l leafStep[I, O]) Validate() error { return validateDefinition(l) }

func (l leafStep[I, O]) Describe() Description {
	return Description{ID: l.id, Kind: KindLeaf}
}

func (l leafStep[I, O]) definition() stepDefinition {
	return stepDefinition{kind: definitionNamed, id: l.id, output: true}
}

// leafExecution owns everything that changes while one leaf runs, so the
// leafStep it came from stays immutable and safe for concurrent use.
type leafExecution[I, O any] struct {
	leaf    leafStep[I, O]
	store   Store
	run     *runState
	started time.Time
}

func (l *leafExecution[I, O]) execute(ctx context.Context) (Store, error) {
	if _, err := admitBoundary(ctx, l.leaf.id, l.leaf.Validate()); err != nil {
		return l.store, err
	}
	replayed, ok, err := l.replay(ctx)
	if err != nil {
		return l.store, err
	}
	if ok {
		return replayed, nil
	}

	if contextErr := l.start(ctx); contextErr != nil {
		return l.fail(ctx, OpRun, contextErr)
	}
	input, err := l.leaf.bind.Bind(l.store)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return l.fail(ctx, OpRun, contextErr)
	}
	if err != nil {
		return l.fail(ctx, OpBind, err)
	}
	output, err := l.runNode(ctx, input)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return l.fail(ctx, OpRun, contextErr)
	}
	if err != nil {
		return l.fail(ctx, OpRun, err)
	}
	return l.complete(ctx, output)
}

func (l *leafExecution[I, O]) runNode(ctx context.Context, input I) (O, error) {
	emitter := l.run.emitter()
	if emitter == nil {
		return l.leaf.node.Run(ctx, input)
	}

	emissionCtx, emission := withEmission(
		ctx,
		l.run,
		l.leaf.id,
		scope(ctx),
		emitter,
	)
	defer emission.cancel(nil)
	output, err := l.leaf.node.Run(emissionCtx, input)
	if emissionErr := emission.close(); emissionErr != nil {
		var zero O
		return zero, emissionErr
	}
	return output, err
}

func (l *leafExecution[I, O]) replay(ctx context.Context) (Store, bool, error) {
	value, ok, err := l.run.replay(ctx, scope(ctx), l.leaf.id)
	if err != nil {
		return Store{}, false, err
	}
	if !ok {
		return Store{}, false, nil
	}
	next := l.store.WithOutput(l.leaf.id, value)
	if err := l.run.emitAndCheck(ctx, Event{
		Kind:  EventSkipped,
		ID:    l.leaf.id,
		Store: next,
	}); err != nil {
		return l.store, false, err
	}
	return next, true, nil
}

func (l *leafExecution[I, O]) start(ctx context.Context) error {
	if !l.run.observing() {
		return context.Cause(ctx)
	}
	l.started = time.Now()
	return l.run.emitAndCheck(ctx, Event{
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

	stepErr := newStepError(ctx, l.leaf.id, op, err)
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
		if contextErr := l.run.emitAndCheck(ctx, Event{
			Kind:    EventSuspended,
			ID:      l.leaf.id,
			Elapsed: time.Since(l.started),
			Err:     err,
		}); contextErr != nil {
			return l.store, contextErr
		}
	}
	return l.store, err
}

func (l *leafExecution[I, O]) complete(ctx context.Context, output O) (Store, error) {
	next := l.store.WithOutput(l.leaf.id, output)
	journalErr := l.run.journal().record(scope(ctx), l.leaf.id, output)
	if err := context.Cause(ctx); err != nil {
		return l.store, err
	}
	if journalErr != nil {
		return l.fail(ctx, OpRun, journalErr)
	}
	if l.run.observing() {
		if err := l.run.emitAndCheck(ctx, Event{
			Kind:    EventCompleted,
			ID:      l.leaf.id,
			Elapsed: time.Since(l.started),
			Store:   next,
		}); err != nil {
			return l.store, err
		}
	}
	return next, nil
}
