package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/Tangerg/flow"
)

// Await returns a [Step] that passes the Store through once ref resolves and
// suspends until then. It is the common shape of a wait: a human approval, a
// callback, a value another system will write.
//
//	approval := workflow.Await("approval", workflow.At("inbox", "decision"))
//	out, err := workflow.Sequence(draft, approval, publish).Run(ctx, in)
//	if errors.Is(err, workflow.ErrSuspended) {
//		// persist the Journal, wait for the decision, then run again
//	}
//
// The step writes nothing of its own, so it re-evaluates on every run rather
// than being skipped as completed, which is what makes the wait meaningful.
// An invalid ID or reference is a validation error rather than a suspension. An
// existing value that cannot provide the JSON view required by a nested ref is
// a bind failure; waiting cannot turn malformed data into a usable value.
func Await(id string, ref Ref) Step { return awaitStep{id: id, ref: ref} }

// awaitStep is the [Step] produced by [Await].
type awaitStep struct {
	id  string
	ref Ref
}

func (a awaitStep) Run(ctx context.Context, store Store) (Store, error) {
	run, err := admitBoundary(ctx, a.id, a.Validate())
	if err != nil {
		return store, err
	}
	_, resolveErr := Get[any](store, a.ref)
	if err := context.Cause(ctx); err != nil {
		return store, err
	}
	if resolveErr == nil {
		if err := run.emitAndCheck(ctx, Event{Kind: EventCompleted, ID: a.id, Store: store}); err != nil {
			return store, err
		}
		return store, nil
	}
	if !errors.Is(resolveErr, ErrNotFound) {
		err := newStepError(ctx, a.id, OpBind, resolveErr)
		run.emit(ctx, Event{Kind: EventFailed, ID: a.id, Err: err})
		return store, err
	}
	suspension := &Suspension{ID: a.id, Scope: Scope(ctx), Await: a.ref}
	if err := run.emitAndCheck(ctx, Event{Kind: EventSuspended, ID: a.id, Err: suspension}); err != nil {
		return store, err
	}
	return store, suspension
}

func (a awaitStep) validate() error {
	if err := validateStepID(a.id); err != nil {
		return newValidationError(a.id, err)
	}
	if err := a.ref.Validate(); err != nil {
		return newValidationError(
			a.id,
			fmt.Errorf("%w: await reference: %w", flow.ErrInvalidConfig, err))
	}
	return nil
}

func (a awaitStep) Validate() error { return validateDefinition(a) }

func (a awaitStep) Describe() Description {
	return Description{ID: a.id, Kind: KindAwait}
}

func (a awaitStep) definition() stepDefinition {
	return stepDefinition{kind: definitionNamed, id: a.id}
}

// AwaitFactory is the [NodeFactory] form of [Await]: a node type a serialized
// workflow can name to place a wait in a graph. It waits on whatever its
// [DefaultPort] is wired to and accepts no config. Use [InterruptFactory] when
// the wait must expose a structured request.
//
//	reg.MustRegisterNode("await", workflow.AwaitFactory())
//	// {"id":"approval","type":"await","inputs":{"in":{"nodeID":"inbox","path":"/decision"}}}
func AwaitFactory() NodeFactory {
	return func(spec NodeSpec) (Step, error) {
		ref, err := defaultPortRef(spec.Inputs)
		if err != nil {
			return nil, err
		}
		if len(spec.Config) > 0 {
			return nil, fmt.Errorf("%w: await config must be omitted", flow.ErrInvalidConfig)
		}
		return Await(spec.ID, ref), nil
	}
}
