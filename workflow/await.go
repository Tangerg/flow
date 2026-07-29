package workflow

import (
	"bytes"
	"context"
	"fmt"
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
// than being skipped as completed — which is what makes the wait meaningful.
// An invalid ID or reference is a validation error rather than a suspension.
func Await(id string, ref Ref) Step { return awaitStep{id: id, ref: ref} }

// awaitStep is the [Step] produced by [Await].
type awaitStep struct {
	id  string
	ref Ref
}

func (await awaitStep) Run(ctx context.Context, store Store) (Store, error) {
	run := runFrom(ctx)
	if await.id == "" {
		err := &StepError{ID: await.id, Op: OpValidate, Err: ErrInvalidStepID}
		run.emit(ctx, Event{Kind: EventFailed, ID: await.id, Err: err})
		return store, err
	}
	if err := await.ref.validate(); err != nil {
		err := &StepError{
			ID:  await.id,
			Op:  OpValidate,
			Err: fmt.Errorf("%w: await reference: %w", ErrInvalidSpec, err),
		}
		run.emit(ctx, Event{Kind: EventFailed, ID: await.id, Err: err})
		return store, err
	}
	if _, ok := store.Lookup(await.ref); ok {
		run.emit(ctx, Event{Kind: EventCompleted, ID: await.id, Store: store})
		return store, nil
	}
	suspension := &Suspension{ID: await.id, Path: Scope(ctx), Await: await.ref}
	run.emit(ctx, Event{Kind: EventSuspended, ID: await.id, Err: suspension})
	return store, suspension
}

func (await awaitStep) Describe() Description {
	return Description{ID: await.id, Kind: "await", Label: await.ref.String()}
}

// AwaitFactory is the [LeafFactory] form of [Await]: a node type a serialized
// workflow can name to place a wait in a graph. It waits on whatever its
// [DefaultPort] is wired to and accepts no config. Use [InterruptFactory] when
// the wait must expose a structured request.
//
//	reg.MustRegisterLeaf("await", workflow.AwaitFactory())
//	// {"id":"approval","type":"await","input":{"nodeID":"inbox","path":"/decision"}}
func AwaitFactory() LeafFactory {
	return func(spec LeafSpec) (Step, error) {
		for _, port := range spec.Inputs.PortNames() {
			if port != DefaultPort {
				return nil, fmt.Errorf("%w %q", ErrUnknownPort, port)
			}
		}
		if len(bytes.TrimSpace(spec.Config)) > 0 {
			return nil, fmt.Errorf("%w: await config must be omitted", ErrInvalidSpec)
		}
		ref, ok := spec.Inputs.Default()
		if !ok {
			return nil, fmt.Errorf("%w %q", ErrMissingPort, DefaultPort)
		}
		return Await(spec.ID, ref), nil
	}
}
