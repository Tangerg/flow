package workflow

import (
	"bytes"
	"context"
	"fmt"
)

// Interrupt returns a value-producing [Step] that exposes value in a
// [Suspension] and stops until its response is recorded in the run's [Journal].
// On the next run the response is restored under [Output](id), exactly like a
// completed leaf result. A run without a Journal cannot hold a response, so the
// Step suspends each time. Value is held as-is; mutable values must not be
// modified after this call.
//
// Interrupt is the graph-native form of a resumable request: the Step is the
// explicit replay boundary, so there is no hidden call-order matching inside a
// node. Resolve one with:
//
//	wait := workflow.Suspensions(err)[0]
//	if err := journal.Record(wait.Key(), response); err != nil { ... }
//	out, err := workflow.Run(ctx, pipeline, paused, cfg)
func Interrupt(id string, value any) Step {
	return interruptStep{id: id, value: value}
}

type interruptStep struct {
	id    string
	value any
}

func (interrupt interruptStep) Run(ctx context.Context, store Store) (Store, error) {
	run := runFrom(ctx)
	if interrupt.id == "" {
		err := &StepError{ID: interrupt.id, Op: OpValidate, Err: ErrInvalidStepID}
		run.emit(ctx, Event{Kind: EventFailed, ID: interrupt.id, Err: err})
		return store, err
	}
	if journal := run.journal(); journal != nil {
		if response, ok := journal.lookup(scope(ctx), interrupt.id); ok {
			next := store.WithOutput(interrupt.id, response)
			run.emit(ctx, Event{Kind: EventSkipped, ID: interrupt.id, Store: next})
			return next, nil
		}
	}

	suspension := &Suspension{
		ID:    interrupt.id,
		Path:  Scope(ctx),
		Value: interrupt.value,
	}
	run.emit(ctx, Event{Kind: EventSuspended, ID: interrupt.id, Err: suspension})
	return store, suspension
}

func (interrupt interruptStep) Describe() Description {
	return Description{ID: interrupt.id, Kind: "interrupt"}
}

// InterruptFactory is the [LeafFactory] form of [Interrupt]. The leaf's JSON
// config becomes the value exposed by the suspension; an omitted config becomes
// nil. Interrupt leaves accept no input ports.
//
//	reg.MustRegisterLeaf("interrupt", workflow.InterruptFactory())
//	// {"id":"approval","type":"interrupt","config":{"question":"approve?"}}
func InterruptFactory() LeafFactory {
	return func(spec LeafSpec) (Step, error) {
		if ports := spec.Inputs.PortNames(); len(ports) > 0 {
			return nil, fmt.Errorf("%w %q", ErrUnknownPort, ports[0])
		}

		var value any
		if config := bytes.TrimSpace(spec.Config); len(config) > 0 {
			decoded, err := jsonDocument(config).value()
			if err != nil {
				return nil, fmt.Errorf("%w: decode interrupt config: %w", ErrInvalidSpec, err)
			}
			value = decoded
		}
		return Interrupt(spec.ID, value), nil
	}
}
