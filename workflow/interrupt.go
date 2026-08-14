package workflow

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
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

func (i interruptStep) Run(ctx context.Context, store Store) (Store, error) {
	run, err := admitBoundary(ctx, i.id, i.Validate())
	if err != nil {
		return store, err
	}
	response, replayed, err := run.replay(ctx, boundaryKey(ctx, i.id))
	if err != nil {
		return store, err
	}
	if replayed {
		next := store.WithOutput(i.id, response)
		if err := run.emitAndCheck(ctx, Event{Kind: EventSkipped, ID: i.id, Store: next}); err != nil {
			return store, err
		}
		return next, nil
	}

	suspension := &Suspension{
		ID:    i.id,
		Scope: Scope(ctx),
		Value: i.value,
	}
	if err := run.emitAndCheck(ctx, Event{Kind: EventSuspended, ID: i.id, Err: suspension}); err != nil {
		return store, err
	}
	return store, suspension
}

func (i interruptStep) validate() error {
	if err := validateStepID(i.id); err != nil {
		return newValidationError(i.id, err)
	}
	return nil
}

func (i interruptStep) Validate() error { return validateDefinition(i) }

func (i interruptStep) Describe() Description {
	return Description{ID: i.id, Kind: KindInterrupt}
}

func (i interruptStep) definition() stepDefinition {
	return stepDefinition{kind: definitionNamed, id: i.id, output: true}
}

// InterruptFactory is the [NodeFactory] form of [Interrupt]. The leaf's JSON
// config becomes the value exposed by the suspension; an omitted config becomes
// nil. Interrupt leaves accept no input ports.
//
//	reg.MustRegisterNode("interrupt", workflow.InterruptFactory())
//	// {"id":"approval","type":"interrupt","config":{"question":"approve?"}}
func InterruptFactory() NodeFactory {
	return func(spec NodeSpec) (Step, error) {
		// An interrupt reads nothing, so every wired port is unknown and the
		// canonical walk names the first of them.
		for port := range spec.Inputs.All() {
			return nil, fmt.Errorf("%w %q", ErrUnknownPort, port)
		}

		var value any
		if len(spec.Config) > 0 {
			decoded, err := jsonDocument(spec.Config).value()
			if err != nil {
				return nil, fmt.Errorf("%w: decode interrupt config: %w", flow.ErrInvalidConfig, err)
			}
			value = decoded
		}
		return Interrupt(spec.ID, value), nil
	}
}
