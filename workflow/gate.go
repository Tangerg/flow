package workflow

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// Gate selects a Graph node when another node's routing output equals Outlet.
// NodeID must name a node in the same Graph whose [NodeSchema] declares Outlet.
// Construct one with [When].
type Gate struct {
	NodeID string `json:"nodeID"`
	Outlet string `json:"outlet"`
}

// When returns a Gate that is satisfied when nodeID outputs outlet.
func When(nodeID, outlet string) Gate {
	return Gate{NodeID: nodeID, Outlet: outlet}
}

// Trigger controls how a Graph node combines its gates.
type Trigger string

// Gate trigger rules. The zero value requires every gate to be satisfied.
const (
	TriggerAll Trigger = ""
	TriggerAny Trigger = "any"
)

func (t Trigger) valid() bool {
	return t == TriggerAll || t == TriggerAny
}

// compiledGate carries the source's complete outlet declaration so execution
// can reject a node that violates its registered routing contract.
type compiledGate struct {
	Gate
	outlets []string
}

type gatedStep struct {
	decoratedStep
	gates   []compiledGate
	trigger Trigger
}

func gated(id string, gates []compiledGate, trigger Trigger, step Step) Step {
	return gatedStep{
		decoratedStep: decoratedStep{id: id, step: step},
		gates:         slices.Clone(gates),
		trigger:       trigger,
	}
}

func (g gatedStep) Run(ctx context.Context, store Store) (Store, error) {
	ctx = ensureRun(ctx)
	satisfied, err := g.satisfied(ctx, store)
	if err != nil {
		return g.fail(ctx, store, OpRun, err)
	}
	if satisfied {
		return g.step.Run(ctx, store)
	}
	run := runFrom(ctx)
	if err := run.claim(scope(ctx), g.id); err != nil {
		return g.fail(ctx, store, OpValidate, err)
	}
	run.markBypassed(scope(ctx), g.id)
	run.emit(ctx, Event{Kind: EventBypassed, ID: g.id})
	return store, nil
}

func (g gatedStep) fail(
	ctx context.Context,
	store Store,
	op StepOp,
	err error,
) (Store, error) {
	stepErr := &StepError{ID: g.id, Op: op, Err: err}
	runFrom(ctx).emit(ctx, Event{
		Kind: EventFailed,
		ID:   g.id,
		Err:  stepErr,
	})
	return store, stepErr
}

func (g gatedStep) satisfied(ctx context.Context, store Store) (bool, error) {
	satisfied := g.trigger == TriggerAll
	for _, gate := range g.gates {
		match, err := gate.satisfied(ctx, store)
		if err != nil {
			return false, err
		}
		if g.trigger == TriggerAny {
			satisfied = satisfied || match
		} else {
			satisfied = satisfied && match
		}
	}
	return satisfied, nil
}

func (c compiledGate) satisfied(ctx context.Context, store Store) (bool, error) {
	if runFrom(ctx).wasBypassed(scope(ctx), c.NodeID) {
		return false, nil
	}
	selected, err := Get[string](store, Output(c.NodeID))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, fmt.Errorf(
				"routing node %q completed without an output: %w",
				c.NodeID,
				err,
			)
		}
		return false, fmt.Errorf("read routing node %q: %w", c.NodeID, err)
	}
	if !slices.Contains(c.outlets, selected) {
		return false, fmt.Errorf(
			"%w %q from routing node %q",
			ErrUnknownOutlet,
			selected,
			c.NodeID,
		)
	}
	return selected == c.Outlet, nil
}
