package workflow

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

// Gate selects a Graph node when another node's routing output equals Outlet.
// Comparison uses the output's JSON string representation, so selection is the
// same on a fresh run and after Journal persistence. A target reads each routing
// source once even when several of its gates name that source. NodeID must name
// a node in the same Graph whose [NodeSchema] declares Outlet. Construct one
// with [When].
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

type routingSelection struct {
	outlet   string
	bypassed bool
}

// gateEvaluation snapshots every routing source used by one target. TriggerAny
// may legitimately name several acceptable outlets on the same source; reading
// that Store value once keeps one routing decision from depending on how many
// comparisons the target declares.
type gateEvaluation struct {
	selections map[string]routingSelection
}

type gatedStep struct {
	decoratedStep
	gates   []compiledGate
	trigger Trigger
}

func gated(gates []compiledGate, trigger Trigger, step definedStep) definedStep {
	return gatedStep{
		decoratedStep: decoratedStep{step: step},
		gates:         slices.Clone(gates),
		trigger:       trigger,
	}
}

func (g gatedStep) Run(ctx context.Context, store Store) (Store, error) {
	ctx = ensureRun(ctx)
	satisfied, err := g.satisfied(ctx, store)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return store, contextErr
	}
	if err != nil {
		return g.fail(ctx, store, OpRun, err)
	}
	if satisfied {
		return g.step.Run(ctx, store)
	}
	run := runFrom(ctx)
	id := g.stepID()
	if err := run.claim(scope(ctx), id); err != nil {
		return g.fail(ctx, store, OpValidate, err)
	}
	if err := context.Cause(ctx); err != nil {
		return store, err
	}
	run.markBypassed(scope(ctx), id)
	if err := context.Cause(ctx); err != nil {
		return store, err
	}
	if err := run.emitAndCheck(ctx, Event{Kind: EventBypassed, ID: id}); err != nil {
		return store, err
	}
	return store, nil
}

func (g gatedStep) fail(
	ctx context.Context,
	store Store,
	op StepOp,
	err error,
) (Store, error) {
	id := g.stepID()
	stepErr := newStepError(ctx, id, op, err)
	runFrom(ctx).emit(ctx, Event{
		Kind: EventFailed,
		ID:   id,
		Err:  stepErr,
	})
	return store, stepErr
}

func (g gatedStep) satisfied(ctx context.Context, store Store) (bool, error) {
	satisfied := g.trigger == TriggerAll
	evaluation := gateEvaluation{
		selections: make(map[string]routingSelection, len(g.gates)),
	}
	for _, gate := range g.gates {
		if err := context.Cause(ctx); err != nil {
			return false, err
		}
		match, err := evaluation.match(ctx, store, gate)
		if contextErr := context.Cause(ctx); contextErr != nil {
			return false, contextErr
		}
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

func (g *gateEvaluation) match(
	ctx context.Context,
	store Store,
	gate compiledGate,
) (bool, error) {
	selected, ok := g.selections[gate.NodeID]
	if !ok {
		var err error
		selected, err = gate.selectOutlet(ctx, store)
		if err != nil {
			return false, err
		}
		g.selections[gate.NodeID] = selected
	}
	return !selected.bypassed && selected.outlet == gate.Outlet, nil
}

func (c compiledGate) selectOutlet(
	ctx context.Context,
	store Store,
) (routingSelection, error) {
	if runFrom(ctx).wasBypassed(scope(ctx), c.NodeID) {
		return routingSelection{bypassed: true}, nil
	}
	selected, err := Get[string](store, Output(c.NodeID))
	if err != nil {
		return routingSelection{}, fmt.Errorf("read routing node %q: %w", c.NodeID, err)
	}
	// A gate is durable control flow, so compare the same JSON-domain string on
	// a fresh run and after Journal replay. Get intentionally preserves an exact
	// Go string in memory, while encoding/json replaces malformed UTF-8 during
	// persistence; normalizing here prevents that representation change from
	// selecting a different branch after restart.
	selected = normalizeJSONString(selected)
	if !slices.Contains(c.outlets, selected) {
		return routingSelection{}, fmt.Errorf(
			"%w %q from routing node %q",
			ErrUnknownOutlet,
			selected,
			c.NodeID,
		)
	}
	return routingSelection{outlet: selected}, nil
}

// normalizeJSONString mirrors encoding/json's treatment of malformed UTF-8:
// each invalid byte becomes U+FFFD. strings.ToValidUTF8 replaces a consecutive
// invalid run only once, so mapping the runes is the exact operation here.
func normalizeJSONString(value string) string {
	if utf8.ValidString(value) {
		return value
	}
	return strings.Map(func(r rune) rune { return r }, value)
}
