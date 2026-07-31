package workflow

import (
	"bytes"
	"context"
	"fmt"
	"maps"
)

// SubgraphConfig configures [Subgraph].
type SubgraphConfig struct {
	// ID names the subgraph and its projected output.
	ID string
	// Inputs maps inner seed IDs to references in the outer Store. Each value is
	// copied to Output(seedID) in the isolated body Store.
	Inputs Inputs
	// Body runs once in an isolated Store and a scope derived from ID.
	Body Step
	// BodyOutput selects the one value projected to Output(ID).
	BodyOutput Ref
}

// Subgraph runs a Step behind an isolated Store and execution scope. It copies
// only the declared Inputs into the body and projects only BodyOutput back out,
// making the composite safe to reuse under different IDs without leaking inner
// cells or colliding execution identities.
//
// A subgraph is a replay boundary only by composition: its inner journaled
// steps replay normally, and the projected output is derived again. Subgraph
// itself writes no Journal record and publishes no partial output when its body
// fails or suspends.
func Subgraph(cfg SubgraphConfig) Step {
	return subgraphStep{
		id:         cfg.ID,
		inputs:     maps.Clone(cfg.Inputs),
		body:       cfg.Body,
		bodyOutput: cfg.BodyOutput,
	}
}

type subgraphStep struct {
	id         string
	inputs     Inputs
	body       Step
	bodyOutput Ref
}

func (s subgraphStep) Run(ctx context.Context, outer Store) (Store, error) {
	ctx = ensureRun(ctx)
	if err := s.validate(); err != nil {
		return outer, err
	}
	run := runFrom(ctx)
	if err := run.validateDefinition(s); err != nil {
		return outer, err
	}
	if err := run.claim(scope(ctx), s.id); err != nil {
		return outer, &StepError{ID: s.id, Op: OpValidate, Err: err}
	}

	inner, err := s.bind(outer)
	if err != nil {
		return outer, &StepError{ID: s.id, Op: OpBind, Err: err}
	}
	result, err := s.body.Run(WithScope(ctx, s.id), inner)
	if err != nil {
		return outer, err
	}
	output, err := Get[any](result, s.bodyOutput)
	if err != nil {
		return outer, &StepError{
			ID:  s.id,
			Op:  OpRun,
			Err: fmt.Errorf("read body output %s: %w", s.bodyOutput, err),
		}
	}
	return outer.WithOutput(s.id, output), nil
}

func (s subgraphStep) validate() error {
	switch {
	case s.id == "":
		return &StepError{ID: s.id, Op: OpValidate, Err: ErrInvalidStepID}
	case isNilNode(s.body):
		return &StepError{ID: s.id, Op: OpValidate, Err: ErrNilStep}
	}
	if err := s.inputs.validate(); err != nil {
		return &StepError{
			ID:  s.id,
			Op:  OpValidate,
			Err: fmt.Errorf("%w: subgraph inputs: %w", ErrInvalidSpec, err),
		}
	}
	if err := s.bodyOutput.validate(); err != nil {
		return &StepError{
			ID:  s.id,
			Op:  OpValidate,
			Err: fmt.Errorf("%w: subgraph body output: %w", ErrInvalidSpec, err),
		}
	}
	return nil
}

func (s subgraphStep) bind(outer Store) (Store, error) {
	inner := NewStore()
	for _, seedID := range s.inputs.PortNames() {
		ref := s.inputs[seedID]
		value, err := Get[any](outer, ref)
		if err != nil {
			return Store{}, fmt.Errorf("input %q from %s: %w", seedID, ref, err)
		}
		inner = inner.WithOutput(seedID, value)
	}
	return inner, nil
}

func (s subgraphStep) Describe() Description {
	return Description{
		ID:       s.id,
		Kind:     "subgraph",
		Children: []Description{Describe(s.body)},
	}
}

func (s subgraphStep) definition() stepDefinition {
	return stepDefinition{kind: definitionSubgraph, id: s.id, body: s.body}
}

// SubgraphFactory returns a [NodeFactory] that instantiates body as a sealed
// subgraph. A Graph node's wired ports become the subgraph's inner seed IDs.
// The factory accepts no config because body and bodyOutput are fixed by the
// registration.
func SubgraphFactory(body Step, bodyOutput Ref) NodeFactory {
	return func(spec NodeSpec) (Step, error) {
		if len(bytes.TrimSpace(spec.Config)) > 0 {
			return nil, fmt.Errorf("%w: subgraph config must be omitted", ErrInvalidSpec)
		}
		step := subgraphStep{
			id:         spec.ID,
			inputs:     maps.Clone(spec.Inputs),
			body:       body,
			bodyOutput: bodyOutput,
		}
		if err := step.validate(); err != nil {
			return nil, err
		}
		return step, nil
	}
}
