package workflow

import (
	"context"
	"fmt"
	"maps"

	"github.com/Tangerg/flow"
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
// fails or suspends. When Body is a completely visible built-in definition,
// construction validation also proves that BodyOutput names a conventional
// output or declared input available on every successful path. An opaque
// caller-defined Body retains responsibility for that contract at run time.
// Ordinary body failures are wrapped in a StepError naming the sealed subgraph;
// suspensions keep their inner step identity and scope.
func Subgraph(cfg SubgraphConfig) Step {
	return cfg.step()
}

func (c SubgraphConfig) step() subgraphStep {
	return subgraphStep{
		id:         c.ID,
		inputs:     maps.Clone(c.Inputs),
		body:       c.Body,
		bodyOutput: c.BodyOutput,
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
	execution := subgraphExecution{
		subgraph: s,
		outer:    outer,
		run:      runFrom(ctx),
	}
	return execution.execute(ctx)
}

// subgraphExecution owns the two Store namespaces and identity state of one
// invocation. Every method either advances the sealed inner execution or
// returns the untouched outer Store, so projection cannot leak partial cells.
type subgraphExecution struct {
	subgraph subgraphStep
	outer    Store
	run      *runState
}

func (s *subgraphExecution) execute(ctx context.Context) (Store, error) {
	if err := s.validate(ctx); err != nil {
		return s.outer, err
	}

	inner, err := s.bind(ctx)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return s.outer, contextErr
	}
	if err != nil {
		return s.outer, newStepError(ctx, s.subgraph.id, OpBind, err)
	}
	result, err := s.subgraph.body.Run(WithScope(ctx, s.subgraph.id), inner)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return s.outer, contextErr
	}
	if err != nil {
		if SuspendedOnly(err) {
			return s.outer, err
		}
		return s.outer, newStepError(ctx, s.subgraph.id, OpRun, err)
	}
	return s.project(ctx, result)
}

func (s *subgraphExecution) validate(ctx context.Context) error {
	if err := admitScopedStep(ctx, s.subgraph.id, s.subgraph.Validate()); err != nil {
		return err
	}
	return context.Cause(ctx)
}

func (s *subgraphExecution) project(ctx context.Context, inner Store) (Store, error) {
	output, err := Get[any](inner, s.subgraph.bodyOutput)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return s.outer, contextErr
	}
	if err != nil {
		return s.outer, newStepError(
			ctx,
			s.subgraph.id,
			OpRun,
			bodyOutputError(s.subgraph.bodyOutput, err),
		)
	}
	return s.outer.WithOutput(s.subgraph.id, output), nil
}

func (s subgraphStep) validate() error {
	if err := validateBody(s.id, s.body); err != nil {
		return err
	}
	if err := s.inputs.validateSeeds(); err != nil {
		return newValidationError(
			s.id,
			fmt.Errorf("%w: subgraph inputs: %w", flow.ErrInvalidConfig, err))
	}
	if err := s.bodyOutput.Validate(); err != nil {
		return newValidationError(
			s.id,
			fmt.Errorf("%w: subgraph body output: %w", flow.ErrInvalidConfig, err))
	}
	return nil
}

func (s subgraphStep) Validate() error { return validateDefinition(s) }

func (s *subgraphExecution) bind(ctx context.Context) (Store, error) {
	inner := NewStore()
	for _, seedID := range s.subgraph.inputs.names() {
		if err := context.Cause(ctx); err != nil {
			return Store{}, err
		}
		ref := s.subgraph.inputs[seedID]
		value, err := Get[any](s.outer, ref)
		if contextErr := context.Cause(ctx); contextErr != nil {
			return Store{}, contextErr
		}
		if err != nil {
			return Store{}, fmt.Errorf("input %q from %s: %w", seedID, ref, err)
		}
		inner = inner.WithOutput(seedID, value)
	}
	return inner, context.Cause(ctx)
}

func (s subgraphStep) Describe() Description {
	return Description{
		ID:       s.id,
		Kind:     KindSubgraph,
		Children: []Description{describe(s.body)},
	}
}

func (s subgraphStep) definition() stepDefinition {
	return stepDefinition{
		kind:       definitionSubgraph,
		id:         s.id,
		output:     true,
		body:       s.body,
		inputs:     s.inputs,
		bodyOutput: s.bodyOutput,
	}
}

// SubgraphFactory returns a [NodeFactory] that instantiates body as a sealed
// subgraph. A Graph node's wired ports become the subgraph's inner seed IDs.
// The factory accepts no config because body and bodyOutput are fixed by the
// registration. Each construction validates the complete visible body
// definition; a defect in that captured definition is therefore reported as an
// invalid node registration rather than as per-node config.
func SubgraphFactory(body Step, bodyOutput Ref) NodeFactory {
	return func(spec NodeSpec) (Step, error) {
		if len(spec.Config) > 0 {
			return nil, fmt.Errorf("%w: subgraph config must be omitted", flow.ErrInvalidConfig)
		}
		step := (SubgraphConfig{
			ID:         spec.ID,
			Inputs:     spec.Inputs,
			Body:       body,
			BodyOutput: bodyOutput,
		}).step()
		if err := step.Validate(); err != nil {
			return nil, &RegistrationError{
				Kind: registrationNode,
				Err:  fmt.Errorf("subgraph factory definition: %w", err),
			}
		}
		return step, nil
	}
}
