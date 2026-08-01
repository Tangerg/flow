package workflow

import (
	"context"
	"slices"
)

// Sequence runs steps in order, threading the Store through each. Before
// running, it rejects nil steps and duplicate IDs in steps built by this
// package. Runtime identity checks cover IDs hidden inside caller-defined steps.
func Sequence(steps ...Step) Step {
	return sequenceStep{steps: stepList(slices.Clone(steps))}
}

// sequenceStep is the [Step] produced by [Sequence].
type sequenceStep struct {
	steps stepList
}

func (s sequenceStep) Run(ctx context.Context, store Store) (Store, error) {
	ctx = ensureRun(ctx)
	if err := s.steps.validate(); err != nil {
		return store, err
	}
	if err := runFrom(ctx).validateDefinition(s); err != nil {
		return store, err
	}
	return s.steps.run(ctx, store)
}

func (s sequenceStep) Describe() Description {
	return Description{Kind: KindSequence, Children: s.steps.describe()}
}

func (s sequenceStep) definition() stepDefinition {
	return stepDefinition{kind: definitionSteps, steps: s.steps}
}
