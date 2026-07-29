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

func (sequence sequenceStep) Run(ctx context.Context, store Store) (Store, error) {
	ctx = ensureRun(ctx)
	if err := sequence.steps.validate(); err != nil {
		return store, err
	}
	if err := runFrom(ctx).validateDefinition(sequence); err != nil {
		return store, err
	}
	return sequence.steps.run(ctx, store)
}

func (sequence sequenceStep) Describe() Description {
	return Description{Kind: "sequence", Children: sequence.steps.describe()}
}

func (sequence sequenceStep) workflowDefinition() stepDefinition {
	return stepDefinition{kind: definitionSteps, steps: sequence.steps}
}
