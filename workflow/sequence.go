package workflow

import (
	"context"
	"slices"
)

// Sequence runs steps in order, threading the Store through each. It rejects a
// nil step before running any step.
func Sequence(steps ...Step) Step {
	return sequenceStep{steps: stepList(slices.Clone(steps))}
}

// sequenceStep is the [Step] produced by [Sequence].
type sequenceStep struct {
	steps stepList
}

func (sequence sequenceStep) Run(ctx context.Context, store Store) (Store, error) {
	if err := sequence.steps.validate(); err != nil {
		return store, err
	}
	return sequence.steps.run(ctx, store)
}

func (sequence sequenceStep) Describe() Description {
	return Description{Kind: "sequence", Children: sequence.steps.describe()}
}
