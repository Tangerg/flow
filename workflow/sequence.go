package workflow

import (
	"context"
	"slices"
)

// Sequence runs steps in order, threading the Store through each. If a child
// returns an error, Sequence returns that child's Store and does not run the
// remaining children. This preserves completed state carried alongside an
// ordinary failure or suspension and keeps nested and flat sequences
// equivalent. Parent cancellation observed when a child returns takes
// precedence and retains the Store from before that child instead.
//
// Before running, Sequence rejects nil steps and duplicate IDs in steps built
// by this package. Built-in steps hidden inside caller-defined steps validate
// and claim their identities when invoked.
func Sequence(steps ...Step) Step {
	return sequenceStep{steps: stepList(slices.Clone(steps))}
}

// sequenceStep is the [Step] produced by [Sequence].
type sequenceStep struct {
	steps stepList
}

func (s sequenceStep) Run(ctx context.Context, store Store) (Store, error) {
	ctx = ensureRun(ctx)
	if err := s.Validate(); err != nil {
		return store, err
	}
	return s.steps.run(ctx, store)
}

func (s sequenceStep) validate() error { return s.steps.validate() }

func (s sequenceStep) Validate() error { return validateDefinition(s) }

func (s sequenceStep) Describe() Description {
	return Description{Kind: KindSequence, Children: s.steps.describe()}
}

func (s sequenceStep) definition() stepDefinition {
	return stepDefinition{kind: definitionSteps, steps: s.steps}
}
