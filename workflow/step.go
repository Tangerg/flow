package workflow

import (
	"context"
	"strconv"

	"github.com/Tangerg/flow"
)

// Step is a workflow node: it reads its inputs from the [Store] and returns a
// Store extended with its output. A Step is a flow.Node[Store, Store], so it
// composes with flow's primitives; steps built by this package also implement
// [Describer].
type Step = flow.Node[Store, Store]

// scopedStep owns one child-step invocation in a repeated execution scope.
// Context remains an explicit method argument, following the standard context
// contract instead of being retained in a struct.
type scopedStep struct {
	step    Step
	segment string
}

func (scoped scopedStep) run(ctx context.Context, store Store) (Store, error) {
	return scoped.step.Run(scoped.childContext(ctx), store)
}

func (scoped scopedStep) indexed(id string, index int) scopedStep {
	scoped.segment = id + "[" + strconv.Itoa(index) + "]"
	return scoped
}

func (scoped scopedStep) childContext(parent context.Context) context.Context {
	return WithScope(parent, scoped.segment)
}

type stepList []Step

func (steps stepList) validate() error {
	for index, step := range steps {
		if step == nil {
			return &flow.IndexError{Index: index, Err: ErrNilStep}
		}
	}
	return nil
}

func (steps stepList) run(ctx context.Context, store Store) (Store, error) {
	current := store
	for _, step := range steps {
		var err error
		current, err = step.Run(ctx, current)
		if err != nil {
			return current, err
		}
	}
	return current, nil
}

func (steps stepList) describe() []Description {
	descriptions := make([]Description, len(steps))
	for index, step := range steps {
		descriptions[index] = Describe(step)
	}
	return descriptions
}
