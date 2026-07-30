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

// decoratedStep is the transparent execution metadata shared by internal
// wrappers. It preserves static validation and public descriptions instead of
// turning a built-in step opaque.
type decoratedStep struct {
	id   string
	step Step
}

func (d decoratedStep) Describe() Description {
	description := Describe(d.step)
	if description.ID == "" {
		description.ID = d.id
	}
	return description
}

func (d decoratedStep) definition() stepDefinition {
	if defined, ok := d.step.(definedStep); ok {
		return defined.definition()
	}
	return stepDefinition{kind: definitionNamed, id: d.id}
}

// scopedStep owns one child-step invocation in a repeated execution scope.
// Context remains an explicit method argument, following the standard context
// contract instead of being retained in a struct.
type scopedStep struct {
	step    Step
	segment string
}

// run invokes the child under its scope. Composites validate their body before
// constructing a scopedStep, so step is never nil here.
func (s scopedStep) run(ctx context.Context, store Store) (Store, error) {
	return s.step.Run(s.childContext(ctx), store)
}

func (s scopedStep) indexed(id string, index int) scopedStep {
	s.segment = id + "[" + strconv.Itoa(index) + "]"
	return s
}

func (s scopedStep) childContext(parent context.Context) context.Context {
	return WithScope(parent, s.segment)
}

type stepList []Step

func (s stepList) validate() error {
	for index, step := range s {
		if step == nil {
			return &flow.IndexError{Index: index, Err: ErrNilStep}
		}
	}
	return nil
}

func (s stepList) run(ctx context.Context, store Store) (Store, error) {
	current := store
	for _, step := range s {
		var err error
		current, err = step.Run(ctx, current)
		if err != nil {
			return current, err
		}
	}
	return current, nil
}

func (s stepList) describe() []Description {
	descriptions := make([]Description, len(s))
	for index, step := range s {
		descriptions[index] = Describe(step)
	}
	return descriptions
}
