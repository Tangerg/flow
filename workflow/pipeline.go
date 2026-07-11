package workflow

import "context"

// Pipeline is an immutable, fluent sequence of workflow steps. Its zero value
// is an empty pipeline that passes the input Store through unchanged.
//
// Pipeline implements [Step] directly, so a completed chain can be run or
// passed to any workflow combinator without a separate Build call.
type Pipeline struct {
	steps []Step
}

var _ Step = Pipeline{}

// Pipe starts a fluent workflow pipeline with steps. The supplied slice is
// copied; later changes to it do not affect the pipeline.
func Pipe(steps ...Step) Pipeline {
	return Pipeline{steps: append([]Step(nil), steps...)}
}

// Then returns a new pipeline with steps appended in order.
func (p Pipeline) Then(steps ...Step) Pipeline {
	return Pipeline{steps: appendSteps(p.steps, steps...)}
}

// Parallel returns a new pipeline with one concurrent stage containing
// branches. It has the same execution semantics as [Parallel].
func (p Pipeline) Parallel(branches ...Step) Pipeline {
	return p.Then(Parallel(branches...))
}

// Run executes each stage in order, threading the Store through the pipeline.
func (p Pipeline) Run(ctx context.Context, input Store) (Store, error) {
	return runSteps(ctx, p.steps, input)
}

// Describe returns the pipeline as a sequence description.
func (p Pipeline) Describe() Description {
	return Description{Kind: "sequence", Children: describeAll(p.steps)}
}

func appendSteps(existing []Step, added ...Step) []Step {
	steps := make([]Step, len(existing)+len(added))
	copy(steps, existing)
	copy(steps[len(existing):], added)
	return steps
}
