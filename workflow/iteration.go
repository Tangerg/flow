package workflow

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
)

const (
	itemKey   = "item"
	indexKey  = "index"
	itemPath  = "/" + itemKey
	indexPath = "/" + indexKey
)

// Item returns the reference under which [Iteration] stores the current item.
func Item(id string) Ref { return Ref{NodeID: id, Path: itemPath} }

// Index returns the reference under which [Iteration] stores the current
// item's zero-based index.
func Index(id string) Ref { return Ref{NodeID: id, Path: indexPath} }

// IterationConfig configures [Iteration].
type IterationConfig struct {
	// ID names the node; each element's result is collected under Output(ID).
	ID string
	// Input references the JSON-compatible array to iterate over.
	Input Ref
	// Body runs once per element on a scoped Store (see [Item] and [Index]).
	Body Step
	// BodyOutput references the value in each post-run Store to collect.
	BodyOutput Ref
	// Concurrency caps concurrent element runs. Zero is unbounded; negative
	// values are invalid.
	Concurrency int
}

// Iteration runs cfg.Body once per element of the array at cfg.Input,
// concurrently, and collects each run's cfg.BodyOutput into a []any written at
// Output(cfg.ID). Typed slices are accepted through [Get]'s JSON conversion.
//
// For element i, Body runs on a scoped Store that adds the element under
// [Item](cfg.ID) and its index via [Index](cfg.ID). The value at cfg.Input must
// be a []any. The first element to fail cancels the rest.
//
// Because Body runs once per element, each element adds an [Event.Path] segment
// naming the iteration node and the element index, so an observer can tell the
// elements' steps apart. That segment is also what lets a [Journal] resume an
// iteration element by element.
//
// A suspended element does not cancel the others: they run to completion and are
// recorded, and the suspensions are returned together. The collected output is
// written only once every element has produced one, since a slice with holes
// would read as a finished result. Iteration validates its ID, references, and
// body before reading the input, so an empty collection cannot hide an invalid
// definition.
func Iteration(cfg IterationConfig) Step {
	return iterationStep{
		id:         cfg.ID,
		input:      cfg.Input,
		body:       cfg.Body,
		bodyOutput: cfg.BodyOutput,
		limit:      cfg.Concurrency,
	}
}

// elementOutcome is one element's result. A suspension travels as a value
// because it is not a failure; anything else travels as the mapper's error.
type elementOutcome struct {
	value       any
	suspensions suspensionList
}

// iterationStep is the [Step] produced by [Iteration].
type iterationStep struct {
	id         string
	input      Ref
	body       Step
	bodyOutput Ref
	limit      int
}

func (i iterationStep) Run(ctx context.Context, s Store) (Store, error) {
	ctx = ensureRun(ctx)
	if err := i.validate(); err != nil {
		return s, err
	}
	if err := runFrom(ctx).validateDefinition(i); err != nil {
		return s, err
	}
	if err := runFrom(ctx).claim(scope(ctx), i.id); err != nil {
		return s, &StepError{ID: i.id, Op: OpValidate, Err: err}
	}
	items, err := Get[[]any](s, i.input)
	if err != nil {
		return s, fmt.Errorf("workflow: iteration %q input: %w", i.id, err)
	}
	outcomes, err := i.runElements(ctx, s, items)
	if err != nil {
		return s, fmt.Errorf("workflow: iteration %q: %w", i.id, err)
	}
	return i.collect(s, outcomes)
}

func (i iterationStep) validate() error {
	switch {
	case i.id == "":
		return &StepError{ID: i.id, Op: OpValidate, Err: ErrInvalidStepID}
	case i.body == nil:
		return &StepError{ID: i.id, Op: OpValidate, Err: ErrNilStep}
	case i.limit < 0:
		return &StepError{
			ID: i.id,
			Op: OpValidate,
			Err: fmt.Errorf(
				"%w: concurrency must be non-negative, got %d",
				flow.ErrInvalidConfig,
				i.limit,
			),
		}
	}
	if err := i.input.validate(); err != nil {
		return &StepError{
			ID:  i.id,
			Op:  OpValidate,
			Err: fmt.Errorf("%w: iteration input: %w", ErrInvalidSpec, err),
		}
	}
	if err := i.bodyOutput.validate(); err != nil {
		return &StepError{
			ID:  i.id,
			Op:  OpValidate,
			Err: fmt.Errorf("%w: iteration body output: %w", ErrInvalidSpec, err),
		}
	}
	return nil
}

func (i iterationStep) runElements(
	ctx context.Context,
	s Store,
	items []any,
) ([]elementOutcome, error) {
	elementIndexes := make([]int, len(items))
	for index := range items {
		elementIndexes[index] = index
	}

	apply := flow.NodeFunc[int, elementOutcome](func(ctx context.Context, index int) (elementOutcome, error) {
		scoped := s.With(i.id, itemKey, items[index]).With(i.id, indexKey, index)
		body := (scopedStep{step: i.body}).indexed(i.id, index)
		result, err := body.run(ctx, scoped)
		if err != nil {
			// As in Parallel, a suspension travels as a value so the other
			// elements finish and get recorded rather than being cancelled.
			if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
				return elementOutcome{suspensions: suspensions}, nil
			}
			return elementOutcome{}, err
		}
		value, err := Get[any](result, i.bodyOutput)
		if err != nil {
			return elementOutcome{}, fmt.Errorf("read body output %s: %w", i.bodyOutput, err)
		}
		return elementOutcome{value: value}, nil
	})
	return flow.Map(apply, flow.MapConfig{Concurrency: i.limit}).Run(ctx, elementIndexes)
}

func (i iterationStep) collect(s Store, outcomes []elementOutcome) (Store, error) {
	outputs := make([]any, len(outcomes))
	var suspensions suspensionList
	for index, outcome := range outcomes {
		if len(outcome.suspensions) > 0 {
			suspensions = append(suspensions, outcome.suspensions...)
			continue
		}
		outputs[index] = outcome.value
	}
	if len(suspensions) > 0 {
		// The collection is incomplete, so it is not written: a partial slice
		// with holes would read as a finished result. The Journal holds what
		// each element did finish, so resuming repeats only the waiting ones.
		return s, suspensions.err()
	}
	return s.WithOutput(i.id, outputs), nil
}

func (i iterationStep) Describe() Description {
	return Description{ID: i.id, Kind: "iteration", Children: []Description{Describe(i.body)}}
}

func (i iterationStep) workflowDefinition() stepDefinition {
	return stepDefinition{kind: definitionIteration, id: i.id, body: i.body}
}
