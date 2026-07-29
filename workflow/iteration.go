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
	// Input references the []any to iterate over.
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
// Output(cfg.ID).
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
	suspensions []*Suspension
}

// iteration is the [Step] produced by [Iteration].
type iterationStep struct {
	id         string
	input      Ref
	body       Step
	bodyOutput Ref
	limit      int
}

func (it iterationStep) Run(ctx context.Context, s Store) (Store, error) {
	switch {
	case it.id == "":
		return s, &StepError{ID: it.id, Op: OpValidate, Err: ErrInvalidStepID}
	case it.body == nil:
		return s, &StepError{ID: it.id, Op: OpValidate, Err: ErrNilStep}
	case it.limit < 0:
		return s, &StepError{
			ID:  it.id,
			Op:  OpValidate,
			Err: fmt.Errorf("%w: negative concurrency", flow.ErrInvalidConfig),
		}
	}
	if err := it.input.validate("iteration input"); err != nil {
		return s, &StepError{
			ID:  it.id,
			Op:  OpValidate,
			Err: fmt.Errorf("%w: %w", ErrInvalidSpec, err),
		}
	}
	if err := it.bodyOutput.validate("iteration bodyOutput"); err != nil {
		return s, &StepError{
			ID:  it.id,
			Op:  OpValidate,
			Err: fmt.Errorf("%w: %w", ErrInvalidSpec, err),
		}
	}

	items, err := Get[[]any](s, it.input)
	if err != nil {
		return s, fmt.Errorf("workflow: iteration %q input: %w", it.id, err)
	}

	indexes := make([]int, len(items))
	for i := range items {
		indexes[i] = i
	}

	apply := flow.NodeFunc[int, elementOutcome](func(ctx context.Context, i int) (elementOutcome, error) {
		scoped := s.With(it.id, itemKey, items[i]).With(it.id, indexKey, i)
		runner := (stepRunner{ctx: ctx}).indexed(it.id, i)
		result, err := runner.run(it.body, scoped)
		if err != nil {
			// As in Parallel, a suspension travels as a value so the other
			// elements finish and get recorded rather than being cancelled.
			if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
				return elementOutcome{suspensions: suspensions}, nil
			}
			return elementOutcome{}, err
		}
		value, err := Get[any](result, it.bodyOutput)
		return elementOutcome{value: value}, err
	})
	outcomes, err := flow.Map(apply, flow.MapConfig{Concurrency: it.limit}).Run(ctx, indexes)
	if err != nil {
		return s, fmt.Errorf("workflow: iteration %q: %w", it.id, err)
	}

	outputs := make([]any, len(outcomes))
	var suspensions []*Suspension
	for i, outcome := range outcomes {
		if len(outcome.suspensions) > 0 {
			suspensions = append(suspensions, outcome.suspensions...)
			continue
		}
		outputs[i] = outcome.value
	}
	if len(suspensions) > 0 {
		// The collection is incomplete, so it is not written: a partial slice
		// with holes would read as a finished result. The Journal holds what
		// each element did finish, so resuming repeats only the waiting ones.
		return s, suspensionList(suspensions).err()
	}
	return s.WithOutput(it.id, outputs), nil
}

func (it iterationStep) Describe() Description {
	return Description{ID: it.id, Kind: "iteration", Children: []Description{Describe(it.body)}}
}
