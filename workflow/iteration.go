package workflow

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
)

const (
	itemKey  = "item"
	indexKey = "index"
)

// Item returns the reference under which [Iteration] stores the current item.
func Item(id string) Ref { return At(id, itemKey) }

// Index returns the reference under which [Iteration] stores the current
// item's zero-based index.
func Index(id string) Ref { return At(id, indexKey) }

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
	// Concurrency caps concurrent element runs. A non-positive value is unbounded.
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
// would read as a finished result.
func Iteration(cfg IterationConfig) Step {
	it := iterationStep{id: cfg.ID, body: cfg.Body}
	it.node = flow.NodeFunc[Store, Store](func(ctx context.Context, s Store) (Store, error) {
		items, err := Get[[]any](s, cfg.Input)
		if err != nil {
			return s, fmt.Errorf("workflow: iteration %q input: %w", cfg.ID, err)
		}

		indexes := make([]int, len(items))
		for i := range items {
			indexes[i] = i
		}

		apply := flow.NodeFunc[int, elementOutcome](func(ctx context.Context, i int) (elementOutcome, error) {
			scoped := s.With(cfg.ID, itemKey, items[i]).With(cfg.ID, indexKey, i)
			result, err := runStep(WithScope(ctx, indexScope(cfg.ID, i)), cfg.Body, scoped)
			if err != nil {
				// As in Parallel, a suspension travels as a value so the other
				// elements finish and get recorded rather than being cancelled.
				if suspensions, only := asSuspensions(err); only {
					return elementOutcome{suspensions: suspensions}, nil
				}
				return elementOutcome{}, err
			}
			value, err := Get[any](result, cfg.BodyOutput)
			return elementOutcome{value: value}, err
		})
		outcomes, err := flow.Map(apply, flow.MapConfig{Concurrency: cfg.Concurrency}).Run(ctx, indexes)
		if err != nil {
			return s, fmt.Errorf("workflow: iteration %q: %w", cfg.ID, err)
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
			return s, joinSuspensions(suspensions)
		}
		return s.WithOutput(cfg.ID, outputs), nil
	})
	return it
}

// elementOutcome is one element's result. A suspension travels as a value
// because it is not a failure; anything else travels as the mapper's error.
type elementOutcome struct {
	value       any
	suspensions []*Suspension
}

// iteration is the [Step] produced by [Iteration].
type iterationStep struct {
	id   string
	body Step
	node Step
}

func (it iterationStep) Run(ctx context.Context, s Store) (Store, error) { return it.node.Run(ctx, s) }

func (it iterationStep) Describe() Description {
	return Description{ID: it.id, Kind: "iteration", Children: []Description{Describe(it.body)}}
}
