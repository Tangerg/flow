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
func Iteration(cfg IterationConfig) Step {
	it := iteration{id: cfg.ID, body: cfg.Body}
	it.node = flow.NodeFunc[Store, Store](func(ctx context.Context, s Store) (Store, error) {
		items, err := Get[[]any](s, cfg.Input)
		if err != nil {
			return s, fmt.Errorf("workflow: iteration %q input: %w", cfg.ID, err)
		}

		indexes := make([]int, len(items))
		for i := range items {
			indexes[i] = i
		}

		apply := flow.NodeFunc[int, any](func(ctx context.Context, i int) (any, error) {
			scoped := s.With(cfg.ID, itemKey, items[i]).With(cfg.ID, indexKey, i)
			result, err := runStep(ctx, cfg.Body, scoped)
			if err != nil {
				return nil, err
			}
			return Get[any](result, cfg.BodyOutput)
		})
		outputs, err := flow.Map(apply, flow.MapConfig{Concurrency: cfg.Concurrency}).Run(ctx, indexes)
		if err != nil {
			return s, fmt.Errorf("workflow: iteration %q: %w", cfg.ID, err)
		}

		return s.WithOutput(cfg.ID, outputs), nil
	})
	return it
}

// iteration is the [Step] produced by [Iteration].
type iteration struct {
	id   string
	body Step
	node Step
}

func (it iteration) Run(ctx context.Context, s Store) (Store, error) { return it.node.Run(ctx, s) }

func (it iteration) Describe() Description {
	return Description{ID: it.id, Kind: "iteration", Children: []Description{Describe(it.body)}}
}
