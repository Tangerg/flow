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

// Iteration runs body once per element of an input array, concurrently, and
// collects each run's output into a []any written at [Output](id).
//
// For element i, body runs on a scoped Store that adds the element under
// [Item] and its index via [Index]. bodyOutput points at the value in the
// post-run Store to collect for that element.
//
// The value at inputRef must be a []any. The first element to fail cancels the
// rest.
func Iteration(id string, inputRef Ref, body Step, bodyOutput Ref) Step {
	return iterationN(0, id, inputRef, body, bodyOutput)
}

// IterationN is like [Iteration] but runs at most limit body executions
// concurrently. A non-positive limit is unbounded.
func IterationN(limit int, id string, inputRef Ref, body Step, bodyOutput Ref) Step {
	return iterationN(limit, id, inputRef, body, bodyOutput)
}

func iterationN(limit int, id string, inputRef Ref, body Step, bodyOutput Ref) Step {
	it := iteration{id: id, body: body}
	it.node = flow.NodeFunc[Store, Store](func(ctx context.Context, s Store) (Store, error) {
		items, err := Get[[]any](s, inputRef)
		if err != nil {
			return s, fmt.Errorf("workflow: iteration %q input: %w", id, err)
		}

		indexes := make([]int, len(items))
		for i := range items {
			indexes[i] = i
		}

		apply := flow.NodeFunc[int, any](func(ctx context.Context, i int) (any, error) {
			scoped := s.With(id, itemKey, items[i]).With(id, indexKey, i)
			result, err := runStep(ctx, body, scoped)
			if err != nil {
				return nil, err
			}
			return Get[any](result, bodyOutput)
		})
		mapper := flow.Map(apply)
		if limit > 0 {
			mapper = flow.MapN(limit, apply)
		}
		outputs, err := mapper.Run(ctx, indexes)
		if err != nil {
			return s, fmt.Errorf("workflow: iteration %q: %w", id, err)
		}

		return s.WithOutput(id, outputs), nil
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
