package workflow

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow/core"
)

// Keys under which [Iteration] scopes each element for the body to read.
const (
	// ItemKey holds the current element, addressed as Ref{NodeID: id, Path: ItemKey}.
	ItemKey = "item"
	// IndexKey holds the current element's zero-based index.
	IndexKey = "index"
)

// Iteration runs body once per element of an input array, concurrently, and
// collects each run's output into a []any written under (id, OutputKey).
//
// For element i, body runs on a scoped Store that adds the element under
// (id, ItemKey) and its index under (id, IndexKey); the body reads the element
// via Ref{NodeID: id, Path: ItemKey}. bodyOutput points at the value in the
// post-run Store to collect for that element.
//
// The value at inputRef must be a []any. The first element to fail cancels the
// rest; bound the concurrency with core.WithConcurrency.
func Iteration(id string, inputRef Ref, body Step, bodyOutput Ref, opts ...core.MapOption) Step {
	it := iteration{id: id, body: body}
	it.node = core.Func[Store, Store](func(ctx context.Context, s Store) (Store, error) {
		raw, ok := s.Get(inputRef.NodeID, inputRef.Path)
		if !ok {
			return s, fmt.Errorf("workflow: iteration %q: input %s.%s not found", id, inputRef.NodeID, inputRef.Path)
		}
		items, ok := raw.([]any)
		if !ok {
			return s, fmt.Errorf("workflow: iteration %q: input is %T, want []any", id, raw)
		}

		indexes := make([]int, len(items))
		for i := range items {
			indexes[i] = i
		}

		outputs, err := core.Map(
			core.Func[int, any](func(ctx context.Context, i int) (any, error) {
				scoped := s.With(id, ItemKey, items[i]).With(id, IndexKey, i)
				result, err := runStep(ctx, body, scoped)
				if err != nil {
					return nil, fmt.Errorf("index %d: %w", i, err)
				}
				out, ok := result.Get(bodyOutput.NodeID, bodyOutput.Path)
				if !ok {
					return nil, fmt.Errorf("index %d: body output %s.%s not found", i, bodyOutput.NodeID, bodyOutput.Path)
				}
				return out, nil
			}),
			opts...,
		).Run(ctx, indexes)
		if err != nil {
			return s, fmt.Errorf("workflow: iteration %q: %w", id, err)
		}

		return s.With(id, OutputKey, outputs), nil
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
