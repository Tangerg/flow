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
	it.node = core.NodeFunc[Store, Store](func(ctx context.Context, s Store) (Store, error) {
		raw, ok := s.Lookup(inputRef)
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
			core.NodeFunc[int, any](func(ctx context.Context, i int) (any, error) {
				scoped := s.With(id, ItemKey, items[i]).With(id, IndexKey, i)
				result, err := runStep(ctx, body, scoped)
				if err != nil {
					return nil, err
				}
				out, ok := result.Lookup(bodyOutput)
				if !ok {
					return nil, fmt.Errorf("body output %s.%s not found", bodyOutput.NodeID, bodyOutput.Path)
				}
				return out, nil
			}),
			core.WithConcurrency(limit),
		).Run(ctx, indexes)
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
