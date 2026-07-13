package flowx

import (
	"context"
	"slices"

	"github.com/Tangerg/flow"
)

// FanOut runs every node on the same input concurrently and returns their
// outputs in argument order. The first failure cancels the rest. Bound
// concurrency with cfg.Concurrency (non-positive is unbounded). It is a thin
// convenience over flow.Map applied to the nodes as data.
func FanOut[I, O any](cfg flow.MapConfig, nodes ...flow.Node[I, O]) flow.Node[I, []O] {
	nodes = slices.Clone(nodes)
	return flow.NodeFunc[I, []O](func(ctx context.Context, in I) ([]O, error) {
		apply := flow.NodeFunc[flow.Node[I, O], O](func(ctx context.Context, n flow.Node[I, O]) (O, error) {
			var zero O
			if n == nil {
				return zero, flow.ErrNilNode
			}
			return n.Run(ctx, in)
		})
		return flow.Map(apply, cfg).Run(ctx, nodes)
	})
}

// Combine runs two differently typed nodes concurrently on the same input and
// merges their outputs. It is the heterogeneous fan-in that flow.Map (which is
// homogeneous) cannot express, while keeping both intermediate values statically
// typed.
func Combine[I, A, B, O any](a flow.Node[I, A], b flow.Node[I, B], merge func(ctx context.Context, a A, b B) (O, error)) flow.Node[I, O] {
	return flow.NodeFunc[I, O](func(ctx context.Context, in I) (O, error) {
		var zero O
		if merge == nil {
			return zero, flow.ErrNilFunc
		}
		var av A
		var bv B
		tasks := flow.NodeFunc[int, struct{}](func(ctx context.Context, task int) (struct{}, error) {
			var err error
			switch task {
			case 0:
				if a == nil {
					return struct{}{}, flow.ErrNilNode
				}
				av, err = a.Run(ctx, in)
			case 1:
				if b == nil {
					return struct{}{}, flow.ErrNilNode
				}
				bv, err = b.Run(ctx, in)
			}
			return struct{}{}, err
		})
		if _, err := flow.Map(tasks).Run(ctx, []int{0, 1}); err != nil {
			return zero, err
		}
		return merge(ctx, av, bv)
	})
}

// Chain composes any number of same-type nodes in sequence via flow.Then. It is
// the variadic convenience for the common same-type case; with no nodes it is a
// pass-through.
func Chain[T any](nodes ...flow.Node[T, T]) flow.Node[T, T] {
	switch len(nodes) {
	case 0:
		return flow.NodeFunc[T, T](func(_ context.Context, in T) (T, error) { return in, nil })
	case 1:
		if nodes[0] == nil {
			return flow.NodeFunc[T, T](nil)
		}
		return nodes[0]
	}
	n := nodes[0]
	for _, next := range nodes[1:] {
		n = flow.Then(n, next)
	}
	return n
}

// Fallback runs primary; if it fails while the parent context remains live, it
// runs alternate with the same input. Cancellation of the outer operation is
// returned as-is and does not trigger the fallback.
func Fallback[I, O any](primary, alternate flow.Node[I, O]) flow.Node[I, O] {
	return flow.NodeFunc[I, O](func(ctx context.Context, in I) (O, error) {
		var out O
		if primary == nil || alternate == nil {
			return out, flow.ErrNilNode
		}
		out, err := primary.Run(ctx, in)
		if err == nil {
			return out, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return out, ctxErr
		}
		return alternate.Run(ctx, in)
	})
}
