package flowx

import (
	"context"
	"reflect"
	"slices"

	"github.com/Tangerg/flow"
)

// FanOut runs every node on the same input concurrently and returns their
// outputs in argument order. The first failure cancels the rest. A zero
// [flow.MapConfig] is unbounded. It is a thin convenience over flow.Map applied
// to the nodes as data. Every node is validated before any of them runs.
func FanOut[I, O any](nodes []flow.Node[I, O], cfg flow.MapConfig) flow.Node[I, []O] {
	nodes = slices.Clone(nodes)
	return flow.NodeFunc[I, []O](func(ctx context.Context, in I) ([]O, error) {
		for i, n := range nodes {
			if isNilNode(n) {
				return nil, &flow.IndexError{Index: i, Err: flow.ErrNilNode}
			}
		}
		apply := flow.NodeFunc[flow.Node[I, O], O](func(ctx context.Context, n flow.Node[I, O]) (O, error) {
			return n.Run(ctx, in)
		})
		return flow.Map(apply, cfg).Run(ctx, nodes)
	})
}

// Combine runs two differently typed nodes concurrently on the same input and
// merges their outputs. It is the heterogeneous fan-in that flow.Map (which is
// homogeneous) cannot express, while keeping both intermediate values statically
// typed. Both nodes and merge are validated before either node runs.
func Combine[I, A, B, O any](a flow.Node[I, A], b flow.Node[I, B], merge func(ctx context.Context, a A, b B) (O, error)) flow.Node[I, O] {
	return flow.NodeFunc[I, O](func(ctx context.Context, in I) (O, error) {
		var zero O
		if merge == nil {
			return zero, flow.ErrNilFunc
		}
		if isNilNode(a) || isNilNode(b) {
			return zero, flow.ErrNilNode
		}
		var av A
		var bv B
		tasks := flow.NodeFunc[int, struct{}](func(ctx context.Context, task int) (struct{}, error) {
			var err error
			switch task {
			case 0:
				av, err = a.Run(ctx, in)
			case 1:
				bv, err = b.Run(ctx, in)
			}
			return struct{}{}, err
		})
		if _, err := flow.Map(tasks, flow.MapConfig{}).Run(ctx, []int{0, 1}); err != nil {
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
		if isNilNode(nodes[0]) {
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
		if isNilNode(primary) || isNilNode(alternate) {
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

// isNilNode mirrors the core's definition validation without exporting a
// helper solely for derived combinators. Nil function adapters are invalid;
// caller-defined nil pointers may still implement a useful nil-safe Run.
func isNilNode[I, O any](node flow.Node[I, O]) bool {
	if node == nil {
		return true
	}
	value := reflect.ValueOf(node)
	return value.Kind() == reflect.Func && value.IsNil()
}
