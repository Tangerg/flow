package flowx

import (
	"context"
	"errors"
	"slices"

	"github.com/Tangerg/flow"
)

// FanOut runs every node on the same input concurrently and returns their
// outputs in argument order. The first failure cancels the rest.
func FanOut[I, O any](nodes ...flow.Node[I, O]) flow.Node[I, []O] {
	return fanOut(nodes, nil)
}

// FanOutN is like [FanOut] but runs at most limit nodes concurrently. A
// non-positive limit is unbounded.
func FanOutN[I, O any](limit int, nodes ...flow.Node[I, O]) flow.Node[I, []O] {
	return fanOut(nodes, []flow.MapOption{flow.WithConcurrency(limit)})
}

func fanOut[I, O any](nodes []flow.Node[I, O], opts []flow.MapOption) flow.Node[I, []O] {
	nodes = slices.Clone(nodes)
	return flow.NodeFunc[I, []O](func(ctx context.Context, in I) ([]O, error) {
		apply := flow.NodeFunc[flow.Node[I, O], O](func(ctx context.Context, n flow.Node[I, O]) (O, error) {
			var zero O
			if n == nil {
				return zero, flow.ErrNilNode
			}
			return n.Run(ctx, in)
		})
		return flow.Map(apply, opts...).Run(ctx, nodes)
	})
}

// FanOutAll runs every node on the same input concurrently and collects a
// [Result] per node. The returned error is non-nil only on context cancellation.
func FanOutAll[I, O any](nodes ...flow.Node[I, O]) flow.Node[I, []Result[O]] {
	return fanOutAll(nodes, nil)
}

// FanOutAllN is like [FanOutAll] but runs at most limit nodes concurrently. A
// non-positive limit is unbounded.
func FanOutAllN[I, O any](limit int, nodes ...flow.Node[I, O]) flow.Node[I, []Result[O]] {
	return fanOutAll(nodes, []flow.MapOption{flow.WithConcurrency(limit)})
}

func fanOutAll[I, O any](nodes []flow.Node[I, O], opts []flow.MapOption) flow.Node[I, []Result[O]] {
	nodes = slices.Clone(nodes)
	return flow.NodeFunc[I, []Result[O]](func(ctx context.Context, in I) ([]Result[O], error) {
		apply := flow.NodeFunc[flow.Node[I, O], Result[O]](func(ctx context.Context, n flow.Node[I, O]) (Result[O], error) {
			var out O
			var err error
			if n == nil {
				err = flow.ErrNilNode
			} else {
				out, err = n.Run(ctx, in)
			}
			return Result[O]{Value: out, Err: err}, nil
		})
		return flow.Map(apply, opts...).Run(ctx, nodes)
	})
}

// MapAll applies node to every element concurrently and collects a [Result] per
// element. It does not fail fast.
func MapAll[I, O any](node flow.Node[I, O]) flow.Node[[]I, []Result[O]] {
	return mapAll(node, nil)
}

// MapAllN is like [MapAll] but processes at most limit elements concurrently. A
// non-positive limit is unbounded.
func MapAllN[I, O any](limit int, node flow.Node[I, O]) flow.Node[[]I, []Result[O]] {
	return mapAll(node, []flow.MapOption{flow.WithConcurrency(limit)})
}

func mapAll[I, O any](node flow.Node[I, O], opts []flow.MapOption) flow.Node[[]I, []Result[O]] {
	wrapped := flow.NodeFunc[I, Result[O]](func(ctx context.Context, in I) (Result[O], error) {
		var out O
		var err error
		if node == nil {
			err = flow.ErrNilNode
		} else {
			out, err = node.Run(ctx, in)
		}
		return Result[O]{Value: out, Err: err}, nil
	})
	return flow.Map(wrapped, opts...)
}

// Combine2 runs two differently typed nodes concurrently on the same input and
// merges their outputs. The implementation uses flow.Map as the concurrency
// primitive while keeping both intermediate values statically typed.
func Combine2[I, A, B, O any](a flow.Node[I, A], b flow.Node[I, B], merge func(ctx context.Context, a A, b B) (O, error)) flow.Node[I, O] {
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

// ErrNoNodes is returned by [Race] when given no nodes.
var ErrNoNodes = errors.New("flowx: no nodes")

// Race runs all nodes concurrently on the same input and returns the first
// successful result, cancelling the rest. If every node fails, it returns their
// joined errors in input order. Cancellation is cooperative; losing nodes must
// honor their context. Race cannot be derived from flow.Map (which waits for
// all), so it uses its own goroutines.
func Race[I, O any](nodes ...flow.Node[I, O]) flow.Node[I, O] {
	nodes = slices.Clone(nodes)
	return flow.NodeFunc[I, O](func(ctx context.Context, in I) (O, error) {
		var zero O
		if len(nodes) == 0 {
			return zero, ErrNoNodes
		}
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		parent := ctx
		ctx, cancel := context.WithCancel(parent)
		defer cancel()

		type result struct {
			index int
			val   O
			err   error
		}
		ch := make(chan result, len(nodes))
		for i, n := range nodes {
			go func() {
				r := result{index: i}
				if n == nil {
					r.err = flow.ErrNilNode
				} else {
					r.val, r.err = n.Run(ctx, in)
				}
				ch <- r
			}()
		}

		errs := make([]error, len(nodes))
		for range nodes {
			var r result
			select {
			case <-parent.Done():
				return zero, parent.Err()
			case r = <-ch:
			}
			if err := parent.Err(); err != nil {
				return zero, err
			}
			if r.err == nil {
				return r.val, nil // cancel() (deferred) stops the losers
			}
			errs[r.index] = &flow.IndexError{Index: r.index, Err: r.err}
		}
		return zero, errors.Join(errs...)
	})
}

// Identity returns a node that returns its input unchanged — the neutral element
// for flow.Then.
func Identity[T any]() flow.Node[T, T] {
	return flow.NodeFunc[T, T](func(_ context.Context, in T) (T, error) { return in, nil })
}

// Chain composes any number of same-type nodes in sequence via flow.Then. With
// no nodes it is [Identity].
func Chain[T any](nodes ...flow.Node[T, T]) flow.Node[T, T] {
	switch len(nodes) {
	case 0:
		return Identity[T]()
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
