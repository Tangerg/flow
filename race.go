package flow

import (
	"context"
	"errors"
	"slices"
)

// Race runs all nodes concurrently on the same input and returns the first
// successful result, cancelling the rest. If every node fails, it returns their
// joined errors in input order, each wrapped in an [IndexError]. Cancellation is
// cooperative; losing nodes must honor their context.
//
// Race is the disjunction concurrency primitive — the "first success wins" twin
// of [Map]'s "wait for all". It cannot be expressed by a wait-for-all map, so it
// is a primitive rather than a derived helper.
func Race[I, O any](nodes ...Node[I, O]) Node[I, O] {
	nodes = slices.Clone(nodes)
	return NodeFunc[I, O](func(ctx context.Context, in I) (O, error) {
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
					r.err = ErrNilNode
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
			errs[r.index] = &IndexError{Index: r.index, Err: r.err}
		}
		return zero, errors.Join(errs...)
	})
}
