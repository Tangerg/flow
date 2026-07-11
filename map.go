package flow

import (
	"context"
	"sync"
	"sync/atomic"
)

// Map applies node to every element of the input slice concurrently and returns
// the outputs in input order. The first failure cancels the remaining calls and
// is returned. Every element runs concurrently. Use [MapN] to bound
// concurrency. Cancellation is cooperative: calls already running must honor
// their context; Map waits for them to return.
//
// Map is the concurrency primitive. Fan-out over several nodes, collecting a
// result per item, and heterogeneous fan-in are all derivable from it and live
// in higher-level packages rather than in flow.
func Map[I, O any](node Node[I, O]) Node[[]I, []O] {
	return mapNode[I, O]{node: node}
}

// MapN is like [Map] but runs at most limit calls concurrently. A non-positive
// limit is unbounded.
func MapN[I, O any](limit int, node Node[I, O]) Node[[]I, []O] {
	return mapNode[I, O]{node: node, limit: limit}
}

type mapNode[I, O any] struct {
	node  Node[I, O]
	limit int
}

func (m mapNode[I, O]) Run(ctx context.Context, in []I) ([]O, error) {
	out := make([]O, len(in))
	err := m.forEach(ctx, len(in), func(ctx context.Context, i int) error {
		v, err := run(ctx, m.node, in[i])
		if err != nil {
			return &IndexError{Index: i, Err: err}
		}
		out[i] = v
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// forEach calls fn(ctx, i) for each i in [0, n) with at most m.limit calls
// running at once (unbounded when m.limit <= 0). The first non-nil error from fn
// cancels the context for the remaining calls, stops new calls from starting,
// and is returned (fail-fast); otherwise the returned error is the parent
// context's error, if any. Parent cancellation takes precedence over an element
// error observed at the same time.
func (m mapNode[I, O]) forEach(parent context.Context, n int, fn func(ctx context.Context, i int) error) error {
	if n <= 0 {
		return parent.Err()
	}
	if n == 1 {
		if err := parent.Err(); err != nil {
			return err
		}
		if err := fn(parent, 0); err != nil {
			if parentErr := parent.Err(); parentErr != nil {
				return parentErr
			}
			return err
		}
		return parent.Err()
	}
	if m.limit == 1 {
		for i := range n {
			if err := parent.Err(); err != nil {
				return err
			}
			if err := fn(parent, i); err != nil {
				if parentErr := parent.Err(); parentErr != nil {
					return parentErr
				}
				return err
			}
		}
		return parent.Err()
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)
	fail := func(err error) {
		once.Do(func() {
			firstErr = err
			cancel()
		})
	}

	if m.limit > 1 && m.limit < n {
		var next atomic.Int64
		for range m.limit {
			wg.Go(func() {
				for {
					if ctx.Err() != nil {
						return
					}
					i := int(next.Add(1) - 1)
					if i >= n || ctx.Err() != nil {
						return
					}
					if err := fn(ctx, i); err != nil {
						fail(err)
						return
					}
				}
			})
		}
	} else {
		for i := range n {
			if ctx.Err() != nil {
				break
			}
			wg.Go(func() {
				if err := fn(ctx, i); err != nil {
					fail(err)
				}
			})
		}
	}

	wg.Wait()
	if err := parent.Err(); err != nil {
		return err
	}
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}
