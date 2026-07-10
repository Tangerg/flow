package core

import (
	"context"
	"sync"
)

// Map applies node to every element of the input slice concurrently and returns
// the outputs in input order. The first failure cancels the remaining calls and
// is returned. By default every element runs concurrently; bound it with
// [WithConcurrency].
//
// Map is the concurrency primitive. Fan-out over several nodes, collecting a
// result per item, and heterogeneous fan-in are all derivable from it and live
// in higher-level packages rather than in core.
func Map[I, O any](node Node[I, O], opts ...MapOption) Node[[]I, []O] {
	var c mapConfig
	for _, opt := range opts {
		opt(&c)
	}
	return mapNode[I, O]{node: node, limit: c.concurrency}
}

// MapOption configures a [Map].
type MapOption func(*mapConfig)

type mapConfig struct {
	concurrency int // <= 0 means unbounded
}

// WithConcurrency caps the number of elements processed at once. A value <= 0
// (the default) means unbounded — every element starts immediately. When mapping
// over a large slice, set a bound to avoid spawning one goroutine per element.
func WithConcurrency(n int) MapOption {
	return func(c *mapConfig) { c.concurrency = n }
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
			return err
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
// context's error, if any.
func (m mapNode[I, O]) forEach(parent context.Context, n int, fn func(ctx context.Context, i int) error) error {
	if n <= 0 {
		return parent.Err()
	}
	if n == 1 {
		if err := parent.Err(); err != nil {
			return err
		}
		return fn(parent, 0)
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var sem chan struct{}
	if m.limit > 0 {
		sem = make(chan struct{}, m.limit)
	}

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

	for i := range n {
		if ctx.Err() != nil {
			break
		}
		if sem != nil {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
			}
			if ctx.Err() != nil {
				break
			}
		}
		wg.Go(func() {
			if sem != nil {
				defer func() { <-sem }()
			}
			if err := fn(ctx, i); err != nil {
				fail(err)
			}
		})
	}

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}
