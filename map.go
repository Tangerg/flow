package flow

import (
	"context"
	"fmt"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// MapConfig configures [Map]. Its zero value runs every element concurrently.
type MapConfig struct {
	// Concurrency caps the number of concurrent calls. Zero is unbounded;
	// negative values are invalid.
	Concurrency int
}

// Map applies node to every element of the input slice concurrently and returns
// the outputs in input order. The first failure cancels the remaining calls and
// is returned. A zero [MapConfig] runs every element concurrently; set
// MapConfig.Concurrency to bound it. Cancellation is cooperative: calls already
// running must honor their context; Map waits for them to return. If the parent
// context is cancelled before Run returns, its cancellation error takes
// precedence and Map discards the output slice, even when every started call
// happened to return successfully. A nil node is rejected even when the input
// is empty.
//
// Map is the concurrency primitive — fan-out, collecting a result per item, and
// heterogeneous fan-in are derivable from it and live in higher-level packages
// rather than in flow.
func Map[I, O any](node Node[I, O], cfg MapConfig) Node[[]I, []O] {
	return mapNode[I, O]{node: node, limit: cfg.Concurrency}
}

type mapNode[I, O any] struct {
	node  Node[I, O]
	limit int
}

func (m mapNode[I, O]) Run(ctx context.Context, input []I) ([]O, error) {
	if isNilNode(m.node) {
		return nil, ErrNilNode
	}
	if m.limit < 0 {
		return nil, fmt.Errorf(
			"%w: concurrency must be non-negative, got %d",
			ErrInvalidConfig,
			m.limit,
		)
	}
	outputs := make([]O, len(input))
	fan := fanOut{
		count: len(input),
		limit: m.limit,
		call: func(ctx context.Context, index int) error {
			value, err := m.node.Run(ctx, input[index])
			if err != nil {
				return &IndexError{Index: index, Err: err}
			}
			outputs[index] = value
			return nil
		},
	}
	if err := fan.run(ctx); err != nil {
		return nil, err
	}
	return outputs, nil
}

// fanOut calls call once for every index in [0, count), optionally limiting how
// many run at a time. It exists apart from mapNode so the scheduling rules are
// stated once, independent of the element types being mapped.
type fanOut struct {
	count int
	limit int
	call  func(context.Context, int) error
}

func (f fanOut) run(parent context.Context) error {
	if f.count <= 0 {
		return parent.Err()
	}
	switch {
	case f.count == 1:
		return f.runOne(parent)
	case f.limit == 1:
		return f.runSequential(parent)
	default:
		return f.runConcurrent(parent)
	}
}

func (f fanOut) runOne(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	err := f.call(parent, 0)
	if parentErr := parent.Err(); parentErr != nil {
		return parentErr
	}
	return err
}

func (f fanOut) runSequential(parent context.Context) error {
	for index := range f.count {
		if err := parent.Err(); err != nil {
			return err
		}
		if err := f.call(parent, index); err != nil {
			if parentErr := parent.Err(); parentErr != nil {
				return parentErr
			}
			return err
		}
	}
	return parent.Err()
}

func (f fanOut) runConcurrent(parent context.Context) error {
	// errgroup owns the bookkeeping: the first error cancels the derived context,
	// and Wait reports that error rather than a later one. Spreading the work
	// across goroutines stays local — see startWorkers.
	group, ctx := errgroup.WithContext(parent)
	if f.bounded() {
		f.startWorkers(ctx, group)
	} else {
		f.startCalls(ctx, group)
	}
	err := group.Wait()
	// Wait cancels ctx before returning, so ctx.Err() is useless here. The parent
	// is the only context whose cancellation still means something, and it
	// outranks a call's error: a cancelled run has no trustworthy result.
	if parentErr := parent.Err(); parentErr != nil {
		return parentErr
	}
	return err
}

func (f fanOut) bounded() bool {
	return f.limit > 1 && f.limit < f.count
}

// startWorkers claims indexes from a shared counter with a pool of exactly limit
// goroutines.
//
// errgroup.SetLimit would express the same bound in one line, but it bounds
// concurrency by making every element wait on a semaphore, so it allocates per
// element rather than per worker: at 256 elements with a limit of 8, 518
// allocations against 23. A caller sets a limit to bound what the fan-out
// consumes, and Map is what Parallel, Iteration, and Graph all fan out through,
// so the bound has to hold for goroutines too — not just for calls in flight.
func (f fanOut) startWorkers(ctx context.Context, group *errgroup.Group) {
	var next atomic.Int64
	count, call := f.count, f.call
	for range f.limit {
		group.Go(func() error {
			// A worker that stops because the group was already cancelled has no
			// error of its own: the failure that cancelled it is the group's
			// error, and a cancelled parent is reported by the check after Wait.
			//
			//nolint:nilerr // A worker that never called the node reports nothing.
			for {
				if ctx.Err() != nil {
					return nil
				}
				index := int(next.Add(1) - 1)
				if index >= count || ctx.Err() != nil {
					return nil
				}
				if err := call(ctx, index); err != nil {
					return err
				}
			}
		})
	}
}

func (f fanOut) startCalls(ctx context.Context, group *errgroup.Group) {
	for index := range f.count {
		if ctx.Err() != nil {
			return
		}
		group.Go(func() error {
			// Cancellation may happen after dispatch but before this goroutine
			// starts. Do not enter user code in that window.
			//
			//nolint:nilerr // A skipped call has no error of its own to report.
			if ctx.Err() != nil {
				return nil
			}
			return f.call(ctx, index)
		})
	}
}
