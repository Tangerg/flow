package flow

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	if m.node == nil {
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
	group := indexGroup{
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
	if err := group.run(ctx); err != nil {
		return nil, err
	}
	return outputs, nil
}

// indexGroup owns one bounded or unbounded fan-out over [0, count).
type indexGroup struct {
	count int
	limit int
	call  func(context.Context, int) error
}

func (group indexGroup) run(parent context.Context) error {
	if group.count <= 0 {
		return parent.Err()
	}
	switch {
	case group.count == 1:
		return group.runOne(parent)
	case group.limit == 1:
		return group.runSequential(parent)
	default:
		return group.runConcurrent(parent)
	}
}

func (group indexGroup) runOne(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	err := group.call(parent, 0)
	if parentErr := parent.Err(); parentErr != nil {
		return parentErr
	}
	return err
}

func (group indexGroup) runSequential(parent context.Context) error {
	for index := range group.count {
		if err := parent.Err(); err != nil {
			return err
		}
		if err := group.call(parent, index); err != nil {
			if parentErr := parent.Err(); parentErr != nil {
				return parentErr
			}
			return err
		}
	}
	return parent.Err()
}

func (group indexGroup) runConcurrent(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	failure := firstFailure{cancel: cancel}
	var workers sync.WaitGroup
	if group.bounded() {
		group.startWorkers(ctx, &workers, &failure)
	} else {
		group.startCalls(ctx, &workers, &failure)
	}
	workers.Wait()

	if err := parent.Err(); err != nil {
		return err
	}
	if failure.err != nil {
		return failure.err
	}
	return ctx.Err()
}

func (group indexGroup) bounded() bool {
	return group.limit > 1 && group.limit < group.count
}

func (group indexGroup) startWorkers(
	ctx context.Context,
	workers *sync.WaitGroup,
	failure *firstFailure,
) {
	var next atomic.Int64
	count, call := group.count, group.call
	for range group.limit {
		workers.Go(func() {
			for {
				if ctx.Err() != nil {
					return
				}
				index := int(next.Add(1) - 1)
				if index >= count || ctx.Err() != nil {
					return
				}
				if err := call(ctx, index); err != nil {
					failure.record(err)
					return
				}
			}
		})
	}
}

func (group indexGroup) startCalls(
	ctx context.Context,
	workers *sync.WaitGroup,
	failure *firstFailure,
) {
	call := group.call
	for index := range group.count {
		if ctx.Err() != nil {
			return
		}
		workers.Go(func() {
			// Cancellation may happen after dispatch but before this
			// goroutine starts. Do not enter user code in that window.
			if ctx.Err() != nil {
				return
			}
			if err := call(ctx, index); err != nil {
				failure.record(err)
			}
		})
	}
}

type firstFailure struct {
	once   sync.Once
	err    error
	cancel context.CancelFunc
}

func (failure *firstFailure) record(err error) {
	failure.once.Do(func() {
		failure.err = err
		failure.cancel()
	})
}
