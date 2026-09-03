package flow

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// MapConfig configures [Map]. Its zero value runs every element concurrently.
type MapConfig struct {
	// Concurrency caps the number of concurrent calls. Zero is unbounded;
	// negative values are invalid.
	Concurrency int
}

// Validate rejects a negative Concurrency; zero means unbounded.
func (c MapConfig) Validate() error {
	return nonNegativeCount("concurrency", c.Concurrency)
}

func nonNegativeCount(name string, value int) error {
	if value < 0 {
		return fmt.Errorf("%w: %s must be non-negative, got %d", ErrInvalidConfig, name, value)
	}
	return nil
}

// Map applies node to every element of the input slice concurrently and returns
// the outputs in input order. The first observed failure cancels the remaining
// calls and is returned; when calls fail concurrently, completion timing decides
// which failure is observed first. A zero [MapConfig] runs every element concurrently; set
// MapConfig.Concurrency to bound it. Cancellation is cooperative: calls already
// running must honor their context; Map waits for them to return. If the parent
// context cancellation is observed before Run commits its result, its cause
// takes precedence and Map discards the output slice, even when every started
// call happened to return successfully. A nil node is rejected even when the
// input is empty.
//
// Map is the concurrency primitive — fan-out, collecting a result per item, and
// heterogeneous fan-in are derivable from it and live in higher-level packages
// rather than in flow.
func Map[I, O any](node Node[I, O], cfg MapConfig) Node[[]I, []O] {
	return mapNode[I, O]{node: node, cfg: cfg}
}

type mapNode[I, O any] struct {
	node Node[I, O]
	cfg  MapConfig
}

func (m mapNode[I, O]) Run(ctx context.Context, input []I) ([]O, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	outputs := make([]O, len(input))
	fan := fanOut{
		count: len(input),
		limit: m.cfg.Concurrency,
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

func (m mapNode[I, O]) Validate() error {
	if err := Validate(m.node); err != nil {
		return err
	}
	return m.cfg.Validate()
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
		return context.Cause(parent)
	}
	// A single call is scheduled by the same rules as a sequential run; it takes
	// that path to skip an errgroup it would never use, not because one element
	// is admitted or cancelled differently.
	switch {
	case f.count == 1 || f.limit == 1:
		return f.runSequential(parent)
	default:
		return f.runConcurrent(parent)
	}
}

func (f fanOut) runSequential(parent context.Context) error {
	for index := range f.count {
		if err := context.Cause(parent); err != nil {
			return err
		}
		if err := f.call(parent, index); err != nil {
			if parentErr := context.Cause(parent); parentErr != nil {
				return parentErr
			}
			return err
		}
	}
	return context.Cause(parent)
}

func (f fanOut) runConcurrent(parent context.Context) error {
	// errgroup owns admission, bookkeeping, and first-error cancellation. A call
	// that was admitted while Go unblocked after another call failed checks the
	// derived context before entering caller code; this closes the only window in
	// which SetLimit can admit work after cancellation.
	group, ctx := errgroup.WithContext(parent)
	// A limit worth setting is at least two: zero means unbounded, and one took
	// the sequential path above, which is why > and >= read the same here.
	if f.limit > 1 && f.limit < f.count {
		group.SetLimit(f.limit)
	}
	for index := range f.count {
		if ctx.Err() != nil {
			break
		}
		group.Go(func() error {
			if err := context.Cause(ctx); err != nil {
				return err
			}
			return f.call(ctx, index)
		})
	}
	err := group.Wait()
	// Wait cancels ctx before returning, so ctx.Err() is useless here. The parent
	// is the only context whose cancellation still means something, and it
	// outranks a call's error: a cancelled run has no trustworthy result.
	if parentErr := context.Cause(parent); parentErr != nil {
		return parentErr
	}
	return err
}
