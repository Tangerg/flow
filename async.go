package flow

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Future represents the result of an asynchronous computation that may not yet
// be available. It supports several retrieval strategies to suit different
// call-site requirements.
//
// Implementations are provided by the caller; this package does not include a
// default implementation, leaving concurrency strategy up to the user.
type Future[V any] interface {
	// Get blocks until the result is ready or an error occurs.
	Get() (V, error)

	// GetWithTimeout blocks until the result is ready, the timeout elapses,
	// or an error occurs.
	GetWithTimeout(timeout time.Duration) (V, error)

	// GetWithContext blocks until the result is ready, the context is cancelled,
	// or an error occurs.
	GetWithContext(ctx context.Context) (V, error)

	// TryGet returns the result immediately without blocking.
	// ready is false when the computation has not yet completed.
	TryGet() (value V, err error, ready bool)
}

var _ Node[any, Future[any]] = (*Async[any, any, Future[any]])(nil)

// Async wraps a processor that starts a background computation and returns a
// Future for deferred result retrieval. The node itself returns immediately
// after launching the operation.
//
// Typical use cases:
//   - Long-running operations that should not block the main workflow
//   - Fan-out scenarios where multiple results are awaited later
//   - Computations whose output is needed only at a later pipeline stage
type Async[I, O any, F Future[O]] struct {
	processor func(context.Context, I) (F, error)
}

// NewAsync creates an Async node with the given processor.
// The processor is responsible for starting the async work and returning a
// Future that eventually resolves to the result.
// Returns an error if the processor is nil.
func NewAsync[I, O any, F Future[O]](processor func(context.Context, I) (F, error)) (*Async[I, O, F], error) {
	if processor == nil {
		return nil, errors.New("async processor cannot be nil")
	}

	return &Async[I, O, F]{
		processor: processor,
	}, nil
}

// Run starts the asynchronous operation and returns a Future.
// The caller can retrieve the result at any later point using the Future's
// Get, GetWithTimeout, GetWithContext, or TryGet methods.
func (a *Async[I, O, F]) Run(ctx context.Context, input I) (F, error) {
	future, err := a.processor(ctx, input)
	if err != nil {
		var zero F
		return zero, fmt.Errorf("failed to start async operation: %w", err)
	}

	return future, nil
}
