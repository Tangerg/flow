package flowx

import (
	"context"
	"errors"
	"time"

	"github.com/Tangerg/flow"
)

// --- Retry ---

// RetryConfig configures [Retry]. The zero value applies the defaults: three
// attempts, no backoff, and retrying every error except a context cancellation
// or deadline.
type RetryConfig struct {
	// Attempts is the maximum number of tries. A value below 1 means 3.
	Attempts int
	// Backoff returns the delay before the try following the given 1-based
	// attempt. A nil Backoff, or a non-positive delay, waits not at all. See
	// [ConstantBackoff] and [ExponentialBackoff].
	Backoff func(attempt int) time.Duration
	// Retryable reports whether an error should be retried. A nil Retryable
	// retries every error except a context cancellation or deadline.
	Retryable func(error) bool
}

// ConstantBackoff waits a fixed duration between attempts.
func ConstantBackoff(d time.Duration) func(int) time.Duration {
	return func(int) time.Duration { return d }
}

// ExponentialBackoff waits base, 2*base, 4*base, ... between attempts.
func ExponentialBackoff(base time.Duration) func(int) time.Duration {
	return func(attempt int) time.Duration {
		if base <= 0 || attempt <= 0 {
			return 0
		}
		const maxDuration = time.Duration(1<<63 - 1)
		d := base
		for range attempt - 1 {
			if d > maxDuration/2 {
				return maxDuration
			}
			d *= 2
		}
		return d
	}
}

func defaultRetryable(err error) bool {
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// Retry runs node up to a number of attempts (default 3), retrying while the
// error is retryable (default: any non-context error) and ctx is live. It
// re-runs with the same input, so node should be idempotent. Backoff, if set,
// waits between attempts and respects ctx.
func Retry[I, O any](node flow.Node[I, O], cfg RetryConfig) flow.Node[I, O] {
	attempts := cfg.Attempts
	if attempts < 1 {
		attempts = 3
	}
	retryable := cfg.Retryable
	if retryable == nil {
		retryable = defaultRetryable
	}
	return flow.NodeFunc[I, O](func(ctx context.Context, in I) (O, error) {
		var out O
		if node == nil {
			return out, flow.ErrNilNode
		}
		var err error
		for attempt := 1; attempt <= attempts; attempt++ {
			if err := ctx.Err(); err != nil {
				return out, err
			}
			if attempt > 1 && cfg.Backoff != nil {
				if d := cfg.Backoff(attempt - 1); d > 0 {
					timer := time.NewTimer(d)
					select {
					case <-ctx.Done():
						timer.Stop()
						return out, ctx.Err()
					case <-timer.C:
					}
				}
			}
			out, err = node.Run(ctx, in)
			if err == nil {
				return out, nil
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return out, ctxErr
			}
			if !retryable(err) {
				return out, err
			}
		}
		return out, err
	})
}

// --- Timeout ---

// Timeout runs node with a context cancelled after d. It is cooperative: node
// must honor ctx for the timeout to take effect promptly.
func Timeout[I, O any](node flow.Node[I, O], d time.Duration) flow.Node[I, O] {
	return flow.NodeFunc[I, O](func(ctx context.Context, in I) (O, error) {
		var out O
		if node == nil {
			return out, flow.ErrNilNode
		}
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return node.Run(ctx, in)
	})
}

// --- Trace ---

// TraceHooks observe a node's execution. Either hook may be nil. When the
// decorated node is run concurrently, hooks may also be called concurrently.
type TraceHooks struct {
	Before func(ctx context.Context, name string)
	After  func(ctx context.Context, name string, elapsed time.Duration, err error)
}

// Trace instruments node, invoking hooks around each run with the elapsed time.
func Trace[I, O any](node flow.Node[I, O], name string, hooks TraceHooks) flow.Node[I, O] {
	return flow.NodeFunc[I, O](func(ctx context.Context, in I) (O, error) {
		var out O
		if node == nil {
			return out, flow.ErrNilNode
		}
		if hooks.Before != nil {
			hooks.Before(ctx, name)
		}
		start := time.Now()
		out, err := node.Run(ctx, in)
		if hooks.After != nil {
			hooks.After(ctx, name, time.Since(start), err)
		}
		return out, err
	})
}

// --- Fallback ---

// Fallback runs primary; if it fails while the parent context remains live, it
// runs alternate with the same input. A timeout applied inside primary may
// therefore trigger the fallback, while cancellation of the outer operation
// does not.
func Fallback[I, O any](primary, alternate flow.Node[I, O]) flow.Node[I, O] {
	return flow.NodeFunc[I, O](func(ctx context.Context, in I) (O, error) {
		var out O
		if primary == nil || alternate == nil {
			return out, flow.ErrNilNode
		}
		out, err := primary.Run(ctx, in)
		if err == nil {
			return out, err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return out, ctxErr
		}
		return alternate.Run(ctx, in)
	})
}
