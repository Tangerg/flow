package flowx

import (
	"context"
	"errors"
	"time"

	"github.com/Tangerg/flow"
)

// --- Retry ---

type retryConfig struct {
	attempts  int
	backoff   func(attempt int) time.Duration
	retryable func(error) bool
}

// RetryOption configures [Retry]. Options are created by this package.
type RetryOption interface {
	applyRetry(*retryConfig)
}

type retryOptionFunc func(*retryConfig)

func (f retryOptionFunc) applyRetry(c *retryConfig) { f(c) }

// WithAttempts sets the maximum number of attempts. Non-positive values leave
// the default unchanged.
func WithAttempts(n int) RetryOption {
	return retryOptionFunc(func(c *retryConfig) {
		if n > 0 {
			c.attempts = n
		}
	})
}

// WithBackoff sets the delay before attempt N+1 (attempt is 1-based). Use
// [ConstantBackoff] or [ExponentialBackoff] for the common cases.
func WithBackoff(fn func(attempt int) time.Duration) RetryOption {
	return retryOptionFunc(func(c *retryConfig) { c.backoff = fn })
}

// WithRetryable sets the predicate deciding whether an error should be retried.
// The default retries any error that is not a context cancellation/deadline.
func WithRetryable(fn func(error) bool) RetryOption {
	return retryOptionFunc(func(c *retryConfig) {
		if fn != nil {
			c.retryable = fn
		}
	})
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
func Retry[I, O any](node flow.Node[I, O], opts ...RetryOption) flow.Node[I, O] {
	cfg := retryConfig{attempts: 3, retryable: defaultRetryable}
	for _, o := range opts {
		if o != nil {
			o.applyRetry(&cfg)
		}
	}
	return flow.NodeFunc[I, O](func(ctx context.Context, in I) (O, error) {
		var out O
		if node == nil {
			return out, flow.ErrNilNode
		}
		var err error
		for attempt := 1; attempt <= cfg.attempts; attempt++ {
			if err := ctx.Err(); err != nil {
				return out, err
			}
			if attempt > 1 && cfg.backoff != nil {
				if d := cfg.backoff(attempt - 1); d > 0 {
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
			if !cfg.retryable(err) {
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

// --- Wrap (fluent, type-preserving) ---

// Builder applies type-preserving decorators to a node in a readable, top-down
// order. The last decorator applied is the outermost at run time.
//
//	node := flowx.Wrap(base).Retry(flowx.WithAttempts(3)).Timeout(2 * time.Second)
type Builder[I, O any] struct{ node flow.Node[I, O] }

var _ flow.Node[any, any] = Builder[any, any]{}

// Wrap starts a decorator chain around node.
func Wrap[I, O any](node flow.Node[I, O]) Builder[I, O] { return Builder[I, O]{node} }

// Retry wraps the current node with [Retry].
func (b Builder[I, O]) Retry(opts ...RetryOption) Builder[I, O] {
	return Builder[I, O]{Retry(b.node, opts...)}
}

// Timeout wraps the current node with [Timeout].
func (b Builder[I, O]) Timeout(d time.Duration) Builder[I, O] {
	return Builder[I, O]{Timeout(b.node, d)}
}

// Trace wraps the current node with [Trace].
func (b Builder[I, O]) Trace(name string, hooks TraceHooks) Builder[I, O] {
	return Builder[I, O]{Trace(b.node, name, hooks)}
}

// Fallback wraps the current node with [Fallback].
func (b Builder[I, O]) Fallback(alt flow.Node[I, O]) Builder[I, O] {
	return Builder[I, O]{Fallback(b.node, alt)}
}

// Run executes the decorated node. The zero Builder and Wrap(nil) return
// [flow.ErrNilNode].
func (b Builder[I, O]) Run(ctx context.Context, in I) (O, error) {
	if b.node == nil {
		var zero O
		return zero, flow.ErrNilNode
	}
	return b.node.Run(ctx, in)
}
