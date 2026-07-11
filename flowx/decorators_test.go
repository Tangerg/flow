package flowx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/flowx"
)

func TestRetry_succeedsAfterFailures(t *testing.T) {
	boom := errors.New("boom")
	calls := 0
	flaky := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		calls++
		if calls < 3 {
			return 0, boom
		}
		return x, nil
	})

	got, err := flowx.Retry(flaky, flowx.RetryConfig{Attempts: 3}).Run(context.Background(), 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 || calls != 3 {
		t.Fatalf("got %d after %d calls; want 42 after 3", got, calls)
	}
}

func TestRetry_exhausts(t *testing.T) {
	boom := errors.New("boom")
	calls := 0
	always := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
		calls++
		return 0, boom
	})

	_, err := flowx.Retry(always, flowx.RetryConfig{Attempts: 2}).Run(context.Background(), 1)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestRetry_notRetryableStopsEarly(t *testing.T) {
	fatal := errors.New("fatal")
	calls := 0
	node := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
		calls++
		return 0, fatal
	})

	_, err := flowx.Retry(node, flowx.RetryConfig{
		Attempts:  5,
		Retryable: func(error) bool { return false },
	}).Run(context.Background(), 1)
	if !errors.Is(err, fatal) || calls != 1 {
		t.Fatalf("err=%v calls=%d; want fatal after 1 call", err, calls)
	}
}

func TestTimeout(t *testing.T) {
	slow := flow.NodeFunc[int, int](func(ctx context.Context, _ int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return 1, nil
		}
	})

	_, err := flowx.Timeout(slow, 10*time.Millisecond).Run(context.Background(), 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}

func TestFallback(t *testing.T) {
	boom := errors.New("boom")
	primary := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	alt := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })

	got, err := flowx.Fallback(primary, alt).Run(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 11 {
		t.Fatalf("got %d, want 11", got)
	}
}

func TestDecoratorComposition(t *testing.T) {
	calls := 0
	boom := errors.New("boom")
	flaky := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		calls++
		if calls < 2 {
			return 0, boom
		}
		return x * 2, nil
	})

	node := flowx.Timeout(
		flowx.Retry(flaky, flowx.RetryConfig{Attempts: 3}),
		time.Second,
	)

	got, err := node.Run(context.Background(), 21)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

func TestRetry_zeroConfigUsesDefaults(t *testing.T) {
	boom := errors.New("boom")
	calls := 0
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		calls++
		if calls == 1 {
			return 0, boom
		}
		return in, nil
	})

	// The zero RetryConfig means three attempts and the default predicate.
	got, err := flowx.Retry(node, flowx.RetryConfig{}).Run(context.Background(), 7)
	if err != nil || got != 7 || calls != 2 {
		t.Fatalf("Retry = %d, %v after %d calls", got, err, calls)
	}
}

func TestRetry_negativeAttemptsUsesDefault(t *testing.T) {
	calls := 0
	boom := errors.New("boom")
	node := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
		calls++
		return 0, boom
	})

	_, err := flowx.Retry(node, flowx.RetryConfig{Attempts: -1}).Run(context.Background(), 0)
	if !errors.Is(err, boom) || calls != 3 {
		t.Fatalf("Attempts=-1 should default to 3; err=%v calls=%d", err, calls)
	}
}

func TestExponentialBackoffSaturates(t *testing.T) {
	backoff := flowx.ExponentialBackoff(time.Hour)
	if got := backoff(1000); got <= 0 {
		t.Fatalf("overflowed backoff = %v", got)
	}
}

func TestRetry_backoffRespectsContext(t *testing.T) {
	boom := errors.New("boom")
	node := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	_, err := flowx.Retry(node, flowx.RetryConfig{
		Attempts: 3,
		Backoff:  flowx.ConstantBackoff(time.Hour),
	}).Run(ctx, 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v; want DeadlineExceeded", err)
	}
}

func TestTraceAndFallback(t *testing.T) {
	boom := errors.New("boom")
	primary := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	alternate := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil })
	var before, after bool
	node := flowx.Fallback(
		flowx.Trace(primary, "primary", flowx.TraceHooks{
			Before: func(context.Context, string) { before = true },
			After: func(_ context.Context, _ string, _ time.Duration, err error) {
				after = errors.Is(err, boom)
			},
		}),
		alternate,
	)

	got, err := node.Run(context.Background(), 4)
	if err != nil || got != 5 || !before || !after {
		t.Fatalf("Trace/Fallback = %d, %v, before=%v after=%v", got, err, before, after)
	}
}

func TestTimeout_nilNode(t *testing.T) {
	_, err := flowx.Timeout[int, int](nil, time.Second).Run(context.Background(), 0)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
}

func TestFallback_rejectsNilAlternate(t *testing.T) {
	primary := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil })
	_, err := flowx.Fallback[int, int](primary, nil).Run(context.Background(), 1)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
}

func TestRetryAndFallbackPreferParentCancellation(t *testing.T) {
	boom := errors.New("boom")
	alternate := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil })

	t.Run("retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
			cancel()
			return 0, boom
		})
		if _, err := flowx.Retry(node, flowx.RetryConfig{}).Run(ctx, 0); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v; want context.Canceled", err)
		}
	})

	t.Run("fallback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
			cancel()
			return 0, boom
		})
		if _, err := flowx.Fallback(node, alternate).Run(ctx, 0); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v; want context.Canceled", err)
		}
	})
}
