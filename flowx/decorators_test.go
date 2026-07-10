package flowx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/flowx"
)

func TestRetry_succeedsAfterFailures(t *testing.T) {
	boom := errors.New("boom")
	calls := 0
	flaky := core.Func[int, int](func(_ context.Context, x int) (int, error) {
		calls++
		if calls < 3 {
			return 0, boom
		}
		return x, nil
	})

	got, err := flowx.Retry(flaky, flowx.WithAttempts(3)).Run(context.Background(), 42)
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
	always := core.Func[int, int](func(_ context.Context, _ int) (int, error) {
		calls++
		return 0, boom
	})

	_, err := flowx.Retry(always, flowx.WithAttempts(2)).Run(context.Background(), 1)
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
	node := core.Func[int, int](func(_ context.Context, _ int) (int, error) {
		calls++
		return 0, fatal
	})

	_, err := flowx.Retry(node,
		flowx.WithAttempts(5),
		flowx.WithRetryable(func(error) bool { return false }),
	).Run(context.Background(), 1)
	if !errors.Is(err, fatal) || calls != 1 {
		t.Fatalf("err=%v calls=%d; want fatal after 1 call", err, calls)
	}
}

func TestTimeout(t *testing.T) {
	slow := core.Func[int, int](func(ctx context.Context, _ int) (int, error) {
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
	primary := core.Func[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	alt := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })

	got, err := flowx.Fallback(primary, alt).Run(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 11 {
		t.Fatalf("got %d, want 11", got)
	}
}

func TestWrap_fluent(t *testing.T) {
	calls := 0
	boom := errors.New("boom")
	flaky := core.Func[int, int](func(_ context.Context, x int) (int, error) {
		calls++
		if calls < 2 {
			return 0, boom
		}
		return x * 2, nil
	})

	node := flowx.Wrap(flaky).
		Retry(flowx.WithAttempts(3)).
		Timeout(time.Second).
		Node()

	got, err := node.Run(context.Background(), 21)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}
