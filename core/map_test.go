package core_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tangerg/flow/core"
)

func TestMap(t *testing.T) {
	square := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x * x, nil })

	got, err := core.Map(square).Run(context.Background(), []int{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{1, 4, 9, 16}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestMap_failFastCancelsSiblings(t *testing.T) {
	boom := errors.New("boom")
	var cancelledSeen atomic.Bool

	node := core.Func[int, int](func(ctx context.Context, x int) (int, error) {
		if x == 0 {
			return 0, boom
		}
		select {
		case <-ctx.Done():
			cancelledSeen.Store(true)
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return x, nil
		}
	})

	_, err := core.Map(node).Run(context.Background(), []int{0, 1, 2})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	if !cancelledSeen.Load() {
		t.Fatal("siblings were not cancelled after a failure")
	}
}

func TestWithConcurrency_bounds(t *testing.T) {
	const limit = 3
	var (
		current atomic.Int32
		max     atomic.Int32
	)

	node := core.Func[int, int](func(_ context.Context, x int) (int, error) {
		c := current.Add(1)
		for {
			old := max.Load()
			if c <= old || max.CompareAndSwap(old, c) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		current.Add(-1)
		return x, nil
	})

	in := make([]int, 30)
	_, err := core.Map(node, core.WithConcurrency(limit)).Run(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := max.Load(); got > limit {
		t.Fatalf("observed %d concurrent, want <= %d", got, limit)
	}
}

func TestMap_cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	node := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x, nil })

	_, err := core.Map(node).Run(ctx, []int{1, 2, 3})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
