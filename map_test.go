package flow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tangerg/flow"
)

func TestMap(t *testing.T) {
	square := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * x, nil })

	got, err := flow.Map(square).Run(context.Background(), []int{1, 2, 3, 4})
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

	node := flow.NodeFunc[int, int](func(ctx context.Context, x int) (int, error) {
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

	_, err := flow.Map(node).Run(context.Background(), []int{0, 1, 2})
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

	node := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
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
	_, err := flow.Map(node, flow.WithConcurrency(limit)).Run(context.Background(), in)
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

	node := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })

	_, err := flow.Map(node).Run(ctx, []int{1, 2, 3})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestMap_parentCancellationIsNotIndexWrapped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	node := flow.NodeFunc[int, int](func(ctx context.Context, in int) (int, error) {
		cancel()
		return 0, ctx.Err()
	})

	_, err := flow.Map(node, flow.WithConcurrency(2)).Run(ctx, []int{1, 2})
	var indexErr *flow.IndexError
	if !errors.Is(err, context.Canceled) || errors.As(err, &indexErr) {
		t.Fatalf("err = %v; want unwrapped parent cancellation", err)
	}
}

func TestMap_singleItemReportsCancellationAfterRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		cancel()
		return in, nil
	})

	_, err := flow.Map(node).Run(ctx, []int{1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
}

func TestMap_nilOptionIsIgnored(t *testing.T) {
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil })
	got, err := flow.Map(node, nil).Run(context.Background(), []int{1})
	if err != nil || len(got) != 1 || got[0] != 1 {
		t.Fatalf("Map with nil option = %v, %v", got, err)
	}
}

func TestMap_errorIncludesIndex(t *testing.T) {
	boom := errors.New("boom")
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		if in == 2 {
			return 0, boom
		}
		return in, nil
	})

	_, err := flow.Map(node, flow.WithConcurrency(1)).Run(context.Background(), []int{1, 2, 3})
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 1 || !errors.Is(err, boom) {
		t.Fatalf("err = %v; want IndexError{Index:1, Err:boom}", err)
	}
}

func TestMap_boundedFailureStopsScheduling(t *testing.T) {
	boom := errors.New("boom")
	secondStarted := make(chan struct{})
	var started atomic.Int32
	node := flow.NodeFunc[int, int](func(ctx context.Context, in int) (int, error) {
		started.Add(1)
		switch in {
		case 0:
			<-secondStarted
			return 0, boom
		case 1:
			close(secondStarted)
			<-ctx.Done()
			return 0, ctx.Err()
		default:
			return in, nil
		}
	})

	_, err := flow.Map(node, flow.WithConcurrency(2)).Run(context.Background(), []int{0, 1, 2, 3, 4})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v; want boom", err)
	}
	if got := started.Load(); got != 2 {
		t.Fatalf("started %d nodes after failure; want exactly initial 2", got)
	}
}
