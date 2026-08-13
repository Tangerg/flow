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

	got, err := flow.Map(square, flow.MapConfig{}).Run(t.Context(), []int{1, 2, 3, 4})
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

func TestMap_rejectsANilNodeEvenForEmptyInput(t *testing.T) {
	_, err := flow.Map[int, int](nil, flow.MapConfig{}).Run(t.Context(), nil)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
}

func TestMap_rejectsTypedNilFunctionNodeEvenForEmptyInput(t *testing.T) {
	var node flow.NodeFunc[int, int]
	_, err := flow.Map[int, int](node, flow.MapConfig{}).Run(t.Context(), nil)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
}

func TestMap_rejectsInvalidNestedNodeEvenForEmptyInput(t *testing.T) {
	invalid := flow.Then(
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value, nil
		}),
		flow.Node[int, int](nil),
	)
	_, err := flow.Map(invalid, flow.MapConfig{}).Run(t.Context(), nil)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
}

func TestMap_failFastCancelsSiblings(t *testing.T) {
	boom := errors.New("boom")
	var cancelledSeen atomic.Bool
	siblingsReady := make(chan struct{}, 2)

	node := flow.NodeFunc[int, int](func(ctx context.Context, x int) (int, error) {
		if x == 0 {
			// Do not fail until both siblings are running. Without this barrier,
			// a valid fail-fast implementation may stop before starting either
			// sibling, leaving the test dependent on goroutine scheduling.
			<-siblingsReady
			<-siblingsReady
			return 0, boom
		}
		siblingsReady <- struct{}{}
		select {
		case <-ctx.Done():
			cancelledSeen.Store(true)
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return x, nil
		}
	})

	_, err := flow.Map(node, flow.MapConfig{}).Run(t.Context(), []int{0, 1, 2})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	if !cancelledSeen.Load() {
		t.Fatal("siblings were not cancelled after a failure")
	}
}

func TestMap_boundsConcurrency(t *testing.T) {
	const limit = 3
	var (
		current atomic.Int32
		peak    atomic.Int32
	)
	started := make(chan struct{}, 30)
	release := make(chan struct{})

	node := flow.NodeFunc[int, int](func(ctx context.Context, x int) (int, error) {
		c := current.Add(1)
		for {
			old := peak.Load()
			if c <= old || peak.CompareAndSwap(old, c) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			current.Add(-1)
			return x, nil
		case <-ctx.Done():
			current.Add(-1)
			return 0, ctx.Err()
		}
	})

	in := make([]int, 30)
	finished := make(chan error, 1)
	go func() {
		_, err := flow.Map(node, flow.MapConfig{Concurrency: limit}).Run(t.Context(), in)
		finished <- err
	}()
	for range limit {
		<-started
	}
	gotBeforeRelease := peak.Load()
	close(release)
	if gotBeforeRelease != limit {
		t.Fatalf("observed %d concurrent calls before release; want %d", gotBeforeRelease, limit)
	}
	err := <-finished
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := peak.Load(); got > limit {
		t.Fatalf("observed %d concurrent, want <= %d", got, limit)
	}
}

// A limit of one means each call runs after the last one returned, which no
// waiting test can prove: proving nothing else started requires waiting forever.
// So assert it from inside the calls instead. Nothing below is synchronized, on
// purpose — the ordered result says each call observed every earlier one, and the
// race detector says they needed no synchronization to do it.
func TestMap_concurrencyOneRunsOneCallAtATime(t *testing.T) {
	visits := 0
	node := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
		visits++
		return visits, nil
	})

	got, err := flow.Map(node, flow.MapConfig{Concurrency: 1}).Run(t.Context(), make([]int, 8))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for index, value := range got {
		if value != index+1 {
			t.Fatalf("Map = %v; want call n to observe the n-1 before it", got)
		}
	}
}

// Two elements is the smallest input that can run concurrently, and so the
// smallest one a scheduler can quietly hand to the sequential path instead.
// Checking the results cannot tell the two apart -- both produce [2 3] -- so
// require the calls to overlap: neither returns until both have started, which
// only an unbounded Map can reach.
func TestMap_zeroConcurrencyIsUnbounded(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	node := flow.NodeFunc[int, int](func(ctx context.Context, value int) (int, error) {
		started <- struct{}{}
		select {
		case <-release:
			return value + 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	type result struct {
		values []int
		err    error
	}
	finished := make(chan result, 1)
	go func() {
		values, err := flow.Map(node, flow.MapConfig{}).Run(t.Context(), []int{1, 2})
		finished <- result{values: values, err: err}
	}()
	for range 2 {
		<-started
	}
	close(release)

	got := <-finished
	if got.err != nil || len(got.values) != 2 || got.values[0] != 2 || got.values[1] != 3 {
		t.Fatalf("Map = %v, %v", got.values, got.err)
	}
}

func TestMap_rejectsNegativeConcurrency(t *testing.T) {
	node := flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	if _, err := flow.Map(node, flow.MapConfig{Concurrency: -1}).Run(t.Context(), nil); !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("err = %v; want ErrInvalidConfig", err)
	}
}

func TestMap_cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	node := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })

	_, err := flow.Map(node, flow.MapConfig{}).Run(ctx, []int{1, 2, 3})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestMap_parentCancellationIsNotIndexWrapped(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	node := flow.NodeFunc[int, int](func(ctx context.Context, _ int) (int, error) {
		cancel()
		return 0, ctx.Err()
	})

	_, err := flow.Map(node, flow.MapConfig{Concurrency: 2}).Run(ctx, []int{1, 2})
	var indexErr *flow.IndexError
	if !errors.Is(err, context.Canceled) || errors.As(err, &indexErr) {
		t.Fatalf("err = %v; want unwrapped parent cancellation", err)
	}
}

func TestMap_singleItemReportsCancellationAfterRun(t *testing.T) {
	cause := errors.New("stop map")
	ctx, cancel := context.WithCancelCause(t.Context())
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		cancel(cause)
		return in, nil
	})

	_, err := flow.Map(node, flow.MapConfig{}).Run(ctx, []int{1})
	if !errors.Is(err, cause) {
		t.Fatalf("err = %v; want cancellation cause", err)
	}
}

func TestMap_singleItemHonorsCancellationBeforeRun(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ran := false
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		ran = true
		return in, nil
	})

	_, err := flow.Map(node, flow.MapConfig{}).Run(ctx, []int{1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
	if ran {
		t.Fatal("single item ran under an already cancelled context")
	}
}

func TestMap_singleItemReturnsNodeError(t *testing.T) {
	boom := errors.New("boom")
	node := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		return 0, boom
	})

	_, err := flow.Map(node, flow.MapConfig{}).Run(t.Context(), []int{1})
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 0 || !errors.Is(err, boom) {
		t.Fatalf("err = %v; want IndexError(0, boom)", err)
	}
}

func TestMap_sequentialSuccessAndCancellation(t *testing.T) {
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in + 1, nil
	})
	got, err := flow.Map(node, flow.MapConfig{Concurrency: 1}).Run(
		t.Context(),
		[]int{1, 2},
	)
	if err != nil || len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("sequential Map = %v, %v; want [2 3], nil", got, err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := flow.Map(node, flow.MapConfig{Concurrency: 1}).Run(
		ctx,
		[]int{1, 2},
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled sequential Map error = %v; want context.Canceled", err)
	}
}

func TestMap_sequentialCancellationTakesPrecedenceOverNodeError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	boom := errors.New("boom")
	node := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		cancel()
		return 0, boom
	})

	_, err := flow.Map(node, flow.MapConfig{Concurrency: 1}).Run(ctx, []int{1, 2})
	if !errors.Is(err, context.Canceled) || errors.Is(err, boom) {
		t.Fatalf("err = %v; want only parent cancellation", err)
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

	_, err := flow.Map(node, flow.MapConfig{Concurrency: 1}).Run(t.Context(), []int{1, 2, 3})
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
			return in, nil
		default:
			return in, nil
		}
	})

	_, err := flow.Map(node, flow.MapConfig{Concurrency: 2}).Run(t.Context(), []int{0, 1, 2, 3, 4})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v; want boom", err)
	}
	if got := started.Load(); got != 2 {
		t.Fatalf("started %d nodes after failure; want exactly initial 2", got)
	}
}
