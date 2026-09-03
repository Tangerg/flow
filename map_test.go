package flow_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Tangerg/flow"
)

// concurrentPanicChild marks the subprocess that must die of its own panic,
// which is why it does not use withBoundedStack: that helper asserts the child
// succeeded, and this one asserts the opposite.
const concurrentPanicChild = "FLOW_CONCURRENT_PANIC_TEST"

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

// TestMap_boundsConcurrency pins the bound every composite in this module
// forwards to: Iteration, Parallel, and flowx.FanOut each hand their limit here
// rather than counting their own admitted work, so this is the one place the rule
// lives. The graph scheduler is the exception and keeps its own count.
//
// The contract is a negative claim -- no more than the limit runs at once -- and
// a waiting test cannot establish one. Waiting for `limit` calls to report proves
// only that they started; the call that broke the bound may not have reached its
// first statement yet, so the check passes or fails on timing. Under
// [testing/synctest] the question is decidable: at quiescence every admitted call
// is durably parked on the release channel, so the count is the whole of what was
// admitted, and no peak needs tracking.
func TestMap_boundsConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			limit    = 3
			elements = 30
		)
		var started atomic.Int32
		release := make(chan struct{})
		node := flow.NodeFunc[int, int](func(ctx context.Context, value int) (int, error) {
			started.Add(1)
			select {
			case <-release:
				return value, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		})

		finished := make(chan error, 1)
		go func() {
			_, err := flow.Map(node, flow.MapConfig{Concurrency: limit}).
				Run(t.Context(), make([]int, elements))
			finished <- err
		}()

		synctest.Wait()
		if got := started.Load(); got != limit {
			t.Fatalf("%d concurrent calls; want the configured limit of %d", got, limit)
		}

		close(release)
		if err := <-finished; err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := started.Load(); got != elements {
			t.Fatalf("%d calls ran in total; want all %d", got, elements)
		}
	})
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

// TestPanicReachesTheCallerOnlyFromItsOwnGoroutine pins the one difference a
// caller can observe between the two paths fanOut dispatches to, and it is not
// the one the fast path exists for. A single element runs on the calling
// goroutine, so a panic in it unwinds through Run and a caller's recover sees
// it; two elements run under an errgroup, which deliberately does not propagate
// a panic -- doing so would delay it, reduce its stack to a value, and risk
// hiding it in a deadlock -- so the process ends with the node's own stack.
//
// A caller who tests recovery against a one-element fan-out and ships a
// two-element one gets a crash, which is why this is written down rather than
// left to the shape of the input.
func TestPanicReachesTheCallerOnlyFromItsOwnGoroutine(t *testing.T) {
	const panicValue = "node panic"
	panicking := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		panic(panicValue)
	})

	if os.Getenv(concurrentPanicChild) == t.Name() {
		defer func() {
			if recovered := recover(); recovered != nil {
				// Reaching the caller is what this child exists to disprove.
				panic("a concurrent child's panic was propagated: " + recovered.(string))
			}
		}()
		_, _ = flow.Map(panicking, flow.MapConfig{}).Run(context.Background(), []int{1, 2})
		return
	}

	recovered := func() (value any) {
		defer func() { value = recover() }()
		_, _ = flow.Map(panicking, flow.MapConfig{}).Run(t.Context(), []int{1})
		return nil
	}()
	if recovered != panicValue {
		t.Fatalf("one element: recovered = %v; want %q", recovered, panicValue)
	}

	//nolint:gosec // Re-executes this test binary with a quoted testing-owned name.
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
	command.Env = append(os.Environ(), concurrentPanicChild+"="+t.Name())
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("two elements: the child exited cleanly; want the panic to end it\n%s", output)
	}
	if !strings.Contains(string(output), panicValue) {
		t.Fatalf("two elements: the panic did not reach the crash output\n%s", output)
	}
	if strings.Contains(string(output), "was propagated") {
		t.Fatalf("two elements: the panic reached the caller's recover\n%s", output)
	}
}

// TestMap_waitsForEveryElementToReturn strengthens what
// TestMap_failFastCancelsSiblings can show. That test reads a flag its siblings
// set before returning, so it proves they observed the cancellation and cannot
// prove Map waited for them: if Map abandoned them, the flag would merely be
// read too early. Counting live calls and reading the count at the moment Map
// returns is exact in both directions. Race states the same promise and is
// already held to it by TestRace_waitsForLosingNodesToStop.
func TestMap_waitsForEveryElementToReturn(t *testing.T) {
	boom := errors.New("boom")
	var live atomic.Int64
	// Element 0 fails, which cancels the rest; every other element holds the
	// input until it observes that, so all of them are in flight when the
	// failure lands.
	element := flow.NodeFunc[int, int](func(ctx context.Context, value int) (int, error) {
		if value == 0 {
			return 0, boom
		}
		live.Add(1)
		defer live.Add(-1)
		<-ctx.Done()
		return 0, context.Cause(ctx)
	})

	_, err := flow.Map(element, flow.MapConfig{}).Run(t.Context(), []int{0, 1, 2, 3})
	if !errors.Is(err, boom) {
		t.Fatalf("Map error = %v; want the element failure", err)
	}
	if running := live.Load(); running != 0 {
		t.Fatalf("%d elements were still running when Map returned", running)
	}
}

// TestMap_closesTheContextItDerived is the Map half of what
// TestRace_closesTheContextItDerived pins for Race: a boundary ends the context
// it derived before returning. Map derives one through errgroup, which cancels
// it on Wait, and nothing here had checked that.
//
// The second subtest is the reason the doc no longer calls the panic the one
// difference between the two paths. A single element runs on the caller's
// goroutine and receives the caller's own context, so it is still live after Map
// returns — observable only by a node that keeps its context past its own
// return, which is exactly what the concurrent path forbids.
func TestMap_closesTheContextItDerived(t *testing.T) {
	t.Run("concurrent", func(t *testing.T) {
		seen := make(chan context.Context, 2)
		node := flow.NodeFunc[int, int](func(ctx context.Context, value int) (int, error) {
			seen <- ctx
			return value, nil
		})
		if _, err := flow.Map(node, flow.MapConfig{}).Run(t.Context(), []int{1, 2}); err != nil {
			t.Fatalf("Map: %v", err)
		}
		close(seen)
		for ctx := range seen {
			select {
			case <-ctx.Done():
				if !errors.Is(context.Cause(ctx), context.Canceled) {
					t.Fatalf("derived context cause = %v; want context.Canceled", context.Cause(ctx))
				}
			default:
				t.Fatal("an element's context remains live after Map returned")
			}
		}
	})

	t.Run("single element", func(t *testing.T) {
		seen := make(chan context.Context, 1)
		node := flow.NodeFunc[int, int](func(ctx context.Context, value int) (int, error) {
			seen <- ctx
			return value, nil
		})
		parent := t.Context()
		if _, err := flow.Map(node, flow.MapConfig{}).Run(parent, []int{1}); err != nil {
			t.Fatalf("Map: %v", err)
		}
		select {
		case <-(<-seen).Done():
			t.Fatal("the sequential path cancelled a context Map did not derive")
		default:
		}
	})
}
