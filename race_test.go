package flow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tangerg/flow"
)

func TestRace_firstWins(t *testing.T) {
	slow := flow.NodeFunc[int, int](func(ctx context.Context, x int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return x, nil
		}
	})
	fast := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 100, nil })

	got, err := flow.Race(slow, fast).Run(t.Context(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 500 {
		t.Fatalf("got %d, want 500 (fast should win)", got)
	}
}

func TestRace_waitsForLosingNodesToStop(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	loser := flow.NodeFunc[int, int](func(ctx context.Context, _ int) (int, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return 0, ctx.Err()
	})
	winner := flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
		<-started
		return value, nil
	})

	if got, err := flow.Race(loser, winner).Run(t.Context(), 7); err != nil || got != 7 {
		t.Fatalf("Race = %d, %v; want 7, nil", got, err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("Race returned while a losing node was still running")
	}
}

func TestRace_parentCancellationWaitsForNodesToStop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	stopped := make(chan struct{})
	node := flow.NodeFunc[int, int](func(ctx context.Context, _ int) (int, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return 0, ctx.Err()
	})

	go func() {
		<-started
		cancel()
	}()
	if _, err := flow.Race(node).Run(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("Race returned from cancellation while a node was still running")
	}
}

func TestRace_allFail(t *testing.T) {
	e1, e2 := errors.New("e1"), errors.New("e2")
	n1 := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, e1 })
	n2 := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, e2 })

	_, err := flow.Race(n1, n2).Run(t.Context(), 1)
	if !errors.Is(err, e1) || !errors.Is(err, e2) {
		t.Fatalf("err = %v, want joined e1 and e2", err)
	}
}

func TestRace_allFailErrorOrderIsStable(t *testing.T) {
	e1, e2 := errors.New("first"), errors.New("second")
	n1 := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
		time.Sleep(time.Millisecond)
		return 0, e1
	})
	n2 := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, e2 })

	_, err := flow.Race(n1, n2).Run(t.Context(), 0)
	if err == nil || err.Error() != "flow: index 0: first\nflow: index 1: second" {
		t.Fatalf("joined error = %q; want input order", err)
	}
}

func TestRace_noNodes(t *testing.T) {
	_, err := flow.Race[int, int]().Run(t.Context(), 0)
	if !errors.Is(err, flow.ErrNoNodes) {
		t.Fatalf("err = %v; want ErrNoNodes", err)
	}
}

func TestRace_cancelledBeforeRun(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil })

	_, err := flow.Race(node).Run(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
}

func TestRace_rejectsNilBeforeRunningAnyNode(t *testing.T) {
	var ran atomic.Bool
	ok := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		ran.Store(true)
		return x, nil
	})
	_, err := flow.Race[int, int](nil, ok).Run(t.Context(), 7)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 0 {
		t.Fatalf("err = %v; want IndexError at index 0", err)
	}
	if ran.Load() {
		t.Fatal("valid sibling ran before the invalid composition was rejected")
	}
}
