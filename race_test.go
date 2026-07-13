package flow_test

import (
	"context"
	"errors"
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

	got, err := flow.Race(slow, fast).Run(context.Background(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 500 {
		t.Fatalf("got %d, want 500 (fast should win)", got)
	}
}

func TestRace_allFail(t *testing.T) {
	e1, e2 := errors.New("e1"), errors.New("e2")
	n1 := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, e1 })
	n2 := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, e2 })

	_, err := flow.Race(n1, n2).Run(context.Background(), 1)
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

	_, err := flow.Race(n1, n2).Run(context.Background(), 0)
	if err == nil || err.Error() != "flow: index 0: first\nflow: index 1: second" {
		t.Fatalf("joined error = %q; want input order", err)
	}
}

func TestRace_noNodes(t *testing.T) {
	_, err := flow.Race[int, int]().Run(context.Background(), 0)
	if !errors.Is(err, flow.ErrNoNodes) {
		t.Fatalf("err = %v; want ErrNoNodes", err)
	}
}

func TestRace_cancelledBeforeRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil })

	_, err := flow.Race(node).Run(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
}

func TestRace_nilNodeFails(t *testing.T) {
	// A nil node yields ErrNilNode; a non-nil sibling can still win.
	ok := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })
	got, err := flow.Race[int, int](nil, ok).Run(context.Background(), 7)
	if err != nil || got != 7 {
		t.Fatalf("Race(nil, ok) = %d, %v; want 7, nil", got, err)
	}
}
