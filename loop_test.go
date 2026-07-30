package flow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
)

func TestLoop_untilDone(t *testing.T) {
	// Double until >= 100.
	node := flow.Loop(func(_ context.Context, _ int, x int) (int, bool, error) {
		x *= 2
		return x, x >= 100, nil
	}, flow.LoopConfig{})

	got, err := node.Run(t.Context(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 128 {
		t.Fatalf("Run(1) = %d, want 128", got)
	}
}

func TestLoop_maxIterations(t *testing.T) {
	node := flow.Loop(
		func(_ context.Context, _ int, x int) (int, bool, error) { return x + 1, false, nil },
		flow.LoopConfig{MaxIterations: 5},
	)

	got, err := node.Run(t.Context(), 0)
	if !errors.Is(err, flow.ErrMaxIterations) {
		t.Fatalf("error = %v, want ErrMaxIterations", err)
	}
	if got != 5 {
		t.Fatalf("value at cap = %d, want 5", got)
	}
}

func TestLoop_zeroLimitUsesDefault(t *testing.T) {
	node := flow.Loop(func(_ context.Context, _ int, value int) (int, bool, error) {
		return value + 1, false, nil
	}, flow.LoopConfig{})
	got, err := node.Run(t.Context(), 0)
	if !errors.Is(err, flow.ErrMaxIterations) || got != flow.DefaultMaxIterations {
		t.Fatalf("Loop = %d, %v", got, err)
	}
}

func TestLoop_rejectsNegativeLimit(t *testing.T) {
	node := flow.Loop(func(_ context.Context, _ int, value int) (int, bool, error) {
		return value + 1, false, nil
	}, flow.LoopConfig{MaxIterations: -1})
	if _, err := node.Run(t.Context(), 0); !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("err = %v; want ErrInvalidConfig", err)
	}
}

func TestLoop_errorReturnsPreviousValue(t *testing.T) {
	boom := errors.New("boom")
	node := flow.Loop(func(_ context.Context, iter int, x int) (int, bool, error) {
		if iter == 2 {
			return 999, false, boom
		}
		return x + 1, false, nil
	}, flow.LoopConfig{})

	got, err := node.Run(t.Context(), 0)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	if got != 2 {
		t.Fatalf("value on error = %d, want 2 (value before failing iteration)", got)
	}
}

func TestLoop_respectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	node := flow.Loop(func(_ context.Context, _ int, x int) (int, bool, error) { return x + 1, false, nil }, flow.LoopConfig{})

	_, err := node.Run(ctx, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestLoop_nilBody(t *testing.T) {
	_, err := flow.Loop[int](nil, flow.LoopConfig{}).Run(t.Context(), 0)
	if !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("error = %v, want ErrNilFunc", err)
	}
}
