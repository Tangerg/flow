package core_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow/core"
)

func TestLoop_untilDone(t *testing.T) {
	// Double until >= 100.
	node := core.Loop(func(_ context.Context, _ int, x int) (int, bool, error) {
		x *= 2
		return x, x >= 100, nil
	})

	got, err := node.Run(context.Background(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 128 {
		t.Fatalf("Run(1) = %d, want 128", got)
	}
}

func TestLoop_maxIterations(t *testing.T) {
	node := core.Loop(
		func(_ context.Context, _ int, x int) (int, bool, error) { return x + 1, false, nil },
		core.WithMaxIterations(5),
	)

	got, err := node.Run(context.Background(), 0)
	if !errors.Is(err, core.ErrMaxIterations) {
		t.Fatalf("error = %v, want ErrMaxIterations", err)
	}
	if got != 5 {
		t.Fatalf("value at cap = %d, want 5", got)
	}
}

func TestLoop_errorReturnsPreviousValue(t *testing.T) {
	boom := errors.New("boom")
	node := core.Loop(func(_ context.Context, iter int, x int) (int, bool, error) {
		if iter == 2 {
			return 999, false, boom
		}
		return x + 1, false, nil
	})

	got, err := node.Run(context.Background(), 0)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	if got != 2 {
		t.Fatalf("value on error = %d, want 2 (value before failing iteration)", got)
	}
}

func TestLoop_respectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	node := core.Loop(func(_ context.Context, _ int, x int) (int, bool, error) { return x + 1, false, nil })

	_, err := node.Run(ctx, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestLoop_nilBody(t *testing.T) {
	_, err := core.Loop[int](nil).Run(context.Background(), 0)
	if !errors.Is(err, core.ErrNilFunc) {
		t.Fatalf("error = %v, want ErrNilFunc", err)
	}
}
