package flowx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/flowx"
)

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

func TestFallback_primarySuccessSkipsAlternate(t *testing.T) {
	altRan := false
	primary := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })
	alt := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { altRan = true; return 0, nil })

	got, err := flowx.Fallback(primary, alt).Run(context.Background(), 7)
	if err != nil || got != 7 || altRan {
		t.Fatalf("got %d, err %v, altRan %v; want 7, nil, false", got, err, altRan)
	}
}

func TestFallback_rejectsNilNodes(t *testing.T) {
	ok := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })
	if _, err := flowx.Fallback[int, int](nil, ok).Run(context.Background(), 1); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil primary err = %v, want ErrNilNode", err)
	}
	if _, err := flowx.Fallback[int, int](ok, nil).Run(context.Background(), 1); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil alternate err = %v, want ErrNilNode", err)
	}
}

func TestFallback_prefersParentCancellation(t *testing.T) {
	boom := errors.New("boom")
	primary := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	altRan := false
	alt := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { altRan = true; return 0, nil })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := flowx.Fallback(primary, alt).Run(ctx, 0)
	if !errors.Is(err, context.Canceled) || altRan {
		t.Fatalf("err = %v, altRan = %v; want context.Canceled, false", err, altRan)
	}
}
