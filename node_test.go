package flow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
)

func TestFunc_Run(t *testing.T) {
	double := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		return x * 2, nil
	})

	got, err := double.Run(t.Context(), 21)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 {
		t.Fatalf("Run(21) = %d, want 42", got)
	}
}

func TestFunc_Run_propagatesError(t *testing.T) {
	sentinel := errors.New("boom")
	fail := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
		return 0, sentinel
	})

	_, err := fail.Run(t.Context(), 1)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Run error = %v, want %v", err, sentinel)
	}
}

func TestFunc_Run_nil(t *testing.T) {
	var f flow.NodeFunc[int, int]

	_, err := f.Run(t.Context(), 1)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("Run error = %v, want ErrNilNode", err)
	}
}

func TestFunc_Run_passesContext(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(t.Context(), ctxKey{}, "v")

	read := flow.NodeFunc[struct{}, string](func(ctx context.Context, _ struct{}) (string, error) {
		s, _ := ctx.Value(ctxKey{}).(string)
		return s, nil
	})

	got, err := read.Run(ctx, struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "v" {
		t.Fatalf("context value = %q, want %q", got, "v")
	}
}
