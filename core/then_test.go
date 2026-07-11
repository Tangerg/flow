package core_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/Tangerg/flow/core"
)

func TestThen(t *testing.T) {
	double := core.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil })
	str := core.NodeFunc[int, string](func(_ context.Context, x int) (string, error) { return strconv.Itoa(x), nil })

	pipe := core.Then(double, str)

	got, err := pipe.Run(context.Background(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "10" {
		t.Fatalf("Run(5) = %q, want %q", got, "10")
	}
}

func TestThen_shortCircuitsOnFirstError(t *testing.T) {
	boom := errors.New("boom")
	first := core.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })

	secondRan := false
	second := core.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		secondRan = true
		return x, nil
	})

	_, err := core.Then(first, second).Run(context.Background(), 1)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	if secondRan {
		t.Fatal("second node ran after first failed")
	}
}

func TestThen_nilNode(t *testing.T) {
	ok := core.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })

	_, err := core.Then(core.Node[int, int](nil), ok).Run(context.Background(), 1)
	if !errors.Is(err, core.ErrNilNode) {
		t.Fatalf("error = %v, want ErrNilNode", err)
	}
}
