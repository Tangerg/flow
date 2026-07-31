package flow_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/Tangerg/flow"
)

func TestThen(t *testing.T) {
	double := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil })
	str := flow.NodeFunc[int, string](func(_ context.Context, x int) (string, error) { return strconv.Itoa(x), nil })

	pipe := flow.Then(double, str)

	got, err := pipe.Run(t.Context(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "10" {
		t.Fatalf("Run(5) = %q, want %q", got, "10")
	}
}

func TestThen_shortCircuitsOnFirstError(t *testing.T) {
	boom := errors.New("boom")
	first := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })

	secondRan := false
	second := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		secondRan = true
		return x, nil
	})

	_, err := flow.Then(first, second).Run(t.Context(), 1)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	if secondRan {
		t.Fatal("second node ran after first failed")
	}
}

func TestThen_nilNode(t *testing.T) {
	ok := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })

	_, err := flow.Then(flow.Node[int, int](nil), ok).Run(t.Context(), 1)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("error = %v, want ErrNilNode", err)
	}
}

func TestThen_validatesBothNodesBeforeRunningEither(t *testing.T) {
	ran := false
	first := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		ran = true
		return x, nil
	})

	_, err := flow.Then(first, flow.Node[int, int](nil)).Run(t.Context(), 1)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
	if ran {
		t.Fatal("first node ran before the invalid composition was rejected")
	}
}

func TestThen_rejectsTypedNilFunctionNodeBeforeRunningEither(t *testing.T) {
	ran := false
	first := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		ran = true
		return x, nil
	})
	var second flow.NodeFunc[int, string]

	_, err := flow.Then(first, second).Run(t.Context(), 1)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
	if ran {
		t.Fatal("first node ran before the typed nil function node was rejected")
	}
}

type nilSafeNode struct{}

func (*nilSafeNode) Run(_ context.Context, value int) (int, error) {
	return value + 1, nil
}

func TestThen_acceptsNilSafePointerNode(t *testing.T) {
	var first *nilSafeNode
	second := flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
		return value * 2, nil
	})

	got, err := flow.Then[int, int, int](first, second).Run(t.Context(), 2)
	if err != nil || got != 6 {
		t.Fatalf("Run = %d, %v; want 6, nil", got, err)
	}
}
