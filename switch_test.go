package flow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
)

func TestSwitch_routes(t *testing.T) {
	cases := map[string]flow.Node[int, string]{
		"even": flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "even", nil }),
		"odd":  flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "odd", nil }),
	}
	resolve := flow.NodeFunc[int, string](func(_ context.Context, n int) (string, error) {
		if n%2 == 0 {
			return "even", nil
		}
		return "odd", nil
	})

	node := flow.Switch(resolve, cases)

	for in, want := range map[int]string{2: "even", 3: "odd"} {
		got, err := node.Run(context.Background(), in)
		if err != nil {
			t.Fatalf("Run(%d) error: %v", in, err)
		}
		if got != want {
			t.Fatalf("Run(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestSwitch_noCase(t *testing.T) {
	cases := map[string]flow.Node[int, int]{
		"a": flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	}
	resolve := flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "missing", nil })

	_, err := flow.Switch(resolve, cases).Run(context.Background(), 1)
	if !errors.Is(err, flow.ErrNoCase) {
		t.Fatalf("error = %v, want ErrNoCase", err)
	}
}

func TestSwitch_resolveError(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]flow.Node[int, int]{}
	resolve := flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "", boom })

	_, err := flow.Switch(resolve, cases).Run(context.Background(), 1)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
}

func TestSwitch_nilResolver(t *testing.T) {
	_, err := flow.Switch[string, int, int](nil, nil).Run(context.Background(), 1)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("error = %v, want ErrNilNode", err)
	}
}

func TestSwitch_composedResolver(t *testing.T) {
	// The router itself is a composed node: double, then bucket by size.
	router := flow.Then(
		flow.NodeFunc[int, int](func(_ context.Context, n int) (int, error) { return n * 2, nil }),
		flow.NodeFunc[int, string](func(_ context.Context, n int) (string, error) {
			if n >= 10 {
				return "big", nil
			}
			return "small", nil
		}),
	)
	cases := map[string]flow.Node[int, string]{
		"big":   flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "BIG", nil }),
		"small": flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "small", nil }),
	}

	got, err := flow.Switch(router, cases).Run(context.Background(), 6) // 6*2=12 >= 10 -> "big"
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "BIG" {
		t.Fatalf("Run(6) = %q, want %q", got, "BIG")
	}
}
