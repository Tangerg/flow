package core_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow/core"
)

func TestSwitch_routes(t *testing.T) {
	cases := map[string]core.Node[int, string]{
		"even": core.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "even", nil }),
		"odd":  core.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "odd", nil }),
	}
	resolve := core.NodeFunc[int, string](func(_ context.Context, n int) (string, error) {
		if n%2 == 0 {
			return "even", nil
		}
		return "odd", nil
	})

	node := core.Switch(resolve, cases)

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
	cases := map[string]core.Node[int, int]{
		"a": core.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	}
	resolve := core.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "missing", nil })

	_, err := core.Switch(resolve, cases).Run(context.Background(), 1)
	if !errors.Is(err, core.ErrNoCase) {
		t.Fatalf("error = %v, want ErrNoCase", err)
	}
}

func TestSwitch_resolveError(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]core.Node[int, int]{}
	resolve := core.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "", boom })

	_, err := core.Switch(resolve, cases).Run(context.Background(), 1)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
}

func TestSwitch_nilResolver(t *testing.T) {
	_, err := core.Switch[string, int, int](nil, nil).Run(context.Background(), 1)
	if !errors.Is(err, core.ErrNilNode) {
		t.Fatalf("error = %v, want ErrNilNode", err)
	}
}

func TestSwitch_composedResolver(t *testing.T) {
	// The router itself is a composed node: double, then bucket by size.
	router := core.Then(
		core.NodeFunc[int, int](func(_ context.Context, n int) (int, error) { return n * 2, nil }),
		core.NodeFunc[int, string](func(_ context.Context, n int) (string, error) {
			if n >= 10 {
				return "big", nil
			}
			return "small", nil
		}),
	)
	cases := map[string]core.Node[int, string]{
		"big":   core.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "BIG", nil }),
		"small": core.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "small", nil }),
	}

	got, err := core.Switch(router, cases).Run(context.Background(), 6) // 6*2=12 >= 10 -> "big"
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "BIG" {
		t.Fatalf("Run(6) = %q, want %q", got, "BIG")
	}
}
