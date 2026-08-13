package flowx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/flowx"
	"github.com/Tangerg/flow/internal/ctxtest"
)

func TestFanOut(t *testing.T) {
	nodes := []flow.Node[int, int]{
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 2, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 3, nil }),
	}
	got, err := flowx.FanOut(nodes, flow.MapConfig{}).Run(t.Context(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{11, 12, 13}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestFanOut_boundsConcurrency(t *testing.T) {
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	node := flow.NodeFunc[int, int](func(ctx context.Context, in int) (int, error) {
		started <- struct{}{}
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	done := make(chan error, 1)
	go func() {
		_, err := flowx.FanOut([]flow.Node[int, int]{node, node, node, node}, flow.MapConfig{Concurrency: 2}).Run(t.Context(), 1)
		done <- err
	}()

	<-started
	<-started
	select {
	case <-started:
		t.Fatal("more than two nodes started before a slot was released")
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestFanOut_failFast(t *testing.T) {
	boom := errors.New("boom")
	nodes := []flow.Node[int, int]{
		flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }),
	}
	if _, err := flowx.FanOut(nodes, flow.MapConfig{}).Run(t.Context(), 1); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

func TestFanOut_validatesEveryNodeBeforeRunning(t *testing.T) {
	ran := false
	node := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		ran = true
		return 0, nil
	})
	_, err := flowx.FanOut(
		[]flow.Node[int, int]{node, nil},
		flow.MapConfig{},
	).Run(t.Context(), 0)
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 1 || !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want IndexError(1, ErrNilNode)", err)
	}
	if ran {
		t.Fatal("a node ran before the invalid fan-out was rejected")
	}
}

func TestFanOut_rejectsTypedNilFunctionBeforeRunning(t *testing.T) {
	ran := false
	node := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		ran = true
		return 0, nil
	})
	var invalid flow.NodeFunc[int, int]

	_, err := flowx.FanOut(
		[]flow.Node[int, int]{node, invalid},
		flow.MapConfig{},
	).Run(t.Context(), 0)
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 1 ||
		!errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want IndexError(1, ErrNilNode)", err)
	}
	if ran {
		t.Fatal("a node ran before the typed nil function node was rejected")
	}
}

func TestFanOut_clonesNodes(t *testing.T) {
	nodes := []flow.Node[int, int]{
		flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil }),
	}
	fan := flowx.FanOut(nodes, flow.MapConfig{})
	nodes[0] = flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 100, nil })

	got, err := fan.Run(t.Context(), 1)
	if err != nil || len(got) != 1 || got[0] != 2 {
		t.Fatalf("FanOut after source mutation = %v, %v", got, err)
	}
}

func TestCombine(t *testing.T) {
	length := flow.NodeFunc[string, int](func(_ context.Context, s string) (int, error) { return len(s), nil })
	upper := flow.NodeFunc[string, string](func(_ context.Context, s string) (string, error) { return s + "!", nil })

	node := flowx.Combine(length, upper, func(_ context.Context, _ int, s string) (string, error) {
		return s, nil
	})
	got, err := node.Run(t.Context(), "hi")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "hi!" {
		t.Fatalf("got %q, want %q", got, "hi!")
	}
}

func TestCombine_rejectsNilInputs(t *testing.T) {
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil })
	if _, err := flowx.Combine[int, int, int, int](node, node, nil).Run(t.Context(), 1); !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("nil merge err = %v", err)
	}
	merge := func(_ context.Context, a, b int) (int, error) { return a + b, nil }
	if _, err := flowx.Combine[int, int, int, int](nil, node, merge).Run(t.Context(), 1); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil node err = %v", err)
	}
}

func TestCombine_validationPrecedesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(errors.New("caller stopped"))
	node := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		t.Fatal("invalid Combine ran a child")
		return 0, nil
	})
	merge := func(context.Context, int, int) (int, error) { return 0, nil }

	tests := []struct {
		name string
		node flow.Node[int, int]
		want error
	}{
		{
			name: "nil merge",
			node: flowx.Combine[int, int, int, int](node, node, nil),
			want: flow.ErrNilFunc,
		},
		{
			name: "nil node",
			node: flowx.Combine[int, int, int, int](node, nil, merge),
			want: flow.ErrNilNode,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.node.Run(ctx, 0); !errors.Is(err, test.want) {
				t.Fatalf("Run error = %v; want %v", err, test.want)
			}
		})
	}
}

func TestCombine_validatesBothNodesBeforeRunning(t *testing.T) {
	ran := false
	node := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		ran = true
		return 0, nil
	})
	_, err := flowx.Combine(
		node,
		flow.Node[int, string](nil),
		func(context.Context, int, string) (int, error) { return 0, nil },
	).Run(t.Context(), 0)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
	if ran {
		t.Fatal("a node ran before the invalid combine was rejected")
	}
}

func TestCombine_rejectsTypedNilFunctionBeforeRunning(t *testing.T) {
	ran := false
	node := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		ran = true
		return 0, nil
	})
	var invalid flow.NodeFunc[int, string]

	_, err := flowx.Combine(
		node,
		invalid,
		func(context.Context, int, string) (int, error) { return 0, nil },
	).Run(t.Context(), 0)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
	if ran {
		t.Fatal("a node ran before the typed nil function node was rejected")
	}
}

func TestDerivedCompositesExposeValidation(t *testing.T) {
	valid := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		return 1, nil
	})
	invalid := flow.Then(valid, flow.NodeFunc[int, int](nil))
	merge := func(context.Context, int, int) (int, error) { return 0, nil }
	tests := map[string]flow.Node[int, int]{
		"combine":  flowx.Combine(invalid, valid, merge),
		"fallback": flowx.Fallback(valid, invalid),
	}
	for name, node := range tests {
		t.Run(name, func(t *testing.T) {
			if err := flow.Validate(node); !errors.Is(err, flow.ErrNilNode) {
				t.Fatalf("Validate error = %v; want ErrNilNode", err)
			}
		})
	}

	fanOut := flowx.FanOut(
		[]flow.Node[int, int]{valid, invalid},
		flow.MapConfig{},
	)
	var indexErr *flow.IndexError
	if err := flow.Validate(fanOut); !errors.As(err, &indexErr) ||
		indexErr.Index != 1 || !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("Validate FanOut error = %v; want index 1 ErrNilNode", err)
	}
	if err := flow.Validate(flowx.FanOut(
		[]flow.Node[int, int]{valid},
		flow.MapConfig{Concurrency: -1},
	)); !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("Validate FanOut config error = %v; want ErrInvalidConfig", err)
	}
}

func TestCombine_propagatesNodeFailure(t *testing.T) {
	boom := errors.New("boom")
	failing := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		return 0, boom
	})
	ok := flow.NodeFunc[int, string](func(context.Context, int) (string, error) {
		return "ok", nil
	})

	_, err := flowx.Combine(
		failing,
		ok,
		func(context.Context, int, string) (string, error) { return "", nil },
	).Run(t.Context(), 1)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v; want node failure", err)
	}
}

func TestCombine_parentCancellationDuringMergeWins(t *testing.T) {
	cause := errors.New("stop combine")
	ctx, cancel := context.WithCancelCause(t.Context())
	node := flowx.Combine(
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 2, nil }),
		func(context.Context, int, int) (int, error) {
			cancel(cause)
			return 3, nil
		},
	)

	output, err := node.Run(ctx, 0)
	if !errors.Is(err, cause) || output != 0 {
		t.Fatalf("Run = %d, %v; want 0, cancellation cause", output, err)
	}
}

func TestCombine_parentCancellationBeforeMergeWins(t *testing.T) {
	cause := errors.New("stop before merge")
	ctx := ctxtest.CancelAtCheck(t.Context(), 2, cause)
	merged := false
	node := flowx.Combine(
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 2, nil }),
		func(context.Context, int, int) (int, error) {
			merged = true
			return 3, nil
		},
	)

	output, err := node.Run(ctx, 0)
	if !errors.Is(err, cause) || output != 0 || merged {
		t.Fatalf("Run = %d, %v, merged %t; want 0, cancellation cause, false", output, err, merged)
	}
}

func TestChain(t *testing.T) {
	inc := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })
	got, err := flowx.Chain(inc, inc, inc).Run(t.Context(), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 3 {
		t.Fatalf("got %d, want 3", got)
	}

	// Repeating one node cannot say which nodes ran, or in what order: dropping the
	// first and running another twice gives the same answer. Two nodes that do
	// different things, composed in an order that does not commute, can.
	double := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil })
	if got, err = flowx.Chain(inc, double).Run(t.Context(), 5); err != nil || got != 12 {
		t.Fatalf("Chain(inc, double)(5) = %d, %v; want 12, nil", got, err)
	}
	if got, err = flowx.Chain(double, inc).Run(t.Context(), 5); err != nil || got != 11 {
		t.Fatalf("Chain(double, inc)(5) = %d, %v; want 11, nil", got, err)
	}
}

func TestChain_emptyAndSingle(t *testing.T) {
	got, err := flowx.Chain[int]().Run(t.Context(), 4)
	if err != nil || got != 4 {
		t.Fatalf("empty Chain = %d, %v", got, err)
	}
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil })
	got, err = flowx.Chain(node).Run(t.Context(), 4)
	if err != nil || got != 5 {
		t.Fatalf("single Chain = %d, %v", got, err)
	}
}

func TestChain_emptyHonorsCancellation(t *testing.T) {
	cause := errors.New("stop empty chain")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)

	got, err := flowx.Chain[int]().Run(ctx, 4)
	if got != 4 || !errors.Is(err, cause) {
		t.Fatalf("empty Chain = %d, %v; want 4, cancellation cause", got, err)
	}
}

func TestChain_singleNilReturnsError(t *testing.T) {
	_, err := flowx.Chain[int](nil).Run(t.Context(), 0)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}

	var invalid flow.NodeFunc[int, int]
	_, err = flowx.Chain[int](invalid).Run(t.Context(), 0)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("typed nil err = %v; want ErrNilNode", err)
	}
}

func TestChain_rejectsTypedNilFunctionBeforeRunning(t *testing.T) {
	ran := false
	node := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		ran = true
		return 0, nil
	})
	var invalid flow.NodeFunc[int, int]

	_, err := flowx.Chain(node, invalid).Run(t.Context(), 0)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
	if ran {
		t.Fatal("a node ran before the typed nil function node was rejected")
	}
}
