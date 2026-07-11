package flowx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/flowx"
)

func TestFanOut(t *testing.T) {
	nodes := []flow.Node[int, int]{
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 2, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 3, nil }),
	}
	got, err := flowx.FanOut(nodes...).Run(context.Background(), 10)
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

func TestFanOutN_boundsConcurrency(t *testing.T) {
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
		_, err := flowx.FanOutN(2, node, node, node, node).Run(context.Background(), 1)
		done <- err
	}()

	<-started
	<-started
	select {
	case <-started:
		t.Fatal("more than two nodes started before a slot was released")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestFanOutAllN_collectsResults(t *testing.T) {
	boom := errors.New("boom")
	ok := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil })
	bad := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, boom })
	results, err := flowx.FanOutAllN(1, ok, bad).Run(context.Background(), 1)
	if err != nil || len(results) != 2 || results[0].Value != 2 || !errors.Is(results[1].Err, boom) {
		t.Fatalf("results = %#v, err = %v", results, err)
	}
}

func TestMapAll_collect(t *testing.T) {
	boom := errors.New("boom")
	node := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		if x == 2 {
			return 0, boom
		}
		return x * 10, nil
	})
	for name, mapper := range map[string]flow.Node[[]int, []flowx.Result[int]]{
		"unbounded": flowx.MapAll(node),
		"bounded":   flowx.MapAllN(1, node),
	} {
		t.Run(name, func(t *testing.T) {
			results, err := mapper.Run(context.Background(), []int{1, 2, 3})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			vals, joined := flowx.Collect(results)
			if !errors.Is(joined, boom) {
				t.Fatalf("joined = %v, want boom", joined)
			}
			if vals[0] != 10 || vals[2] != 30 {
				t.Fatalf("vals = %v", vals)
			}
		})
	}
}

func TestCollect_preservesPartialValuesAndIndexesErrors(t *testing.T) {
	boom := errors.New("boom")
	values, err := flowx.Collect([]flowx.Result[int]{
		{Value: 10},
		{Value: 20, Err: boom},
	})
	if len(values) != 2 || values[1] != 20 {
		t.Fatalf("values = %v; partial result was lost", values)
	}
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 1 || !errors.Is(err, boom) {
		t.Fatalf("err = %v; want index 1", err)
	}
}

func TestFanOutAll_collectsFailures(t *testing.T) {
	boom := errors.New("boom")
	nodes := []flow.Node[int, int]{
		flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }),
		nil,
	}
	results, err := flowx.FanOutAll(nodes...).Run(context.Background(), 1)
	if err != nil {
		t.Fatalf("FanOutAll: %v", err)
	}
	if len(results) != 3 || results[0].Value != 2 || !errors.Is(results[1].Err, boom) || !errors.Is(results[2].Err, flow.ErrNilNode) {
		t.Fatalf("results = %+v", results)
	}
}

func TestCombine2(t *testing.T) {
	length := flow.NodeFunc[string, int](func(_ context.Context, s string) (int, error) { return len(s), nil })
	upper := flow.NodeFunc[string, string](func(_ context.Context, s string) (string, error) { return s + "!", nil })

	node := flowx.Combine2(length, upper, func(_ context.Context, n int, s string) (string, error) {
		return s, nil
	})
	got, err := node.Run(context.Background(), "hi")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "hi!" {
		t.Fatalf("got %q, want %q", got, "hi!")
	}
}

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

	got, err := flowx.Race(slow, fast).Run(context.Background(), 5)
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

	_, err := flowx.Race(n1, n2).Run(context.Background(), 1)
	if !errors.Is(err, e1) || !errors.Is(err, e2) {
		t.Fatalf("err = %v, want joined e1 and e2", err)
	}
}

func TestChain(t *testing.T) {
	inc := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })
	got, err := flowx.Chain(inc, inc, inc).Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 3 {
		t.Fatalf("got %d, want 3", got)
	}
}

func TestIdentity(t *testing.T) {
	got, err := flowx.Identity[string]().Run(context.Background(), "x")
	if err != nil || got != "x" {
		t.Fatalf("Identity = %q, %v", got, err)
	}
}

func TestChain_emptyAndSingle(t *testing.T) {
	got, err := flowx.Chain[int]().Run(context.Background(), 4)
	if err != nil || got != 4 {
		t.Fatalf("empty Chain = %d, %v", got, err)
	}
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil })
	got, err = flowx.Chain(node).Run(context.Background(), 4)
	if err != nil || got != 5 {
		t.Fatalf("single Chain = %d, %v", got, err)
	}
}

func TestChain_singleNilReturnsError(t *testing.T) {
	_, err := flowx.Chain[int](nil).Run(context.Background(), 0)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
}

func TestCombine2_rejectsNilInputs(t *testing.T) {
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil })
	if _, err := flowx.Combine2[int, int, int, int](node, node, nil).Run(context.Background(), 1); !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("nil merge err = %v", err)
	}
	merge := func(_ context.Context, a, b int) (int, error) { return a + b, nil }
	if _, err := flowx.Combine2[int, int, int, int](nil, node, merge).Run(context.Background(), 1); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil node err = %v", err)
	}
}

func TestRace_noNodes(t *testing.T) {
	_, err := flowx.Race[int, int]().Run(context.Background(), 0)
	if !errors.Is(err, flowx.ErrNoNodes) {
		t.Fatalf("err = %v; want ErrNoNodes", err)
	}
}

func TestFanOut_clonesNodes(t *testing.T) {
	nodes := []flow.Node[int, int]{
		flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil }),
	}
	fan := flowx.FanOut(nodes...)
	nodes[0] = flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 100, nil })

	got, err := fan.Run(context.Background(), 1)
	if err != nil || len(got) != 1 || got[0] != 2 {
		t.Fatalf("FanOut after source mutation = %v, %v", got, err)
	}
}

func TestRace_cancelledBeforeRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil })

	_, err := flowx.Race(node).Run(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
}

func TestRace_allFailErrorOrderIsStable(t *testing.T) {
	e1, e2 := errors.New("first"), errors.New("second")
	n1 := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
		time.Sleep(time.Millisecond)
		return 0, e1
	})
	n2 := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, e2 })

	_, err := flowx.Race(n1, n2).Run(context.Background(), 0)
	if err == nil || err.Error() != "flow: index 0: first\nflow: index 1: second" {
		t.Fatalf("joined error = %q; want input order", err)
	}
}
