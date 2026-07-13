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
	got, err := flowx.FanOut(flow.MapConfig{}, nodes...).Run(context.Background(), 10)
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
		_, err := flowx.FanOut(flow.MapConfig{Concurrency: 2}, node, node, node, node).Run(context.Background(), 1)
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

func TestFanOut_failFast(t *testing.T) {
	boom := errors.New("boom")
	nodes := []flow.Node[int, int]{
		flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }),
	}
	if _, err := flowx.FanOut(flow.MapConfig{}, nodes...).Run(context.Background(), 1); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

func TestFanOut_clonesNodes(t *testing.T) {
	nodes := []flow.Node[int, int]{
		flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil }),
	}
	fan := flowx.FanOut(flow.MapConfig{}, nodes...)
	nodes[0] = flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 100, nil })

	got, err := fan.Run(context.Background(), 1)
	if err != nil || len(got) != 1 || got[0] != 2 {
		t.Fatalf("FanOut after source mutation = %v, %v", got, err)
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
