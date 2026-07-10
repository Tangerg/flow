package flowx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/flowx"
)

func TestFanOut(t *testing.T) {
	nodes := []core.Node[int, int]{
		core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil }),
		core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + 2, nil }),
		core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + 3, nil }),
	}
	got, err := flowx.FanOut(nodes).Run(context.Background(), 10)
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

func TestMapAll_collect(t *testing.T) {
	boom := errors.New("boom")
	node := core.Func[int, int](func(_ context.Context, x int) (int, error) {
		if x == 2 {
			return 0, boom
		}
		return x * 10, nil
	})
	results, err := flowx.MapAll(node).Run(context.Background(), []int{1, 2, 3})
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
}

func TestCombine2(t *testing.T) {
	length := core.Func[string, int](func(_ context.Context, s string) (int, error) { return len(s), nil })
	upper := core.Func[string, string](func(_ context.Context, s string) (string, error) { return s + "!", nil })

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
	slow := core.Func[int, int](func(ctx context.Context, x int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return x, nil
		}
	})
	fast := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x * 100, nil })

	got, err := flowx.Race([]core.Node[int, int]{slow, fast}).Run(context.Background(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 500 {
		t.Fatalf("got %d, want 500 (fast should win)", got)
	}
}

func TestRace_allFail(t *testing.T) {
	e1, e2 := errors.New("e1"), errors.New("e2")
	n1 := core.Func[int, int](func(_ context.Context, _ int) (int, error) { return 0, e1 })
	n2 := core.Func[int, int](func(_ context.Context, _ int) (int, error) { return 0, e2 })

	_, err := flowx.Race([]core.Node[int, int]{n1, n2}).Run(context.Background(), 1)
	if !errors.Is(err, e1) || !errors.Is(err, e2) {
		t.Fatalf("err = %v, want joined e1 and e2", err)
	}
}

func TestChain(t *testing.T) {
	inc := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })
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
