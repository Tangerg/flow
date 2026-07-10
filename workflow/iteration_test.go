package workflow_test

import (
	"context"
	"testing"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/workflow"
)

func TestIteration_mapsAndCollects(t *testing.T) {
	// body doubles each element, read from the scoped (iter, item) slot.
	body := workflow.Adapt("el",
		workflow.FromRef[int](workflow.Ref{NodeID: "iter", Path: workflow.ItemKey}),
		core.Func[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil }),
	)

	iter := workflow.Iteration(
		"iter",
		workflow.Ref{NodeID: "start", Path: "output"},
		body,
		workflow.Ref{NodeID: "el", Path: workflow.OutputKey},
	)

	in := workflow.NewStore().With("start", "output", []any{1, 2, 3})
	out, err := iter.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	raw, ok := out.Get("iter", workflow.OutputKey)
	if !ok {
		t.Fatal("iteration output missing")
	}
	got := raw.([]any)
	want := []any{2, 4, 6}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].(int) != want[i].(int) {
			t.Fatalf("got[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestIteration_usesIndex(t *testing.T) {
	// body returns the element's index, proving the scope carries it.
	body := workflow.Adapt("el",
		workflow.FromRef[int](workflow.Ref{NodeID: "iter", Path: workflow.IndexKey}),
		core.Func[int, int](func(_ context.Context, i int) (int, error) { return i, nil }),
	)

	iter := workflow.Iteration(
		"iter",
		workflow.Ref{NodeID: "start", Path: "output"},
		body,
		workflow.Ref{NodeID: "el", Path: workflow.OutputKey},
	)

	in := workflow.NewStore().With("start", "output", []any{"a", "b", "c"})
	out, err := iter.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := mustSlice(t, out, "iter")
	for i := range got {
		if got[i].(int) != i {
			t.Fatalf("index at %d = %v, want %d", i, got[i], i)
		}
	}
}

func TestIteration_inputNotArray(t *testing.T) {
	body := workflow.Adapt("el",
		workflow.FromRef[int](workflow.Ref{NodeID: "iter", Path: workflow.ItemKey}),
		core.Func[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	iter := workflow.Iteration("iter", workflow.Ref{NodeID: "start", Path: "output"}, body, workflow.Ref{NodeID: "el", Path: workflow.OutputKey})

	_, err := iter.Run(context.Background(), workflow.NewStore().With("start", "output", 42))
	if err == nil {
		t.Fatal("expected error for non-array input")
	}
}

func mustSlice(t *testing.T, s workflow.Store, nodeID string) []any {
	t.Helper()
	raw, ok := s.Get(nodeID, workflow.OutputKey)
	if !ok {
		t.Fatalf("output missing for %q", nodeID)
	}
	return raw.([]any)
}
