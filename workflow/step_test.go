package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/workflow"
)

func TestSequence_threadsStore(t *testing.T) {
	double := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil })
	inc := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })

	step1 := workflow.Adapt("double", workflow.FromRef[int](workflow.Ref{NodeID: "start", Path: "output"}), double)
	step2 := workflow.Adapt("inc", workflow.FromRef[int](workflow.Ref{NodeID: "double", Path: workflow.OutputKey}), inc)

	flow := workflow.Sequence(step1, step2)

	in := workflow.NewStore().With("start", "output", 5)
	out, err := flow.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v, ok := out.Get("inc", workflow.OutputKey); !ok || v.(int) != 11 {
		t.Fatalf("final output = %v, %v; want 11", v, ok) // 5*2=10, +1=11
	}
	// Intermediate output is retained (snapshot semantics).
	if v, ok := out.Get("double", workflow.OutputKey); !ok || v.(int) != 10 {
		t.Fatalf("intermediate output = %v, %v; want 10", v, ok)
	}
}

func TestAdapt_missingInput(t *testing.T) {
	leaf := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x, nil })
	step := workflow.Adapt("n", workflow.FromRef[int](workflow.Ref{NodeID: "absent", Path: "output"}), leaf)

	if _, err := step.Run(context.Background(), workflow.NewStore()); err == nil {
		t.Fatal("expected error for missing input")
	}
}

func TestAdapt_propagatesLeafError(t *testing.T) {
	boom := errors.New("boom")
	leaf := core.Func[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	step := workflow.Adapt("n", workflow.FromRef[int](workflow.Ref{NodeID: "start", Path: "output"}), leaf)

	_, err := step.Run(context.Background(), workflow.NewStore().With("start", "output", 1))
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
}

func TestSequence_empty(t *testing.T) {
	s := workflow.NewStore().With("x", "output", 1)

	out, err := workflow.Sequence().Run(context.Background(), s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Get("x", "output"); !ok || v.(int) != 1 {
		t.Fatalf("empty sequence should pass the store through, got %v, %v", v, ok)
	}
}
