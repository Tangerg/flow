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

func TestAdapt_errorIncludesStepAndOperation(t *testing.T) {
	boom := errors.New("boom")
	step := workflow.Adapt("load",
		func(workflow.Store) (int, error) { return 0, nil },
		core.Func[int, int](func(context.Context, int) (int, error) { return 0, boom }),
	)

	_, err := step.Run(context.Background(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "load" || stepErr.Op != "run" || !errors.Is(err, boom) {
		t.Fatalf("err = %v; want load/run StepError", err)
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

func TestSequence_singleNilStep(t *testing.T) {
	_, err := workflow.Sequence(nil).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
}

func TestAdapt_rejectsEmptyIDAndNilBinder(t *testing.T) {
	node := core.Func[int, int](func(_ context.Context, in int) (int, error) { return in, nil })
	if _, err := workflow.Adapt("", func(workflow.Store) (int, error) { return 1, nil }, node).
		Run(context.Background(), workflow.NewStore()); !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("empty ID err = %v", err)
	}
	if _, err := workflow.Adapt[int, int]("x", nil, node).
		Run(context.Background(), workflow.NewStore()); !errors.Is(err, core.ErrNilFunc) {
		t.Fatalf("nil binder err = %v", err)
	}
}

func TestFromRef_nilValue(t *testing.T) {
	store := workflow.NewStore().With("n", "value", nil)

	if got, err := workflow.FromRef[any](workflow.Ref{NodeID: "n", Path: "value"})(store); err != nil || got != nil {
		t.Fatalf("FromRef[any](nil) = %v, %v", got, err)
	}
	if got, err := workflow.FromRef[*int](workflow.Ref{NodeID: "n", Path: "value"})(store); err != nil || got != nil {
		t.Fatalf("FromRef[*int](nil) = %v, %v", got, err)
	}
	if _, err := workflow.FromRef[int](workflow.Ref{NodeID: "n", Path: "value"})(store); err == nil {
		t.Fatal("FromRef[int](nil) unexpectedly succeeded")
	}
}
