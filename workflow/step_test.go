package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

type staticBinder int

func (b staticBinder) Bind(workflow.Store) (int, error) { return int(b), nil }

func TestSequence_threadsStore(t *testing.T) {
	double := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil })
	inc := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })

	step1 := workflow.Leaf("double", workflow.From[int](workflow.Ref{NodeID: "start", Path: "output"}), double)
	step2 := workflow.Leaf("inc", workflow.From[int](workflow.Ref{NodeID: "double", Path: workflow.OutputKey}), inc)

	flow := workflow.Sequence(step1, step2)

	in := workflow.NewStore().WithOutput("start", 5)
	out, err := flow.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v, ok := out.Lookup(workflow.Output("inc")); !ok || v.(int) != 11 {
		t.Fatalf("final output = %v, %v; want 11", v, ok) // 5*2=10, +1=11
	}
	// Intermediate output is retained (snapshot semantics).
	if v, ok := out.Lookup(workflow.Output("double")); !ok || v.(int) != 10 {
		t.Fatalf("intermediate output = %v, %v; want 10", v, ok)
	}
}

func TestLeaf_missingInput(t *testing.T) {
	leaf := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })
	step := workflow.Leaf("n", workflow.From[int](workflow.Ref{NodeID: "absent", Path: "output"}), leaf)

	if _, err := step.Run(context.Background(), workflow.NewStore()); err == nil {
		t.Fatal("expected error for missing input")
	}
}

func TestLeaf_propagatesLeafError(t *testing.T) {
	boom := errors.New("boom")
	leaf := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	step := workflow.Leaf("n", workflow.From[int](workflow.Ref{NodeID: "start", Path: "output"}), leaf)

	_, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", 1))
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
}

func TestLeaf_errorIncludesStepAndOperation(t *testing.T) {
	boom := errors.New("boom")
	step := workflow.Leaf("load",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 0, boom }),
	)

	_, err := step.Run(context.Background(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "load" || stepErr.Op != workflow.OpRun || !errors.Is(err, boom) {
		t.Fatalf("err = %v; want load/run StepError", err)
	}
}

func TestSequence_empty(t *testing.T) {
	s := workflow.NewStore().WithOutput("x", 1)

	out, err := workflow.Sequence().Run(context.Background(), s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Lookup(workflow.At("x", "output")); !ok || v.(int) != 1 {
		t.Fatalf("empty sequence should pass the store through, got %v, %v", v, ok)
	}
}

func TestSequence_singleNilStep(t *testing.T) {
	_, err := workflow.Sequence(nil).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
}

func TestLeaf_rejectsEmptyIDAndNilBinder(t *testing.T) {
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil })
	if _, err := workflow.Leaf("", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }), node).
		Run(context.Background(), workflow.NewStore()); !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("empty ID err = %v", err)
	}
	if _, err := workflow.Leaf[int, int]("x", nil, node).
		Run(context.Background(), workflow.NewStore()); !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("nil binder err = %v", err)
	}
	var bind workflow.BindFunc[int]
	if _, err := workflow.Leaf("x", bind, node).
		Run(context.Background(), workflow.NewStore()); !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("nil BindFunc err = %v", err)
	}
}

func TestLeaf_acceptsCustomBinder(t *testing.T) {
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in * 2, nil })
	out, err := workflow.Leaf("double", staticBinder(21), node).Run(context.Background(), workflow.NewStore())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got, err := workflow.Get[int](out, workflow.Output("double"))
	if err != nil || got != 42 {
		t.Fatalf("Get = %d, %v; want 42, nil", got, err)
	}
}

func TestGet_nilValue(t *testing.T) {
	store := workflow.NewStore().With("n", "value", nil)

	if got, err := workflow.Get[any](store, workflow.At("n", "value")); err != nil || got != nil {
		t.Fatalf("From[any](nil) = %v, %v", got, err)
	}
	if got, err := workflow.Get[*int](store, workflow.At("n", "value")); err != nil || got != nil {
		t.Fatalf("From[*int](nil) = %v, %v", got, err)
	}
	if _, err := workflow.Get[int](store, workflow.At("n", "value")); err == nil {
		t.Fatal("From[int](nil) unexpectedly succeeded")
	}
}
