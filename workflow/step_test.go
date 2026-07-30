package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestSequence_threadsStore(t *testing.T) {
	double := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil })
	inc := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })

	step1 := workflow.Leaf("double", workflow.From[int](workflow.Ref{NodeID: "start", Path: "/output"}), double)
	step2 := workflow.Leaf("inc", workflow.From[int](workflow.Output("double")), inc)

	flow := workflow.Sequence(step1, step2)

	in := workflow.NewStore().WithOutput("start", 5)
	out, err := flow.Run(t.Context(), in)
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
	step := workflow.Leaf("n", workflow.From[int](workflow.Ref{NodeID: "absent", Path: "/output"}), leaf)

	if _, err := step.Run(t.Context(), workflow.NewStore()); err == nil {
		t.Fatal("expected error for missing input")
	}
}

func TestLeafFunc_andFirstOf(t *testing.T) {
	refs := []workflow.Ref{workflow.Output("missing"), workflow.Output("input")}
	step := workflow.LeafFunc(
		"double",
		refs[0],
		func(_ context.Context, value int) (int, error) {
			return value * 2, nil
		},
	)
	if _, err := step.Run(
		t.Context(),
		workflow.NewStore().WithOutput("input", 21),
	); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("LeafFunc missing input error = %v; want ErrNotFound", err)
	}

	bind := workflow.FirstOf[int](refs...)
	refs[1] = workflow.Output("changed")
	value, err := bind(workflow.NewStore().WithOutput("input", 21))
	if err != nil || value != 21 {
		t.Fatalf("FirstOf = %d, %v; want 21, nil", value, err)
	}

	out, err := workflow.Leaf(
		"double",
		bind,
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value * 2, nil
		}),
	).Run(t.Context(), workflow.NewStore().WithOutput("input", 21))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := workflow.Get[int](out, workflow.Output("double")); err != nil || got != 42 {
		t.Fatalf("output = %d, %v; want 42, nil", got, err)
	}
}

func TestFirstOf_reportsConversionAndMissingErrors(t *testing.T) {
	bind := workflow.FirstOf[int](
		workflow.Output("wrong"),
		workflow.Output("valid"),
	)
	store := workflow.NewStore().
		WithOutput("wrong", "not an integer").
		WithOutput("valid", 42)
	if _, err := bind(store); !errors.Is(err, workflow.ErrTypeMismatch) {
		t.Fatalf("conversion error = %v; want ErrTypeMismatch", err)
	}
	if _, err := workflow.FirstOf[int]()(store); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("empty FirstOf error = %v; want ErrNotFound", err)
	}
}

func TestLeaf_propagatesLeafError(t *testing.T) {
	boom := errors.New("boom")
	leaf := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	step := workflow.Leaf("n", workflow.From[int](workflow.Ref{NodeID: "start", Path: "/output"}), leaf)

	_, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
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

	_, err := step.Run(t.Context(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "load" || stepErr.Op != workflow.OpRun || !errors.Is(err, boom) {
		t.Fatalf("err = %v; want load/run StepError", err)
	}
}

func TestSequence_empty(t *testing.T) {
	s := workflow.NewStore().WithOutput("x", 1)

	out, err := workflow.Sequence().Run(t.Context(), s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Lookup(workflow.At("x", "output")); !ok || v.(int) != 1 {
		t.Fatalf("empty sequence should pass the store through, got %v, %v", v, ok)
	}
}

func TestSequence_singleNilStep(t *testing.T) {
	_, err := workflow.Sequence(nil).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
}

func TestSequence_validatesEveryStepBeforeRunning(t *testing.T) {
	ran := false
	first := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		ran = true
		return store, nil
	})
	_, err := workflow.Sequence(first, nil).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 1 {
		t.Fatalf("err = %v; want IndexError at step 1", err)
	}
	if ran {
		t.Fatal("first step ran before the invalid sequence was rejected")
	}
}

func TestLeaf_rejectsEmptyIDAndNilBind(t *testing.T) {
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil })
	if _, err := workflow.Leaf("", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }), node).
		Run(t.Context(), workflow.NewStore()); !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("empty ID err = %v", err)
	}
	if _, err := workflow.Leaf("x", nil, node).
		Run(t.Context(), workflow.NewStore()); !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("nil binder err = %v", err)
	}
	var bind workflow.BindFunc[int]
	if _, err := workflow.Leaf("x", bind, node).
		Run(t.Context(), workflow.NewStore()); !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("nil BindFunc err = %v", err)
	}
}

func TestSequence_rejectsExcessiveDefinitionNesting(t *testing.T) {
	step := workflow.Leaf(
		"leaf",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value, nil
		}),
	)
	for range workflow.MaxNestingDepth {
		step = workflow.Sequence(step)
	}

	if _, err := step.Run(
		t.Context(),
		workflow.NewStore(),
	); !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("err = %v; want ErrMaxDepth", err)
	}
}

func TestLeaf_validatesDefinitionBeforeJournalReplay(t *testing.T) {
	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "broken"}, 42); err != nil {
		t.Fatalf("Record: %v", err)
	}
	bind := workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil })
	step := workflow.Leaf[int, int]("broken", bind, nil)

	if _, err := workflow.Run(t.Context(), step, workflow.NewStore(),
		workflow.RunConfig{Journal: journal}); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode instead of Journal replay", err)
	}
}

func TestLeaf_validatesNilNodeFuncBeforeJournalReplay(t *testing.T) {
	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "broken"}, 42); err != nil {
		t.Fatalf("Record: %v", err)
	}
	bind := workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil })
	var node flow.NodeFunc[int, int]
	step := workflow.Leaf("broken", bind, node)

	if _, err := workflow.Run(t.Context(), step, workflow.NewStore(),
		workflow.RunConfig{Journal: journal}); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode instead of Journal replay", err)
	}
}

func TestLeaf_acceptsCustomBindFunc(t *testing.T) {
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in * 2, nil })
	// A custom binder is just a BindFunc; this one ignores the store.
	bind := workflow.BindFunc[int](func(workflow.Store) (int, error) { return 21, nil })
	out, err := workflow.Leaf("double", bind, node).Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got, err := workflow.Get[int](out, workflow.Output("double"))
	if err != nil || got != 42 {
		t.Fatalf("Get = %d, %v; want 42, nil", got, err)
	}
}

func TestLeaf_rejectsExcessiveExecutionScopeDepth(t *testing.T) {
	ctx := t.Context()
	for index := range workflow.MaxNestingDepth + 1 {
		ctx = workflow.WithScope(ctx, fmt.Sprintf("scope-%d", index))
	}
	step := workflow.Leaf(
		"leaf",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
			return 1, nil
		}),
	)
	_, err := workflow.Run(
		ctx,
		step,
		workflow.NewStore(),
		workflow.RunConfig{},
	)
	if !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("error = %v; want ErrMaxDepth", err)
	}
}

func TestGet_nilValue(t *testing.T) {
	store := workflow.NewStore().WithCell("n", "value", nil)

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
