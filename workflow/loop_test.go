package workflow_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestLoop_untilDone(t *testing.T) {
	// Increment a counter until it reaches 3, threading through the Store.
	bind := workflow.BindFunc[int](func(s workflow.Store) (int, error) {
		if v, ok := s.Lookup(workflow.Output("step")); ok {
			return v.(int), nil
		}
		v, _ := s.Lookup(workflow.At("start", "output"))
		return v.(int), nil
	})
	body := workflow.Leaf("step", bind, flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil }))

	done := func(_ context.Context, _ int, s workflow.Store) (bool, error) {
		v, _ := s.Lookup(workflow.Output("step"))
		return v.(int) >= 3, nil
	}

	loop := workflow.Loop("loop", body, done, workflow.LoopConfig{MaxIterations: 10})

	out, err := loop.Run(context.Background(), workflow.NewStore().WithOutput("start", 0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("step")); !ok || v.(int) != 3 {
		t.Fatalf("final counter = %v, %v; want 3", v, ok) // 0 -> 1 -> 2 -> 3
	}
}

func TestLoop_nilCondition(t *testing.T) {
	body := workflow.Leaf("x",
		workflow.From[int](workflow.Ref{NodeID: "start", Path: "/output"}),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)

	_, err := workflow.Loop("loop", body, nil, workflow.LoopConfig{}).Run(context.Background(), workflow.NewStore().WithOutput("start", 1))
	if !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("err = %v; want ErrNilFunc", err)
	}
}

func TestLoop_maxIterations(t *testing.T) {
	body := workflow.Leaf("x",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	done := func(context.Context, int, workflow.Store) (bool, error) { return false, nil } // never done

	_, err := workflow.Loop("loop", body, done, workflow.LoopConfig{MaxIterations: 3}).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, flow.ErrMaxIterations) {
		t.Fatalf("err = %v; want ErrMaxIterations", err)
	}
}

func TestLoop_rejectsNegativeMaxIterations(t *testing.T) {
	body := workflow.Leaf("x",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	done := func(context.Context, int, workflow.Store) (bool, error) { return true, nil }

	_, err := workflow.Loop("loop", body, done, workflow.LoopConfig{MaxIterations: -1}).
		Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("err = %v; want ErrInvalidConfig", err)
	}
}

func TestLoop_conditionError(t *testing.T) {
	boom := errors.New("condition failed")
	body := workflow.Leaf("x",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	done := func(context.Context, int, workflow.Store) (bool, error) { return false, boom }

	_, err := workflow.Loop("loop", body, done, workflow.LoopConfig{}).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v; want condition error", err)
	}
}

func TestLoop_siblingBodiesWithTheSameIDHaveDistinctJournalScopes(t *testing.T) {
	var firstRuns, secondRuns int
	body := func(runs *int, output string) workflow.Step {
		return workflow.Leaf(
			"tick",
			workflow.BindFunc[string](func(workflow.Store) (string, error) {
				return output, nil
			}),
			flow.NodeFunc[string, string](func(_ context.Context, value string) (string, error) {
				(*runs)++
				return value, nil
			}),
		)
	}
	done := func(context.Context, int, workflow.Store) (bool, error) {
		return true, nil
	}
	pipeline := workflow.Sequence(
		workflow.Loop("first", body(&firstRuns, "FROM_FIRST"), done, workflow.LoopConfig{}),
		workflow.Loop("second", body(&secondRuns, "FROM_SECOND"), done, workflow.LoopConfig{}),
	)

	journal := workflow.NewJournal()
	cfg := workflow.RunConfig{Journal: journal}
	for range 2 {
		output, err := workflow.Run(
			context.Background(),
			pipeline,
			workflow.NewStore(),
			cfg,
		)
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		if got, err := workflow.Get[string](output, workflow.Output("tick")); err != nil ||
			got != "FROM_SECOND" {
			t.Fatalf("tick = %q, %v; want FROM_SECOND", got, err)
		}
	}
	if firstRuns != 1 || secondRuns != 1 {
		t.Fatalf("body runs = %d,%d; want 1,1 after replay", firstRuns, secondRuns)
	}

	want := []workflow.JournalKey{
		{ID: "first", Path: []string{"first[0]"}},
		{ID: "tick", Path: []string{"first[0]"}},
		{ID: "second", Path: []string{"second[0]"}},
		{ID: "tick", Path: []string{"second[0]"}},
	}
	if keys := journal.Keys(); !slices.EqualFunc(
		keys,
		want,
		func(left, right workflow.JournalKey) bool {
			return left.ID == right.ID && slices.Equal(left.Path, right.Path)
		},
	) {
		t.Fatalf("journal keys = %v; want %v", keys, want)
	}
}

func TestValidateSpec_siblingLoopBodiesMayReuseAnID(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterLeaf("addN", addN()).
		MustRegisterCondition(
			"done",
			func(context.Context, int, workflow.Store) (bool, error) {
				return true, nil
			},
		)
	loop := func(id string) workflow.Spec {
		return workflow.Spec{
			Kind:      workflow.KindLoop,
			ID:        id,
			Condition: "done",
			Body: &workflow.Spec{
				Kind: workflow.KindLeaf,
				ID:   "tick",
				Type: "addN",
			},
		}
	}
	spec := workflow.Spec{
		Kind:  workflow.KindSequence,
		Steps: []workflow.Spec{loop("first"), loop("second")},
	}
	if err := registry.ValidateSpec(spec); err != nil {
		t.Fatalf("ValidateSpec: %v", err)
	}

	colliding := loop("same")
	colliding.Body.ID = "same"
	if err := registry.ValidateSpec(colliding); !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("same body and loop ID error = %v; want ErrDuplicateStep", err)
	}
}
