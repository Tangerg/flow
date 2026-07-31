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

	out, err := loop.Run(t.Context(), workflow.NewStore().WithOutput("start", 0))
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

	_, err := workflow.Loop("loop", body, nil, workflow.LoopConfig{}).Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("err = %v; want ErrNilFunc", err)
	}
}

func TestLoop_nilBody(t *testing.T) {
	_, err := workflow.Loop(
		"loop",
		nil,
		func(context.Context, int, workflow.Store) (bool, error) { return true, nil },
		workflow.LoopConfig{},
	).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("error = %v; want ErrNilStep", err)
	}

	var body flow.NodeFunc[workflow.Store, workflow.Store]
	_, err = workflow.Loop(
		"loop",
		body,
		func(context.Context, int, workflow.Store) (bool, error) { return true, nil },
		workflow.LoopConfig{},
	).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("typed nil error = %v; want ErrNilStep", err)
	}
}

func TestLoop_maxIterations(t *testing.T) {
	body := workflow.Leaf("x",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	done := func(context.Context, int, workflow.Store) (bool, error) { return false, nil } // never done

	_, err := workflow.Loop("loop", body, done, workflow.LoopConfig{MaxIterations: 3}).Run(t.Context(), workflow.NewStore())
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
		Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("err = %v; want ErrInvalidConfig", err)
	}
}

func TestLoop_honorsCancellationBeforeAnIteration(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ran := false
	body := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			ran = true
			return store, nil
		},
	)

	_, err := workflow.Loop(
		"loop",
		body,
		func(context.Context, int, workflow.Store) (bool, error) { return true, nil },
		workflow.LoopConfig{},
	).Run(ctx, workflow.NewStore())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
	if ran {
		t.Fatal("body ran under an already canceled context")
	}
}

func TestLoop_conditionError(t *testing.T) {
	boom := errors.New("condition failed")
	body := workflow.Leaf("x",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	done := func(context.Context, int, workflow.Store) (bool, error) { return false, boom }

	_, err := workflow.Loop("loop", body, done, workflow.LoopConfig{}).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v; want condition error", err)
	}
}

func TestLoop_suspensionPreservesCompletedIterationWrites(t *testing.T) {
	write := workflow.Leaf(
		"write",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value, nil
		}),
	)

	t.Run("body", func(t *testing.T) {
		body := workflow.Parallel(
			[]workflow.Step{
				write,
				workflow.Await("wait", workflow.Output("missing")),
			},
			workflow.ParallelConfig{},
		)
		output, err := workflow.Loop(
			"loop",
			body,
			func(context.Context, int, workflow.Store) (bool, error) {
				t.Fatal("condition ran while the body was suspended")
				return false, nil
			},
			workflow.LoopConfig{},
		).Run(t.Context(), workflow.NewStore())
		if !workflow.SuspendedOnly(err) {
			t.Fatalf("err = %v; want suspension", err)
		}
		if value, getErr := workflow.Get[int](output, workflow.Output("write")); getErr != nil ||
			value != 1 {
			t.Fatalf("write = %d, %v; want 1, nil", value, getErr)
		}
	})

	t.Run("condition", func(t *testing.T) {
		output, err := workflow.Loop(
			"loop",
			write,
			func(context.Context, int, workflow.Store) (bool, error) {
				return false, workflow.Suspend("waiting to decide")
			},
			workflow.LoopConfig{},
		).Run(t.Context(), workflow.NewStore())
		if !workflow.SuspendedOnly(err) {
			t.Fatalf("err = %v; want suspension", err)
		}
		if value, getErr := workflow.Get[int](output, workflow.Output("write")); getErr != nil ||
			value != 1 {
			t.Fatalf("write = %d, %v; want 1, nil", value, getErr)
		}
	})
}

func TestLoop_failureRollsBackTheFailingIteration(t *testing.T) {
	boom := errors.New("boom")
	body := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store.WithOutput("partial", 1), boom
		},
	)
	output, err := workflow.Loop(
		"loop",
		body,
		func(context.Context, int, workflow.Store) (bool, error) { return true, nil },
		workflow.LoopConfig{},
	).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v; want boom", err)
	}
	if _, ok := output.Lookup(workflow.Output("partial")); ok {
		t.Fatal("ordinary body failure leaked the failing iteration's Store")
	}

	body = flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store.WithOutput("complete", 1), nil
		},
	)
	output, err = workflow.Loop(
		"loop",
		body,
		func(context.Context, int, workflow.Store) (bool, error) {
			return false, boom
		},
		workflow.LoopConfig{},
	).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, boom) {
		t.Fatalf("condition err = %v; want boom", err)
	}
	if _, ok := output.Lookup(workflow.Output("complete")); ok {
		t.Fatal("ordinary condition failure leaked the failing iteration's Store")
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
			t.Context(),
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
		{ID: "first", Scope: []string{"first[0]"}},
		{ID: "tick", Scope: []string{"first[0]"}},
		{ID: "second", Scope: []string{"second[0]"}},
		{ID: "tick", Scope: []string{"second[0]"}},
	}
	if keys := journal.Keys(); !slices.EqualFunc(
		keys,
		want,
		func(left, right workflow.JournalKey) bool {
			return left.ID == right.ID && slices.Equal(left.Scope, right.Scope)
		},
	) {
		t.Fatalf("journal keys = %v; want %v", keys, want)
	}
}

func TestValidateSpec_siblingLoopBodiesMayReuseAnID(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
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

func TestLoop_rejectsInvalidStaticIdentities(t *testing.T) {
	leaf := func(id string) workflow.Step {
		return workflow.Leaf(
			id,
			workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				return 1, nil
			}),
		)
	}
	done := func(context.Context, int, workflow.Store) (bool, error) {
		return true, nil
	}
	tests := map[string]workflow.Step{
		"loop ID collides before loop": workflow.Sequence(
			leaf("loop"),
			workflow.Loop("loop", leaf("body"), done, workflow.LoopConfig{}),
		),
		"body collides with loop ID": workflow.Loop(
			"loop",
			leaf("loop"),
			done,
			workflow.LoopConfig{},
		),
	}
	for name, step := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := step.Run(
				t.Context(),
				workflow.NewStore(),
			); !errors.Is(err, workflow.ErrDuplicateStep) {
				t.Fatalf("error = %v; want ErrDuplicateStep", err)
			}
		})
	}
}

func TestLoop_rejectsDuplicateOpaqueInvocation(t *testing.T) {
	body := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store, nil
		},
	)
	loop := workflow.Loop(
		"loop",
		body,
		func(context.Context, int, workflow.Store) (bool, error) { return true, nil },
		workflow.LoopConfig{},
	)
	twice := flow.NodeFunc[workflow.Store, workflow.Store](
		func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
			next, err := loop.Run(ctx, store)
			if err != nil {
				return next, err
			}
			return loop.Run(ctx, next)
		},
	)
	if _, err := workflow.Run(
		t.Context(),
		twice,
		workflow.NewStore(),
		workflow.RunConfig{},
	); !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("error = %v; want ErrDuplicateStep", err)
	}
}

func TestLoop_stopRejectsIdentityClaimedByOpaqueBody(t *testing.T) {
	await := workflow.Await("loop", workflow.Output("ready"))
	body := flow.NodeFunc[workflow.Store, workflow.Store](
		func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
			return await.Run(ctx, store)
		},
	)
	loop := workflow.Loop(
		"loop",
		body,
		func(context.Context, int, workflow.Store) (bool, error) { return true, nil },
		workflow.LoopConfig{},
	)
	_, err := workflow.Run(
		t.Context(),
		loop,
		workflow.NewStore().WithOutput("ready", true),
		workflow.RunConfig{},
	)
	if !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("error = %v; want ErrDuplicateStep", err)
	}
}

func TestLoop_reportsJournalDecisionConflict(t *testing.T) {
	journal := workflow.NewJournal()
	loop := workflow.Loop(
		"loop",
		flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				return store, nil
			},
		),
		func(ctx context.Context, _ int, _ workflow.Store) (bool, error) {
			if err := journal.Record(
				workflow.JournalKey{ID: "loop", Scope: workflow.Scope(ctx)},
				true,
			); err != nil {
				return false, err
			}
			return true, nil
		},
		workflow.LoopConfig{},
	)
	_, err := workflow.Run(
		t.Context(),
		loop,
		workflow.NewStore(),
		workflow.RunConfig{Journal: journal},
	)
	if !errors.Is(err, workflow.ErrJournalConflict) {
		t.Fatalf("error = %v; want ErrJournalConflict", err)
	}
}
