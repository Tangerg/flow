package workflow_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestIteration_mapsAndCollects(t *testing.T) {
	// body doubles each element, read from the scoped (iter, item) slot.
	body := workflow.Leaf("el",
		workflow.From[int](workflow.Item("iter")),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil }),
	)

	iter := workflow.Iteration(workflow.IterationConfig{
		ID:          "iter",
		Input:       workflow.Ref{NodeID: "start", Path: "/output"},
		Body:        body,
		BodyOutput:  workflow.Output("el"),
		Concurrency: 2,
	})

	in := workflow.NewStore().WithOutput("start", []any{1, 2, 3})
	out, err := iter.Run(t.Context(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	raw, ok := out.Lookup(workflow.Output("iter"))
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
	body := workflow.Leaf("el",
		workflow.From[int](workflow.ItemIndex("iter")),
		flow.NodeFunc[int, int](func(_ context.Context, i int) (int, error) { return i, nil }),
	)

	iter := workflow.Iteration(workflow.IterationConfig{
		ID:         "iter",
		Input:      workflow.Ref{NodeID: "start", Path: "/output"},
		Body:       body,
		BodyOutput: workflow.Output("el"),
	})

	in := workflow.NewStore().WithOutput("start", []any{"a", "b", "c"})
	out, err := iter.Run(t.Context(), in)
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
	body := workflow.Leaf("el",
		workflow.From[int](workflow.Item("iter")),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	iter := workflow.Iteration(workflow.IterationConfig{ID: "iter", Input: workflow.Output("start"), Body: body, BodyOutput: workflow.Output("el")})

	_, err := iter.Run(t.Context(), workflow.NewStore().WithOutput("start", 42))
	var stepErr *workflow.StepError
	if !errors.Is(err, workflow.ErrTypeMismatch) ||
		!errors.As(err, &stepErr) ||
		stepErr.ID != "iter" || stepErr.Op != workflow.OpBind {
		t.Fatalf("err = %v; want iter/bind StepError wrapping ErrTypeMismatch", err)
	}
}

func TestIteration_validatesItsStructureBeforeReadingInput(t *testing.T) {
	empty := workflow.NewStore().WithOutput("start", []any{})
	identity := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s, nil
	})
	var nilBody flow.NodeFunc[workflow.Store, workflow.Store]

	tests := map[string]struct {
		config workflow.IterationConfig
		want   error
	}{
		"empty ID": {
			config: workflow.IterationConfig{
				Input: workflow.Output("start"), Body: identity, BodyOutput: workflow.Output("value"),
			},
			want: workflow.ErrInvalidStepID,
		},
		"nil body": {
			config: workflow.IterationConfig{
				ID: "each", Input: workflow.Output("start"), BodyOutput: workflow.Output("value"),
			},
			want: workflow.ErrNilStep,
		},
		"typed nil body": {
			config: workflow.IterationConfig{
				ID: "each", Input: workflow.Output("start"), Body: nilBody,
				BodyOutput: workflow.Output("value"),
			},
			want: workflow.ErrNilStep,
		},
		"invalid input": {
			config: workflow.IterationConfig{
				ID: "each", Body: identity, BodyOutput: workflow.Output("value"),
			},
			want: flow.ErrInvalidConfig,
		},
		"invalid body output": {
			config: workflow.IterationConfig{
				ID: "each", Input: workflow.Output("start"), Body: identity,
			},
			want: flow.ErrInvalidConfig,
		},
		"negative concurrency": {
			config: workflow.IterationConfig{
				ID: "each", Input: workflow.Output("start"), Body: identity,
				BodyOutput: workflow.Output("value"), Concurrency: -1,
			},
			want: flow.ErrInvalidConfig,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := workflow.Iteration(tt.config).Run(t.Context(), empty)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v; want %v", err, tt.want)
			}
			var stepErr *workflow.StepError
			if !errors.As(err, &stepErr) || stepErr.Op != workflow.OpValidate {
				t.Fatalf("err = %v; want validation StepError", err)
			}
		})
	}
}

func TestIteration_rejectsInvalidStaticBodyDefinition(t *testing.T) {
	body := workflow.Leaf(
		"",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
			return 1, nil
		}),
	)
	step := workflow.Iteration(workflow.IterationConfig{
		ID:         "items",
		Input:      workflow.Output("start"),
		Body:       body,
		BodyOutput: workflow.Output("body"),
	})
	_, err := step.Run(
		t.Context(),
		workflow.NewStore().WithOutput("start", []any{}),
	)
	if !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("error = %v; want ErrInvalidStepID", err)
	}
}

func TestIteration_rejectsImpossibleOutputFromVisibleBody(t *testing.T) {
	bodyRan := false
	body := workflow.Leaf(
		"produced",
		workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
			bodyRan = true
			return struct{}{}, nil
		}),
		flow.NodeFunc[struct{}, int](func(context.Context, struct{}) (int, error) {
			return 1, nil
		}),
	)
	step := workflow.Iteration(workflow.IterationConfig{
		ID:         "each",
		Input:      workflow.Output("items"),
		Body:       body,
		BodyOutput: workflow.Output("missing"),
	})

	_, err := step.Run(
		t.Context(),
		workflow.NewStore().WithOutput("items", []any{1}),
	)
	if !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("Run error = %v; want ErrInvalidConfig", err)
	}
	if bodyRan {
		t.Fatal("visible body ran before its impossible collection was rejected")
	}
}

func TestIteration_rejectsChildPathBelowItemIndex(t *testing.T) {
	step := workflow.Iteration(workflow.IterationConfig{
		ID:         "each",
		Input:      workflow.Output("items"),
		Body:       workflow.Sequence(),
		BodyOutput: workflow.ItemIndex("each").Child("value"),
	})

	_, err := step.Run(
		t.Context(),
		workflow.NewStore().WithOutput("items", []any{}),
	)
	if !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("Run error = %v; want ErrInvalidConfig", err)
	}
}

func TestIteration_collectsInjectedItemWithoutABodyOutput(t *testing.T) {
	step := workflow.Iteration(workflow.IterationConfig{
		ID:         "each",
		Input:      workflow.Output("items"),
		Body:       workflow.Sequence(),
		BodyOutput: workflow.Item("each"),
	})

	output, err := step.Run(
		t.Context(),
		workflow.NewStore().WithOutput("items", []any{"a", "b"}),
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	values := mustSlice(t, output, "each")
	if len(values) != 2 || values[0] != "a" || values[1] != "b" {
		t.Fatalf("collected values = %v; want [a b]", values)
	}
}

func TestIteration_rejectsIDUsedBeforeIteration(t *testing.T) {
	step := workflow.Sequence(
		leafStep("items"),
		workflow.Iteration(workflow.IterationConfig{
			ID:         "items",
			Input:      workflow.Output("start"),
			Body:       leafStep("body"),
			BodyOutput: workflow.Output("body"),
		}),
	)
	_, err := step.Run(
		t.Context(),
		workflow.NewStore().WithOutput("start", []any{}),
	)
	if !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("error = %v; want ErrDuplicateStep", err)
	}
}

func TestIteration_rejectsDuplicateOpaqueInvocation(t *testing.T) {
	body := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store.WithOutput("value", 1), nil
		},
	)
	iteration := workflow.Iteration(workflow.IterationConfig{
		ID:         "items",
		Input:      workflow.Output("start"),
		Body:       body,
		BodyOutput: workflow.Output("value"),
	})
	twice := flow.NodeFunc[workflow.Store, workflow.Store](
		func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
			next, err := iteration.Run(ctx, store)
			if err != nil {
				return next, err
			}
			return iteration.Run(ctx, next)
		},
	)
	_, err := workflow.Run(
		t.Context(),
		twice,
		workflow.NewStore().WithOutput("start", []any{}),
		workflow.RunConfig{},
	)
	if !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("error = %v; want ErrDuplicateStep", err)
	}
}

func TestIteration_reportsBodyFailureAndMissingOutput(t *testing.T) {
	boom := errors.New("boom")
	tests := map[string]struct {
		body       workflow.Step
		bodyOutput workflow.Ref
		want       error
	}{
		"body failure": {
			body: flow.NodeFunc[workflow.Store, workflow.Store](
				func(context.Context, workflow.Store) (workflow.Store, error) {
					return workflow.Store{}, boom
				},
			),
			bodyOutput: workflow.Output("value"),
			want:       boom,
		},
		"missing body output": {
			body: flow.NodeFunc[workflow.Store, workflow.Store](
				func(_ context.Context, store workflow.Store) (workflow.Store, error) {
					return store, nil
				},
			),
			bodyOutput: workflow.Output("missing"),
			want:       workflow.ErrNotFound,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			step := workflow.Iteration(workflow.IterationConfig{
				ID:         "items",
				Input:      workflow.Output("start"),
				Body:       test.body,
				BodyOutput: test.bodyOutput,
			})
			_, err := step.Run(
				t.Context(),
				workflow.NewStore().WithOutput("start", []any{1}),
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v; want %v", err, test.want)
			}
			var stepErr *workflow.StepError
			if !errors.As(err, &stepErr) ||
				stepErr.ID != "items" || stepErr.Op != workflow.OpRun {
				t.Fatalf("error = %v; want items/run StepError", err)
			}
		})
	}
}

func TestIteration_failureReturnsTheInputStore(t *testing.T) {
	boom := errors.New("boom")
	body := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			index, err := workflow.Get[int](store, workflow.ItemIndex("items"))
			if err != nil {
				return store, err
			}
			if index == 1 {
				return store, boom
			}
			return store.WithOutput("value", index), nil
		},
	)
	input := workflow.NewStore().WithOutput("source", []int{1, 2})
	output, err := workflow.Iteration(workflow.IterationConfig{
		ID:          "items",
		Input:       workflow.Output("source"),
		Body:        body,
		BodyOutput:  workflow.Output("value"),
		Concurrency: 1,
	}).Run(t.Context(), input)
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v; want boom", err)
	}
	if _, present := output.Lookup(workflow.Output("items")); present {
		t.Fatal("ordinary failure published a partial collection")
	}
	if values, getErr := workflow.Get[[]int](output, workflow.Output("source")); getErr != nil || !slices.Equal(values, []int{1, 2}) {
		t.Fatalf("source = %v, %v; want [1 2], nil", values, getErr)
	}
}

func mustSlice(t *testing.T, s workflow.Store, nodeID string) []any {
	t.Helper()
	raw, ok := s.Lookup(workflow.Output(nodeID))
	if !ok {
		t.Fatalf("output missing for %q", nodeID)
	}
	return raw.([]any)
}
