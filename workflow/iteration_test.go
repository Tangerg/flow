package workflow_test

import (
	"context"
	"errors"
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
	out, err := iter.Run(context.Background(), in)
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
		workflow.From[int](workflow.Index("iter")),
		flow.NodeFunc[int, int](func(_ context.Context, i int) (int, error) { return i, nil }),
	)

	iter := workflow.Iteration(workflow.IterationConfig{
		ID:         "iter",
		Input:      workflow.Ref{NodeID: "start", Path: "/output"},
		Body:       body,
		BodyOutput: workflow.Output("el"),
	})

	in := workflow.NewStore().WithOutput("start", []any{"a", "b", "c"})
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
	body := workflow.Leaf("el",
		workflow.From[int](workflow.Item("iter")),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	iter := workflow.Iteration(workflow.IterationConfig{ID: "iter", Input: workflow.Output("start"), Body: body, BodyOutput: workflow.Output("el")})

	_, err := iter.Run(context.Background(), workflow.NewStore().WithOutput("start", 42))
	if err == nil {
		t.Fatal("expected error for non-array input")
	}
}

func TestIteration_validatesItsStructureBeforeReadingInput(t *testing.T) {
	empty := workflow.NewStore().WithOutput("start", []any{})
	identity := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s, nil
	})

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
		"invalid input": {
			config: workflow.IterationConfig{
				ID: "each", Body: identity, BodyOutput: workflow.Output("value"),
			},
			want: workflow.ErrInvalidSpec,
		},
		"invalid body output": {
			config: workflow.IterationConfig{
				ID: "each", Input: workflow.Output("start"), Body: identity,
			},
			want: workflow.ErrInvalidSpec,
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
			_, err := workflow.Iteration(tt.config).Run(context.Background(), empty)
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
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
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
		context.Background(),
		workflow.NewStore().WithOutput("start", []any{}),
	)
	if !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("error = %v; want ErrInvalidStepID", err)
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
		context.Background(),
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
		context.Background(),
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
				context.Background(),
				workflow.NewStore().WithOutput("start", []any{1}),
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v; want %v", err, test.want)
			}
		})
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
