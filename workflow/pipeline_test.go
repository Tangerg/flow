package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestPipeline(t *testing.T) {
	tests := []struct {
		name string
		pipe workflow.Pipeline
		want map[string]int
	}{
		{
			name: "zero value passes store through",
			pipe: workflow.Pipeline{},
			want: map[string]int{"seed": 1},
		},
		{
			name: "then threads store",
			pipe: workflow.Pipe(pipelineAdd("a", "seed", 1)).
				Then(pipelineAdd("b", "a", 2)),
			want: map[string]int{"seed": 1, "a": 2, "b": 4},
		},
		{
			name: "parallel appends one stage",
			pipe: workflow.Pipe(pipelineAdd("a", "seed", 1)).
				Parallel(
					pipelineAdd("b", "a", 2),
					pipelineAdd("c", "a", 3),
				),
			want: map[string]int{"seed": 1, "a": 2, "b": 4, "c": 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := tt.pipe.Run(context.Background(), workflow.NewStore().WithOutput("seed", 1))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			for id, want := range tt.want {
				got, err := workflow.Get[int](out, workflow.Output(id))
				if err != nil || got != want {
					t.Fatalf("%s = %d, %v; want %d, nil", id, got, err, want)
				}
			}
		})
	}
}

func TestPipeline_compositeMethods(t *testing.T) {
	branch := func(_ context.Context, _ workflow.Store) (string, error) { return "yes", nil }
	loopBody := pipelineAdd("loop", "chosen", 1)
	done := func(_ context.Context, iteration int, _ workflow.Store) (bool, error) {
		return iteration == 1, nil
	}
	iterationBody := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		item, err := workflow.Get[any](s, workflow.At("each", workflow.ItemKey))
		if err != nil {
			return s, err
		}
		return s.WithOutput("item", item), nil
	})

	pipe := workflow.Pipe().
		Branch(branch, map[string]workflow.Step{
			"yes": pipelineAdd("chosen", "seed", 1),
		}).
		LoopLimit(3, loopBody, done).
		IterationN(1, "each", workflow.Output("items"), iterationBody, workflow.Output("item"))

	input := workflow.NewStore().
		WithOutput("seed", 1).
		WithOutput("items", []any{"a", "b"})
	out, err := pipe.Run(context.Background(), input)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := workflow.Get[int](out, workflow.Output("loop")); err != nil || got != 3 {
		t.Fatalf("loop = %d, %v; want 3, nil", got, err)
	}
	items, err := workflow.Get[[]any](out, workflow.Output("each"))
	if err != nil || len(items) != 2 || items[0] != "a" || items[1] != "b" {
		t.Fatalf("items = %#v, %v; want [a b], nil", items, err)
	}
}

func TestPipeline_isImmutable(t *testing.T) {
	base := workflow.Pipe(pipelineAdd("a", "seed", 1))
	left := base.Then(pipelineAdd("left", "a", 1))
	right := base.Then(pipelineAdd("right", "a", 2))
	input := workflow.NewStore().WithOutput("seed", 1)

	leftOut, err := left.Run(context.Background(), input)
	if err != nil {
		t.Fatalf("left.Run: %v", err)
	}
	if _, ok := leftOut.Lookup(workflow.Output("right")); ok {
		t.Fatal("left pipeline was mutated by right pipeline")
	}

	rightOut, err := right.Run(context.Background(), input)
	if err != nil {
		t.Fatalf("right.Run: %v", err)
	}
	if _, ok := rightOut.Lookup(workflow.Output("left")); ok {
		t.Fatal("right pipeline was mutated by left pipeline")
	}
}

func TestPipeline_nilStep(t *testing.T) {
	_, err := workflow.Pipe(nil).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
}

func TestPipeline_describesSequence(t *testing.T) {
	description := workflow.Pipe(pipelineAdd("a", "seed", 1)).
		Parallel(pipelineAdd("b", "a", 1), pipelineAdd("c", "a", 1)).
		Describe()

	if description.Kind != "sequence" || len(description.Children) != 2 {
		t.Fatalf("description = %+v; want sequence with two children", description)
	}
	if description.Children[1].Kind != "parallel" {
		t.Fatalf("child = %+v; want parallel", description.Children[1])
	}
}

func pipelineAdd(id, input string, n int) workflow.Step {
	return workflow.Leaf(
		id,
		workflow.From[int](workflow.Output(input)),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value + n, nil
		}),
	)
}
