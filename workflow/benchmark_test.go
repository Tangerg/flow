package workflow_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/workflow"
)

func BenchmarkStoreWithGet(b *testing.B) {
	base := workflow.NewStore().WithOutput("seed", 1)

	b.ReportAllocs()
	for b.Loop() {
		s := base.WithOutput("node", 2)
		_, _ = s.Lookup(workflow.Output("node"))
	}
}

func BenchmarkSequenceRun(b *testing.B) {
	ctx := context.Background()
	inc := func(id, input string) workflow.Step {
		node := core.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
			return in + 1, nil
		})
		return workflow.Leaf(id, workflow.From[int](workflow.Ref{
			NodeID: input,
			Path:   workflow.OutputKey,
		}), node)
	}
	step := workflow.Sequence(
		inc("a", "seed"),
		inc("b", "a"),
		inc("c", "b"),
	)
	input := workflow.NewStore().WithOutput("seed", 1)

	b.ReportAllocs()
	for b.Loop() {
		_, _ = step.Run(ctx, input)
	}
}

func BenchmarkParallelMerge(b *testing.B) {
	ctx := context.Background()
	base := workflow.NewStore()
	for i := range 128 {
		base = base.WithOutput("base-"+strconv.Itoa(i), i)
	}
	branches := make([]workflow.Step, 8)
	for i := range branches {
		id := "branch-" + strconv.Itoa(i)
		branches[i] = core.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
			return s.WithOutput(id, i), nil
		})
	}
	node := workflow.Parallel(branches...)

	b.ReportAllocs()
	for b.Loop() {
		_, _ = node.Run(ctx, base)
	}
}
