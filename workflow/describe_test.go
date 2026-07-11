package workflow_test

import (
	"context"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func leafStep(id string) workflow.Step {
	return workflow.Leaf(id,
		workflow.From[int](workflow.Ref{NodeID: "start", Path: "output"}),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
}

func TestDescribe_tree(t *testing.T) {
	step := workflow.Sequence(
		leafStep("a"),
		workflow.Parallel(leafStep("b"), leafStep("c")),
	)

	d := workflow.Describe(step)
	if d.Kind != "sequence" || len(d.Children) != 2 {
		t.Fatalf("root = %+v; want sequence with 2 children", d)
	}
	if d.Children[0].Kind != "leaf" || d.Children[0].ID != "a" {
		t.Fatalf("child 0 = %+v; want leaf:a", d.Children[0])
	}
	par := d.Children[1]
	if par.Kind != "parallel" || len(par.Children) != 2 {
		t.Fatalf("child 1 = %+v; want parallel with 2 children", par)
	}
	if par.Children[0].ID != "b" || par.Children[1].ID != "c" {
		t.Fatalf("parallel children = %+v; want leaf:b, leaf:c", par.Children)
	}
}

func TestDescribe_opaque(t *testing.T) {
	// A bare flow.NodeFunc is not a Describer.
	bare := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s, nil
	})
	if d := workflow.Describe(bare); d.Kind != "opaque" {
		t.Fatalf("Describe(bare) = %+v; want opaque", d)
	}
}

func TestBranchDescriptionPreservesIDAndCaseLabel(t *testing.T) {
	step := workflow.Branch(
		func(context.Context, workflow.Store) (string, error) { return "yes", nil },
		map[string]workflow.Step{"yes": leafStep("actual-id")},
	)
	d := workflow.Describe(step)
	if len(d.Children) != 1 || d.Children[0].ID != "actual-id" || d.Children[0].Label != "yes" {
		t.Fatalf("branch child = %+v", d.Children)
	}
}
