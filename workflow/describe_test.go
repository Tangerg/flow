package workflow_test

import (
	"context"
	"errors"
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
		workflow.Parallel([]workflow.Step{leafStep("b"), leafStep("c")}),
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
	step := workflow.Branch("route",
		func(context.Context, workflow.Store) (string, error) { return "yes", nil },
		map[string]workflow.Step{"yes": leafStep("actual-id")},
	)
	d := workflow.Describe(step)
	if len(d.Children) != 1 || d.Children[0].ID != "actual-id" || d.Children[0].Label != "yes" {
		t.Fatalf("branch child = %+v", d.Children)
	}
}

func TestDescribe_everyCompositeReportsItsID(t *testing.T) {
	yes := func(context.Context, workflow.Store) (string, error) { return "yes", nil }
	stop := func(context.Context, int, workflow.Store) (bool, error) { return true, nil }

	steps := map[string]workflow.Step{
		"leaf":   leafStep("leaf"),
		"branch": workflow.Branch("branch", yes, map[string]workflow.Step{"yes": leafStep("y")}),
		"loop":   workflow.Loop("loop", leafStep("body"), stop),
		"await":  workflow.Await("await", workflow.Output("x")),
		"iteration": workflow.Iteration(workflow.IterationConfig{
			ID: "iteration", Input: workflow.Output("in"),
			Body: leafStep("body"), BodyOutput: workflow.Output("body"),
		}),
	}
	for kind, step := range steps {
		t.Run(kind, func(t *testing.T) {
			d := workflow.Describe(step)
			if d.Kind != kind {
				t.Fatalf("Kind = %q; want %q", d.Kind, kind)
			}
			// Every kind that records something under its own name must report it,
			// since that ID is also its Journal key.
			if d.ID != kind {
				t.Fatalf("ID = %q; want %q", d.ID, kind)
			}
		})
	}

	// Sequence and parallel are structural and carry no ID.
	for kind, step := range map[string]workflow.Step{
		"sequence": workflow.Sequence(leafStep("a")),
		"parallel": workflow.Parallel([]workflow.Step{leafStep("a")}),
	} {
		t.Run(kind, func(t *testing.T) {
			if d := workflow.Describe(step); d.Kind != kind || d.ID != "" {
				t.Fatalf("Describe = %+v; want %s with no ID", d, kind)
			}
		})
	}
}

func TestBranchAndLoop_requireAnID(t *testing.T) {
	yes := func(context.Context, workflow.Store) (string, error) { return "yes", nil }
	stop := func(context.Context, int, workflow.Store) (bool, error) { return true, nil }
	body := workflow.Leaf("b", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	for kind, step := range map[string]workflow.Step{
		"branch": workflow.Branch("", yes, map[string]workflow.Step{"yes": leafStep("y")}),
		"loop":   workflow.Loop("", body, stop),
	} {
		t.Run(kind, func(t *testing.T) {
			_, err := step.Run(context.Background(), workflow.NewStore())
			if !errors.Is(err, workflow.ErrInvalidStepID) {
				t.Fatalf("err = %v; want ErrInvalidStepID", err)
			}
		})
	}
}

func TestBranchAndLoop_propagateDecisionErrors(t *testing.T) {
	boom := errors.New("boom")
	body := workflow.Leaf("b", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	// A resolver or condition that fails is a step error naming the composite.
	_, err := workflow.Branch("route", func(context.Context, workflow.Store) (string, error) { return "", boom },
		map[string]workflow.Step{"a": leafStep("a")}).Run(context.Background(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "route" || !errors.Is(err, boom) {
		t.Fatalf("branch err = %v; want a StepError for route wrapping boom", err)
	}

	_, err = workflow.Loop("repeat", body,
		func(context.Context, int, workflow.Store) (bool, error) { return false, boom }).Run(context.Background(), workflow.NewStore())
	if !errors.As(err, &stepErr) || stepErr.ID != "repeat" || !errors.Is(err, boom) {
		t.Fatalf("loop err = %v; want a StepError for repeat wrapping boom", err)
	}

	// A resolver or condition may also suspend, which is not a step error.
	_, err = workflow.Branch("route", func(context.Context, workflow.Store) (string, error) {
		return "", workflow.Suspend("routing needs a person")
	}, map[string]workflow.Step{"a": leafStep("a")}).Run(context.Background(), workflow.NewStore())
	if suspensions := workflow.Suspensions(err); len(suspensions) != 1 || suspensions[0].ID != "route" {
		t.Fatalf("branch err = %v; want a suspension naming route", err)
	}

	_, err = workflow.Loop("repeat", body, func(context.Context, int, workflow.Store) (bool, error) {
		return false, workflow.Suspend("deciding needs a person")
	}).Run(context.Background(), workflow.NewStore())
	if suspensions := workflow.Suspensions(err); len(suspensions) != 1 || suspensions[0].ID != "repeat" {
		t.Fatalf("loop err = %v; want a suspension naming repeat", err)
	}
}
