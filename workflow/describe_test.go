package workflow_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func leafStep(id string) workflow.Step {
	return workflow.Leaf(id,
		workflow.From[int](workflow.Ref{NodeID: "start", Path: "/output"}),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
}

func TestDescribe_tree(t *testing.T) {
	step := workflow.Sequence(
		leafStep("a"),
		workflow.Parallel([]workflow.Step{leafStep("b"), leafStep("c")}, workflow.ParallelConfig{}),
	)

	d := workflow.Describe(step)
	if d.Kind != workflow.KindSequence || len(d.Children) != 2 {
		t.Fatalf("root = %+v; want sequence with 2 children", d)
	}
	if d.Children[0].Kind != workflow.KindLeaf || d.Children[0].ID != "a" {
		t.Fatalf("child 0 = %+v; want leaf:a", d.Children[0])
	}
	par := d.Children[1]
	if par.Kind != workflow.KindParallel || len(par.Children) != 2 {
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
	if d := workflow.Describe(bare); d.Kind != workflow.KindOpaque {
		t.Fatalf("Describe(bare) = %+v; want opaque", d)
	}
}

type borrowedDescriptionStep struct {
	description workflow.Description
}

func (b *borrowedDescriptionStep) Run(
	_ context.Context,
	store workflow.Store,
) (workflow.Store, error) {
	return store, nil
}

func (b *borrowedDescriptionStep) Describe() workflow.Description {
	return b.description
}

func newBorrowedDescriptionStep() *borrowedDescriptionStep {
	return &borrowedDescriptionStep{description: workflow.Description{
		ID:   "custom",
		Kind: "custom",
		Children: []workflow.Description{{
			ID:   "nested",
			Kind: "nested",
			Children: []workflow.Description{{
				ID:   "leaf",
				Kind: "leaf-like",
			}},
		}},
	}}
}

func assertOwnedDescription(
	t *testing.T,
	custom *borrowedDescriptionStep,
	describe func() workflow.Description,
) {
	t.Helper()

	first := describe()
	first.Children[0].ID = "changed"
	first.Children[0].Children[0].ID = "changed-nested"
	first.Children[0].Children[0].Children[0].ID = "changed-leaf"

	second := describe()
	if second.Children[0].ID != "custom" ||
		second.Children[0].Children[0].ID != "nested" ||
		second.Children[0].Children[0].Children[0].ID != "leaf" {
		t.Fatalf("second description = %+v; caller mutation leaked into the Step", second)
	}
	if custom.description.Children[0].ID != "nested" ||
		custom.description.Children[0].Children[0].ID != "leaf" {
		t.Fatalf("custom description = %+v; description leaked its storage", custom.description)
	}
}

func TestDescribe_returnsAnOwnedRecursiveSnapshot(t *testing.T) {
	custom := newBorrowedDescriptionStep()
	step := workflow.Sequence(custom)
	assertOwnedDescription(t, custom, func() workflow.Description {
		return workflow.Describe(step)
	})
}

func TestBuiltInDescriber_returnsAnOwnedRecursiveSnapshot(t *testing.T) {
	custom := newBorrowedDescriptionStep()
	describer := workflow.Sequence(custom).(workflow.Describer)
	assertOwnedDescription(t, custom, describer.Describe)
}

func TestDescribe_streamFuncUsesLeafBoundary(t *testing.T) {
	step := workflow.Leaf(
		"stream",
		workflow.From[int](workflow.Output("start")),
		workflow.StreamFunc[int, int, int](
			func(_ context.Context, input int, _ func(int) bool) (int, error) {
				return input, nil
			},
		),
	)
	if description := workflow.Describe(step); description.Kind != workflow.KindLeaf ||
		description.ID != "stream" || len(description.Children) != 0 {
		t.Fatalf("Describe = %+v; want leaf:stream", description)
	}
}

func TestBranchDescriptionPreservesIDAndCaseLabel(t *testing.T) {
	step := workflow.Branch("route",
		resolverNode(func(context.Context, workflow.Store) (string, error) { return "yes", nil }),
		map[string]workflow.Step{"yes": leafStep("actual-id")},
	)
	d := workflow.Describe(step)
	if len(d.Children) != 1 || d.Children[0].ID != "actual-id" || d.Children[0].Label != "yes" {
		t.Fatalf("branch child = %+v", d.Children)
	}
}

func TestBranchDescriptionOrdersCasesByName(t *testing.T) {
	step := workflow.Branch(
		"route",
		resolverNode(func(context.Context, workflow.Store) (string, error) { return "a", nil }),
		map[string]workflow.Step{
			"z": leafStep("last"),
			"a": leafStep("first"),
			"m": leafStep("middle"),
		},
	)
	description := workflow.Describe(step)
	labels := make([]string, len(description.Children))
	for index, child := range description.Children {
		labels[index] = child.Label
	}
	if !slices.Equal(labels, []string{"a", "m", "z"}) {
		t.Fatalf("case labels = %v; want [a m z]", labels)
	}
}

func TestDescriptionLabelBelongsToTheParentRelationship(t *testing.T) {
	wait := workflow.Await("wait", workflow.Output("approval"))
	if description := workflow.Describe(wait); description.Label != "" {
		t.Fatalf("top-level Await label = %q; want no parent relationship", description.Label)
	}

	branch := workflow.Branch(
		"route",
		resolverNode(func(context.Context, workflow.Store) (string, error) { return "approved", nil }),
		map[string]workflow.Step{"approved": wait},
	)
	description := workflow.Describe(branch)
	if len(description.Children) != 1 || description.Children[0].Label != "approved" {
		t.Fatalf("branch description = %+v; want the case as the child relationship", description)
	}
}

func TestDescribe_everyCompositeReportsItsID(t *testing.T) {
	yes := resolverNode(func(context.Context, workflow.Store) (string, error) { return "yes", nil })
	stop := func(context.Context, int, workflow.Store) (bool, error) { return true, nil }

	steps := map[workflow.Kind]workflow.Step{
		workflow.KindLeaf:      leafStep("leaf"),
		workflow.KindBranch:    workflow.Branch("branch", yes, map[string]workflow.Step{"yes": leafStep("y")}),
		workflow.KindLoop:      workflow.Loop("loop", leafStep("body"), stop, workflow.LoopConfig{}),
		workflow.KindAwait:     workflow.Await("await", workflow.Output("x")),
		workflow.KindInterrupt: workflow.Interrupt("interrupt", "continue?"),
		workflow.KindIteration: workflow.Iteration(workflow.IterationConfig{
			ID: "iteration", Input: workflow.Output("in"),
			Body: leafStep("body"), BodyOutput: workflow.Output("body"),
		}),
	}
	for kind, step := range steps {
		t.Run(string(kind), func(t *testing.T) {
			d := workflow.Describe(step)
			if d.Kind != kind {
				t.Fatalf("Kind = %q; want %q", d.Kind, kind)
			}
			// Every kind that records something under its own name must report it,
			// since that ID is also its Journal key.
			if d.ID != string(kind) {
				t.Fatalf("ID = %q; want %q", d.ID, kind)
			}
		})
	}

	// Sequence and parallel are structural and carry no ID.
	for kind, step := range map[workflow.Kind]workflow.Step{
		workflow.KindSequence: workflow.Sequence(leafStep("a")),
		workflow.KindParallel: workflow.Parallel([]workflow.Step{leafStep("a")}, workflow.ParallelConfig{}),
	} {
		t.Run(string(kind), func(t *testing.T) {
			if d := workflow.Describe(step); d.Kind != kind || d.ID != "" {
				t.Fatalf("Describe = %+v; want %s with no ID", d, kind)
			}
		})
	}
}

func TestBranchAndLoop_requireAnID(t *testing.T) {
	yes := resolverNode(func(context.Context, workflow.Store) (string, error) { return "yes", nil })
	stop := func(context.Context, int, workflow.Store) (bool, error) { return true, nil }
	body := workflow.Leaf("b", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	for kind, step := range map[string]workflow.Step{
		"branch": workflow.Branch("", yes, map[string]workflow.Step{"yes": leafStep("y")}),
		"loop":   workflow.Loop("", body, stop, workflow.LoopConfig{}),
	} {
		t.Run(kind, func(t *testing.T) {
			_, err := step.Run(t.Context(), workflow.NewStore())
			if !errors.Is(err, workflow.ErrInvalidStepID) {
				t.Fatalf("err = %v; want ErrInvalidStepID", err)
			}
		})
	}
}

func TestBranchAndLoop_propagateDecisionErrors(t *testing.T) {
	boom := errors.New("boom")
	body := workflow.Leaf("b", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	// A resolver or condition that fails is a step error naming the composite.
	_, err := workflow.Branch("route", resolverNode(func(context.Context, workflow.Store) (string, error) { return "", boom }),
		map[string]workflow.Step{"a": leafStep("a")}).Run(t.Context(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "route" || !errors.Is(err, boom) {
		t.Fatalf("branch err = %v; want a StepError for route wrapping boom", err)
	}

	_, err = workflow.Loop("repeat", body, func(context.Context, int, workflow.Store) (bool, error) { return false, boom }, workflow.LoopConfig{}).
		Run(t.Context(), workflow.NewStore())
	if !errors.As(err, &stepErr) || stepErr.ID != "repeat" || !errors.Is(err, boom) {
		t.Fatalf("loop err = %v; want a StepError for repeat wrapping boom", err)
	}

	// A resolver or condition may also suspend, which is not a step error.
	_, err = workflow.Branch("route", resolverNode(func(context.Context, workflow.Store) (string, error) {
		return "", workflow.Suspend("routing needs a person")
	}), map[string]workflow.Step{"a": leafStep("a")}).Run(t.Context(), workflow.NewStore())
	if suspensions := workflow.Suspensions(err); len(suspensions) != 1 || suspensions[0].ID != "route" {
		t.Fatalf("branch err = %v; want a suspension naming route", err)
	}

	_, err = workflow.Loop("repeat", body, func(context.Context, int, workflow.Store) (bool, error) {
		return false, workflow.Suspend("deciding needs a person")
	}, workflow.LoopConfig{}).
		Run(t.Context(), workflow.NewStore())
	if suspensions := workflow.Suspensions(err); len(suspensions) != 1 || suspensions[0].ID != "repeat" {
		t.Fatalf("loop err = %v; want a suspension naming repeat", err)
	}
}
