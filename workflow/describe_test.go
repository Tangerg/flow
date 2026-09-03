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
		workflow.Ref{NodeID: "start", Path: "/output"}.Bind[int](),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
}

func TestDescribe_tree(t *testing.T) {
	step := workflow.Sequence(
		leafStep("a"),
		workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{leafStep("b"), leafStep("c")}}),
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

// newBorrowedDescriptionStep carries a Label at every level on purpose. The
// copy this package takes of a caller-defined description exists to own its
// storage, and a copy that quietly dropped a field would own less than it was
// given: only a built-in composite sets Label from a relationship it knows, so
// nothing else in these tests puts one on the borrowed path.
func newBorrowedDescriptionStep() *borrowedDescriptionStep {
	return &borrowedDescriptionStep{description: workflow.Description{
		ID:    "custom",
		Label: "custom label",
		Kind:  "custom",
		Children: []workflow.Description{{
			ID:    "nested",
			Label: "nested label",
			Kind:  "nested",
			Children: []workflow.Description{{
				ID:    "leaf",
				Label: "leaf label",
				Kind:  "leaf-like",
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

	// Ownership is not fidelity: a copy that dropped a field would pass every
	// assertion below, all of which mutate and re-read the one field they chose.
	for got, want := describe().Children[0], custom.description; ; {
		if got.ID != want.ID || got.Label != want.Label || got.Kind != want.Kind {
			t.Fatalf("copied node = %+v; want %+v", got, want)
		}
		if len(want.Children) == 0 {
			if len(got.Children) != 0 {
				t.Fatalf("copied node = %+v; want no children", got)
			}
			break
		}
		if len(got.Children) != len(want.Children) {
			t.Fatalf("copied node = %+v; want %d children", got, len(want.Children))
		}
		got, want = got.Children[0], want.Children[0]
	}

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

// A caller-defined Describer is allowed to return any Description tree; it is
// not a workflow definition and therefore does not pass through definition
// depth validation. Taking the ownership copy must spend heap, not one call
// frame per presentation node.
func TestDescribeDeepCallerTreeDoesNotSpendStackPerNode(t *testing.T) {
	withBoundedStack(t, func() {
		const depth = 20_000
		root := workflow.Description{Kind: "root"}
		cursor := &root
		for range depth {
			cursor.Children = []workflow.Description{{Kind: "child"}}
			cursor = &cursor.Children[0]
		}

		description := workflow.Describe(&borrowedDescriptionStep{description: root})
		got := 0
		for len(description.Children) != 0 {
			description = description.Children[0]
			got++
		}
		if got != depth {
			t.Fatalf("description depth = %d; want %d", got, depth)
		}
	})
}

// TestDescribeNormalizesCallerCyclesWithoutCollapsingSharedSubtrees keeps the
// public result a finite, independently mutable tree even when a caller-defined
// Describer returns slice topology that a Description tree cannot express.
func TestDescribeNormalizesCallerCyclesWithoutCollapsingSharedSubtrees(t *testing.T) {
	t.Run("cycle", func(t *testing.T) {
		children := make([]workflow.Description, 1)
		root := workflow.Description{ID: "root", Kind: "root", Children: children}
		children[0] = workflow.Description{ID: "cycle", Kind: "cycle", Children: children}

		got := workflow.Describe(&borrowedDescriptionStep{description: root})
		if len(got.Children) != 1 || got.Children[0].ID != "cycle" ||
			len(got.Children[0].Children) != 0 {
			t.Fatalf("Describe = %+v; want the repeated node preserved as a leaf", got)
		}
		got.Children[0].ID = "changed"
		if children[0].ID != "cycle" {
			t.Fatalf("source cycle = %+v; cloned tree retained its storage", children[0])
		}
	})

	// A descendant whose Children is a shorter view of the same array is not the
	// slice its ancestor is walking, and its own node is genuinely there. Keying
	// an active slice by its first element alone would read this as the cycle it
	// is not, and drop a level. The two cases below cannot show that: a cycle
	// repeats the whole slice, and siblings are never inside one at the same time.
	t.Run("overlapping prefix", func(t *testing.T) {
		shared := make([]workflow.Description, 2)
		shared[0] = workflow.Description{ID: "first", Kind: "first", Children: shared[:1]}
		shared[1] = workflow.Description{ID: "second", Kind: "second"}
		root := workflow.Description{ID: "root", Kind: "root", Children: shared[:2]}

		got := workflow.Describe(&borrowedDescriptionStep{description: root})
		if len(got.Children) != 2 ||
			got.Children[0].ID != "first" || got.Children[1].ID != "second" {
			t.Fatalf("Describe = %+v; want both members of the outer view", got)
		}
		repeated := got.Children[0].Children
		if len(repeated) != 1 || repeated[0].ID != "first" ||
			len(repeated[0].Children) != 0 {
			t.Fatalf("Describe = %+v; want the shorter view walked once, then kept as a leaf", got)
		}
	})

	t.Run("shared subtree", func(t *testing.T) {
		shared := []workflow.Description{{ID: "leaf", Kind: "leaf"}}
		root := workflow.Description{Kind: "root", Children: []workflow.Description{
			{Kind: "left", Children: shared},
			{Kind: "right", Children: shared},
		}}

		got := workflow.Describe(&borrowedDescriptionStep{description: root})
		if len(got.Children) != 2 || len(got.Children[0].Children) != 1 ||
			len(got.Children[1].Children) != 1 {
			t.Fatalf("Describe = %+v; want both shared-subtree occurrences", got)
		}
		got.Children[0].Children[0].ID = "changed"
		if got.Children[1].Children[0].ID != "leaf" || shared[0].ID != "leaf" {
			t.Fatalf("Describe = %+v, source = %+v; shared storage crossed the clone", got, shared)
		}
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
		workflow.Output("start").Bind[int](),
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
	step := workflow.Branch(workflow.BranchConfig{
		ID:       "route",
		Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) { return "yes", nil }),
		Cases:    map[string]workflow.Step{"yes": leafStep("actual-id")},
	})

	d := workflow.Describe(step)
	if len(d.Children) != 1 || d.Children[0].ID != "actual-id" || d.Children[0].Label != "yes" {
		t.Fatalf("branch child = %+v", d.Children)
	}
}

func TestBranchDescriptionOrdersCasesByName(t *testing.T) {
	step := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) { return "a", nil }), Cases: map[string]workflow.Step{
		"z": leafStep("last"),
		"a": leafStep("first"),
		"m": leafStep("middle"),
	}})

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

	branch := workflow.Branch(workflow.BranchConfig{
		ID:       "route",
		Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) { return "approved", nil }),
		Cases:    map[string]workflow.Step{"approved": wait},
	})

	description := workflow.Describe(branch)
	if len(description.Children) != 1 || description.Children[0].Label != "approved" {
		t.Fatalf("branch description = %+v; want the case as the child relationship", description)
	}
}

func TestDescribe_everyCompositeReportsItsID(t *testing.T) {
	yes := resolverNode(func(context.Context, workflow.Store) (string, error) { return "yes", nil })
	stop := flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) { return true, nil })

	steps := map[workflow.Kind]workflow.Step{
		workflow.KindLeaf: leafStep("leaf"),
		workflow.KindBranch: workflow.Branch(workflow.BranchConfig{
			ID:       "branch",
			Resolver: yes,
			Cases:    map[string]workflow.Step{"yes": leafStep("y")},
		}),
		workflow.KindLoop: workflow.Loop(workflow.LoopConfig{
			ID:        "loop",
			Body:      leafStep("body"),
			Condition: stop,
		}),
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
		workflow.KindParallel: workflow.Parallel(workflow.ParallelConfig{
			Steps: []workflow.Step{leafStep("a")},
		}),
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
	stop := flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) { return true, nil })
	body := workflow.Leaf("b", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	for kind, step := range map[string]workflow.Step{
		"branch": workflow.Branch(workflow.BranchConfig{
			ID:       "",
			Resolver: yes,
			Cases:    map[string]workflow.Step{"yes": leafStep("y")},
		}),
		"loop": workflow.Loop(workflow.LoopConfig{ID: "", Body: body, Condition: stop}),
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
	_, err := workflow.Branch(workflow.BranchConfig{
		ID:       "route",
		Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) { return "", boom }),
		Cases:    map[string]workflow.Step{"a": leafStep("a")},
	}).
		Run(t.Context(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "route" || !errors.Is(err, boom) {
		t.Fatalf("branch err = %v; want a StepError for route wrapping boom", err)
	}

	_, err = workflow.Loop(workflow.LoopConfig{
		ID:        "repeat",
		Body:      body,
		Condition: flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) { return false, boom }),
	}).
		Run(t.Context(), workflow.NewStore())
	if !errors.As(err, &stepErr) || stepErr.ID != "repeat" || !errors.Is(err, boom) {
		t.Fatalf("loop err = %v; want a StepError for repeat wrapping boom", err)
	}

	// A resolver or condition may also suspend, which is not a step error.
	_, err = workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
		return "", workflow.Suspend("routing needs a person")
	}), Cases: map[string]workflow.Step{"a": leafStep("a")}}).
			Run(t.Context(), workflow.NewStore())
	if suspensions := workflow.Suspensions(err); len(suspensions) != 1 || suspensions[0].ID != "route" {
		t.Fatalf("branch err = %v; want a suspension naming route", err)
	}

	_, err = workflow.Loop(workflow.LoopConfig{ID: "repeat", Body: body, Condition: flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) {
		return false, workflow.Suspend("deciding needs a person")
	})}).
		Run(t.Context(), workflow.NewStore())
	if suspensions := workflow.Suspensions(err); len(suspensions) != 1 || suspensions[0].ID != "repeat" {
		t.Fatalf("loop err = %v; want a suspension naming repeat", err)
	}
}

// TestDescribe_reportsTheBodyOfEveryCompositeThatHasOne pins what a rendering
// tool reads. A composite holding one body reports it as its single child, and
// only Iteration and Subgraph project a body output, so those two assemble the
// child list themselves rather than describing a slice they already hold.
func TestDescribe_reportsTheBodyOfEveryCompositeThatHasOne(t *testing.T) {
	body := leafStep("inner")
	composites := map[string]workflow.Step{
		"iteration": workflow.Iteration(workflow.IterationConfig{
			ID:         "each",
			Input:      workflow.Output("items"),
			Body:       body,
			BodyOutput: workflow.Output("inner"),
		}),
		"subgraph": workflow.Subgraph(workflow.SubgraphConfig{
			ID:         "sealed",
			Body:       body,
			BodyOutput: workflow.Output("inner"),
		}),
		"loop": workflow.Loop(workflow.LoopConfig{ID: "repeat", Body: body}),
	}
	for name, composite := range composites {
		t.Run(name, func(t *testing.T) {
			described := workflow.Describe(composite)
			if len(described.Children) != 1 || described.Children[0].ID != "inner" {
				t.Fatalf("Describe = %+v; want the body as its only child", described)
			}
		})
	}
}
