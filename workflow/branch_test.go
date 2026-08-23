package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func resolverNode(
	fn func(context.Context, workflow.Store) (string, error),
) workflow.Resolver {
	return flow.NodeFunc[workflow.Store, string](fn)
}

type nilSafeResolver struct{}

func (*nilSafeResolver) Run(context.Context, workflow.Store) (string, error) {
	return "case", nil
}

func TestBranch_routes(t *testing.T) {
	label := func(text string) workflow.Step {
		return workflow.Leaf(text,
			workflow.Ref{NodeID: "start", Path: "/output"}.Bind[int](),
			flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return text, nil }),
		)
	}
	cases := map[string]workflow.Step{"pos": label("pos"), "neg": label("neg")}

	resolve := resolverNode(func(_ context.Context, s workflow.Store) (string, error) {
		v, _ := s.Lookup(workflow.At("start", "output"))
		if v.(int) >= 0 {
			return "pos", nil
		}
		return "neg", nil
	})
	b := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolve, Cases: cases})

	out, err := b.Run(t.Context(), workflow.NewStore().WithOutput("start", 5))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("pos")); !ok || v.(string) != "pos" {
		t.Fatalf("expected pos branch to run, got %v, %v", v, ok)
	}
	if _, ok := out.Lookup(workflow.Output("neg")); ok {
		t.Fatal("neg branch should not have run")
	}
}

func TestBranch_ownsCaseMapStructure(t *testing.T) {
	cases := map[string]workflow.Step{"selected": leafStep("original")}
	branch := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
		return "selected", nil
	}), Cases: cases})

	cases["selected"] = leafStep("changed")

	out, err := branch.Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, getErr := out.Get[int](workflow.Output("original")); getErr != nil || got != 1 {
		t.Fatalf("original output = %d, %v; want 1, nil", got, getErr)
	}
	if _, ok := out.Lookup(workflow.Output("changed")); ok {
		t.Fatal("source-map mutation reconfigured Branch")
	}
}

func TestBranch_acceptsComposedResolverNode(t *testing.T) {
	read := flow.NodeFunc[workflow.Store, int](
		func(_ context.Context, store workflow.Store) (int, error) {
			return store.Get[int](workflow.Output("start"))
		},
	)
	classify := flow.NodeFunc[int, string](
		func(_ context.Context, value int) (string, error) {
			if value >= 0 {
				return "positive", nil
			}
			return "negative", nil
		},
	)
	step := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: flow.Then(read, classify), Cases: map[string]workflow.Step{
		"positive": workflow.Sequence(),
		"negative": workflow.Interrupt("negative", nil),
	}})

	if _, err := step.Run(
		t.Context(),
		workflow.NewStore().WithOutput("start", 1),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// A branch's cases are mutually exclusive, so they may share a step ID. That is
// how a branch converges on a single output reference a downstream step can read
// without knowing which case ran.
func TestValidateSpec_branchCasesMayShareAStepID(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterResolver("pick", resolverNode(func(context.Context, workflow.Store) (string, error) { return "a", nil }))

	leaf := func(n string) workflow.Spec {
		return workflow.Spec{
			Kind: workflow.KindLeaf, ID: "out", Type: "addN",
			Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
			Config: json.RawMessage(`{"n":` + n + `}`),
		}
	}
	spec := workflow.Spec{
		Kind: workflow.KindSequence,
		Steps: []workflow.Spec{
			{
				Kind:     workflow.KindBranch,
				ID:       "route",
				Resolver: "pick",
				Cases:    map[string]workflow.Spec{"a": leaf("1"), "b": leaf("2")},
			},
			// Reads the branch's output without knowing which case produced it.
			{
				Kind: workflow.KindLeaf, ID: "after", Type: "addN",
				Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("out")},
				Config: json.RawMessage(`{"n":10}`),
			},
		},
	}

	step, err := reg.CompileSpec(spec)
	if err != nil {
		t.Fatalf("CompileSpec: %v", err)
	}
	out, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", 5))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, err := out.Get[int](workflow.Output("after")); err != nil || v != 16 {
		t.Fatalf("after = %v, %v; want 16", v, err) // (5+1) + 10
	}
}

func TestValidateSpec_stillRejectsIDsThatCanCollide(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterResolver("pick", resolverNode(func(context.Context, workflow.Store) (string, error) { return "a", nil }))

	leaf := func(id string) workflow.Spec {
		return workflow.Spec{
			Kind:   workflow.KindLeaf,
			ID:     id,
			Type:   "addN",
			Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
		}
	}
	branch := func(cases map[string]workflow.Spec) workflow.Spec {
		return workflow.Spec{Kind: workflow.KindBranch, ID: "route", Resolver: "pick", Cases: cases}
	}

	specs := map[string]workflow.Spec{
		"parallel siblings": {Kind: workflow.KindParallel, Steps: []workflow.Spec{leaf("x"), leaf("x")}},
		"sequence siblings": {Kind: workflow.KindSequence, Steps: []workflow.Spec{leaf("x"), leaf("x")}},
		"within one case": branch(map[string]workflow.Spec{
			"a": {Kind: workflow.KindSequence, Steps: []workflow.Spec{leaf("x"), leaf("x")}},
		}),
		"case and an outer step before it": {Kind: workflow.KindSequence, Steps: []workflow.Spec{
			leaf("x"),
			branch(map[string]workflow.Spec{"a": leaf("x")}),
		}},
		"case and an outer step after it": {Kind: workflow.KindSequence, Steps: []workflow.Spec{
			branch(map[string]workflow.Spec{"a": leaf("x")}),
			leaf("x"),
		}},
		"case and a concurrent step": {Kind: workflow.KindParallel, Steps: []workflow.Spec{
			branch(map[string]workflow.Spec{"a": leaf("x")}),
			leaf("x"),
		}},
	}

	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			if err := reg.ValidateSpec(spec); !errors.Is(err, workflow.ErrDuplicateStep) {
				t.Fatalf("err = %v; want ErrDuplicateStep", err)
			}
		})
	}
}

func TestBranch_noCase(t *testing.T) {
	resolve := resolverNode(func(_ context.Context, _ workflow.Store) (string, error) { return "missing", nil })
	cases := map[string]workflow.Step{
		"present": workflow.Interrupt("present", nil),
	}

	_, err := workflow.Branch(workflow.BranchConfig{
		ID:       "route",
		Resolver: resolve,
		Cases:    cases,
	}).Run(t.Context(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.Is(err, flow.ErrNoCase) ||
		!errors.As(err, &stepErr) ||
		stepErr.ID != "route" ||
		stepErr.Op != workflow.OpRun {
		t.Fatalf("error = %v, want route StepError wrapping flow.ErrNoCase", err)
	}
}

func TestBranch_rejectsEmptyCasesBeforeRunningResolver(t *testing.T) {
	ran := false
	resolve := resolverNode(func(_ context.Context, _ workflow.Store) (string, error) {
		ran = true
		return "missing", nil
	})

	_, err := workflow.Branch(workflow.BranchConfig{
		ID:       "route",
		Resolver: resolve,
		Cases:    nil,
	}).Run(t.Context(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.Is(err, flow.ErrInvalidConfig) ||
		!errors.As(err, &stepErr) || stepErr.Op != workflow.OpValidate {
		t.Fatalf("error = %v; want validation StepError wrapping ErrInvalidConfig", err)
	}
	if ran {
		t.Fatal("resolver ran before the empty case set was rejected")
	}
}

func TestBranch_rejectsEmptyCaseNameBeforeRunningResolver(t *testing.T) {
	ran := false
	resolve := resolverNode(func(_ context.Context, _ workflow.Store) (string, error) {
		ran = true
		return "", nil
	})

	_, err := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolve, Cases: map[string]workflow.Step{
		"": workflow.Interrupt("result", nil),
	}}).
		Run(t.Context(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.Is(err, flow.ErrInvalidConfig) ||
		!errors.As(err, &stepErr) || stepErr.Op != workflow.OpValidate {
		t.Fatalf("error = %v; want validation StepError wrapping ErrInvalidConfig", err)
	}
	if ran {
		t.Fatal("resolver ran before the empty case name was rejected")
	}
}

func TestBranch_rejectsNonUTF8CaseNameBeforeRunningResolver(t *testing.T) {
	ran := false
	resolve := resolverNode(func(_ context.Context, _ workflow.Store) (string, error) {
		ran = true
		return "", nil
	})
	invalid := string([]byte{0xff})

	_, err := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolve, Cases: map[string]workflow.Step{
		invalid: workflow.Interrupt("result", nil),
	}}).
		Run(t.Context(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.Op != workflow.OpValidate {
		t.Fatalf("error = %v; want validation StepError", err)
	}
	if ran {
		t.Fatal("resolver ran before the non-UTF-8 case name was rejected")
	}
}

func TestBranch_doesNotJournalAnUnknownDecision(t *testing.T) {
	var calls atomic.Int64
	resolve := resolverNode(func(_ context.Context, _ workflow.Store) (string, error) {
		if calls.Add(1) == 1 {
			return "missing", nil
		}
		return "ready", nil
	})
	journal := workflow.NewJournal()
	cfg := workflow.RunConfig{Journal: journal}
	branch := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolve, Cases: map[string]workflow.Step{
		"ready": leafStep("ready"),
	}})

	input := workflow.NewStore().WithOutput("start", 1)

	if _, err := workflow.Run(t.Context(), branch, input, cfg); !errors.Is(err, flow.ErrNoCase) {
		t.Fatalf("first run err = %v; want ErrNoCase", err)
	}
	if journal.Len() != 0 {
		t.Fatalf("unknown decision polluted Journal: %v", journal.Keys())
	}
	if _, err := workflow.Run(t.Context(), branch, input, cfg); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("resolver calls = %d; want 2", calls.Load())
	}
}

func TestBranch_doesNotJournalANilCase(t *testing.T) {
	journal := workflow.NewJournal()
	cfg := workflow.RunConfig{Journal: journal}
	var calls atomic.Int64
	branch := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
		calls.Add(1)
		return "broken", nil
	}), Cases: map[string]workflow.Step{"broken": nil}})

	if _, err := workflow.Run(t.Context(), branch, workflow.NewStore(), cfg); !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
	if journal.Len() != 0 {
		t.Fatalf("nil case polluted Journal: %v", journal.Keys())
	}
	if calls.Load() != 0 {
		t.Fatalf("resolver ran %d times; want 0", calls.Load())
	}
}

func TestBranch_rejectsTypedNilFunctionCaseBeforeResolver(t *testing.T) {
	var calls atomic.Int64
	var invalid flow.NodeFunc[workflow.Store, workflow.Store]
	branch := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
		calls.Add(1)
		return "broken", nil
	}), Cases: map[string]workflow.Step{"broken": invalid}})

	_, err := branch.Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("resolver ran %d times; want 0", calls.Load())
	}
}

func TestBranch_nilResolver(t *testing.T) {
	_, err := workflow.Branch(workflow.BranchConfig{
		ID:       "route",
		Resolver: nil,
		Cases:    map[string]workflow.Step{"x": leafStep("x")},
	}).
		Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "route" || stepErr.Op != workflow.OpValidate {
		t.Fatalf("err = %v; want route validation StepError", err)
	}
}

func TestBranch_acceptsNilSafePointerResolver(t *testing.T) {
	var resolve *nilSafeResolver
	_, err := workflow.Branch(workflow.BranchConfig{
		ID:       "route",
		Resolver: resolve,
		Cases:    map[string]workflow.Step{"case": workflow.Sequence()},
	}).
		Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestBranch_validatesComposedResolverBeforeJournalReplay(t *testing.T) {
	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "route"}, "case"); err != nil {
		t.Fatalf("Record: %v", err)
	}
	var calls int
	first := flow.NodeFunc[workflow.Store, int](
		func(context.Context, workflow.Store) (int, error) {
			calls++
			return 1, nil
		},
	)
	var second flow.NodeFunc[int, string]
	branch := workflow.Branch(workflow.BranchConfig{
		ID:       "route",
		Resolver: flow.Then(first, second),
		Cases:    map[string]workflow.Step{"case": leafStep("case")},
	})

	_, err := workflow.Run(
		t.Context(),
		branch,
		workflow.NewStore(),
		workflow.RunConfig{Journal: journal},
	)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("Run error = %v; want ErrNilNode before Journal replay", err)
	}
	if calls != 0 {
		t.Fatalf("first resolver node calls = %d; want 0", calls)
	}
}

func TestBranch_rejectsInvalidStaticIdentities(t *testing.T) {
	leaf := func(id string) workflow.Step {
		return workflow.Leaf(
			id,
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				return 1, nil
			}),
		)
	}
	resolve := resolverNode(func(context.Context, workflow.Store) (string, error) {
		return "case", nil
	})
	tests := map[string]workflow.Step{
		"branch ID collides before branch": workflow.Sequence(
			leaf("route"),
			workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolve, Cases: map[string]workflow.Step{
				"case": leaf("out"),
			}}),
		),
		"case collides with branch ID": workflow.Branch(workflow.BranchConfig{
			ID:       "route",
			Resolver: resolve,
			Cases:    map[string]workflow.Step{"case": leaf("route")},
		}),
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

// Cases may reuse an ID with one another, since only one of them runs, but not
// with the branch around them. Running the branch reports that too -- the run
// claims each ID as it reaches it -- which leaves the definition check observable
// only before execution. That is where a compiled workflow asks, so it is where
// the rule has to hold.
func TestBranch_rejectsACaseIDCollisionBeforeRunning(t *testing.T) {
	step := workflow.Branch(workflow.BranchConfig{
		ID: "route",
		Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
			return "case", nil
		}),
		Cases: map[string]workflow.Step{"case": workflow.Interrupt("route", nil)},
	})

	if err := flow.Validate(step); !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("Validate error = %v; want ErrDuplicateStep", err)
	}
}

func TestBranch_rejectsDuplicateOpaqueInvocation(t *testing.T) {
	branch := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) { return "ok", nil }), Cases: map[string]workflow.Step{
		"ok": flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				return store, nil
			},
		),
	}})

	twice := flow.NodeFunc[workflow.Store, workflow.Store](
		func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
			next, err := branch.Run(ctx, store)
			if err != nil {
				return next, err
			}
			return branch.Run(ctx, next)
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

func TestBranch_preservesTheSelectedCaseStoreOnError(t *testing.T) {
	boom := errors.New("case failed")
	branch := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
		return "selected", nil
	}), Cases: map[string]workflow.Step{
		"selected": flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				return store.WithOutput("partial", true), boom
			},
		),
	}})

	output, err := branch.Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v; want case failure", err)
	}
	value, getErr := output.Get[bool](workflow.Output("partial"))
	if getErr != nil || !value {
		t.Fatalf("partial output = %v, %v; want true, nil", value, getErr)
	}
}

func TestBranch_reportsJournalDecisionConflict(t *testing.T) {
	journal := workflow.NewJournal()
	branch := workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
		if err := journal.Record(workflow.JournalKey{ID: "route"}, "ok"); err != nil {
			return "", err
		}
		return "ok", nil
	}), Cases: map[string]workflow.Step{"ok": leafStep("ok")}})

	_, err := workflow.Run(
		t.Context(),
		branch,
		workflow.NewStore().WithOutput("start", 1),
		workflow.RunConfig{Journal: journal},
	)
	if !errors.Is(err, workflow.ErrJournalConflict) {
		t.Fatalf("error = %v; want ErrJournalConflict", err)
	}
}
