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

func TestBranch_routes(t *testing.T) {
	label := func(text string) workflow.Step {
		return workflow.Leaf(text,
			workflow.From[int](workflow.Ref{NodeID: "start", Path: "/output"}),
			flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return text, nil }),
		)
	}
	cases := map[string]workflow.Step{"pos": label("pos"), "neg": label("neg")}

	resolve := func(_ context.Context, s workflow.Store) (string, error) {
		v, _ := s.Lookup(workflow.At("start", "output"))
		if v.(int) >= 0 {
			return "pos", nil
		}
		return "neg", nil
	}
	b := workflow.Branch("route", resolve, cases)

	out, err := b.Run(context.Background(), workflow.NewStore().WithOutput("start", 5))
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

// A branch's cases are mutually exclusive, so they may share a step ID. That is
// how a branch converges on a single output reference a downstream step can read
// without knowing which case ran.
func TestValidateSpec_branchCasesMayShareAStepID(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterLeaf("addN", addN()).
		MustRegisterResolver("pick", func(context.Context, workflow.Store) (string, error) { return "a", nil })

	leaf := func(n string) workflow.Spec {
		return workflow.Spec{
			Kind: workflow.KindLeaf, ID: "out", Type: "addN",
			Input:  refPtr(workflow.Output("start")),
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
				Input:  refPtr(workflow.Output("out")),
				Config: json.RawMessage(`{"n":10}`),
			},
		},
	}

	step, err := reg.CompileSpec(spec)
	if err != nil {
		t.Fatalf("CompileSpec: %v", err)
	}
	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", 5))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, err := workflow.Get[int](out, workflow.Output("after")); err != nil || v != 16 {
		t.Fatalf("after = %v, %v; want 16", v, err) // (5+1) + 10
	}
}

func TestValidateSpec_stillRejectsIDsThatCanCollide(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterLeaf("addN", addN()).
		MustRegisterResolver("pick", func(context.Context, workflow.Store) (string, error) { return "a", nil })

	leaf := func(id string) workflow.Spec {
		return workflow.Spec{Kind: workflow.KindLeaf, ID: id, Type: "addN", Input: refPtr(workflow.Output("start"))}
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
	resolve := func(_ context.Context, _ workflow.Store) (string, error) { return "missing", nil }

	_, err := workflow.Branch("route", resolve, map[string]workflow.Step{}).Run(context.Background(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.Is(err, flow.ErrNoCase) ||
		!errors.As(err, &stepErr) ||
		stepErr.ID != "route" ||
		stepErr.Op != workflow.OpRun {
		t.Fatalf("error = %v, want route StepError wrapping flow.ErrNoCase", err)
	}
}

func TestBranch_doesNotJournalAnUnknownDecision(t *testing.T) {
	var calls atomic.Int64
	resolve := func(_ context.Context, _ workflow.Store) (string, error) {
		if calls.Add(1) == 1 {
			return "missing", nil
		}
		return "ready", nil
	}
	journal := workflow.NewJournal()
	cfg := workflow.RunConfig{Journal: journal}
	branch := workflow.Branch("route", resolve, map[string]workflow.Step{
		"ready": leafStep("ready"),
	})
	input := workflow.NewStore().WithOutput("start", 1)

	if _, err := workflow.Run(context.Background(), branch, input, cfg); !errors.Is(err, flow.ErrNoCase) {
		t.Fatalf("first run err = %v; want ErrNoCase", err)
	}
	if journal.Len() != 0 {
		t.Fatalf("unknown decision polluted Journal: %v", journal.Keys())
	}
	if _, err := workflow.Run(context.Background(), branch, input, cfg); err != nil {
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
	branch := workflow.Branch(
		"route",
		func(context.Context, workflow.Store) (string, error) {
			calls.Add(1)
			return "broken", nil
		},
		map[string]workflow.Step{"broken": nil},
	)

	if _, err := workflow.Run(context.Background(), branch, workflow.NewStore(), cfg); !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
	if journal.Len() != 0 {
		t.Fatalf("nil case polluted Journal: %v", journal.Keys())
	}
	if calls.Load() != 0 {
		t.Fatalf("resolver ran %d times; want 0", calls.Load())
	}
}

func TestBranch_nilResolver(t *testing.T) {
	_, err := workflow.Branch("route", nil, map[string]workflow.Step{"x": leafStep("x")}).
		Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("err = %v; want ErrNilFunc", err)
	}
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "route" || stepErr.Op != workflow.OpValidate {
		t.Fatalf("err = %v; want route validation StepError", err)
	}
}
