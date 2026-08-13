package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestErrNilStepIsANilNode(t *testing.T) {
	if !errors.Is(workflow.ErrNilStep, flow.ErrNilNode) {
		t.Fatal("ErrNilStep does not match flow.ErrNilNode")
	}
	if errors.Is(flow.ErrNilNode, workflow.ErrNilStep) {
		t.Fatal("ErrNilNode unexpectedly matches the narrower ErrNilStep")
	}
}

func TestStructuredErrorFormatting(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "step", err: &workflow.StepError{ID: "load", Op: workflow.OpRun, Err: errors.New("boom")}, want: `step "load" run`},
		{
			name: "scoped step",
			err: &workflow.StepError{
				ID:    "load",
				Scope: indexedScope("items", 2),
				Op:    workflow.OpRun,
				Err:   errors.New("boom"),
			},
			want: `step "load" in items[2] run`,
		},
		{name: "ref missing", err: &workflow.RefError{Ref: workflow.Output("load"), Want: "int", Err: workflow.ErrNotFound}, want: "value not found"},
		{name: "ref nil", err: &workflow.RefError{Ref: workflow.Output("load"), Want: "int", Err: workflow.ErrTypeMismatch}, want: "got <nil>, want int"},
		{name: "ref mismatch", err: &workflow.RefError{Ref: workflow.Output("load"), Want: "int", Got: "string", Err: workflow.ErrTypeMismatch}, want: "got string, want int"},
		{name: "registration unnamed", err: &workflow.RegistrationError{Kind: "leaf", Err: workflow.ErrInvalidRegistration}, want: "register leaf:"},
		{name: "registration named", err: &workflow.RegistrationError{Kind: "leaf", Name: "add", Err: workflow.ErrDuplicateRegistration}, want: `leaf "add"`},
		{name: "graph path node field", err: &workflow.GraphError{Path: "/nodes/1", NodeID: "a", Field: "type", Err: workflow.ErrUnknownNodeType}, want: `graph at "/nodes/1" node "a" field type`},
		{name: "graph node", err: &workflow.GraphError{NodeID: "a", Err: workflow.ErrInvalidGraph}, want: `node "a":`},
		{name: "graph field", err: &workflow.GraphError{Field: "nodes", Err: workflow.ErrInvalidGraph}, want: "field nodes:"},
		// The location and the condition each appear once: the wrapper
		// identifies this package, so the sentinel does not repeat it.
		{name: "whole graph", err: &workflow.GraphError{Err: workflow.ErrCycle}, want: "workflow: graph: graph cycle"},
		{name: "spec", err: &workflow.SpecError{Path: "/steps/0", Kind: workflow.KindLeaf, ID: "a", Field: "type", Err: workflow.ErrUnknownNodeType}, want: `spec at "/steps/0" leaf "a" field type`},
		{name: "spec nil cause", err: &workflow.SpecError{}, want: "spec: <nil>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); !strings.Contains(got, tt.want) {
				t.Fatalf("Error() = %q; want substring %q", got, tt.want)
			}
		})
	}
}

func TestRefError(t *testing.T) {
	tests := []struct {
		name  string
		store workflow.Store
		want  error
	}{
		{name: "missing", want: workflow.ErrNotFound},
		{name: "type mismatch", store: workflow.NewStore().WithOutput("n", "text"), want: workflow.ErrTypeMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := workflow.Get[int](tt.store, workflow.Output("n"))
			var refErr *workflow.RefError
			if !errors.Is(err, tt.want) || !errors.As(err, &refErr) || refErr.Ref != workflow.Output("n") {
				t.Fatalf("err = %v; want RefError wrapping %v", err, tt.want)
			}
		})
	}
}

func TestRegistrationError(t *testing.T) {
	reg := workflow.NewRegistry()
	if err := reg.RegisterNode("add", addN()); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	err := reg.RegisterNode("add", addN())
	var registrationErr *workflow.RegistrationError
	if !errors.Is(err, workflow.ErrInvalidRegistration) ||
		!errors.Is(err, workflow.ErrDuplicateRegistration) ||
		!errors.As(err, &registrationErr) || registrationErr.Kind != "node" || registrationErr.Name != "add" {
		t.Fatalf("err = %v; want invalid-registration duplicate node RegistrationError", err)
	}
}

func TestGraphError(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("add", addN())
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "same", Type: "add"},
		{ID: "same", Type: "add"},
	}}
	err := reg.ValidateGraph(graph)
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.Is(err, workflow.ErrDuplicateNode) ||
		!errors.As(err, &graphErr) || graphErr.Path != "/nodes/1" ||
		graphErr.NodeID != "same" || graphErr.Field != "id" {
		t.Fatalf("err = %v; want invalid-graph duplicate node GraphError", err)
	}
}

func TestSpecError(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("add", addN())
	spec := workflow.Spec{Kind: workflow.KindParallel, Steps: []workflow.Spec{
		{Kind: workflow.KindLeaf, ID: "same", Type: "add"},
		{Kind: workflow.KindLeaf, ID: "same", Type: "add"},
	}}
	_, err := reg.CompileSpec(spec)
	var specErr *workflow.SpecError
	if !errors.Is(err, workflow.ErrInvalidSpec) ||
		!errors.Is(err, workflow.ErrDuplicateStep) ||
		!errors.As(err, &specErr) || specErr.ID != "same" || specErr.Field != "id" {
		t.Fatalf("err = %v; want invalid-spec duplicate step SpecError", err)
	}
}

// Every error this package surfaces names it exactly once. The structured error
// types and the top-level guards supply that prefix, so a sentinel must not
// carry one of its own; a cause from another package keeps its prefix, which is
// what marks a kernel failure inside a workflow error.
func TestSurfacedErrorsNamePackageExactlyOnce(t *testing.T) {
	registry := workflow.NewRegistry()
	registry.MustRegisterNode("n", workflow.Factory(
		func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](
				func(_ context.Context, value int) (int, error) { return value, nil },
			), nil
		},
	))
	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "a"}, 1); err != nil {
		t.Fatalf("Record: %v", err)
	}
	cyclic := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "n", Inputs: workflow.OneInput(workflow.Output("b"))},
		{ID: "b", Type: "n", Inputs: workflow.OneInput(workflow.Output("a"))},
	}}

	errs := map[string]error{
		"journal conflict": journal.Record(workflow.JournalKey{ID: "a"}, 1),
		"missing value": func() error {
			_, err := workflow.Get[int](workflow.NewStore(), workflow.Output("x"))
			return err
		}(),
		"type mismatch": func() error {
			_, err := workflow.Get[int](workflow.NewStore().WithOutput("x", "s"), workflow.Output("x"))
			return err
		}(),
		"duplicate registration": registry.RegisterNode("n", func(workflow.NodeSpec) (workflow.Step, error) {
			return nil, nil
		}),
		"nil factory":       workflow.NewRegistry().RegisterNode("n", nil),
		"graph cycle":       registry.ValidateGraph(cyclic),
		"unknown node type": registry.ValidateGraph(workflow.Graph{Nodes: []workflow.GraphNode{{ID: "a", Type: "?"}}}),
		"unwired port": registry.ValidateGraph(workflow.Graph{Nodes: []workflow.GraphNode{
			{ID: "a", Type: "n", Inputs: workflow.OneInput(workflow.Ref{})},
		}}),
		"invalid spec": registry.ValidateSpec(workflow.Spec{Kind: workflow.KindSequence, Type: "x"}),
		"unknown spec node type": registry.ValidateSpec(workflow.Spec{
			Kind: workflow.KindLeaf, ID: "x", Type: "?",
		}),
		"bad graph JSON":    workflow.ValidateGraphJSON([]byte(`{"nodes":[`)),
		"bad journal wire":  json.Unmarshal([]byte(`{"version":1,"records":[]}`), workflow.NewJournal()),
		"nil journal key":   (*workflow.JournalKey)(nil).UnmarshalJSON([]byte(`{"id":"a"}`)),
		"nil scope frame":   (*workflow.ScopeFrame)(nil).UnmarshalJSON([]byte(`{"id":"a"}`)),
		"nil step to Run":   runError(t, nil),
		"nil loop body":     runError(t, workflow.Loop(workflow.LoopConfig{ID: "l", Body: nil})),
		"duplicate step ID": runError(t, workflow.Sequence(passthrough("same"), passthrough("same"))),
		// The same conflict as "journal conflict", but reached during a run so a
		// StepError supplies the prefix instead of Record. The two paths format
		// separately, so both have to be checked.
		"journal conflict during a run": recordedDuringARun(t),
	}

	for name, err := range errs {
		t.Run(name, func(t *testing.T) {
			if err == nil {
				t.Fatal("expected an error")
			}
			message := err.Error()
			if got := strings.Count(message, "workflow:"); got != 1 {
				t.Fatalf("names %q %d times, want exactly 1: %s", "workflow:", got, message)
			}
		})
	}
}

func runError(t *testing.T, step workflow.Step) error {
	t.Helper()
	_, err := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{})
	return err
}

func passthrough(id string) workflow.Step {
	return workflow.LeafFunc(id, workflow.Output("seed"),
		func(_ context.Context, value int) (int, error) { return value, nil })
}

// recordedDuringARun reaches ErrJournalConflict the one way a single run can:
// a record created after the run began is deliberately not replayed, so the
// step it names still executes and then collides on its own identity. That is
// the hazard Journal documents for a host that admits two runs at once.
func recordedDuringARun(t *testing.T) error {
	t.Helper()
	journal := workflow.NewJournal()
	claimTarget := workflow.LeafFunc("claim", workflow.Output("seed"),
		func(_ context.Context, value int) (int, error) {
			if err := journal.Record(workflow.JournalKey{ID: "target"}, value); err != nil {
				t.Errorf("Record: %v", err)
			}
			return value, nil
		})
	_, err := workflow.Run(
		t.Context(),
		workflow.Sequence(claimTarget, passthrough("target")),
		workflow.NewStore().WithOutput("seed", 1),
		workflow.RunConfig{Journal: journal},
	)
	return err
}
