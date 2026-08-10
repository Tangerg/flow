package workflow_test

import (
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
		{name: "whole graph", err: &workflow.GraphError{Err: workflow.ErrCycle}, want: "graph: workflow: graph cycle"},
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
