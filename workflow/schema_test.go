package workflow_test

import (
	"testing"

	"github.com/Tangerg/flow/workflow"
)

func TestValidateGraph_compatible(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterLeaf("toNumber", addN()).
		MustRegisterSchema("toNumber", workflow.Schema{Input: workflow.TypeNumber, Output: workflow.TypeNumber})

	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "toNumber", Input: &workflow.Ref{NodeID: "start", Path: "output"}},
		{ID: "b", Type: "toNumber", Input: &workflow.Ref{NodeID: "a", Path: "output"}},
	}}

	if err := reg.ValidateGraph(g); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateGraph_incompatible(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterLeaf("num", addN()).
		MustRegisterLeaf("str", addN()).
		MustRegisterSchema("num", workflow.Schema{Input: workflow.TypeNumber, Output: workflow.TypeNumber}).
		MustRegisterSchema("str", workflow.Schema{Input: workflow.TypeString, Output: workflow.TypeString})

	// num.output (number) -> str.input (string): incompatible.
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "num", Input: &workflow.Ref{NodeID: "start", Path: "output"}},
		{ID: "b", Type: "str", Input: &workflow.Ref{NodeID: "a", Path: "output"}},
	}}

	if err := reg.ValidateGraph(g); err == nil {
		t.Fatal("expected incompatible-type error")
	}
}

func TestValidateGraph_unknownType(t *testing.T) {
	reg := workflow.NewRegistry()
	g := workflow.Graph{Nodes: []workflow.NodeSpec{{ID: "a", Type: "nope"}}}
	if err := reg.ValidateGraph(g); err == nil {
		t.Fatal("expected unknown-type error")
	}
}

func TestValidateGraph_cycle(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", Input: &workflow.Ref{NodeID: "b", Path: "output"}},
		{ID: "b", Type: "addN", Input: &workflow.Ref{NodeID: "a", Path: "output"}},
	}}
	if err := reg.ValidateGraph(g); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestValidateGraph_unregisteredSchemaIsAny(t *testing.T) {
	// No schemas registered: everything is TypeAny, so any wiring validates.
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", Input: &workflow.Ref{NodeID: "start", Path: "output"}},
		{ID: "b", Type: "addN", Input: &workflow.Ref{NodeID: "a", Path: "output"}},
	}}
	if err := reg.ValidateGraph(g); err != nil {
		t.Fatalf("Validate with no schemas should pass: %v", err)
	}
}

func TestRegisterSchema_rejectsInvalidType(t *testing.T) {
	reg := workflow.NewRegistry()
	if err := reg.RegisterSchema("bad", workflow.Schema{Input: workflow.ValueType("wat")}); err == nil {
		t.Fatal("invalid schema type unexpectedly succeeded")
	}
}
