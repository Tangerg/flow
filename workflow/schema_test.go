package workflow_test

import (
	"testing"

	"github.com/Tangerg/flow/workflow"
)

func TestValidate_compatible(t *testing.T) {
	reg := workflow.NewRegistry().
		RegisterLeaf("toNumber", addN()).
		RegisterSchema("toNumber", workflow.Schema{Input: workflow.TypeNumber, Output: workflow.TypeNumber})

	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "toNumber", Input: &workflow.Ref{NodeID: "start", Path: "output"}},
		{ID: "b", Type: "toNumber", Input: &workflow.Ref{NodeID: "a", Path: "output"}},
	}}

	if err := reg.Validate(g); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidate_incompatible(t *testing.T) {
	reg := workflow.NewRegistry().
		RegisterLeaf("num", addN()).
		RegisterLeaf("str", addN()).
		RegisterSchema("num", workflow.Schema{Input: workflow.TypeNumber, Output: workflow.TypeNumber}).
		RegisterSchema("str", workflow.Schema{Input: workflow.TypeString, Output: workflow.TypeString})

	// num.output (number) -> str.input (string): incompatible.
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "num", Input: &workflow.Ref{NodeID: "start", Path: "output"}},
		{ID: "b", Type: "str", Input: &workflow.Ref{NodeID: "a", Path: "output"}},
	}}

	if err := reg.Validate(g); err == nil {
		t.Fatal("expected incompatible-type error")
	}
}

func TestValidate_unknownType(t *testing.T) {
	reg := workflow.NewRegistry()
	g := workflow.Graph{Nodes: []workflow.NodeSpec{{ID: "a", Type: "nope"}}}
	if err := reg.Validate(g); err == nil {
		t.Fatal("expected unknown-type error")
	}
}

func TestValidate_cycle(t *testing.T) {
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", Input: &workflow.Ref{NodeID: "b", Path: "output"}},
		{ID: "b", Type: "addN", Input: &workflow.Ref{NodeID: "a", Path: "output"}},
	}}
	if err := reg.Validate(g); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestValidate_unregisteredSchemaIsAny(t *testing.T) {
	// No schemas registered: everything is TypeAny, so any wiring validates.
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", Input: &workflow.Ref{NodeID: "start", Path: "output"}},
		{ID: "b", Type: "addN", Input: &workflow.Ref{NodeID: "a", Path: "output"}},
	}}
	if err := reg.Validate(g); err != nil {
		t.Fatalf("Validate with no schemas should pass: %v", err)
	}
}

func TestRegisterSchema_rejectsInvalidType(t *testing.T) {
	reg := workflow.NewRegistry().RegisterSchema("bad", workflow.Schema{Input: workflow.ValueType("wat")})
	if reg.Err() == nil {
		t.Fatal("expected invalid schema type error")
	}
}
