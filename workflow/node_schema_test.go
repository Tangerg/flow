package workflow_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/Tangerg/flow/workflow"
)

func TestValidateGraph_compatible(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("toNumber", addN()).
		MustRegisterSchema("toNumber", workflow.NodeSchema{Inputs: workflow.OnePort(workflow.TypeNumber), Output: workflow.TypeNumber})

	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "toNumber", Input: workflow.Output("start")},
		{ID: "b", Type: "toNumber", Input: workflow.Output("a")},
	}}

	if err := registry.ValidateGraph(g); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateGraph_incompatible(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("num", addN()).
		MustRegisterNode("str", addN()).
		MustRegisterSchema("num", workflow.NodeSchema{Inputs: workflow.OnePort(workflow.TypeNumber), Output: workflow.TypeNumber}).
		MustRegisterSchema("str", workflow.NodeSchema{Inputs: workflow.OnePort(workflow.TypeString), Output: workflow.TypeString})

	// num.output (number) -> str.input (string): incompatible.
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "num", Input: workflow.Output("start")},
		{ID: "b", Type: "str", Input: workflow.Output("a")},
	}}

	if err := reg.ValidateGraph(g); err == nil {
		t.Fatal("expected incompatible-type error")
	}
}

func TestValidateGraph_doesNotGuessNestedOutputTypes(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("object", addN()).
		MustRegisterNode("string", addN()).
		MustRegisterSchema("object", workflow.NodeSchema{Output: workflow.TypeObject}).
		MustRegisterSchema("string", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeString),
			Output: workflow.TypeString,
		})

	nested := workflow.Output("a").Child("name")
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "object"},
		{ID: "b", Type: "string", Input: nested},
	}}
	if err := reg.ValidateGraph(g); err != nil {
		t.Fatalf("nested output member has no declared type and should remain TypeAny: %v", err)
	}
}

func TestValidateGraph_unknownType(t *testing.T) {
	reg := workflow.NewRegistry()
	g := workflow.Graph{Nodes: []workflow.GraphNode{{ID: "a", Type: "nope"}}}
	if err := reg.ValidateGraph(g); err == nil {
		t.Fatal("expected unknown-type error")
	}
}

func TestValidateGraph_cycle(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", Input: workflow.Output("b")},
		{ID: "b", Type: "addN", Input: workflow.Output("a")},
	}}
	if err := reg.ValidateGraph(g); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestValidateGraph_unregisteredSchemaIsAny(t *testing.T) {
	// No schemas registered: everything is TypeAny, so any wiring validates.
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", Input: workflow.Output("start")},
		{ID: "b", Type: "addN", Input: workflow.Output("a")},
	}}
	if err := reg.ValidateGraph(g); err != nil {
		t.Fatalf("Validate with no schemas should pass: %v", err)
	}
}

func TestRegisterSchema_rejectsInvalidPortsAndTypes(t *testing.T) {
	schemas := map[string]workflow.NodeSchema{
		"bad output":    {Output: workflow.ValueType("wat")},
		"bad port type": {Inputs: workflow.OnePort(workflow.ValueType("wat"))},
		"empty port":    {Inputs: workflow.Ports{"": workflow.TypeNumber}},
	}
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			if err := workflow.NewRegistry().RegisterSchema("bad", schema); !errors.Is(err, workflow.ErrInvalidRegistration) {
				t.Fatalf("err = %v; want ErrInvalidRegistration", err)
			}
		})
	}
}

func TestRegisterSchema_rejectsEmptyAndDuplicateNames(t *testing.T) {
	reg := workflow.NewRegistry()
	if err := reg.RegisterSchema("", workflow.NodeSchema{}); !errors.Is(err, workflow.ErrInvalidRegistration) {
		t.Fatalf("empty name error = %v; want ErrInvalidRegistration", err)
	}
	if err := reg.RegisterSchema("node", workflow.NodeSchema{}); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := reg.RegisterSchema("node", workflow.NodeSchema{}); !errors.Is(err, workflow.ErrDuplicateRegistration) {
		t.Fatalf("duplicate error = %v; want ErrDuplicateRegistration", err)
	}
}

func TestMustRegisterSchema_panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustRegisterSchema did not panic")
		}
	}()
	workflow.NewRegistry().MustRegisterSchema("", workflow.NodeSchema{})
}

func TestRegistry_introspection(t *testing.T) {
	configSchema := json.RawMessage(`{"type":"object"}`)
	reg := workflow.NewRegistry().
		MustRegisterNode("sum", sumPorts()).
		MustRegisterNode("addN", addN()).
		MustRegisterSchema("sum", workflow.NodeSchema{
			Inputs:       workflow.Ports{"a": workflow.TypeNumber, "b": workflow.TypeNumber},
			Output:       workflow.TypeNumber,
			ConfigSchema: configSchema,
		})

	if got := reg.NodeTypes(); !slices.Equal(got, []string{"addN", "sum"}) {
		t.Fatalf("NodeTypes = %v; want [addN sum]", got)
	}
	if _, ok := reg.NodeSchema("addN"); ok {
		t.Fatal("NodeSchema reported a schema for a node registered without one")
	}

	schema, ok := reg.NodeSchema("sum")
	if !ok {
		t.Fatal("NodeSchema did not report the registered schema")
	}
	if !maps.Equal(schema.Inputs, workflow.Ports{"a": workflow.TypeNumber, "b": workflow.TypeNumber}) {
		t.Fatalf("Inputs = %v", schema.Inputs)
	}

	// Mutating the returned copy must not affect the Registry.
	schema.Inputs["c"] = workflow.TypeString
	schema.ConfigSchema[0] = 'x'
	again, _ := reg.NodeSchema("sum")
	if len(again.Inputs) != 2 || !bytes.Equal(again.ConfigSchema, configSchema) {
		t.Fatalf("Registry state changed through the returned copy: %+v", again)
	}
}

func TestRegisterSchema_clonesPorts(t *testing.T) {
	ports := workflow.OnePort(workflow.TypeNumber)
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterSchema("addN", workflow.NodeSchema{Inputs: ports, Output: workflow.TypeNumber})

	// Mutating the caller's map must not change what the Registry enforces.
	ports["sneaky"] = workflow.TypeString

	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", Input: workflow.Output("start")},
	}}
	if err := reg.ValidateGraph(g); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}
