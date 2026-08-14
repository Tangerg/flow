package workflow_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/Tangerg/flow/workflow"
)

func TestValidateGraph_compatible(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("toNumber", addN()).
		MustRegisterSchema("toNumber", workflow.NodeSchema{Inputs: workflow.OnePort(workflow.TypeNumber), Output: workflow.TypeNumber})

	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "toNumber", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{ID: "b", Type: "toNumber", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("a")}},
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
		{ID: "a", Type: "num", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{ID: "b", Type: "str", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("a")}},
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
		{ID: "b", Type: "string", Inputs: workflow.Inputs{workflow.DefaultPort: nested}},
	}}
	if err := reg.ValidateGraph(g); err != nil {
		t.Fatalf("nested output member has no declared type and should remain TypeAny: %v", err)
	}
}

func TestValidateGraph_rejectsImpossibleNestedOutputPaths(t *testing.T) {
	for name, test := range map[string]struct {
		output workflow.ValueType
		child  string
		valid  bool
	}{
		"string child":       {output: workflow.TypeString, child: "name"},
		"number child":       {output: workflow.TypeNumber, child: "name"},
		"boolean child":      {output: workflow.TypeBool, child: "name"},
		"array object key":   {output: workflow.TypeArray, child: "name"},
		"array leading zero": {output: workflow.TypeArray, child: "01"},
		"array index":        {output: workflow.TypeArray, child: "0", valid: true},
		"object member":      {output: workflow.TypeObject, child: "name", valid: true},
		"unknown member":     {output: workflow.TypeAny, child: "name", valid: true},
	} {
		t.Run(name, func(t *testing.T) {
			registry := workflow.NewRegistry().
				MustRegisterNode("producer", addN()).
				MustRegisterSchema("producer", workflow.NodeSchema{Output: test.output}).
				MustRegisterNode("consumer", addN()).
				MustRegisterSchema("consumer", workflow.NodeSchema{
					Inputs: workflow.OnePort(workflow.TypeAny),
					Output: workflow.TypeAny,
				})
			graph := workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "source", Type: "producer"},
				{
					ID:   "target",
					Type: "consumer",
					Inputs: workflow.OneInput(
						workflow.Output("source").Child(test.child)),
				},
			}}

			err := registry.ValidateGraph(graph)
			if test.valid {
				if err != nil {
					t.Fatalf("ValidateGraph: %v", err)
				}
				return
			}
			var graphErr *workflow.GraphError
			if !errors.Is(err, workflow.ErrIncompatibleType) ||
				!errors.As(err, &graphErr) || graphErr.Field != "inputs" {
				t.Fatalf("ValidateGraph error = %v; want inputs ErrIncompatibleType", err)
			}
		})
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
		{ID: "a", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("b")}},
		{ID: "b", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("a")}},
	}}
	if err := reg.ValidateGraph(g); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestValidateGraph_unregisteredSchemaIsAny(t *testing.T) {
	// No schemas registered: everything is TypeAny, so any wiring validates.
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{ID: "b", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("a")}},
	}}
	if err := reg.ValidateGraph(g); err != nil {
		t.Fatalf("Validate with no schemas should pass: %v", err)
	}
}

func TestValidateGraph_unregisteredProducerSchemaIsAny(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("source", addN()).
		MustRegisterNode("target", addN()).
		MustRegisterSchema("target", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeString),
			Output: workflow.TypeString,
		})
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "source", Type: "source", Inputs: workflow.OneInput(workflow.Output("external"))},
		{ID: "target", Type: "target", Inputs: workflow.OneInput(workflow.Output("source"))},
	}}

	if err := registry.ValidateGraph(graph); err != nil {
		t.Fatalf("ValidateGraph with untyped producer: %v", err)
	}
}

func TestValidateGraph_rejectsDataEdgeFromNodeWithoutOutput(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("wait", workflow.AwaitFactory()).
		MustRegisterSchema("wait", workflow.NodeSchema{Inputs: workflow.OnePort(workflow.TypeAny)}).
		MustRegisterNode("target", addN()).
		MustRegisterSchema("target", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeAny),
			Output: workflow.TypeAny,
		})
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "wait", Type: "wait", Inputs: workflow.OneInput(workflow.Output("external"))},
		{ID: "target", Type: "target", Inputs: workflow.OneInput(workflow.Output("wait"))},
	}}

	err := registry.ValidateGraph(graph)
	if !errors.Is(err, workflow.ErrIncompatibleType) {
		t.Fatalf("ValidateGraph error = %v; want ErrIncompatibleType", err)
	}
}

func TestValidateGraph_rejectsInternalCellOutsideNodeOutput(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("source", addN()).
		MustRegisterNode("target", addN())
	// The target reads an external cell on a port that sorts before the offending
	// one. Ports are checked in name order, and an external producer is not this
	// rule's business, so the check has to pass over that port and go on to the
	// next rather than stop at it.
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "source", Type: "source", Inputs: workflow.OneInput(workflow.Output("external"))},
		{ID: "target", Type: "target", Inputs: workflow.Inputs{
			"aux":                workflow.Output("external"),
			workflow.DefaultPort: workflow.At("source", "private"),
		}},
	}}

	err := registry.ValidateGraph(graph)
	if !errors.Is(err, workflow.ErrIncompatibleType) {
		t.Fatalf("ValidateGraph error = %v; want ErrIncompatibleType", err)
	}
}

func TestValidateGraph_registeredZeroInputSchemaRejectsWiring(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("source", addN()).
		MustRegisterSchema("source", workflow.NodeSchema{Output: workflow.TypeNumber})
	graph := workflow.Graph{Nodes: []workflow.GraphNode{{
		ID:     "source",
		Type:   "source",
		Inputs: workflow.OneInput(workflow.Output("external")),
	}}}

	err := registry.ValidateGraph(graph)
	if !errors.Is(err, workflow.ErrUnknownPort) {
		t.Fatalf("ValidateGraph error = %v; want ErrUnknownPort", err)
	}
}

func TestRegisterSchema_rejectsInvalidPortsAndTypes(t *testing.T) {
	invalid := string([]byte{0xff})
	schemas := map[string]workflow.NodeSchema{
		"bad output":        {Output: workflow.ValueType("wat")},
		"missing port type": {Inputs: workflow.OnePort(""), Output: workflow.TypeAny},
		"bad port type":     {Inputs: workflow.OnePort(workflow.ValueType("wat")), Output: workflow.TypeAny},
		"empty port":        {Inputs: workflow.Ports{"": workflow.TypeNumber}, Output: workflow.TypeAny},
		"non-UTF-8 port":    {Inputs: workflow.Ports{invalid: workflow.TypeNumber}, Output: workflow.TypeAny},
		"non-UTF-8 outlet":  {Output: workflow.TypeString, Outlets: []string{invalid}},
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
	schema := workflow.NodeSchema{}
	if err := reg.RegisterSchema("", schema); !errors.Is(err, workflow.ErrInvalidRegistration) {
		t.Fatalf("empty name error = %v; want ErrInvalidRegistration", err)
	}
	if err := reg.RegisterSchema(string([]byte{0xff}), schema); !errors.Is(err, workflow.ErrInvalidRegistration) {
		t.Fatalf("non-UTF-8 name error = %v; want ErrInvalidRegistration", err)
	}
	if err := reg.RegisterSchema("node", schema); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := reg.RegisterSchema("node", schema); !errors.Is(err, workflow.ErrDuplicateRegistration) {
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

	// Reading a Registry does not seal it. Introspection takes the same lock a
	// later registration waits for, so a read that answered correctly but kept the
	// lock would not report a wrong schema -- it would stop the next registration
	// from ever returning, which no assertion about the answers above can see.
	reg.MustRegisterNode("addM", addN())
	if got := reg.NodeTypes(); !slices.Equal(got, []string{"addM", "addN", "sum"}) {
		t.Fatalf("NodeTypes after registering behind an introspecting reader = %v", got)
	}
}

func TestRegisterSchema_ownsDefinitionStorage(t *testing.T) {
	ports := workflow.OnePort(workflow.TypeNumber)
	wantConfigSchema := json.RawMessage(`{"type":"object"}`)
	configSchema := bytes.Clone(wantConfigSchema)
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterSchema("addN", workflow.NodeSchema{
			Inputs:       ports,
			Output:       workflow.TypeNumber,
			ConfigSchema: configSchema,
		})

	// Mutating caller-owned definition storage must not change what the Registry
	// enforces or returns to tooling.
	ports["sneaky"] = workflow.TypeString
	configSchema[0] = 'x'

	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
	}}
	if err := reg.ValidateGraph(g); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	schema, ok := reg.NodeSchema("addN")
	if !ok || !bytes.Equal(schema.ConfigSchema, wantConfigSchema) {
		t.Fatalf("ConfigSchema = %q, %t; want %q, true", schema.ConfigSchema, ok, wantConfigSchema)
	}
}

// TestRegistry_synchronizesTheTablesItPublishes covers the two accessors an editor
// calls while another goroutine is still registering: the palette of node types
// and one type's schema. The Registry documents that it is safe for concurrent
// access, and its compilation path is already exercised that way, but nothing read
// the tables through these two while a registration was writing them. The race
// detector is the assertion; with a lock removed both keep answering plausibly.
func TestRegistry_synchronizesTheTablesItPublishes(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("first", addN()).
		MustRegisterSchema("first", workflow.NodeSchema{Output: workflow.TypeNumber})

	var group sync.WaitGroup
	for index := range 4 {
		name := "later" + strconv.Itoa(index)
		group.Go(func() {
			if err := registry.RegisterNode(name, addN()); err != nil {
				t.Errorf("RegisterNode %s: %v", name, err)
			}
			if err := registry.RegisterSchema(name, workflow.NodeSchema{Output: workflow.TypeNumber}); err != nil {
				t.Errorf("RegisterSchema %s: %v", name, err)
			}
		})
		group.Go(func() { _ = registry.NodeTypes() })
		group.Go(func() {
			if _, ok := registry.NodeSchema("first"); !ok {
				t.Error("NodeSchema lost the type registered before the others")
			}
		})
	}
	group.Wait()

	if types := registry.NodeTypes(); len(types) != 5 {
		t.Fatalf("NodeTypes = %v; want the first and all four later ones", types)
	}
}
