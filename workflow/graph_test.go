package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestCompileGraph_diamond(t *testing.T) {
	// start=0
	//   a = start + 1        (= 1)
	//   b = a + 10           (= 11)
	//   c = a + 100          (= 101)
	//   d = b + c            (= 112)   <- fan-in through two named ports
	reg := workflow.NewRegistry().
		MustRegisterLeaf("addN", addN()).
		MustRegisterLeaf("sum", sumPorts())

	ref := func(id string) *workflow.Ref { value := workflow.Output(id); return &value }
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", Input: &workflow.Ref{NodeID: "start", Path: "/output"}, Config: json.RawMessage(`{"n":1}`)},
		{ID: "b", Type: "addN", Input: ref("a"), Config: json.RawMessage(`{"n":10}`)},
		{ID: "c", Type: "addN", Input: ref("a"), Config: json.RawMessage(`{"n":100}`)},
		// No DependsOn: wired ports are dependencies.
		{ID: "d", Type: "sum", Inputs: workflow.Inputs{"a": workflow.Output("b"), "b": workflow.Output("c")}},
	}}

	step, err := reg.CompileGraph(g)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", 0))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("d")); !ok || v.(int) != 112 {
		t.Fatalf("d = %v, %v; want 112", v, ok)
	}

	// The fan-in node must be layered after both producers.
	description := workflow.Describe(step)
	last := description.Children[len(description.Children)-1]
	if last.ID != "d" {
		t.Fatalf("last layer = %+v; want the fan-in node d", last)
	}
}

func TestCompileGraph_portsInferDependencies(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterLeaf("addN", addN()).
		MustRegisterLeaf("sum", sumPorts())

	// b is declared before its producer a; layering must still order them.
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "b", Type: "sum", Inputs: workflow.Inputs{"a": workflow.Output("a"), "b": workflow.Output("start")}},
		{ID: "a", Type: "addN", Input: refPtr(workflow.Output("start")), Config: json.RawMessage(`{"n":1}`)},
	}}

	step, err := reg.CompileGraph(g)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", 5))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, err := workflow.Get[int](out, workflow.Output("b")); err != nil || v != 11 {
		t.Fatalf("b = %v, %v; want 11", v, err) // (5+1) + 5
	}
}

func TestCompileGraph_rejectsDuplicateDefaultPort(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{{
		ID:     "a",
		Type:   "addN",
		Input:  refPtr(workflow.Output("start")),
		Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("other")},
	}}}
	if err := reg.ValidateGraph(g); !errors.Is(err, workflow.ErrDuplicatePort) {
		t.Fatalf("err = %v; want ErrDuplicatePort", err)
	}
}

func TestCompileGraph_cycle(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", Input: &workflow.Ref{NodeID: "b", Path: "/output"}},
		{ID: "b", Type: "addN", Input: &workflow.Ref{NodeID: "a", Path: "/output"}},
	}}
	if _, err := reg.CompileGraph(g); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestCompileGraph_duplicateID(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN"},
		{ID: "a", Type: "addN"},
	}}
	if _, err := reg.CompileGraph(g); err == nil {
		t.Fatal("expected duplicate ID error")
	}
}

func TestCompileGraphJSON(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	g := `{"nodes":[
	  {"id":"a","type":"addN","input":{"nodeID":"start","path":"/output"},"config":{"n":2}},
	  {"id":"b","type":"addN","input":{"nodeID":"a","path":"/output"},"config":{"n":3}}
	]}`

	step, err := reg.CompileGraphJSON([]byte(g))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", 0))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, _ := out.Lookup(workflow.Output("b")); v.(int) != 5 {
		t.Fatalf("b = %v; want 5", v)
	}
}

func TestCompileGraphJSON_rejectsUnknownAndTrailingData(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	for _, data := range []string{
		`{"nodes":[],"unknown":true}`,
		`{"nodes":[]} {"nodes":[]}`,
	} {
		if _, err := reg.CompileGraphJSON([]byte(data)); err == nil {
			t.Fatalf("CompileJSON(%q) unexpectedly succeeded", data)
		}
	}
}

func TestCompileGraph_keepsFactoryErrorsAtTheGraphBoundary(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{{
		ID:     "a",
		Type:   "addN",
		Input:  refPtr(workflow.Output("start")),
		Config: json.RawMessage(`{"unknown":true}`),
	}}}

	_, err := reg.CompileGraph(g)
	var graphErr *workflow.GraphError
	var specErr *workflow.SpecError
	if !errors.As(err, &graphErr) || errors.As(err, &specErr) {
		t.Fatalf("err = %v; want GraphError and no SpecError", err)
	}
	if graphErr.NodeID != "a" || graphErr.Field != "config" ||
		!errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("err = %v; want a/config with both invalid sentinels", err)
	}
}

func TestCompileGraph_rejectsANilStepFromAFactory(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("broken", func(workflow.LeafSpec) (workflow.Step, error) {
		return nil, nil
	})
	_, err := reg.CompileGraph(workflow.Graph{Nodes: []workflow.NodeSpec{{ID: "a", Type: "broken"}}})
	if !errors.Is(err, workflow.ErrNilStep) || !errors.Is(err, workflow.ErrInvalidGraph) {
		t.Fatalf("err = %v; want ErrNilStep and ErrInvalidGraph", err)
	}
}

func TestCompileGraph_rejectsSelfDependency(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", DependsOn: []string{"a"}},
	}}
	_, err := reg.CompileGraph(g)
	var graphErr *workflow.GraphError
	if !errors.As(err, &graphErr) || graphErr.Field != "dependsOn" {
		t.Fatalf("err = %v; want dependsOn GraphError", err)
	}
}

func TestCompileGraph_reportsSelfInputAsInputError(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{{
		ID:    "a",
		Type:  "addN",
		Input: &workflow.Ref{NodeID: "a", Path: "/output"},
	}}}
	_, err := reg.CompileGraph(g)
	var graphErr *workflow.GraphError
	if !errors.As(err, &graphErr) || graphErr.Field != "inputs" {
		t.Fatalf("err = %v; want inputs GraphError", err)
	}
}

func TestCompileGraph_rejectsUnknownExplicitDependency(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", DependsOn: []string{"typo"}},
	}}
	if _, err := reg.CompileGraph(g); err == nil {
		t.Fatal("expected unknown dependency error")
	}
}

func TestCompileGraph_programmaticValidationMatchesJSONSchema(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	tests := map[string]workflow.NodeSpec{
		"empty type": {
			ID: "a",
		},
		"empty dependency": {
			ID: "a", Type: "addN", DependsOn: []string{""},
		},
		"duplicate dependency": {
			ID: "a", Type: "addN", DependsOn: []string{"parent", "parent"},
		},
		"empty port": {
			ID: "a", Type: "addN", Inputs: workflow.Inputs{"": workflow.Output("start")},
		},
	}
	for name, node := range tests {
		t.Run(name, func(t *testing.T) {
			g := workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "parent", Type: "addN"},
				node,
			}}
			if err := reg.ValidateGraph(g); !errors.Is(err, workflow.ErrInvalidGraph) {
				t.Fatalf("err = %v; want ErrInvalidGraph", err)
			}
		})
	}
}

func TestValidateGraph_rejectsMalformedConfigWithoutANodeSchema(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{{
		ID: "a", Type: "addN", Config: json.RawMessage(`{"n":`),
	}}}
	if err := reg.ValidateGraph(g); !errors.Is(err, workflow.ErrInvalidGraph) {
		t.Fatalf("err = %v; want ErrInvalidGraph", err)
	}
}

func TestCompileGraph_runsSchemaValidation(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterLeaf("addN", addN()).
		MustRegisterSchema("addN", workflow.NodeSchema{Inputs: workflow.OnePort(workflow.TypeNumber), Output: workflow.TypeNumber}).
		MustRegisterLeaf("stringNode", addN()).
		MustRegisterSchema("stringNode", workflow.NodeSchema{Inputs: workflow.OnePort(workflow.TypeString), Output: workflow.TypeString})
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", Input: refPtr(workflow.Output("start"))},
		{ID: "b", Type: "stringNode", Input: refPtr(workflow.Output("a"))},
	}}
	if _, err := reg.CompileGraph(g); !errors.Is(err, workflow.ErrIncompatibleType) {
		t.Fatalf("err = %v; want ErrIncompatibleType", err)
	}
}

func TestCompileGraph_reportsUnwiredAndUnknownPorts(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterLeaf("sum", sumPorts()).
		MustRegisterSchema("sum", workflow.NodeSchema{
			Inputs: workflow.Ports{"a": workflow.TypeNumber, "b": workflow.TypeNumber},
			Output: workflow.TypeNumber,
		})

	tests := map[string]struct {
		inputs workflow.Inputs
		want   error
	}{
		"unwired": {
			inputs: workflow.Inputs{"a": workflow.Output("start")},
			want:   workflow.ErrMissingPort,
		},
		"unknown": {
			inputs: workflow.Inputs{"a": workflow.Output("start"), "b": workflow.Output("start"), "c": workflow.Output("start")},
			want:   workflow.ErrUnknownPort,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			g := workflow.Graph{Nodes: []workflow.NodeSpec{{ID: "n", Type: "sum", Inputs: tt.inputs}}}
			if err := reg.ValidateGraph(g); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v; want %v", err, tt.want)
			}
		})
	}
}

func TestCompileGraph_rejectsMalformedPortRef(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("sum", sumPorts())
	for name, ref := range map[string]workflow.Ref{
		"empty nodeID": {Path: "/output"},
		"empty path":   {NodeID: "start"},
	} {
		t.Run(name, func(t *testing.T) {
			g := workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "n", Type: "sum", Inputs: workflow.Inputs{"a": ref, "b": workflow.Output("start")}},
			}}
			if err := reg.ValidateGraph(g); !errors.Is(err, workflow.ErrInvalidGraph) {
				t.Fatalf("err = %v; want ErrInvalidGraph", err)
			}
		})
	}
}

func TestValidateGraph_identifiesEmptyNodeIDByIndex(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	err := reg.ValidateGraph(workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "valid", Type: "addN"},
		{Type: "addN"},
	}})
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.As(err, &graphErr) ||
		graphErr.Field != "id" ||
		!strings.Contains(err.Error(), "index 1") {
		t.Fatalf("err = %v; want empty node ID at index 1", err)
	}
}

func TestGraph_inputsSkipMalformedNodes(t *testing.T) {
	// A node whose default port is wired twice cannot be resolved; Graph.Inputs
	// reports what it can and leaves rejecting the graph to ValidateGraph.
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "bad", Type: "addN", Input: refPtr(workflow.Output("x")),
			Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("y")}},
		{ID: "ok", Type: "addN", Input: refPtr(workflow.Output("z"))},
	}}
	if got := g.Inputs(); !slices.Equal(got, []workflow.Ref{workflow.Output("z")}) {
		t.Fatalf("Graph.Inputs = %v; want [z.output]", got)
	}
}

func TestGraph_inputs(t *testing.T) {
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", Input: refPtr(workflow.Output("seed"))},
		{ID: "b", Type: "sum", Inputs: workflow.Inputs{
			"a": workflow.Output("a"),          // internal
			"b": workflow.At("params", "rate"), // external
		}},
		{ID: "c", Type: "addN", Input: refPtr(workflow.Output("seed"))}, // duplicate external
	}}

	got := g.Inputs()
	want := []workflow.Ref{workflow.At("params", "rate"), workflow.Output("seed")}
	if !slices.Equal(got, want) {
		t.Fatalf("Graph.Inputs = %v; want %v", got, want)
	}

	seeded := workflow.NewStore().WithOutput("seed", 1)
	if missing := g.MissingInputs(seeded); !slices.Equal(missing, []workflow.Ref{workflow.At("params", "rate")}) {
		t.Fatalf("Graph.MissingInputs = %v; want params.rate", missing)
	}

	complete := seeded.With("params", "rate", 0.5)
	if missing := g.MissingInputs(complete); len(missing) != 0 {
		t.Fatalf("Graph.MissingInputs = %v; want none", missing)
	}
}

func TestCompileGraph_preservesSpecOrderWithinLayer(t *testing.T) {
	constant := func(spec workflow.LeafSpec) (workflow.Step, error) {
		return workflow.Leaf(
			spec.ID,
			workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
			flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) { return value, nil }),
		), nil
	}
	reg := workflow.NewRegistry().MustRegisterLeaf("constant", constant)
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "parent-a", Type: "constant"},
		{ID: "parent-b", Type: "constant"},
		{ID: "child-b", Type: "constant", DependsOn: []string{"parent-b"}},
		{ID: "child-a", Type: "constant", DependsOn: []string{"parent-a"}},
	}}

	step, err := reg.CompileGraph(g)
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	description := workflow.Describe(step)
	if len(description.Children) != 2 {
		t.Fatalf("description = %+v; want two layers", description)
	}
	second := description.Children[1]
	if len(second.Children) != 2 || second.Children[0].ID != "child-b" || second.Children[1].ID != "child-a" {
		t.Fatalf("second layer = %+v; want child-b then child-a", second)
	}
}
