package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestJSONSchemasAreDraft2020AndReturnedByValue(t *testing.T) {
	for name, schema := range map[string]func() json.RawMessage{
		"spec":  workflow.SpecJSONSchema,
		"graph": workflow.GraphJSONSchema,
	} {
		t.Run(name, func(t *testing.T) {
			first := schema()
			var header struct {
				Schema string `json:"$schema"`
				ID     string `json:"$id"`
			}
			if err := json.Unmarshal(first, &header); err != nil {
				t.Fatalf("schema is not JSON: %v", err)
			}
			if header.Schema != "https://json-schema.org/draft/2020-12/schema" {
				t.Fatalf("$schema = %q", header.Schema)
			}
			if header.ID == "" {
				t.Fatal("missing $id")
			}

			first[0] = 'x'
			if next := schema(); len(next) == 0 || next[0] != '{' {
				t.Fatal("caller mutation changed embedded schema")
			}
		})
	}
}

func TestJSONSchemasAcceptMarshalableZeroValueComposites(t *testing.T) {
	tests := map[string]struct {
		value    any
		validate func([]byte) error
	}{
		"empty sequence": {workflow.Spec{Kind: workflow.KindSequence}, workflow.ValidateSpecJSON},
		"empty parallel": {workflow.Spec{Kind: workflow.KindParallel}, workflow.ValidateSpecJSON},
		"empty graph":    {workflow.Graph{}, workflow.ValidateGraphJSON},
		"bounded graph": {
			workflow.Graph{Concurrency: 2},
			workflow.ValidateGraphJSON,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(test.value)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if err := test.validate(data); err != nil {
				t.Fatalf("validate Marshal output %s: %v", data, err)
			}
		})
	}
}

func TestDefinitionsUseTheStrictJSONContractWhenUnmarshaledDirectly(t *testing.T) {
	t.Run("Graph", func(t *testing.T) {
		var nilGraph *workflow.Graph
		if err := nilGraph.UnmarshalJSON([]byte(`{"nodes":[]}`)); !errors.Is(err, workflow.ErrInvalidGraph) {
			t.Fatalf("nil receiver error = %v; want ErrInvalidGraph", err)
		}

		graph := workflow.Graph{Concurrency: 7}
		if err := json.Unmarshal(
			[]byte(`{"nodes":[],"concurrency":2.0}`),
			&graph,
		); err != nil {
			t.Fatalf("Unmarshal mathematical integer: %v", err)
		}
		if graph.Concurrency != 2 {
			t.Fatalf("Concurrency = %d; want 2", graph.Concurrency)
		}

		before := graph
		err := json.Unmarshal([]byte(`{"nodes":[],"unknown":true}`), &graph)
		if !errors.Is(err, workflow.ErrInvalidGraph) {
			t.Fatalf("unknown field error = %v; want ErrInvalidGraph", err)
		}
		if !reflect.DeepEqual(graph, before) {
			t.Fatalf("failed Unmarshal changed Graph from %+v to %+v", before, graph)
		}
	})

	t.Run("Spec", func(t *testing.T) {
		var nilSpec *workflow.Spec
		if err := nilSpec.UnmarshalJSON([]byte(`{"kind":"sequence","steps":[]}`)); !errors.Is(err, workflow.ErrInvalidSpec) {
			t.Fatalf("nil receiver error = %v; want ErrInvalidSpec", err)
		}

		spec := workflow.Spec{Kind: workflow.KindSequence}
		if err := json.Unmarshal([]byte(`{
			"kind":"parallel",
			"steps":[{"kind":"sequence","steps":[]}],
			"concurrency":2e0
		}`), &spec); err != nil {
			t.Fatalf("Unmarshal recursive definition: %v", err)
		}
		if spec.Kind != workflow.KindParallel || spec.Concurrency != 2 || len(spec.Steps) != 1 {
			t.Fatalf("Spec = %+v; want parallel with one child and concurrency 2", spec)
		}

		before := spec
		err := json.Unmarshal(
			[]byte(`{"kind":"sequence","steps":[],"kind":"parallel"}`),
			&spec,
		)
		if !errors.Is(err, workflow.ErrInvalidSpec) {
			t.Fatalf("duplicate member error = %v; want ErrInvalidSpec", err)
		}
		if !reflect.DeepEqual(spec, before) {
			t.Fatalf("failed Unmarshal changed Spec from %+v to %+v", before, spec)
		}
	})
}

func TestDefinitionsRejectJSONThatWouldRewriteIdentityText(t *testing.T) {
	invalid := string([]byte{0xff})

	t.Run("Graph", func(t *testing.T) {
		_, err := json.Marshal(workflow.Graph{Nodes: []workflow.GraphNode{{
			ID:   invalid,
			Type: "node",
		}}})
		var graphErr *workflow.GraphError
		if !errors.Is(err, workflow.ErrInvalidGraph) ||
			!errors.As(err, &graphErr) || graphErr.Path != "/nodes/0" ||
			graphErr.Field != "id" {
			t.Fatalf("Marshal error = %v; want node 0 ID GraphError", err)
		}
	})

	t.Run("Spec", func(t *testing.T) {
		_, err := json.Marshal(workflow.Spec{
			Kind: workflow.KindSequence,
			Steps: []workflow.Spec{{
				Kind: workflow.KindLeaf,
				ID:   invalid,
				Type: "node",
			}},
		})
		var specErr *workflow.SpecError
		if !errors.Is(err, workflow.ErrInvalidSpec) ||
			!errors.As(err, &specErr) || specErr.Path != "/steps/0" ||
			specErr.Field != "id" {
			t.Fatalf("Marshal error = %v; want nested ID SpecError", err)
		}
	})
}

func TestGraphMarshalReportsEveryLosslessEncodingBoundary(t *testing.T) {
	invalid := string([]byte{0xff})
	tests := map[string]struct {
		node  workflow.GraphNode
		field string
	}{
		"type": {
			node:  workflow.GraphNode{ID: "node", Type: invalid},
			field: "type",
		},
		"trigger": {
			node: workflow.GraphNode{
				ID: "node", Type: "type", Trigger: workflow.Trigger(invalid),
			},
			field: "trigger",
		},
		"input port": {
			node: workflow.GraphNode{
				ID: "node", Type: "type",
				Inputs: workflow.Inputs{invalid: workflow.Output("source")},
			},
			field: "inputs",
		},
		"input node": {
			node: workflow.GraphNode{
				ID: "node", Type: "type",
				Inputs: workflow.OneInput(workflow.Output(invalid)),
			},
			field: "inputs",
		},
		"input path": {
			node: workflow.GraphNode{
				ID: "node", Type: "type",
				Inputs: workflow.OneInput(workflow.Ref{NodeID: "source", Path: invalid}),
			},
			field: "inputs",
		},
		"config": {
			node: workflow.GraphNode{
				ID: "node", Type: "type", Config: json.RawMessage(`"\ud800"`),
			},
			field: "config",
		},
		"dependency": {
			node: workflow.GraphNode{
				ID: "node", Type: "type", DependsOn: []string{invalid},
			},
			field: "dependsOn",
		},
		"gate source": {
			node: workflow.GraphNode{
				ID: "node", Type: "type", When: []workflow.Gate{{NodeID: invalid, Outlet: "next"}},
			},
			field: "when",
		},
		"gate outlet": {
			node: workflow.GraphNode{
				ID: "node", Type: "type", When: []workflow.Gate{{NodeID: "route", Outlet: invalid}},
			},
			field: "when",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := json.Marshal(workflow.Graph{Nodes: []workflow.GraphNode{test.node}})
			var graphErr *workflow.GraphError
			if !errors.Is(err, workflow.ErrInvalidGraph) ||
				!errors.As(err, &graphErr) || graphErr.Path != "/nodes/0" ||
				graphErr.Field != test.field {
				t.Fatalf("Marshal error = %v; want node 0 field %s", err, test.field)
			}
		})
	}
}

func TestSpecMarshalReportsEveryLosslessEncodingBoundary(t *testing.T) {
	invalid := string([]byte{0xff})
	tests := map[string]struct {
		spec  workflow.Spec
		field string
	}{
		"kind": {
			spec:  workflow.Spec{Kind: workflow.Kind(invalid)},
			field: "kind",
		},
		"type": {
			spec:  workflow.Spec{Kind: workflow.KindLeaf, Type: invalid},
			field: "type",
		},
		"resolver": {
			spec:  workflow.Spec{Kind: workflow.KindBranch, Resolver: invalid},
			field: "resolver",
		},
		"condition": {
			spec:  workflow.Spec{Kind: workflow.KindLoop, Condition: invalid},
			field: "condition",
		},
		"config": {
			spec: workflow.Spec{
				Kind: workflow.KindLeaf, Config: json.RawMessage(`"\ud800"`),
			},
			field: "config",
		},
		"input": {
			spec: workflow.Spec{
				Kind:  workflow.KindIteration,
				Input: workflow.Ref{NodeID: invalid, Path: "/output"},
			},
			field: "input",
		},
		"inputs": {
			spec: workflow.Spec{
				Kind: workflow.KindLeaf,
				Inputs: workflow.Inputs{
					"in": {NodeID: "source", Path: invalid},
				},
			},
			field: "inputs",
		},
		"body output": {
			spec: workflow.Spec{
				Kind:       workflow.KindSubgraph,
				BodyOutput: workflow.Ref{NodeID: "body", Path: invalid},
			},
			field: "bodyOutput",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := json.Marshal(test.spec)
			var specErr *workflow.SpecError
			if !errors.Is(err, workflow.ErrInvalidSpec) ||
				!errors.As(err, &specErr) || specErr.Field != test.field {
				t.Fatalf("Marshal error = %v; want field %s", err, test.field)
			}
		})
	}

	t.Run("case name", func(t *testing.T) {
		_, err := json.Marshal(workflow.Spec{
			Kind:  workflow.KindBranch,
			Cases: map[string]workflow.Spec{invalid: {Kind: workflow.KindSequence}},
		})
		var specErr *workflow.SpecError
		if !errors.As(err, &specErr) || specErr.Field != "cases" {
			t.Fatalf("Marshal error = %v; want cases SpecError", err)
		}
	})

	for name, spec := range map[string]workflow.Spec{
		"case child": {
			Kind: workflow.KindBranch,
			Cases: map[string]workflow.Spec{"bad": {
				Kind: workflow.KindLeaf, ID: invalid,
			}},
		},
		"body child": {
			Kind: workflow.KindLoop,
			Body: &workflow.Spec{Kind: workflow.KindLeaf, ID: invalid},
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := json.Marshal(spec)
			var specErr *workflow.SpecError
			if !errors.As(err, &specErr) || specErr.Field != "id" || specErr.Path == "" {
				t.Fatalf("Marshal error = %v; want located child ID SpecError", err)
			}
		})
	}
}

func TestSpecMarshalRejectsCyclesAndExcessiveDepth(t *testing.T) {
	cyclic := &workflow.Spec{Kind: workflow.KindLoop}
	cyclic.Body = cyclic
	if _, err := json.Marshal(cyclic); !errors.Is(err, workflow.ErrInvalidSpec) ||
		!strings.Contains(err.Error(), "cyclic spec body") {
		t.Fatalf("cycle error = %v; want ErrInvalidSpec cycle", err)
	}

	deep := workflow.Spec{Kind: workflow.KindLoop}
	current := &deep
	for range workflow.MaxNestingDepth + 1 {
		current.Body = &workflow.Spec{Kind: workflow.KindLoop}
		current = current.Body
	}
	if _, err := json.Marshal(deep); !errors.Is(err, workflow.ErrInvalidSpec) ||
		!errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("depth error = %v; want ErrInvalidSpec and ErrMaxDepth", err)
	}
}

func TestDefinitionMarshalRejectsAssembledDocumentBeyondDepth(t *testing.T) {
	config := func(t *testing.T, depth int) json.RawMessage {
		t.Helper()
		data, err := json.Marshal(nestedArrays(depth))
		if err != nil {
			t.Fatalf("Marshal config at depth %d: %v", depth, err)
		}
		return data
	}

	t.Run("Graph config gains enclosing containers", func(t *testing.T) {
		_, err := json.Marshal(workflow.Graph{Nodes: []workflow.GraphNode{{
			ID:     "node",
			Type:   "type",
			Config: config(t, workflow.MaxNestingDepth-2),
		}}})
		var graphErr *workflow.GraphError
		if !errors.Is(err, workflow.ErrInvalidGraph) ||
			!errors.Is(err, workflow.ErrMaxDepth) ||
			!errors.As(err, &graphErr) || graphErr.Field != "json" {
			t.Fatalf("Marshal error = %v; want graph JSON ErrMaxDepth", err)
		}
	})

	t.Run("Spec config gains enclosing containers", func(t *testing.T) {
		_, err := json.Marshal(workflow.Spec{
			Kind:   workflow.KindLeaf,
			ID:     "leaf",
			Type:   "type",
			Config: config(t, workflow.MaxNestingDepth),
		})
		var specErr *workflow.SpecError
		if !errors.Is(err, workflow.ErrInvalidSpec) ||
			!errors.Is(err, workflow.ErrMaxDepth) ||
			!errors.As(err, &specErr) || specErr.Field != "json" {
			t.Fatalf("Marshal error = %v; want spec JSON ErrMaxDepth", err)
		}
	})

	t.Run("Spec containers exceed logical recursion", func(t *testing.T) {
		deep := workflow.Spec{Kind: workflow.KindLeaf, ID: "leaf", Type: "type"}
		for range workflow.MaxNestingDepth/2 + 1 {
			deep = workflow.Spec{Kind: workflow.KindSequence, Steps: []workflow.Spec{deep}}
		}

		_, err := json.Marshal(deep)
		var specErr *workflow.SpecError
		if !errors.Is(err, workflow.ErrInvalidSpec) ||
			!errors.Is(err, workflow.ErrMaxDepth) ||
			!errors.As(err, &specErr) || specErr.Field != "json" {
			t.Fatalf("Marshal error = %v; want spec JSON ErrMaxDepth", err)
		}
	})
}

func TestCanonicalDefinitionsHaveOneGoAndJSONValidationContract(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("add", addN()).
		MustRegisterResolver("pick", flow.NodeFunc[workflow.Store, string](
			func(context.Context, workflow.Store) (string, error) { return "case", nil },
		)).
		MustRegisterCondition("done", flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) {
			return true, nil
		}))

	specs := map[string]workflow.Spec{
		"leaf": {
			Kind: workflow.KindLeaf, ID: "leaf", Type: "add",
			Inputs: workflow.OneInput(workflow.Output("seed")),
		},
		"sequence": {Kind: workflow.KindSequence},
		"parallel": {
			Kind: workflow.KindParallel, Concurrency: 2,
			Steps: []workflow.Spec{{
				Kind: workflow.KindLeaf, ID: "leaf", Type: "add",
				Inputs: workflow.OneInput(workflow.Output("seed")),
			}},
		},
		"branch": {
			Kind: workflow.KindBranch, ID: "branch", Resolver: "pick",
			Cases: map[string]workflow.Spec{"case": {Kind: workflow.KindSequence}},
		},
		"loop": {
			Kind: workflow.KindLoop, ID: "loop", Condition: "done",
			Body: &workflow.Spec{Kind: workflow.KindSequence},
		},
		"iteration": {
			Kind: workflow.KindIteration, ID: "each",
			Input: workflow.Output("items"), BodyOutput: workflow.Output("element"),
			Body: &workflow.Spec{
				Kind: workflow.KindLeaf, ID: "element", Type: "add",
				Inputs: workflow.OneInput(workflow.Item("each")),
			},
		},
		"subgraph": {
			Kind: workflow.KindSubgraph, ID: "sub",
			Inputs:     workflow.Inputs{"seed": workflow.Output("outer")},
			BodyOutput: workflow.Output("inner"),
			Body: &workflow.Spec{
				Kind: workflow.KindLeaf, ID: "inner", Type: "add",
				Inputs: workflow.OneInput(workflow.Output("seed")),
			},
		},
	}
	for name, spec := range specs {
		t.Run("spec/"+name, func(t *testing.T) {
			if err := registry.ValidateSpec(spec); err != nil {
				t.Fatalf("ValidateSpec: %v", err)
			}
			data, err := json.Marshal(spec)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if err := workflow.ValidateSpecJSON(data); err != nil {
				t.Fatalf("ValidateSpecJSON(%s): %v", data, err)
			}
			if _, err := registry.CompileSpecJSON(data); err != nil {
				t.Fatalf("CompileSpecJSON(%s): %v", data, err)
			}
		})
	}

	graphs := map[string]workflow.Graph{
		"empty": {},
		"single": {Nodes: []workflow.GraphNode{{
			ID: "first", Type: "add",
			Inputs: workflow.OneInput(workflow.Output("seed")),
		}}},
		"dependency": {
			Concurrency: 2,
			Nodes: []workflow.GraphNode{
				{ID: "first", Type: "add", Inputs: workflow.OneInput(workflow.Output("seed"))},
				{ID: "second", Type: "add", Inputs: workflow.OneInput(workflow.Output("first"))},
			},
		},
	}
	for name, graph := range graphs {
		t.Run("graph/"+name, func(t *testing.T) {
			if err := registry.ValidateGraph(graph); err != nil {
				t.Fatalf("ValidateGraph: %v", err)
			}
			data, err := json.Marshal(graph)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if err := workflow.ValidateGraphJSON(data); err != nil {
				t.Fatalf("ValidateGraphJSON(%s): %v", data, err)
			}
			if _, err := registry.CompileGraphJSON(data); err != nil {
				t.Fatalf("CompileGraphJSON(%s): %v", data, err)
			}
		})
	}
}

func TestEmptyGraphEdgeListsHaveOneGoAndJSONContract(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode(
		"interrupt",
		workflow.InterruptFactory(),
	)
	graph := workflow.Graph{Nodes: []workflow.GraphNode{{
		ID:        "node",
		Type:      "interrupt",
		DependsOn: []string{},
		When:      []workflow.Gate{},
	}}}
	if err := registry.ValidateGraph(graph); err != nil {
		t.Fatalf("ValidateGraph: %v", err)
	}
	const data = `{
		"nodes":[{
			"id":"node", "type":"interrupt",
			"dependsOn":[], "when":[]
		}]
	}`
	if err := workflow.ValidateGraphJSON([]byte(data)); err != nil {
		t.Fatalf("ValidateGraphJSON: %v", err)
	}
	if _, err := registry.CompileGraphJSON([]byte(data)); err != nil {
		t.Fatalf("CompileGraphJSON: %v", err)
	}
}

func TestCompileDefinitionJSONAcceptsSchemaIntegralRepresentations(t *testing.T) {
	graphData := []byte(`{"nodes":[],"concurrency":1e1}`)
	if err := workflow.ValidateGraphJSON(graphData); err != nil {
		t.Fatalf("ValidateGraphJSON: %v", err)
	}
	if _, err := workflow.NewRegistry().CompileGraphJSON(graphData); err != nil {
		t.Fatalf("CompileGraphJSON: %v", err)
	}

	specData := []byte(`{
		"kind":"loop", "id":"loop", "condition":"done", "maxIterations":2.0,
		"body":{"kind":"parallel","steps":[],"concurrency":1e1}
	}`)
	registry := workflow.NewRegistry().MustRegisterCondition(
		"done",
		flow.NodeFunc[workflow.Store, bool](func(_ context.Context, _ workflow.Store) (bool, error) { return true, nil }),
	)
	if err := workflow.ValidateSpecJSON(specData); err != nil {
		t.Fatalf("ValidateSpecJSON: %v", err)
	}
	if _, err := registry.CompileSpecJSON(specData); err != nil {
		t.Fatalf("CompileSpecJSON: %v", err)
	}
}

func TestValidateDefinitionJSONRejectsUnrepresentableEngineIntegers(t *testing.T) {
	graphErr := workflow.ValidateGraphJSON([]byte(`{"nodes":[],"concurrency":1e1000}`))
	var graphDiagnostic *workflow.GraphError
	if !errors.As(graphErr, &graphDiagnostic) ||
		graphDiagnostic.Field != "json" ||
		!strings.Contains(graphErr.Error(), "overflows int") {
		t.Fatalf("ValidateGraphJSON error = %v; want JSON int overflow", graphErr)
	}

	specErr := workflow.ValidateSpecJSON([]byte(`{
		"kind":"loop", "id":"loop", "condition":"done",
		"maxIterations":1e1000,
		"body":{"kind":"sequence","steps":[]}
	}`))
	var specDiagnostic *workflow.SpecError
	if !errors.As(specErr, &specDiagnostic) ||
		specDiagnostic.Field != "json" ||
		!strings.Contains(specErr.Error(), "overflows int") {
		t.Fatalf("ValidateSpecJSON error = %v; want JSON int overflow", specErr)
	}
}

func TestValidateDefinitionJSONRejectsUnrepresentableExponent(t *testing.T) {
	err := workflow.ValidateGraphJSON([]byte(
		`{"nodes":[],"concurrency":1e999999999999999999999}`,
	))
	if !errors.Is(err, workflow.ErrInvalidGraph) {
		t.Fatalf("ValidateGraphJSON error = %v; want ErrInvalidGraph", err)
	}
}

func TestSpecAndNodeSpecOmitZeroReferences(t *testing.T) {
	for name, value := range map[string]any{
		"spec": workflow.Spec{
			Kind: workflow.KindLeaf,
			ID:   "leaf",
			Type: "node",
		},
		"node": workflow.GraphNode{
			ID:   "leaf",
			Type: "node",
		},
	} {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(value)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if strings.Contains(string(data), `"input"`) ||
				strings.Contains(string(data), `"bodyOutput"`) {
				t.Fatalf("zero reference was not omitted: %s", data)
			}
		})
	}
}

func TestJSONSchemasValidateNamedInputs(t *testing.T) {
	tests := map[string]struct {
		data    string
		graph   bool
		wantErr bool
	}{
		"graph named ports": {
			data:  `{"nodes":[{"id":"a","type":"t","inputs":{"left":{"nodeID":"x","path":"/output"}}}]}`,
			graph: true,
		},
		"graph empty port name": {
			data:    `{"nodes":[{"id":"a","type":"t","inputs":{"":{"nodeID":"x","path":"/output"}}}]}`,
			graph:   true,
			wantErr: true,
		},
		"graph port is not a ref": {
			data:    `{"nodes":[{"id":"a","type":"t","inputs":{"left":"x.output"}}]}`,
			graph:   true,
			wantErr: true,
		},
		"graph port with unknown ref field": {
			data:    `{"nodes":[{"id":"a","type":"t","inputs":{"left":{"nodeID":"x","path":"/output","extra":1}}}]}`,
			graph:   true,
			wantErr: true,
		},
		"spec named ports": {
			data: `{"kind":"leaf","id":"a","type":"t","inputs":{"left":{"nodeID":"x","path":"/output"}}}`,
		},
		"escaped JSON Pointer": {
			data: `{"kind":"leaf","id":"a","type":"t","inputs":{"left":{"nodeID":"x","path":"/output/a~1b/~0"}}}`,
		},
		"spec port is not a ref": {
			data:    `{"kind":"leaf","id":"a","type":"t","inputs":{"left":3}}`,
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			validate := workflow.ValidateSpecJSON
			if tt.graph {
				validate = workflow.ValidateGraphJSON
			}
			err := validate([]byte(tt.data))
			if tt.wantErr && err == nil {
				t.Fatalf("validate(%s) unexpectedly succeeded", tt.data)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validate(%s): %v", tt.data, err)
			}
		})
	}
}

func TestCompileSpecJSONAcceptsEveryKind(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterResolver("pick", resolverNode(func(context.Context, workflow.Store) (string, error) {
			return "yes", nil
		})).
		MustRegisterCondition("done", flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) {
			return true, nil
		}))

	tests := map[string]string{
		"leaf": `{
			"kind":"leaf", "id":"leaf", "type":"addN",
			"inputs":{"in":{"nodeID":"seed","path":"/output"}}, "config":{"n":1}
		}`,
		"sequence": `{"kind":"sequence","steps":[]}`,
		"parallel": `{"kind":"parallel","steps":[],"concurrency":2}`,
		"branch": `{
			"kind":"branch", "id":"route", "resolver":"pick",
			"cases":{"yes":{"kind":"sequence","steps":[]}}
		}`,
		"loop": `{
			"kind":"loop", "id":"repeat", "condition":"done", "maxIterations":2,
			"body":{"kind":"sequence","steps":[]}
		}`,
		"iteration": `{
			"kind":"iteration", "id":"each",
			"input":{"nodeID":"seed","path":"/output"},
			"body":{"kind":"leaf","id":"item","type":"addN","inputs":{"in":{"nodeID":"each","path":"/item"}}},
			"bodyOutput":{"nodeID":"item","path":"/output"}, "concurrency":2
		}`,
		"subgraph": `{
			"kind":"subgraph", "id":"sub",
			"inputs":{"value":{"nodeID":"seed","path":"/output"}},
			"body":{"kind":"leaf","id":"inner","type":"addN","inputs":{"in":{"nodeID":"value","path":"/output"}}},
			"bodyOutput":{"nodeID":"inner","path":"/output"}
		}`,
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := reg.CompileSpecJSON([]byte(data)); err != nil {
				t.Fatalf("CompileSpecJSON: %v", err)
			}
		})
	}
}

func TestValidateSpecJSONRejectsSchemaViolations(t *testing.T) {
	tests := map[string]string{
		"missing kind":         `{"steps":[]}`,
		"wrong steps type":     `{"kind":"sequence","steps":{}}`,
		"irrelevant field":     `{"kind":"sequence","steps":[],"type":"x"}`,
		"negative concurrency": `{"kind":"parallel","steps":[],"concurrency":-1}`,
		"singular leaf input":  `{"kind":"leaf","id":"x","type":"x","input":{"nodeID":"seed","path":"/output"}}`,
		"empty ref path":       `{"kind":"leaf","id":"x","type":"x","inputs":{"in":{"nodeID":"seed","path":""}}}`,
		"legacy dotted path":   `{"kind":"leaf","id":"x","type":"x","inputs":{"in":{"nodeID":"seed","path":"output.value"}}}`,
		"bad pointer escape":   `{"kind":"leaf","id":"x","type":"x","inputs":{"in":{"nodeID":"seed","path":"/output/~2"}}}`,
		"duplicate member":     `{"kind":"sequence","kind":"parallel","steps":[]}`,
		"unpaired surrogate":   `{"kind":"leaf","id":"\ud800","type":"x"}`,
		"unknown field":        `{"kind":"sequence","steps":[],"unknown":true}`,
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			err := workflow.ValidateSpecJSON([]byte(data))
			if !errors.Is(err, workflow.ErrInvalidSpec) {
				t.Fatalf("error = %v; want ErrInvalidSpec", err)
			}
			var specErr *workflow.SpecError
			if !errors.As(err, &specErr) || specErr.Field != "json" {
				t.Fatalf("error = %v; want JSON SpecError", err)
			}
		})
	}
}

func TestJSONBoundariesRejectExcessiveNesting(t *testing.T) {
	deep := []byte(
		strings.Repeat("[", workflow.MaxNestingDepth+1) +
			"0" +
			strings.Repeat("]", workflow.MaxNestingDepth+1),
	)
	levels := workflow.MaxNestingDepth/2 + 1
	nestedSpec := []byte(
		strings.Repeat(`{"kind":"sequence","steps":[`, levels) +
			`{"kind":"sequence","steps":[]}` +
			strings.Repeat(`]}`, levels),
	)
	if err := workflow.ValidateSpecJSON(nestedSpec); !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("ValidateSpecJSON error = %v; want ErrMaxDepth", err)
	}
	graphWithDeepConfig := append(
		[]byte(`{"nodes":[{"id":"deep","type":"deep","config":`),
		deep...,
	)
	graphWithDeepConfig = append(graphWithDeepConfig, []byte(`}]}`)...)
	if err := workflow.ValidateGraphJSON(graphWithDeepConfig); !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("ValidateGraphJSON error = %v; want ErrMaxDepth", err)
	}

	_, err := workflow.InterruptFactory()(workflow.NodeSpec{
		ID:     "deep",
		Config: json.RawMessage(deep),
	})
	if !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("config error = %v; want ErrMaxDepth", err)
	}
}

func TestValidateGraphJSONReportsDuplicateMemberPath(t *testing.T) {
	err := workflow.ValidateGraphJSON([]byte(
		`{"nodes":[{"id":"first","id":"second","type":"x"}]}`,
	))
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!strings.Contains(err.Error(), `duplicate object member "id" at /nodes/0/id`) {
		t.Fatalf("err = %v; want duplicate member path", err)
	}
}

func TestValidateSpecJSONReportsOnlySelectedKind(t *testing.T) {
	err := workflow.ValidateSpecJSON([]byte(
		`{"kind":"leaf","id":"x","type":"x","inputs":{"in":{"nodeID":"seed","path":""}}}`,
	))
	message := err.Error()
	if !strings.Contains(message, "/inputs/in/path") {
		t.Fatalf("error lacks failing path: %v", err)
	}
	if strings.Contains(message, "github.com/Tangerg/flow/schema") || strings.Contains(message, "\n") {
		t.Fatalf("error exposes validator internals: %v", err)
	}
	for _, unrelated := range []string{"sequence", "parallel", "branch", "loop", "iteration"} {
		if strings.Contains(message, "must be '"+unrelated+"'") {
			t.Fatalf("error includes unrelated %s diagnostics: %v", unrelated, err)
		}
	}
}

func TestJSONSchemaDiagnosticsAreStable(t *testing.T) {
	tests := map[string]struct {
		data     string
		validate func([]byte) error
	}{
		"spec": {
			data:     `{"kind":"leaf","inputs":{"":{"nodeID":"","path":""}}}`,
			validate: workflow.ValidateSpecJSON,
		},
		"graph": {
			data: `{"nodes":[{"id":"","type":"","inputs":{"":{"nodeID":"","path":""}},` +
				`"dependsOn":["",""],"when":[{"nodeID":"","outlet":""}]}]}`,
			validate: workflow.ValidateGraphJSON,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			first := test.validate([]byte(test.data))
			if first == nil {
				t.Fatal("invalid document unexpectedly passed validation")
			}
			want := first.Error()
			for range 100 {
				if err := test.validate([]byte(test.data)); err == nil || err.Error() != want {
					t.Fatalf("diagnostic = %v; want stable %q", err, want)
				}
			}
		})
	}
}

func TestValidateGraphJSONRejectsSchemaViolations(t *testing.T) {
	tests := map[string]string{
		"missing nodes":         `{}`,
		"missing node type":     `{"nodes":[{"id":"x"}]}`,
		"empty node id":         `{"nodes":[{"id":"","type":"x"}]}`,
		"duplicate dependency":  `{"nodes":[{"id":"x","type":"x","dependsOn":["a","a"]}]}`,
		"negative concurrency":  `{"nodes":[],"concurrency":-1}`,
		"incomplete gate":       `{"nodes":[{"id":"x","type":"x","when":[{"nodeID":"route"}]}]}`,
		"duplicate gate":        `{"nodes":[{"id":"x","type":"x","when":[{"nodeID":"route","outlet":"yes"},{"nodeID":"route","outlet":"yes"}]}]}`,
		"unknown trigger":       `{"nodes":[{"id":"x","type":"x","when":[{"nodeID":"route","outlet":"yes"}],"trigger":"sometimes"}]}`,
		"trigger without gate":  `{"nodes":[{"id":"x","type":"x","trigger":"any"}]}`,
		"trigger with no gates": `{"nodes":[{"id":"x","type":"x","when":[],"trigger":"any"}]}`,
		"singular node input":   `{"nodes":[{"id":"x","type":"x","input":{"nodeID":"seed","path":"/output"}}]}`,
		"unpaired surrogate":    `{"nodes":[{"id":"\ud800","type":"x"}]}`,
		"unknown field":         `{"nodes":[],"unknown":true}`,
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			err := workflow.ValidateGraphJSON([]byte(data))
			if !errors.Is(err, workflow.ErrInvalidGraph) {
				t.Fatalf("error = %v; want ErrInvalidGraph", err)
			}
			var graphErr *workflow.GraphError
			if !errors.As(err, &graphErr) || graphErr.Field != "json" {
				t.Fatalf("error = %v; want JSON GraphError", err)
			}
		})
	}
}

func TestCompileJSONPreservesSyntaxErrors(t *testing.T) {
	tests := map[string]func() error{
		"spec": func() error {
			_, err := workflow.NewRegistry().CompileSpecJSON([]byte(`{"kind":]}`))
			return err
		},
		"graph": func() error {
			_, err := workflow.NewRegistry().CompileGraphJSON([]byte(`{"nodes":]}`))
			return err
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			var syntaxErr *json.SyntaxError
			if err := run(); !errors.As(err, &syntaxErr) {
				t.Fatalf("error chain lacks json.SyntaxError: %v", err)
			}
		})
	}
}

func TestRegisterSchemaValidatesNodeConfig(t *testing.T) {
	configSchema := json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"type":"object",
		"properties":{"n":{"$ref":"#/$defs/positiveInteger"}},
		"required":["n"],
		"additionalProperties":false,
		"$defs":{"positiveInteger":{"type":"integer","minimum":1}}
	}`)
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterSchema("addN", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeNumber), Output: workflow.TypeNumber, ConfigSchema: configSchema,
		})

	valid := workflow.Spec{
		Kind: workflow.KindLeaf, ID: "ok", Type: "addN",
		Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
		Config: json.RawMessage(`{"n":2}`),
	}
	if err := reg.ValidateSpec(valid); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	for name, config := range map[string]json.RawMessage{
		"missing":       nil,
		"wrong type":    json.RawMessage(`{"n":"two"}`),
		"too small":     json.RawMessage(`{"n":0}`),
		"unknown field": json.RawMessage(`{"n":2,"extra":true}`),
	} {
		t.Run(name, func(t *testing.T) {
			spec := workflow.Spec{Kind: workflow.KindLeaf, ID: "bad", Type: "addN", Config: config}
			err := reg.ValidateSpec(spec)
			var specErr *workflow.SpecError
			if !errors.As(err, &specErr) || specErr.Field != "config" {
				t.Fatalf("error = %v; want config SpecError", err)
			}
		})
	}

	graph := workflow.Graph{Nodes: []workflow.GraphNode{{ID: "bad", Type: "addN"}}}
	err := reg.ValidateGraph(graph)
	var graphErr *workflow.GraphError
	if !errors.As(err, &graphErr) || graphErr.Field != "config" {
		t.Fatalf("error = %v; want config GraphError", err)
	}

	invalidJSON := workflow.Spec{
		Kind: workflow.KindLeaf, ID: "invalid-json", Type: "addN", Config: json.RawMessage(`{"n":]}`),
	}
	var syntaxErr *json.SyntaxError
	if err := reg.ValidateSpec(invalidJSON); !errors.As(err, &syntaxErr) {
		t.Fatalf("error = %v; want wrapped json.SyntaxError", err)
	}
}

func TestRegisterSchemaRejectsInvalidAndExternalConfigSchemas(t *testing.T) {
	tests := map[string]json.RawMessage{
		"invalid schema": json.RawMessage(`{"type":42}`),
		"external ref":   json.RawMessage(`{"$ref":"https://example.com/schema.json"}`),
		"whitespace":     json.RawMessage(" \n\t"),
	}
	for name, schema := range tests {
		t.Run(name, func(t *testing.T) {
			err := workflow.NewRegistry().RegisterSchema("node", workflow.NodeSchema{
				Output:       workflow.TypeAny,
				ConfigSchema: schema,
			})
			if !errors.Is(err, workflow.ErrInvalidRegistration) {
				t.Fatalf("error = %v; want ErrInvalidRegistration", err)
			}
			var registrationErr *workflow.RegistrationError
			if !errors.As(err, &registrationErr) {
				t.Fatalf("error chain lacks RegistrationError: %v", err)
			}
		})
	}
}

func TestRegisterSchemaEnforcesDraft2020WithoutInspectingInstanceValues(t *testing.T) {
	const draft2020 = "https://json-schema.org/draft/2020-12/schema"
	accepted := map[string]json.RawMessage{
		"omitted dialect": json.RawMessage(`{"type":"object"}`),
		"canonical dialect": json.RawMessage(`{
			"$schema":"https://json-schema.org/draft/2020-12/schema",
			"type":"object"
		}`),
		"HTTP draft alias": json.RawMessage(`{
			"$schema":"http://json-schema.org/draft/2020-12/schema",
			"type":"object"
		}`),
		"empty URI fragment": json.RawMessage(`{
			"$schema":"https://json-schema.org/draft/2020-12/schema#",
			"type":"object"
		}`),
		"nested canonical resource": json.RawMessage(`{
			"$defs":{"nested":{"$id":"nested","$schema":"https://json-schema.org/draft/2020-12/schema"}}
		}`),
		"instance values with schema members": json.RawMessage(`{
			"const":{"$schema":"http://json-schema.org/draft-04/schema"},
			"enum":[{"$schema":"http://json-schema.org/draft-06/schema"}],
			"default":{"$schema":"http://json-schema.org/draft-07/schema"},
			"examples":[{"$schema":"https://json-schema.org/draft/2019-09/schema"}]
		}`),
	}
	for name, schema := range accepted {
		t.Run(name, func(t *testing.T) {
			if err := workflow.NewRegistry().RegisterSchema("node", workflow.NodeSchema{
				Output:       workflow.TypeAny,
				ConfigSchema: schema,
			}); err != nil {
				t.Fatalf("RegisterSchema: %v", err)
			}
		})
	}

	rejected := map[string]struct {
		schema json.RawMessage
		path   string
	}{
		"root legacy draft": {
			schema: json.RawMessage(`{"$schema":"http://json-schema.org/draft-04/schema"}`),
			path:   "<root>",
		},
		"non-empty URI fragment": {
			schema: json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema#other"}`),
			path:   "<root>",
		},
		"nested legacy resource": {
			schema: json.RawMessage(`{
				"$defs":{"legacy":{"$id":"legacy","$schema":"http://json-schema.org/draft-04/schema"}}
			}`),
			path: "/$defs/legacy",
		},
		"legacy subschema without resource ID": {
			schema: json.RawMessage(`{
				"properties":{"value":{"$schema":"http://json-schema.org/draft-07/schema"}}
			}`),
			path: "/properties/value",
		},
		"single subschema keyword": {
			schema: json.RawMessage(`{
				"not":{"$schema":"http://json-schema.org/draft-07/schema"}
			}`),
			path: "/not",
		},
		"subschema list keyword": {
			schema: json.RawMessage(`{
				"allOf":[{"$schema":"http://json-schema.org/draft-07/schema"}]
			}`),
			path: "/allOf/0",
		},
		"legacy items schema list": {
			schema: json.RawMessage(`{
				"items":[{"$schema":"http://json-schema.org/draft-07/schema"}]
			}`),
			path: "/items/0",
		},
		"non-string dialect": {
			schema: json.RawMessage(`{"$schema":{"draft":2020}}`),
			path:   "<root>",
		},
	}
	for name, test := range rejected {
		t.Run(name, func(t *testing.T) {
			err := workflow.NewRegistry().RegisterSchema("node", workflow.NodeSchema{
				Output:       workflow.TypeAny,
				ConfigSchema: test.schema,
			})
			if !errors.Is(err, workflow.ErrInvalidRegistration) ||
				!strings.Contains(err.Error(), test.path) ||
				!strings.Contains(err.Error(), draft2020) {
				t.Fatalf(
					"RegisterSchema error = %v; want invalid registration at %s requiring Draft 2020-12",
					err,
					test.path,
				)
			}
		})
	}
}
