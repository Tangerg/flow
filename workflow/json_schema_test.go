package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

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

func TestSpecAndNodeSpecOmitZeroReferences(t *testing.T) {
	for name, value := range map[string]any{
		"spec": workflow.Spec{
			Kind: workflow.KindLeaf,
			ID:   "leaf",
			Type: "node",
		},
		"node": workflow.NodeSpec{
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
		MustRegisterLeaf("addN", addN()).
		MustRegisterResolver("pick", func(context.Context, workflow.Store) (string, error) {
			return "yes", nil
		}).
		MustRegisterCondition("done", func(context.Context, int, workflow.Store) (bool, error) {
			return true, nil
		})

	tests := map[string]string{
		"leaf": `{
			"kind":"leaf", "id":"leaf", "type":"addN",
			"input":{"nodeID":"seed","path":"/output"}, "config":{"n":1}
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
			"body":{"kind":"leaf","id":"item","type":"addN","input":{"nodeID":"each","path":"/item"}},
			"bodyOutput":{"nodeID":"item","path":"/output"}, "concurrency":2
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
		"empty ref path":       `{"kind":"leaf","id":"x","type":"x","input":{"nodeID":"seed","path":""}}`,
		"legacy dotted path":   `{"kind":"leaf","id":"x","type":"x","input":{"nodeID":"seed","path":"output.value"}}`,
		"bad pointer escape":   `{"kind":"leaf","id":"x","type":"x","input":{"nodeID":"seed","path":"/output/~2"}}`,
		"duplicate member":     `{"kind":"sequence","kind":"parallel","steps":[]}`,
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

	_, err := workflow.InterruptFactory()(workflow.LeafSpec{
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
		`{"kind":"leaf","id":"x","type":"x","input":{"nodeID":"seed","path":""}}`,
	))
	message := err.Error()
	if !strings.Contains(message, "/input/path") {
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

func TestValidateGraphJSONRejectsSchemaViolations(t *testing.T) {
	tests := map[string]string{
		"missing nodes":        `{}`,
		"missing node type":    `{"nodes":[{"id":"x"}]}`,
		"empty node id":        `{"nodes":[{"id":"","type":"x"}]}`,
		"duplicate dependency": `{"nodes":[{"id":"x","type":"x","dependsOn":["a","a"]}]}`,
		"negative concurrency": `{"nodes":[],"concurrency":-1}`,
		"empty gates":          `{"nodes":[{"id":"x","type":"x","when":[]}]}`,
		"incomplete gate":      `{"nodes":[{"id":"x","type":"x","when":[{"nodeID":"route"}]}]}`,
		"duplicate gate":       `{"nodes":[{"id":"x","type":"x","when":[{"nodeID":"route","outlet":"yes"},{"nodeID":"route","outlet":"yes"}]}]}`,
		"unknown trigger":      `{"nodes":[{"id":"x","type":"x","when":[{"nodeID":"route","outlet":"yes"}],"trigger":"sometimes"}]}`,
		"trigger without gate": `{"nodes":[{"id":"x","type":"x","trigger":"any"}]}`,
		"unknown field":        `{"nodes":[],"unknown":true}`,
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
		MustRegisterLeaf("addN", addN()).
		MustRegisterSchema("addN", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeNumber), Output: workflow.TypeNumber, ConfigSchema: configSchema,
		})

	valid := workflow.Spec{
		Kind: workflow.KindLeaf, ID: "ok", Type: "addN",
		Input:  workflow.Output("start"),
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

	graph := workflow.Graph{Nodes: []workflow.NodeSpec{{ID: "bad", Type: "addN"}}}
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
	}
	for name, schema := range tests {
		t.Run(name, func(t *testing.T) {
			err := workflow.NewRegistry().RegisterSchema("node", workflow.NodeSchema{ConfigSchema: schema})
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
