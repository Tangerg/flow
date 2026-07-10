package workflow_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/workflow"
)

// sum2 reads two refs named in its config and adds them.
func sum2() workflow.LeafFactory {
	return func(id string, _ workflow.Ref, config json.RawMessage) (workflow.Step, error) {
		var cfg struct {
			A workflow.Ref `json:"a"`
			B workflow.Ref `json:"b"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, err
		}
		bind := func(s workflow.Store) ([2]int, error) {
			av, _ := s.Get(cfg.A.NodeID, cfg.A.Path)
			bv, _ := s.Get(cfg.B.NodeID, cfg.B.Path)
			return [2]int{av.(int), bv.(int)}, nil
		}
		leaf := core.Func[[2]int, int](func(_ context.Context, p [2]int) (int, error) { return p[0] + p[1], nil })
		return workflow.Adapt(id, bind, leaf), nil
	}
}

func TestCompile_diamond(t *testing.T) {
	// start=0
	//   a = start + 1        (= 1)
	//   b = a + 10           (= 11)
	//   c = a + 100          (= 101)
	//   d = b + c            (= 112)   <- fan-in
	reg := workflow.NewRegistry().
		RegisterLeaf("addN", addN()).
		RegisterLeaf("sum2", sum2())

	ref := func(id string) *workflow.Ref { return &workflow.Ref{NodeID: id, Path: workflow.OutputKey} }
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", Input: &workflow.Ref{NodeID: "start", Path: "output"}, Config: json.RawMessage(`{"n":1}`)},
		{ID: "b", Type: "addN", Input: ref("a"), Config: json.RawMessage(`{"n":10}`)},
		{ID: "c", Type: "addN", Input: ref("a"), Config: json.RawMessage(`{"n":100}`)},
		{ID: "d", Type: "sum2", DependsOn: []string{"b", "c"},
			Config: json.RawMessage(`{"a":{"nodeID":"b","path":"output"},"b":{"nodeID":"c","path":"output"}}`)},
	}}

	step, err := reg.Compile(g)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := step.Run(context.Background(), workflow.NewStore().With("start", "output", 0))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, ok := out.Get("d", workflow.OutputKey); !ok || v.(int) != 112 {
		t.Fatalf("d = %v, %v; want 112", v, ok)
	}
}

func TestCompile_cycle(t *testing.T) {
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", Input: &workflow.Ref{NodeID: "b", Path: "output"}},
		{ID: "b", Type: "addN", Input: &workflow.Ref{NodeID: "a", Path: "output"}},
	}}
	if _, err := reg.Compile(g); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestCompile_duplicateID(t *testing.T) {
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN"},
		{ID: "a", Type: "addN"},
	}}
	if _, err := reg.Compile(g); err == nil {
		t.Fatal("expected duplicate ID error")
	}
}

func TestCompileJSON(t *testing.T) {
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())
	g := `{"nodes":[
	  {"id":"a","type":"addN","input":{"nodeID":"start","path":"output"},"config":{"n":2}},
	  {"id":"b","type":"addN","input":{"nodeID":"a","path":"output"},"config":{"n":3}}
	]}`

	step, err := reg.CompileJSON([]byte(g))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := step.Run(context.Background(), workflow.NewStore().With("start", "output", 0))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, _ := out.Get("b", workflow.OutputKey); v.(int) != 5 {
		t.Fatalf("b = %v; want 5", v)
	}
}

func TestCompileJSON_rejectsUnknownAndTrailingData(t *testing.T) {
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())
	for _, data := range []string{
		`{"nodes":[],"unknown":true}`,
		`{"nodes":[]} {"nodes":[]}`,
	} {
		if _, err := reg.CompileJSON([]byte(data)); err == nil {
			t.Fatalf("CompileJSON(%q) unexpectedly succeeded", data)
		}
	}
}

func TestCompile_rejectsSelfDependency(t *testing.T) {
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", DependsOn: []string{"a"}},
	}}
	if _, err := reg.Compile(g); err == nil {
		t.Fatal("expected self-dependency error")
	}
}

func TestCompile_rejectsUnknownExplicitDependency(t *testing.T) {
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN", DependsOn: []string{"typo"}},
	}}
	if _, err := reg.Compile(g); err == nil {
		t.Fatal("expected unknown dependency error")
	}
}

func TestCompile_runsSchemaValidation(t *testing.T) {
	reg := workflow.NewRegistry().
		RegisterLeaf("addN", addN()).
		RegisterSchema("addN", workflow.Schema{Input: workflow.TypeNumber, Output: workflow.TypeNumber}).
		RegisterLeaf("stringNode", addN()).
		RegisterSchema("stringNode", workflow.Schema{Input: workflow.TypeString, Output: workflow.TypeString})
	g := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "a", Type: "addN"},
		{ID: "b", Type: "stringNode", Input: &workflow.Ref{NodeID: "a", Path: workflow.OutputKey}},
	}}
	if _, err := reg.Compile(g); err == nil {
		t.Fatal("expected incompatible schema error")
	}
}
