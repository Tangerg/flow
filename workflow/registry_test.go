package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/workflow"
)

// addN is a reusable leaf factory that reads its int input and adds config "n".
func addN() workflow.LeafFactory {
	return func(id string, input workflow.Ref, config json.RawMessage) (workflow.Step, error) {
		var cfg struct {
			N int `json:"n"`
		}
		if len(config) > 0 {
			if err := json.Unmarshal(config, &cfg); err != nil {
				return nil, err
			}
		}
		leaf := core.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + cfg.N, nil })
		return workflow.Leaf(id, workflow.From[int](input), leaf), nil
	}
}

func TestRegistry_compileSequenceJSON(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())

	spec := `{
	  "kind": "sequence",
	  "steps": [
	    {"kind":"leaf","id":"a","type":"addN","input":{"nodeID":"start","path":"output"},"config":{"n":10}},
	    {"kind":"leaf","id":"b","type":"addN","input":{"nodeID":"a","path":"output"},"config":{"n":5}}
	  ]
	}`

	step, err := reg.CompileSpecJSON([]byte(spec))
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", 1))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("b")); !ok || v.(int) != 16 {
		t.Fatalf("result = %v, %v; want 16", v, ok) // 1 +10 +5
	}
}

func TestRegistry_compileBranch(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterLeaf("addN", addN()).
		MustRegisterResolver("sign", func(_ context.Context, s workflow.Store) (string, error) {
			v, _ := s.Lookup(workflow.At("start", "output"))
			if v.(int) >= 0 {
				return "pos", nil
			}
			return "neg", nil
		})

	spec := workflow.Spec{
		Kind:     workflow.KindBranch,
		Resolver: "sign",
		Cases: map[string]workflow.Spec{
			"pos": {Kind: workflow.KindLeaf, ID: "p", Type: "addN", Input: &workflow.Ref{NodeID: "start", Path: "output"}, Config: json.RawMessage(`{"n":100}`)},
			"neg": {Kind: workflow.KindLeaf, ID: "n", Type: "addN", Input: &workflow.Ref{NodeID: "start", Path: "output"}, Config: json.RawMessage(`{"n":-100}`)},
		},
	}

	step, err := reg.CompileSpec(spec)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", 5))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("p")); !ok || v.(int) != 105 {
		t.Fatalf("pos branch = %v, %v; want 105", v, ok)
	}
}

func TestRegistry_compileIteration(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())

	spec := workflow.Spec{
		Kind:       workflow.KindIteration,
		ID:         "iter",
		Input:      &workflow.Ref{NodeID: "start", Path: "output"},
		BodyOutput: &workflow.Ref{NodeID: "el", Path: workflow.OutputKey},
		Body: &workflow.Spec{
			Kind: workflow.KindLeaf, ID: "el", Type: "addN",
			Input:  &workflow.Ref{NodeID: "iter", Path: workflow.ItemKey},
			Config: json.RawMessage(`{"n":1}`),
		},
	}

	step, err := reg.CompileSpec(spec)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", []any{1, 2, 3}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out
	raw, ok := got.Lookup(workflow.Output("iter"))
	if !ok {
		t.Fatal("iteration output missing")
	}
	res := raw.([]any)
	want := []int{2, 3, 4}
	for i := range want {
		if res[i].(int) != want[i] {
			t.Fatalf("res[%d] = %v, want %d", i, res[i], want[i])
		}
	}
}

func TestRegistry_unknownType(t *testing.T) {
	reg := workflow.NewRegistry()
	_, err := reg.CompileSpec(workflow.Spec{Kind: workflow.KindLeaf, Type: "nope"})
	if err == nil {
		t.Fatal("expected error for unknown leaf type")
	}
}

func TestRegistry_unknownKind(t *testing.T) {
	reg := workflow.NewRegistry()
	_, err := reg.CompileSpec(workflow.Spec{Kind: "bogus"})
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
}

func TestRegistry_reportsInvalidAndDuplicateRegistrations(t *testing.T) {
	factory := addN()
	reg := workflow.NewRegistry()
	for name, f := range map[string]workflow.LeafFactory{"": factory, "nil": nil} {
		if err := reg.RegisterLeaf(name, f); err == nil {
			t.Fatalf("RegisterLeaf(%q) unexpectedly succeeded", name)
		}
	}
	if err := reg.RegisterLeaf("addN", factory); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := reg.RegisterLeaf("addN", factory); err == nil {
		t.Fatal("duplicate registration unexpectedly succeeded")
	}
}

func TestRegistry_mustRegisterPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustRegisterLeaf did not panic")
		}
	}()
	workflow.NewRegistry().MustRegisterLeaf("", addN())
}

func TestRegistry_rejectsDuplicateIDsInNestedSpec(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	spec := workflow.Spec{Kind: workflow.KindParallel, Steps: []workflow.Spec{
		{Kind: workflow.KindLeaf, ID: "same", Type: "addN"},
		{Kind: workflow.KindLeaf, ID: "same", Type: "addN"},
	}}
	if _, err := reg.CompileSpec(spec); err == nil {
		t.Fatal("expected duplicate step ID error")
	}
}

func TestRegistry_rejectsNegativeConcurrency(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	spec := workflow.Spec{Kind: workflow.KindParallel, Concurrency: -1}
	if _, err := reg.CompileSpec(spec); err == nil {
		t.Fatal("expected negative concurrency error")
	}
}

func TestRegistry_concurrentRegistrationIsRaceFree(t *testing.T) {
	reg := workflow.NewRegistry()
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			if err := reg.RegisterLeaf(fmt.Sprintf("leaf-%d", i), addN()); err != nil {
				t.Errorf("RegisterLeaf: %v", err)
			}
		})
	}
	wg.Wait()
}

func TestRegistry_zeroValueIsUsable(t *testing.T) {
	var reg workflow.Registry
	if err := reg.RegisterLeaf("addN", addN()); err != nil {
		t.Fatalf("zero Registry: %v", err)
	}
	if _, err := reg.CompileSpec(workflow.Spec{Kind: workflow.KindLeaf, ID: "a", Type: "addN"}); err != nil {
		t.Fatalf("zero Registry Build: %v", err)
	}
}
