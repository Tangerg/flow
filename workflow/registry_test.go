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
		leaf := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + cfg.N, nil })
		return workflow.Adapt(id, workflow.FromRef[int](input), leaf), nil
	}
}

func TestRegistry_buildSequenceJSON(t *testing.T) {
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())

	spec := `{
	  "kind": "sequence",
	  "steps": [
	    {"kind":"leaf","id":"a","type":"addN","input":{"nodeID":"start","path":"output"},"config":{"n":10}},
	    {"kind":"leaf","id":"b","type":"addN","input":{"nodeID":"a","path":"output"},"config":{"n":5}}
	  ]
	}`

	step, err := reg.BuildJSON([]byte(spec))
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	out, err := step.Run(context.Background(), workflow.NewStore().With("start", "output", 1))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, ok := out.Get("b", workflow.OutputKey); !ok || v.(int) != 16 {
		t.Fatalf("result = %v, %v; want 16", v, ok) // 1 +10 +5
	}
}

func TestRegistry_buildBranch(t *testing.T) {
	reg := workflow.NewRegistry().
		RegisterLeaf("addN", addN()).
		RegisterResolver("sign", func(_ context.Context, s workflow.Store) (string, error) {
			v, _ := s.Get("start", "output")
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

	step, err := reg.Build(spec)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	out, err := step.Run(context.Background(), workflow.NewStore().With("start", "output", 5))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, ok := out.Get("p", workflow.OutputKey); !ok || v.(int) != 105 {
		t.Fatalf("pos branch = %v, %v; want 105", v, ok)
	}
}

func TestRegistry_buildIteration(t *testing.T) {
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())

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

	step, err := reg.Build(spec)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	out, err := step.Run(context.Background(), workflow.NewStore().With("start", "output", []any{1, 2, 3}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out
	raw, ok := got.Get("iter", workflow.OutputKey)
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
	_, err := reg.Build(workflow.Spec{Kind: workflow.KindLeaf, Type: "nope"})
	if err == nil {
		t.Fatal("expected error for unknown leaf type")
	}
}

func TestRegistry_unknownKind(t *testing.T) {
	reg := workflow.NewRegistry()
	_, err := reg.Build(workflow.Spec{Kind: "bogus"})
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
}

func TestRegistry_reportsInvalidAndDuplicateRegistrations(t *testing.T) {
	factory := addN()
	reg := workflow.NewRegistry().
		RegisterLeaf("", factory).
		RegisterLeaf("addN", nil).
		RegisterLeaf("addN", factory).
		RegisterLeaf("addN", factory)

	if reg.Err() == nil {
		t.Fatal("expected accumulated registration errors")
	}
	if _, err := reg.Build(workflow.Spec{Kind: workflow.KindLeaf, ID: "a", Type: "addN"}); err == nil {
		t.Fatal("Build must reject an invalid registry")
	}
}

func TestRegistry_rejectsDuplicateIDsInNestedSpec(t *testing.T) {
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())
	spec := workflow.Spec{Kind: workflow.KindParallel, Steps: []workflow.Spec{
		{Kind: workflow.KindLeaf, ID: "same", Type: "addN"},
		{Kind: workflow.KindLeaf, ID: "same", Type: "addN"},
	}}
	if _, err := reg.Build(spec); err == nil {
		t.Fatal("expected duplicate step ID error")
	}
}

func TestRegistry_rejectsNegativeConcurrency(t *testing.T) {
	reg := workflow.NewRegistry().RegisterLeaf("addN", addN())
	spec := workflow.Spec{Kind: workflow.KindParallel, Concurrency: -1}
	if _, err := reg.Build(spec); err == nil {
		t.Fatal("expected negative concurrency error")
	}
}

func TestRegistry_concurrentRegistrationIsRaceFree(t *testing.T) {
	reg := workflow.NewRegistry()
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			reg.RegisterLeaf(fmt.Sprintf("leaf-%d", i), addN())
			_ = reg.Err()
		})
	}
	wg.Wait()
	if err := reg.Err(); err != nil {
		t.Fatalf("unexpected registry error: %v", err)
	}
}

func TestRegistry_zeroValueIsUsable(t *testing.T) {
	var reg workflow.Registry
	reg.RegisterLeaf("addN", addN())
	if reg.Err() != nil {
		t.Fatalf("zero Registry: %v", reg.Err())
	}
	if _, err := reg.Build(workflow.Spec{Kind: workflow.KindLeaf, ID: "a", Type: "addN"}); err != nil {
		t.Fatalf("zero Registry Build: %v", err)
	}
}
