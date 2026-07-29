package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// addN is a reusable leaf factory that reads its int input and adds config "n".
func addN() workflow.LeafFactory {
	type config struct {
		N int `json:"n"`
	}
	return workflow.Factory(func(cfg config) (flow.Node[int, int], error) {
		return flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + cfg.N, nil }), nil
	})
}

func refPtr(ref workflow.Ref) *workflow.Ref { return &ref }

func TestRegistry_compileSequenceJSON(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())

	spec := `{
	  "kind": "sequence",
	  "steps": [
	    {"kind":"leaf","id":"a","type":"addN","input":{"nodeID":"start","path":"/output"},"config":{"n":10}},
	    {"kind":"leaf","id":"b","type":"addN","input":{"nodeID":"a","path":"/output"},"config":{"n":5}}
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
		ID:       "route",
		Resolver: "sign",
		Cases: map[string]workflow.Spec{
			"pos": {Kind: workflow.KindLeaf, ID: "p", Type: "addN", Input: &workflow.Ref{NodeID: "start", Path: "/output"}, Config: json.RawMessage(`{"n":100}`)},
			"neg": {Kind: workflow.KindLeaf, ID: "n", Type: "addN", Input: &workflow.Ref{NodeID: "start", Path: "/output"}, Config: json.RawMessage(`{"n":-100}`)},
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
		Input:      &workflow.Ref{NodeID: "start", Path: "/output"},
		BodyOutput: refPtr(workflow.Output("el")),
		Body: &workflow.Spec{
			Kind: workflow.KindLeaf, ID: "el", Type: "addN",
			Input:  refPtr(workflow.Item("iter")),
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

func TestRegistry_reportsInvalidResolverAndConditionRegistrations(t *testing.T) {
	resolver := workflow.Resolver(func(context.Context, workflow.Store) (string, error) {
		return "", nil
	})
	condition := workflow.Condition(func(context.Context, int, workflow.Store) (bool, error) {
		return false, nil
	})

	for name, register := range map[string]func(*workflow.Registry) error{
		"resolver empty name": func(reg *workflow.Registry) error {
			return reg.RegisterResolver("", resolver)
		},
		"resolver nil": func(reg *workflow.Registry) error {
			return reg.RegisterResolver("resolver", nil)
		},
		"resolver duplicate": func(reg *workflow.Registry) error {
			if err := reg.RegisterResolver("resolver", resolver); err != nil {
				return err
			}
			return reg.RegisterResolver("resolver", resolver)
		},
		"condition empty name": func(reg *workflow.Registry) error {
			return reg.RegisterCondition("", condition)
		},
		"condition nil": func(reg *workflow.Registry) error {
			return reg.RegisterCondition("condition", nil)
		},
		"condition duplicate": func(reg *workflow.Registry) error {
			if err := reg.RegisterCondition("condition", condition); err != nil {
				return err
			}
			return reg.RegisterCondition("condition", condition)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := register(workflow.NewRegistry()); err == nil {
				t.Fatal("registration unexpectedly succeeded")
			}
		})
	}
}

func TestRegistry_mustRegisterPanics(t *testing.T) {
	tests := map[string]func(){
		"leaf": func() {
			workflow.NewRegistry().MustRegisterLeaf("", addN())
		},
		"resolver": func() {
			workflow.NewRegistry().MustRegisterResolver("", func(context.Context, workflow.Store) (string, error) {
				return "", nil
			})
		},
		"condition": func() {
			workflow.NewRegistry().MustRegisterCondition("", func(context.Context, int, workflow.Store) (bool, error) {
				return false, nil
			})
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("MustRegister%s did not panic", name)
				}
			}()
			run()
		})
	}
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

func TestValidateSpec_rejectsFieldsItsKindWouldIgnore(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	tests := map[string]struct {
		spec  workflow.Spec
		field string
	}{
		"sequence type": {
			spec:  workflow.Spec{Kind: workflow.KindSequence, Type: "addN"},
			field: "type",
		},
		"leaf concurrency": {
			spec:  workflow.Spec{Kind: workflow.KindLeaf, ID: "a", Type: "addN", Concurrency: 1},
			field: "concurrency",
		},
		"parallel condition": {
			spec:  workflow.Spec{Kind: workflow.KindParallel, Condition: "ignored"},
			field: "condition",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := reg.ValidateSpec(tt.spec)
			var specErr *workflow.SpecError
			if !errors.As(err, &specErr) || !errors.Is(err, workflow.ErrInvalidSpec) || specErr.Field != tt.field {
				t.Fatalf("err = %v; want invalid field %q", err, tt.field)
			}
		})
	}
}

func TestValidateSpec_rejectsMalformedConfigWithoutANodeSchema(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	spec := workflow.Spec{
		Kind:   workflow.KindLeaf,
		ID:     "a",
		Type:   "addN",
		Config: json.RawMessage(`{"n":`),
	}
	if err := reg.ValidateSpec(spec); !errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("err = %v; want ErrInvalidSpec", err)
	}
}

func TestValidateSpec_rejectsEveryStructuralBoundary(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterLeaf("addN", addN()).
		MustRegisterResolver("pick", func(context.Context, workflow.Store) (string, error) {
			return "case", nil
		}).
		MustRegisterCondition("done", func(context.Context, int, workflow.Store) (bool, error) {
			return true, nil
		})
	leaf := func(id string) workflow.Spec {
		return workflow.Spec{Kind: workflow.KindLeaf, ID: id, Type: "addN"}
	}
	body := func() *workflow.Spec {
		value := workflow.Spec{Kind: workflow.KindSequence}
		return &value
	}
	input := func(ref workflow.Ref) *workflow.Ref { return &ref }

	tests := map[string]workflow.Spec{
		"negative max iterations": {
			Kind: workflow.KindLoop, ID: "loop", Body: body(),
			Condition: "done", MaxIterations: -1,
		},
		"duplicate loop ID": {
			Kind: workflow.KindSequence,
			Steps: []workflow.Spec{
				leaf("same"),
				{Kind: workflow.KindLoop, ID: "same", Body: body(), Condition: "done"},
			},
		},
		"missing loop body": {
			Kind: workflow.KindLoop, ID: "loop", Condition: "done",
		},
		"unknown loop condition": {
			Kind: workflow.KindLoop, ID: "loop", Body: body(), Condition: "missing",
		},
		"empty leaf type": {
			Kind: workflow.KindLeaf, ID: "leaf",
		},
		"unknown leaf type": {
			Kind: workflow.KindLeaf, ID: "leaf", Type: "missing",
		},
		"duplicate default input": {
			Kind: workflow.KindLeaf, ID: "leaf", Type: "addN",
			Input: input(workflow.Output("a")),
			Inputs: workflow.Inputs{
				workflow.DefaultPort: workflow.Output("b"),
			},
		},
		"invalid leaf input": {
			Kind: workflow.KindLeaf, ID: "leaf", Type: "addN",
			Input: input(workflow.Ref{NodeID: "source", Path: "/~2"}),
		},
		"duplicate branch ID": {
			Kind: workflow.KindSequence,
			Steps: []workflow.Spec{
				leaf("same"),
				{
					Kind: workflow.KindBranch, ID: "same", Resolver: "pick",
					Cases: map[string]workflow.Spec{"case": {Kind: workflow.KindSequence}},
				},
			},
		},
		"missing branch cases": {
			Kind: workflow.KindBranch, ID: "branch", Resolver: "pick",
		},
		"unknown branch resolver": {
			Kind: workflow.KindBranch, ID: "branch", Resolver: "missing",
			Cases: map[string]workflow.Spec{"case": {Kind: workflow.KindSequence}},
		},
		"empty branch case name": {
			Kind: workflow.KindBranch, ID: "branch", Resolver: "pick",
			Cases: map[string]workflow.Spec{"": {Kind: workflow.KindSequence}},
		},
		"duplicate iteration ID": {
			Kind: workflow.KindSequence,
			Steps: []workflow.Spec{
				leaf("same"),
				{
					Kind: workflow.KindIteration, ID: "same",
					Input: input(workflow.Output("items")), Body: body(),
					BodyOutput: input(workflow.Output("value")),
				},
			},
		},
		"missing iteration input": {
			Kind: workflow.KindIteration, ID: "each", Body: body(),
			BodyOutput: input(workflow.Output("value")),
		},
		"missing iteration body": {
			Kind: workflow.KindIteration, ID: "each",
			Input:      input(workflow.Output("items")),
			BodyOutput: input(workflow.Output("value")),
		},
		"missing iteration body output": {
			Kind: workflow.KindIteration, ID: "each",
			Input: input(workflow.Output("items")), Body: body(),
		},
		"invalid iteration input": {
			Kind: workflow.KindIteration, ID: "each",
			Input: input(workflow.Ref{NodeID: "items", Path: "/~2"}), Body: body(),
			BodyOutput: input(workflow.Output("value")),
		},
		"invalid iteration body output": {
			Kind: workflow.KindIteration, ID: "each",
			Input: input(workflow.Output("items")), Body: body(),
			BodyOutput: input(workflow.Ref{NodeID: "value", Path: "/~2"}),
		},
	}

	for name, spec := range tests {
		t.Run(name, func(t *testing.T) {
			if err := reg.ValidateSpec(spec); err == nil {
				t.Fatal("ValidateSpec unexpectedly succeeded")
			}
		})
	}
}

func TestValidateSpec_requiresDeclaredLeafPorts(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterLeaf("addN", addN()).
		MustRegisterSchema("addN", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeNumber),
			Output: workflow.TypeNumber,
		})
	err := reg.ValidateSpec(workflow.Spec{
		Kind: workflow.KindLeaf,
		ID:   "leaf",
		Type: "addN",
	})
	if !errors.Is(err, workflow.ErrMissingPort) {
		t.Fatalf("error = %v; want ErrMissingPort", err)
	}
}

func TestValidateSpec_iterationBodyIDsAreLocalToEachElement(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())
	spec := workflow.Spec{
		Kind: workflow.KindSequence,
		Steps: []workflow.Spec{
			{
				Kind: workflow.KindLeaf, ID: "value", Type: "addN",
				Input: refPtr(workflow.Output("seed")),
			},
			{
				Kind:       workflow.KindIteration,
				ID:         "each",
				Input:      refPtr(workflow.Output("items")),
				BodyOutput: refPtr(workflow.Output("value")),
				Body: &workflow.Spec{
					Kind: workflow.KindLeaf, ID: "value", Type: "addN",
					Input:  refPtr(workflow.Item("each")),
					Config: json.RawMessage(`{"n":1}`),
				},
			},
		},
	}

	step, err := reg.CompileSpec(spec)
	if err != nil {
		t.Fatalf("CompileSpec: %v", err)
	}
	in := workflow.NewStore().
		WithOutput("seed", 10).
		WithOutput("items", []any{1, 2})
	out, err := step.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := workflow.Get[int](out, workflow.Output("value")); err != nil || got != 10 {
		t.Fatalf("outer value = %v, %v; want 10", got, err)
	}
	items, err := workflow.Get[[]int](out, workflow.Output("each"))
	if err != nil || len(items) != 2 || items[0] != 2 || items[1] != 3 {
		t.Fatalf("iteration output = %v, %v; want [2 3]", items, err)
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
	spec := workflow.Spec{Kind: workflow.KindLeaf, ID: "a", Type: "addN", Input: refPtr(workflow.Output("start"))}
	if _, err := reg.CompileSpec(spec); err != nil {
		t.Fatalf("zero Registry Build: %v", err)
	}
}
