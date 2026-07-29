package expr_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Tangerg/flow/workflow"
	"github.com/Tangerg/flow/workflow/expr"
)

func TestCondition(t *testing.T) {
	condition, err := expr.Condition("loop.output >= 3")
	if err != nil {
		t.Fatalf("Condition: %v", err)
	}

	for value, want := range map[int]bool{2: false, 3: true, 4: true} {
		stop, err := condition(context.Background(), 0, store("loop.output", value))
		if err != nil {
			t.Fatalf("condition(%d): %v", value, err)
		}
		if stop != want {
			t.Fatalf("condition(%d) = %v; want %v", value, stop, want)
		}
	}
}

func TestCondition_reportsEvaluationFailureRatherThanFalse(t *testing.T) {
	// A condition that cannot be evaluated must not read as "keep looping".
	condition, err := expr.Condition("missing.output >= 3")
	if err != nil {
		t.Fatalf("Condition: %v", err)
	}
	if _, err := condition(context.Background(), 0, workflow.NewStore()); !errors.Is(err, expr.ErrUndefined) {
		t.Fatalf("err = %v; want ErrUndefined", err)
	}
}

func TestCondition_rejectsNonBoolean(t *testing.T) {
	condition, err := expr.Condition("n.output + 1")
	if err != nil {
		t.Fatalf("Condition: %v", err)
	}
	if _, err := condition(context.Background(), 0, store("n.output", 1)); !errors.Is(err, expr.ErrType) {
		t.Fatalf("err = %v; want ErrType", err)
	}
}

func TestCondition_parseErrorSurfacesAtBuildTime(t *testing.T) {
	if _, err := expr.Condition("counter"); !errors.Is(err, expr.ErrUnsupported) {
		t.Fatalf("err = %v; want ErrUnsupported", err)
	}
}

func TestResolver(t *testing.T) {
	resolver, err := expr.Resolver("classify.output.intent")
	if err != nil {
		t.Fatalf("Resolver: %v", err)
	}

	s := store("classify.output", map[string]any{"intent": "refund"})
	got, err := resolver(context.Background(), s)
	if err != nil || got != "refund" {
		t.Fatalf("resolver = %q, %v; want refund", got, err)
	}

	if _, err := resolver(context.Background(), store("classify.output", map[string]any{"intent": 1})); !errors.Is(err, expr.ErrType) {
		t.Fatalf("non-string branch name err = %v; want ErrType", err)
	}
}

func TestResolver_parseErrorSurfacesAtBuildTime(t *testing.T) {
	if _, err := expr.Resolver("counter"); !errors.Is(err, expr.ErrUnsupported) {
		t.Fatalf("err = %v; want ErrUnsupported", err)
	}
}

func TestBindings_RefsReportsTheOffendingExpression(t *testing.T) {
	bindings := map[string]expr.Bindings{
		"condition": {Conditions: map[string]string{"bad": "counter"}},
		"resolver":  {Resolvers: map[string]string{"bad": "counter"}},
		"switch":    {Switches: map[string]expr.SwitchSpec{"bad": {Cases: []expr.Case{{When: "counter", Then: "x"}}}}},
	}
	for kind, b := range bindings {
		t.Run(kind, func(t *testing.T) {
			_, err := b.Refs()
			if err == nil {
				t.Fatal("Refs unexpectedly succeeded")
			}
			if !strings.Contains(err.Error(), kind) || !strings.Contains(err.Error(), `"bad"`) {
				t.Fatalf("err = %v; want it to name the %s %q", err, kind, "bad")
			}
		})
	}
}

func TestSwitch(t *testing.T) {
	resolver, err := expr.Switch(expr.SwitchSpec{
		Cases: []expr.Case{
			{When: "score.output >= 0.9", Then: "auto"},
			{When: "score.output >= 0.5", Then: "review"},
		},
		Fallback: "escalate",
	})
	if err != nil {
		t.Fatalf("Switch: %v", err)
	}

	for score, want := range map[float64]string{0.95: "auto", 0.5: "review", 0.1: "escalate"} {
		got, err := resolver(context.Background(), store("score.output", score))
		if err != nil {
			t.Fatalf("resolver(%v): %v", score, err)
		}
		if got != want {
			t.Fatalf("resolver(%v) = %q; want %q", score, got, want)
		}
	}
}

func TestSwitch_withoutFallbackFailsOnNoMatch(t *testing.T) {
	resolver, err := expr.Switch(expr.SwitchSpec{
		Cases: []expr.Case{{When: "n.output > 10", Then: "big"}},
	})
	if err != nil {
		t.Fatalf("Switch: %v", err)
	}
	if _, err := resolver(context.Background(), store("n.output", 1)); !errors.Is(err, expr.ErrUndefined) {
		t.Fatalf("err = %v; want an error naming the unmatched switch", err)
	}
}

func TestSwitch_rejectsInvalidSpecs(t *testing.T) {
	specs := map[string]struct {
		spec expr.SwitchSpec
		want error
	}{
		"no cases":   {want: workflow.ErrInvalidSpec},
		"no branch":  {spec: expr.SwitchSpec{Cases: []expr.Case{{When: "true"}}}, want: workflow.ErrInvalidSpec},
		"bad when":   {spec: expr.SwitchSpec{Cases: []expr.Case{{When: "counter", Then: "a"}}}, want: expr.ErrUnsupported},
		"empty when": {spec: expr.SwitchSpec{Cases: []expr.Case{{When: "", Then: "a"}}}, want: expr.ErrSyntax},
	}
	for name, test := range specs {
		t.Run(name, func(t *testing.T) {
			if _, err := expr.Switch(test.spec); !errors.Is(err, test.want) {
				t.Fatalf("Switch err = %v; want %v", err, test.want)
			}
		})
	}
}

func TestSwitchSpec_Refs(t *testing.T) {
	spec := expr.SwitchSpec{Cases: []expr.Case{
		{When: "b.output > 1", Then: "x"},
		{When: "a.output > 1 && b.output > 1", Then: "y"},
	}}
	refs, err := spec.Refs()
	if err != nil {
		t.Fatalf("Refs: %v", err)
	}
	want := []workflow.Ref{workflow.Output("a"), workflow.Output("b")}
	if !slices.Equal(refs, want) {
		t.Fatalf("Refs = %v; want %v", refs, want)
	}
	if _, err := (expr.SwitchSpec{Cases: []expr.Case{{When: "counter", Then: "x"}}}).Refs(); err == nil {
		t.Fatal("Refs on an invalid case unexpectedly succeeded")
	}
}

func TestBindings_registerAndRefs(t *testing.T) {
	data := []byte(`{
	  "conditions": {"settled": "poll.output.status == \"done\""},
	  "resolvers":  {"byIntent": "classify.output.intent"},
	  "switches":   {"bySize": {
	    "cases": [{"when": "size.output > 100", "then": "large"}],
	    "fallback": "small"
	  }}
	}`)

	var bindings expr.Bindings
	if err := json.Unmarshal(data, &bindings); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	refs, err := bindings.Refs()
	if err != nil {
		t.Fatalf("Refs: %v", err)
	}
	want := []workflow.Ref{
		workflow.At("classify", "output", "intent"),
		workflow.At("poll", "output", "status"),
		workflow.Output("size"),
	}
	if !slices.Equal(refs, want) {
		t.Fatalf("Refs = %v; want %v", refs, want)
	}

	reg := workflow.NewRegistry()
	if err := bindings.Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// The registered names are now usable from a serialized Spec.
	spec := workflow.Spec{
		Kind:     workflow.KindBranch,
		ID:       "route",
		Resolver: "bySize",
		Cases: map[string]workflow.Spec{
			"large": {Kind: workflow.KindSequence},
			"small": {Kind: workflow.KindSequence},
		},
	}
	if err := reg.ValidateSpec(spec); err != nil {
		t.Fatalf("ValidateSpec: %v", err)
	}

	// Re-registering the same names is a duplicate registration.
	if err := bindings.Register(reg); !errors.Is(err, workflow.ErrDuplicateRegistration) {
		t.Fatalf("second Register err = %v; want ErrDuplicateRegistration", err)
	}
}

func TestBindings_leavesRegistryUntouchedOnError(t *testing.T) {
	bindings := expr.Bindings{
		Conditions: map[string]string{"good": "a.output > 1", "bad": "counter"},
	}
	reg := workflow.NewRegistry()
	if err := bindings.Register(reg); err == nil {
		t.Fatal("Register unexpectedly succeeded")
	}

	// "good" compiled fine but must not have been registered, since one bad
	// expression should not leave a half-applied Registry.
	spec := workflow.Spec{
		Kind:      workflow.KindLoop,
		ID:        "repeat",
		Condition: "good",
		Body:      &workflow.Spec{Kind: workflow.KindSequence},
	}
	if err := reg.ValidateSpec(spec); err == nil {
		t.Fatal("a partially applied Bindings registered a condition")
	}
}

func TestBindings_rejectsNameUsedTwiceAndNilRegistry(t *testing.T) {
	clash := expr.Bindings{
		Resolvers: map[string]string{"pick": "a.output"},
		Switches:  map[string]expr.SwitchSpec{"pick": {Cases: []expr.Case{{When: "true", Then: "x"}}}},
	}
	if err := clash.Register(workflow.NewRegistry()); !errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("err = %v; want ErrInvalidSpec", err)
	}
	if err := (expr.Bindings{}).Register(nil); !errors.Is(err, workflow.ErrInvalidRegistration) {
		t.Fatalf("nil registry err = %v; want ErrInvalidRegistration", err)
	}
}

func TestBindings_emptyIsANoOp(t *testing.T) {
	reg := workflow.NewRegistry()
	if err := (expr.Bindings{}).Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	refs, err := (expr.Bindings{}).Refs()
	if err != nil || len(refs) != 0 {
		t.Fatalf("Refs = %v, %v; want none", refs, err)
	}
}
