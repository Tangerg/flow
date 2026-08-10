package expr_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
	"github.com/Tangerg/flow/workflow/expr"
)

type cancellationJSON struct {
	cancel  context.CancelCauseFunc
	cause   error
	encoded string
	called  *bool
}

func (c cancellationJSON) MarshalJSON() ([]byte, error) {
	if c.called != nil {
		*c.called = true
	}
	if c.cancel != nil {
		c.cancel(c.cause)
	}
	return []byte(c.encoded), nil
}

func TestCondition(t *testing.T) {
	condition, err := expr.Condition("loop.output >= 3")
	if err != nil {
		t.Fatalf("Condition: %v", err)
	}

	for value, want := range map[int]bool{2: false, 3: true, 4: true} {
		stop, err := condition(t.Context(), 0, store("loop.output", value))
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
	if _, err := condition(t.Context(), 0, workflow.NewStore()); !errors.Is(err, expr.ErrUndefined) {
		t.Fatalf("err = %v; want ErrUndefined", err)
	}
}

func TestCondition_rejectsNonBoolean(t *testing.T) {
	condition, err := expr.Condition("n.output + 1")
	if err != nil {
		t.Fatalf("Condition: %v", err)
	}
	if _, err := condition(t.Context(), 0, store("n.output", 1)); !errors.Is(err, expr.ErrType) {
		t.Fatalf("err = %v; want ErrType", err)
	}
}

func TestCondition_parseErrorSurfacesAtBuildTime(t *testing.T) {
	if _, err := expr.Condition("counter"); !errors.Is(err, expr.ErrUnsupported) {
		t.Fatalf("err = %v; want ErrUnsupported", err)
	}
}

func TestExpressionAdapters_preferCancellationDuringEvaluation(t *testing.T) {
	cause := errors.New("stop expression")
	alreadyCanceled := func(t *testing.T) context.Context {
		t.Helper()
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(cause)
		return ctx
	}

	t.Run("condition", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		condition, err := expr.Condition("value.output.flag")
		if err != nil {
			t.Fatal(err)
		}
		_, err = condition(ctx, 0, store("value.output", cancellationJSON{
			cancel: cancel, cause: cause, encoded: `{"flag":true}`,
		}))
		if !errors.Is(err, cause) {
			t.Fatalf("condition error = %v; want cancellation cause", err)
		}
	})

	t.Run("condition already canceled", func(t *testing.T) {
		condition, err := expr.Condition("true")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := condition(alreadyCanceled(t), 0, workflow.NewStore()); !errors.Is(err, cause) {
			t.Fatalf("condition error = %v; want cancellation cause", err)
		}
	})

	t.Run("resolver", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		resolver, err := expr.Resolver("value.output.route")
		if err != nil {
			t.Fatal(err)
		}
		_, err = resolver.Run(ctx, store("value.output", cancellationJSON{
			cancel: cancel, cause: cause, encoded: `{"route":"go"}`,
		}))
		if !errors.Is(err, cause) {
			t.Fatalf("resolver error = %v; want cancellation cause", err)
		}
	})

	t.Run("resolver already canceled", func(t *testing.T) {
		resolver, err := expr.Resolver(`"unused"`)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := resolver.Run(alreadyCanceled(t), workflow.NewStore()); !errors.Is(err, cause) {
			t.Fatalf("resolver error = %v; want cancellation cause", err)
		}
	})

	t.Run("switch stops before later case", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		laterRead := false
		resolver, err := expr.Switch(expr.SwitchSpec{
			Cases: []expr.Case{
				{When: "first.output.match", Then: "first"},
				{When: "later.output.match", Then: "later"},
			},
			Fallback: "fallback",
		})
		if err != nil {
			t.Fatal(err)
		}
		input := store(
			"first.output", cancellationJSON{
				cancel: cancel, cause: cause, encoded: `{"match":false}`,
			},
			"later.output", cancellationJSON{
				encoded: `{"match":true}`, called: &laterRead,
			},
		)
		_, err = resolver.Run(ctx, input)
		if !errors.Is(err, cause) || laterRead {
			t.Fatalf("resolver error = %v, later read = %t; want cause, false", err, laterRead)
		}
	})

	t.Run("switch already canceled", func(t *testing.T) {
		resolver, err := expr.Switch(expr.SwitchSpec{
			Cases: []expr.Case{{When: "true", Then: "unused"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := resolver.Run(alreadyCanceled(t), workflow.NewStore()); !errors.Is(err, cause) {
			t.Fatalf("resolver error = %v; want cancellation cause", err)
		}
	})
}

func TestResolver(t *testing.T) {
	resolver, err := expr.Resolver("classify.output.intent")
	if err != nil {
		t.Fatalf("Resolver: %v", err)
	}

	s := store("classify.output", map[string]any{"intent": "refund"})
	got, err := resolver.Run(t.Context(), s)
	if err != nil || got != "refund" {
		t.Fatalf("resolver = %q, %v; want refund", got, err)
	}

	if _, err := resolver.Run(t.Context(), store("classify.output", map[string]any{"intent": 1})); !errors.Is(err, expr.ErrType) {
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
		got, err := resolver.Run(t.Context(), store("score.output", score))
		if err != nil {
			t.Fatalf("resolver(%v): %v", score, err)
		}
		if got != want {
			t.Fatalf("resolver(%v) = %q; want %q", score, got, want)
		}
	}
}

func TestSwitch_ownsDefinitionAndSupportsConcurrentReuse(t *testing.T) {
	spec := expr.SwitchSpec{
		Cases:    []expr.Case{{When: "value.output > 0", Then: "positive"}},
		Fallback: "non-positive",
	}
	resolver, err := expr.Switch(spec)
	if err != nil {
		t.Fatalf("Switch: %v", err)
	}
	// Compilation owns executable state rather than retaining the caller's
	// mutable definition.
	spec.Cases[0] = expr.Case{When: "false", Then: "changed"}
	spec.Fallback = "changed"

	var callers sync.WaitGroup
	for index := range 64 {
		callers.Go(func() {
			value := index%3 - 1
			want := "non-positive"
			if value > 0 {
				want = "positive"
			}
			got, runErr := resolver.Run(t.Context(), store("value.output", value))
			if runErr != nil || got != want {
				t.Errorf("resolver(%d) = %q, %v; want %q, nil", value, got, runErr, want)
			}
		})
	}
	callers.Wait()
}

func TestSwitch_withoutFallbackFailsOnNoMatch(t *testing.T) {
	resolver, err := expr.Switch(expr.SwitchSpec{
		Cases: []expr.Case{{When: "n.output > 10", Then: "big"}},
	})
	if err != nil {
		t.Fatalf("Switch: %v", err)
	}
	if _, err := resolver.Run(t.Context(), store("n.output", 1)); !errors.Is(err, flow.ErrNoCase) {
		t.Fatalf("err = %v; want ErrNoCase", err)
	}
}

func TestSwitch_reportsCaseEvaluationError(t *testing.T) {
	resolver, err := expr.Switch(expr.SwitchSpec{
		Cases:    []expr.Case{{When: "missing.output > 0", Then: "matched"}},
		Fallback: "fallback",
	})
	if err != nil {
		t.Fatalf("Switch: %v", err)
	}
	if _, err := resolver.Run(t.Context(), workflow.NewStore()); !errors.Is(err, expr.ErrUndefined) {
		t.Fatalf("error = %v; want ErrUndefined", err)
	}
}

func TestSwitch_rejectsInvalidSpecs(t *testing.T) {
	specs := map[string]struct {
		spec expr.SwitchSpec
		want error
	}{
		"no cases":         {want: flow.ErrInvalidConfig},
		"no branch":        {spec: expr.SwitchSpec{Cases: []expr.Case{{When: "true"}}}, want: flow.ErrInvalidConfig},
		"invalid branch":   {spec: expr.SwitchSpec{Cases: []expr.Case{{When: "true", Then: string([]byte{0xff})}}}, want: flow.ErrInvalidConfig},
		"invalid fallback": {spec: expr.SwitchSpec{Cases: []expr.Case{{When: "true", Then: "a"}}, Fallback: string([]byte{0xff})}, want: flow.ErrInvalidConfig},
		"bad when":         {spec: expr.SwitchSpec{Cases: []expr.Case{{When: "counter", Then: "a"}}}, want: expr.ErrUnsupported},
		"empty when":       {spec: expr.SwitchSpec{Cases: []expr.Case{{When: "", Then: "a"}}}, want: expr.ErrSyntax},
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
	} else if !strings.Contains(err.Error(), "switch case 0") {
		t.Fatalf("Refs error = %v; want switch case index", err)
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

func TestBindings_rejectsInvalidResolversAndSwitches(t *testing.T) {
	tests := map[string]expr.Bindings{
		"resolver": {
			Resolvers: map[string]string{"bad": "counter"},
		},
		"switch": {
			Switches: map[string]expr.SwitchSpec{"bad": {}},
		},
	}
	for name, bindings := range tests {
		t.Run(name, func(t *testing.T) {
			if err := bindings.Register(workflow.NewRegistry()); err == nil {
				t.Fatal("Register unexpectedly succeeded")
			}
		})
	}
}

func TestBindings_reportsDuplicateResolverRegistration(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterResolver(
		"pick",
		flow.NodeFunc[workflow.Store, string](func(context.Context, workflow.Store) (string, error) {
			return "", nil
		}),
	)
	bindings := expr.Bindings{Resolvers: map[string]string{"pick": `"case"`}}
	if err := bindings.Register(reg); !errors.Is(err, workflow.ErrDuplicateRegistration) {
		t.Fatalf("error = %v; want ErrDuplicateRegistration", err)
	}
}

func TestBindings_rejectsNameUsedTwiceAndNilRegistry(t *testing.T) {
	clash := expr.Bindings{
		Resolvers: map[string]string{"pick": "a.output"},
		Switches:  map[string]expr.SwitchSpec{"pick": {Cases: []expr.Case{{When: "true", Then: "x"}}}},
	}
	if err := clash.Register(workflow.NewRegistry()); !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("err = %v; want ErrInvalidConfig", err)
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

func TestBindings_JSONIsStrictAndAtomic(t *testing.T) {
	invalid := map[string][]byte{
		"null document":           []byte(`null`),
		"array document":          []byte(`[]`),
		"scalar document":         []byte(`true`),
		"unknown top-level field": []byte(`{"condition":{"a":"true"}}`),
		"unknown switch field":    []byte(`{"switches":{"pick":{"cases":[{"when":"true","then":"a"}],"fallbak":"a"}}}`),
		"unknown case field":      []byte(`{"switches":{"pick":{"cases":[{"when":"true","then":"a","extra":1}]}}}`),
		"duplicate member":        []byte(`{"conditions":{},"conditions":{}}`),
		"duplicate nested member": []byte(`{"switches":{"pick":{"cases":[],"cases":[]}}}`),
		"invalid UTF-8":           {'{', '"', 0xff, '"', ':', '{', '}', '}'},
		"unpaired surrogate":      []byte(`{"conditions":{"\ud800":"true"}}`),
	}
	for name, data := range invalid {
		t.Run(name, func(t *testing.T) {
			original := expr.Bindings{Conditions: map[string]string{"kept": "true"}}
			if err := json.Unmarshal(data, &original); err == nil {
				t.Fatal("Unmarshal unexpectedly succeeded")
			}
			if len(original.Conditions) != 1 || original.Conditions["kept"] != "true" {
				t.Fatalf("failed Unmarshal changed receiver: %#v", original)
			}
		})
	}

	tooDeep := []byte(`{"conditions":{"rule":` +
		strings.Repeat(`[`, workflow.MaxNestingDepth) + `0` +
		strings.Repeat(`]`, workflow.MaxNestingDepth) + `}}`)
	if err := json.Unmarshal(tooDeep, new(expr.Bindings)); !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("depth error = %v; want ErrMaxDepth", err)
	}
}

func TestExpressionJSONUnmarshalRejectsNilReceivers(t *testing.T) {
	var bindings *expr.Bindings
	if err := bindings.UnmarshalJSON([]byte(`{}`)); err == nil ||
		!strings.Contains(err.Error(), "nil receiver") {
		t.Fatalf("Bindings.UnmarshalJSON error = %v; want nil receiver", err)
	}

	var spec *expr.SwitchSpec
	if err := spec.UnmarshalJSON([]byte(`{"cases":[]}`)); err == nil ||
		!strings.Contains(err.Error(), "nil receiver") {
		t.Fatalf("SwitchSpec.UnmarshalJSON error = %v; want nil receiver", err)
	}
}

func TestBindings_JSONRoundTrip(t *testing.T) {
	original := expr.Bindings{
		Conditions: map[string]string{"done": "poll.output == true"},
		Resolvers:  map[string]string{"route": "classify.output"},
		Switches: map[string]expr.SwitchSpec{
			"size": {
				Cases:    []expr.Case{{When: "size.output > 10", Then: "large"}},
				Fallback: "small",
			},
		},
	}
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var restored expr.Bindings
	if decodeErr := json.Unmarshal(encoded, &restored); decodeErr != nil {
		t.Fatalf("Unmarshal: %v", decodeErr)
	}
	reencoded, err := json.Marshal(restored)
	if err != nil {
		t.Fatalf("second Marshal: %v", err)
	}
	if string(reencoded) != string(encoded) {
		t.Fatalf("round trip = %s; want %s", reencoded, encoded)
	}
}

func TestBindings_MarshalJSONRejectsInvalidUTF8(t *testing.T) {
	bad := string([]byte{0xff})
	tests := map[string]expr.Bindings{
		"condition name":       {Conditions: map[string]string{bad: "true"}},
		"condition expression": {Conditions: map[string]string{"rule": bad}},
		"resolver name":        {Resolvers: map[string]string{bad: `"case"`}},
		"resolver expression":  {Resolvers: map[string]string{"rule": bad}},
		"switch name":          {Switches: map[string]expr.SwitchSpec{bad: {Cases: []expr.Case{{When: "true", Then: "case"}}}}},
		"case expression":      {Switches: map[string]expr.SwitchSpec{"rule": {Cases: []expr.Case{{When: bad, Then: "case"}}}}},
		"case branch":          {Switches: map[string]expr.SwitchSpec{"rule": {Cases: []expr.Case{{When: "true", Then: bad}}}}},
		"fallback":             {Switches: map[string]expr.SwitchSpec{"rule": {Cases: []expr.Case{{When: "true", Then: "case"}}, Fallback: bad}}},
	}
	for name, bindings := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := json.Marshal(bindings); err == nil ||
				!strings.Contains(err.Error(), "not valid UTF-8") {
				t.Fatalf("Marshal error = %v; want invalid UTF-8", err)
			}
		})
	}
}

func TestSwitchSpec_JSONIsStrictAtomicAndLossless(t *testing.T) {
	original := expr.SwitchSpec{
		Cases:    []expr.Case{{When: "true", Then: "yes"}},
		Fallback: "no",
	}
	for _, data := range [][]byte{
		[]byte(`null`),
		[]byte(`[]`),
		[]byte(`true`),
		[]byte(`{"cases":[],"unknown":true}`),
		[]byte(`{"cases":[],"cases":[]}`),
		[]byte(`{"cases":[{"when":"true","then":"yes","then":"no"}]}`),
	} {
		target := original
		if err := json.Unmarshal(data, &target); err == nil {
			t.Fatalf("Unmarshal(%s) unexpectedly succeeded", data)
		}
		if len(target.Cases) != 1 || target.Cases[0].Then != "yes" || target.Fallback != "no" {
			t.Fatalf("failed Unmarshal changed receiver: %#v", target)
		}
	}
	bad := string([]byte{0xff})
	if _, err := json.Marshal(expr.SwitchSpec{Fallback: bad}); err == nil {
		t.Fatal("Marshal accepted invalid UTF-8")
	}
}
