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
		stop, err := condition.Run(t.Context(), store("loop.output", value))
		if err != nil {
			t.Fatalf("condition(%d): %v", value, err)
		}
		if stop != want {
			t.Fatalf("condition(%d) = %v; want %v", value, stop, want)
		}
	}
}

func TestExpressionAdaptersRejectUncompiledExpressions(t *testing.T) {
	tests := map[string]*expr.Expr{
		"zero": new(expr.Expr),
		"nil":  nil,
	}
	for name, expression := range tests {
		t.Run(name, func(t *testing.T) {
			assertInvalid := func(operation string, err error) {
				t.Helper()
				var expressionErr *expr.Error
				if !errors.As(err, &expressionErr) || !errors.Is(err, flow.ErrInvalidConfig) {
					t.Fatalf("%s error = %v; want expr.Error wrapping ErrInvalidConfig", operation, err)
				}
			}

			condition := expression.Condition()
			assertInvalid("validate condition", flow.Validate(condition))
			_, err := condition.Run(context.Background(), workflow.Store{})
			assertInvalid("run condition", err)
			registry := workflow.NewRegistry()
			assertInvalid("register condition", registry.RegisterCondition("invalid", condition))

			resolver := expression.Resolver()
			assertInvalid("validate resolver", flow.Validate(resolver))
			_, err = resolver.Run(context.Background(), workflow.Store{})
			assertInvalid("run resolver", err)
			assertInvalid("register resolver", registry.RegisterResolver("invalid", resolver))
		})
	}
}

func TestCondition_reportsEvaluationFailureRatherThanFalse(t *testing.T) {
	// A condition that cannot be evaluated must not read as "keep looping".
	condition, err := expr.Condition("missing.output >= 3")
	if err != nil {
		t.Fatalf("Condition: %v", err)
	}
	if _, err := condition.Run(t.Context(), workflow.NewStore()); !errors.Is(err, expr.ErrUndefined) {
		t.Fatalf("err = %v; want ErrUndefined", err)
	}
}

func TestCondition_rejectsNonBoolean(t *testing.T) {
	condition, err := expr.Condition("n.output + 1")
	if err != nil {
		t.Fatalf("Condition: %v", err)
	}
	if _, err := condition.Run(t.Context(), store("n.output", 1)); !errors.Is(err, expr.ErrType) {
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
		_, err = condition.Run(ctx, store("value.output", cancellationJSON{
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
		if _, err := condition.Run(alreadyCanceled(t), workflow.NewStore()); !errors.Is(err, cause) {
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

// TestBindings_reportOneBadExpressionTheSameWayEverywhere pins the traversal the
// operations over a Bindings share. Each of them stops at the first expression it
// cannot use, so with more than one to choose from, which one a caller hears about
// is decided by the order the tables are walked in and by the order of names
// within a table. Reading a rule set and encoding it must agree on that choice,
// or the same file reports a different problem depending on what was asked of it.
func TestBindings_reportOneBadExpressionTheSameWayEverywhere(t *testing.T) {
	bindings := expr.Bindings{
		Conditions: map[string]string{"zzz": "counter", "aaa": "counter"},
		Resolvers:  map[string]string{"aaa": "counter"},
	}
	// The condition table is walked first, and "aaa" before "zzz" within it.
	const want = `condition "aaa"`

	_, refsErr := bindings.Refs()
	if refsErr == nil || !strings.Contains(refsErr.Error(), want) {
		t.Fatalf("Refs err = %v; want it to name %s", refsErr, want)
	}

	bad := string([]byte{0xff})
	invalid := expr.Bindings{
		Conditions: map[string]string{"zzz": bad, "aaa": bad},
		Resolvers:  map[string]string{"aaa": bad},
	}
	_, marshalErr := json.Marshal(invalid)
	if marshalErr == nil || !strings.Contains(marshalErr.Error(), want) {
		t.Fatalf("Marshal err = %v; want it to name %s", marshalErr, want)
	}
}

// TestBindings_RefsMatchTheOrderAGraphPresents holds the two packages to one
// reference order. Bindings.Refs exists so an editor can check that a rule set
// only reads values the graph produces, which means diffing it against
// Graph.Inputs -- and a diff of two sorted lists is nonsense unless both were
// sorted the same way. workflow.Ref.Compare is that order, and it is exported for
// exactly this: a caller assembling its own list gets the order workflow's own
// results are in, rather than a second order that happens to agree today.
func TestBindings_RefsMatchTheOrderAGraphPresents(t *testing.T) {
	bindings := expr.Bindings{
		Conditions: map[string]string{"done": `zeta.output > 1 && alpha.output.b > 0`},
		Resolvers:  map[string]string{"pick": `alpha.output.a`},
	}
	read, err := bindings.Refs()
	if err != nil {
		t.Fatalf("Refs: %v", err)
	}

	// The ports are named so their own order disagrees with the reference order,
	// which is what makes this an assertion about sorting references.
	graph := workflow.Graph{Nodes: []workflow.GraphNode{{
		ID:   "consume",
		Type: "double",
		Inputs: workflow.Inputs{
			"a": workflow.Output("zeta"),
			"y": workflow.At("alpha", "output", "b"),
			"z": workflow.At("alpha", "output", "a"),
		},
	}}}
	// Spelled out rather than derived from the comparator: asking both lists to
	// agree would pass however Compare orders them, which is the drift the two
	// producers used to be able to have.
	want := []workflow.Ref{
		workflow.At("alpha", "output", "a"),
		workflow.At("alpha", "output", "b"),
		workflow.Output("zeta"),
	}
	if !slices.Equal(read, want) {
		t.Fatalf("Bindings.Refs = %v; want %v, node ID before path", read, want)
	}
	if presented := graph.Inputs(); !slices.Equal(presented, want) {
		t.Fatalf("Graph.Inputs = %v; want the same order %v", presented, want)
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
	} else if !strings.HasPrefix(err.Error(), "case 0: ") {
		t.Fatalf("Refs error = %v; want the failing case index", err)
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

// TestBindings_MarshalJSONRejectsInvalidUTF8 asks for the location as well as
// the rule. The strict encoder underneath refuses the same document, so
// "not valid UTF-8" alone is satisfied whether or not these wrappers say which
// binding carried it -- and the location is the whole reason they exist, since a
// caller repairs one named entry rather than a document.
func TestBindings_MarshalJSONRejectsInvalidUTF8(t *testing.T) {
	bad := string([]byte{0xff})
	tests := map[string]struct {
		bindings expr.Bindings
		want     string
	}{
		"condition name": {
			bindings: expr.Bindings{Conditions: map[string]string{bad: "true"}},
			want:     "expr: condition name is not valid UTF-8",
		},
		"condition expression": {
			bindings: expr.Bindings{Conditions: map[string]string{"rule": bad}},
			want:     `expr: condition "rule" is not valid UTF-8`,
		},
		"resolver name": {
			bindings: expr.Bindings{Resolvers: map[string]string{bad: `"case"`}},
			want:     "expr: resolver name is not valid UTF-8",
		},
		"resolver expression": {
			bindings: expr.Bindings{Resolvers: map[string]string{"rule": bad}},
			want:     `expr: resolver "rule" is not valid UTF-8`,
		},
		"switch name": {
			bindings: expr.Bindings{Switches: map[string]expr.SwitchSpec{
				bad: {Cases: []expr.Case{{When: "true", Then: "case"}}},
			}},
			want: "expr: switch name is not valid UTF-8",
		},
		"case expression": {
			bindings: expr.Bindings{Switches: map[string]expr.SwitchSpec{
				"rule": {Cases: []expr.Case{{When: bad, Then: "case"}}},
			}},
			want: `expr: switch "rule": case 0 expression is not valid UTF-8`,
		},
		"case branch": {
			bindings: expr.Bindings{Switches: map[string]expr.SwitchSpec{
				"rule": {Cases: []expr.Case{{When: "true", Then: bad}}},
			}},
			want: `expr: switch "rule": case 0 branch name is not valid UTF-8`,
		},
		"fallback": {
			bindings: expr.Bindings{Switches: map[string]expr.SwitchSpec{
				"rule": {Cases: []expr.Case{{When: "true", Then: "case"}}, Fallback: bad},
			}},
			want: `expr: switch "rule": fallback branch name is not valid UTF-8`,
		},
	}
	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := json.Marshal(testCase.bindings)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("Marshal error = %v; want one containing %q", err, testCase.want)
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

// namingsOfPackage counts how many times a message introduces itself as coming
// from this package, in either of the two forms it uses: the expression prefix
// an [expr.Error] writes and the plain prefix the JSON boundary writes.
func namingsOfPackage(message string) int {
	return strings.Count(message, `expr "`) + strings.Count(message, "expr: ")
}

// TestErrorsNameThePackageAtMostOnce pins the rule the sentinels rely on. Every
// expression failure is wrapped in an expr.Error that names the package, so a
// sentinel or an intermediate wrapper that named it again would stutter. The
// same rule holds at the JSON boundary, which names the package itself because
// nothing wraps it.
func TestErrorsNameThePackageAtMostOnce(t *testing.T) {
	registry := workflow.NewRegistry()
	badExpr := "foo(1)"
	badSwitch := expr.SwitchSpec{Cases: []expr.Case{{When: badExpr, Then: "a"}}}
	notUTF8 := string([]byte{0xff})
	store := workflow.NewStore().WithOutput("a", "text")

	failures := map[string]func() error{
		"Parse syntax":       func() error { _, err := expr.Parse("1 +"); return err },
		"Parse construct":    func() error { _, err := expr.Parse(badExpr); return err },
		"Parse reference":    func() error { _, err := expr.Parse("counter"); return err },
		"Eval undefined":     func() error { _, err := expr.MustParse("missing.output").Eval[any](store); return err },
		"Eval type":          func() error { _, err := expr.MustParse(`a.output + 1`).Eval[any](store); return err },
		"Eval zero":          func() error { _, err := expr.MustParse("1 / 0").Eval[any](store); return err },
		"Eval bool result":   func() error { _, err := expr.MustParse("a.output").Eval[bool](store); return err },
		"Eval string result": func() error { _, err := expr.MustParse("1").Eval[string](store); return err },
		"Condition":          func() error { _, err := expr.Condition(badExpr); return err },
		"Resolver":           func() error { _, err := expr.Resolver(badExpr); return err },
		"Switch case":        func() error { _, err := expr.Switch(badSwitch); return err },
		"Switch text": func() error {
			_, err := expr.Switch(expr.SwitchSpec{Cases: []expr.Case{{When: "true", Then: notUTF8}}})
			return err
		},
		"SwitchSpec.Refs": func() error { _, err := badSwitch.Refs(); return err },
		"Register switch": func() error {
			return expr.Bindings{Switches: map[string]expr.SwitchSpec{"r": badSwitch}}.Register(registry)
		},
		"Register cond":    func() error { return expr.Bindings{Conditions: map[string]string{"c": badExpr}}.Register(registry) },
		"Register resolve": func() error { return expr.Bindings{Resolvers: map[string]string{"r": badExpr}}.Register(registry) },
		"Bindings.Refs": func() error {
			_, err := expr.Bindings{Switches: map[string]expr.SwitchSpec{"r": badSwitch}}.Refs()
			return err
		},
		"Marshal text": func() error {
			_, err := json.Marshal(expr.Bindings{Switches: map[string]expr.SwitchSpec{"r": {Cases: []expr.Case{{When: "true", Then: notUTF8}}}}})
			return err
		},
		"Unmarshal bindings": func() error { return json.Unmarshal([]byte(`[]`), &expr.Bindings{}) },
		"Unmarshal switch":   func() error { return json.Unmarshal([]byte(`{"cases":1}`), &expr.SwitchSpec{}) },
	}

	for name, fail := range failures {
		t.Run(name, func(t *testing.T) {
			err := fail()
			if err == nil {
				t.Fatal("want an error")
			}
			if got := namingsOfPackage(err.Error()); got > 1 {
				t.Fatalf("names the package %d times: %v", got, err)
			}
		})
	}
}

// TestSwitchSpecTextRuleHoldsAtBothBoundaries pins the single statement of which
// text a SwitchSpec may carry. Encoding and compiling ask the same question, so
// a spec one rejects must be rejected by the other for the same stated reason.
func TestSwitchSpecTextRuleHoldsAtBothBoundaries(t *testing.T) {
	notUTF8 := string([]byte{0xff})
	specs := map[string]expr.SwitchSpec{
		"case expression": {Cases: []expr.Case{{When: notUTF8, Then: "a"}}},
		"case branch":     {Cases: []expr.Case{{When: "true", Then: notUTF8}}},
		"fallback":        {Cases: []expr.Case{{When: "true", Then: "a"}}, Fallback: notUTF8},
	}
	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			// MarshalJSON is called directly because encoding/json prefixes its
			// own wrapper, which would hide the reason being compared.
			_, encodeErr := spec.MarshalJSON()
			_, compileErr := expr.Switch(spec)
			if encodeErr == nil || compileErr == nil {
				t.Fatalf("MarshalJSON err = %v, Switch err = %v; want both to reject", encodeErr, compileErr)
			}
			if !strings.HasSuffix(compileErr.Error(), encodeErr.Error()) {
				t.Fatalf("Switch err = %q does not end in the encoder's reason %q", compileErr, encodeErr)
			}
			if _, err := json.Marshal(spec); err == nil {
				t.Fatal("json.Marshal accepted a spec MarshalJSON rejects")
			}
		})
	}
}
