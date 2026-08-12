package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/internal/ctxtest"
	jschema "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

type schemaValidatorFunc func(any) error

func (s schemaValidatorFunc) Validate(value any) error {
	return s(value)
}

type opaqueTestStepFunc func(context.Context, Store) (Store, error)

func (o opaqueTestStepFunc) Run(ctx context.Context, store Store) (Store, error) {
	return o(ctx, store)
}

type definitionFixture struct{ shape stepDefinition }

func (definitionFixture) Run(context.Context, Store) (Store, error) { return Store{}, nil }
func (definitionFixture) validate() error                           { return nil }
func (definitionFixture) Describe() Description                     { return Description{} }
func (d definitionFixture) definition() stepDefinition              { return d.shape }

func TestReplayBoundaries_resampleCancellationAfterLookup(t *testing.T) {
	cause := errors.New("cancel during replay lookup")

	t.Run("leaf", func(t *testing.T) {
		called := false
		step := Leaf(
			"leaf",
			BinderFunc[Store](func(store Store) (Store, error) { return store, nil }),
			opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
				called = true
				return Store{}, nil
			}),
		)
		ctx := ctxtest.CancelAtCheck(t.Context(), 2, cause)
		output, err := step.Run(ctx, NewStore())
		_, published := output.Lookup(Output("leaf"))
		if !errors.Is(err, cause) || called || published {
			t.Fatalf("Run = %v, called %t, output %+v; want cancellation before node", err, called, output)
		}
	})

	t.Run("interrupt", func(t *testing.T) {
		ctx := ctxtest.CancelAtCheck(t.Context(), 2, cause)
		_, err := Interrupt("wait", "request").Run(ctx, NewStore())
		if !errors.Is(err, cause) || SuspendedOnly(err) {
			t.Fatalf("Run error = %v; want cancellation, not suspension", err)
		}
	})

	t.Run("branch decision", func(t *testing.T) {
		ctx := withConfig(ctxtest.CancelAtCheck(t.Context(), 1, cause), RunConfig{})
		execution := branchExecution{
			branch: branchStep{id: "branch"},
			input:  NewStore(),
			run:    runFrom(ctx),
		}
		_, _, err := execution.decide(ctx)
		if !errors.Is(err, cause) {
			t.Fatalf("decide error = %v; want cancellation", err)
		}
	})

	t.Run("loop decision", func(t *testing.T) {
		ctx := withConfig(ctxtest.CancelAtCheck(t.Context(), 1, cause), RunConfig{})
		execution := loopExecution{loop: loopStep{config: LoopConfig{ID: "loop"}}, run: runFrom(ctx)}
		_, err := execution.stop(ctx, NewStore())
		if !errors.Is(err, cause) {
			t.Fatalf("stop error = %v; want cancellation", err)
		}
	})
}

func TestLoopStop_resamplesCancellationAfterJournalRecord(t *testing.T) {
	cause := errors.New("cancel during loop checkpoint")
	tests := []struct {
		name     string
		conflict bool
	}{
		{name: "successful record"},
		{name: "journal conflict", conflict: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			journal := NewJournal()
			cancelCtx := ctxtest.CancelAtCheck(t.Context(), 3, cause)
			ctx := withConfig(cancelCtx, RunConfig{Journal: journal})
			if test.conflict {
				// Add the conflicting record after the run snapshot so replay does
				// not consume it as historical work.
				if err := journal.Record(JournalKey{ID: "loop"}, false); err != nil {
					t.Fatal(err)
				}
			}

			execution := loopExecution{
				loop: loopStep{config: LoopConfig{
					ID: "loop",
					Condition: flow.NodeFunc[Store, bool](func(context.Context, Store) (bool, error) {
						return true, nil
					}),
				}},
				run: runFrom(ctx),
			}
			stop, err := execution.stop(ctx, NewStore())
			if !errors.Is(err, cause) || stop {
				t.Fatalf("stop = %t, error = %v; want cancellation", stop, err)
			}
			if journal.Len() != 1 {
				t.Fatalf("Journal.Len = %d; want completed checkpoint write", journal.Len())
			}
		})
	}
}

func TestBranch_resamplesCancellationBeforeCaseAdmission(t *testing.T) {
	cause := errors.New("cancel after decision")
	for _, cancelAt := range []int{4, 5} {
		caseCalled := false
		step := Branch(BranchConfig{ID: "branch", Resolver: flow.NodeFunc[Store, string](func(context.Context, Store) (string, error) { return "selected", nil }), Cases: map[string]Step{
			"selected": opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
				caseCalled = true
				return Store{}, nil
			}),
		}})

		ctx := ctxtest.CancelAtCheck(t.Context(), cancelAt, cause)
		_, err := step.Run(ctx, NewStore())
		if !errors.Is(err, cause) || caseCalled {
			t.Fatalf("check %d: Run error = %v, case called = %t; want cancellation before case", cancelAt, err, caseCalled)
		}
	}
}

func TestGatedStep_resamplesCancellationAroundBypassCommit(t *testing.T) {
	cause := errors.New("cancel bypass")
	innerCalled := false
	inner := Leaf(
		"target",
		BinderFunc[Store](func(store Store) (Store, error) { return store, nil }),
		opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
			innerCalled = true
			return Store{}, nil
		}),
	).(definedStep)
	step := gated(
		[]compiledGate{{Gate: When("route", "yes"), outlets: []string{"yes", "no"}}},
		TriggerAll,
		inner,
	)
	store := NewStore().WithOutput("route", "no")

	for _, cancelAt := range []int{1, 2, 3, 4, 5} {
		ctx := ctxtest.CancelAtCheck(t.Context(), cancelAt, cause)
		if _, err := step.Run(ctx, store); !errors.Is(err, cause) {
			t.Fatalf("cancel check %d error = %v; want cancellation", cancelAt, err)
		}
	}
	if innerCalled {
		t.Fatal("bypassed inner step ran")
	}
}

func TestSubgraphBind_rejectsCancellationBeforeInputRead(t *testing.T) {
	cause := errors.New("cancel before subgraph input")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)
	execution := subgraphExecution{
		subgraph: subgraphStep{inputs: Inputs{"seed": Output("missing")}},
		outer:    NewStore(),
	}
	_, err := execution.bind(ctx)
	if !errors.Is(err, cause) {
		t.Fatalf("bind error = %v; want cancellation cause", err)
	}
}

func TestEmissionSession_retainsFirstEmitterError(t *testing.T) {
	boom := errors.New("emitter failed")
	ctx, cancel := context.WithCancelCause(t.Context())
	session := emissionSession{
		run:     new(runState),
		cancel:  cancel,
		emitter: EmitterFunc(func(context.Context, Chunk) error { return boom }),
		id:      "stream",
	}
	first := session.emit(ctx, 1)
	second := session.emit(ctx, 2)
	if !errors.Is(first, boom) || second != first {
		t.Fatalf("emit errors = %v, %v; want the same first emitter error", first, second)
	}
}

func TestGatedStep_resamplesCancellationAfterBypassEvent(t *testing.T) {
	cause := errors.New("cancel during bypass event")
	ctx, cancel := context.WithCancelCause(t.Context())
	ctx = withConfig(ctx, RunConfig{
		Observer: ObserverFunc(func(context.Context, Event) { cancel(cause) }),
	})
	inner := Leaf(
		"target",
		BinderFunc[Store](func(store Store) (Store, error) { return store, nil }),
		opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
			t.Fatal("bypassed inner step ran")
			return Store{}, nil
		}),
	).(definedStep)
	step := gated(
		[]compiledGate{{Gate: When("route", "yes"), outlets: []string{"yes", "no"}}},
		TriggerAll,
		inner,
	)

	_, err := step.Run(ctx, NewStore().WithOutput("route", "no"))
	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v; want cancellation cause", err)
	}
}

func TestLeaf_resamplesCancellationAfterJournalCommit(t *testing.T) {
	cause := errors.New("cancel after journal commit")
	ctx := withConfig(ctxtest.CancelAtCheck(t.Context(), 1, cause), RunConfig{
		Journal: NewJournal(),
	})
	execution := leafExecution[struct{}, int]{
		leaf:  leafStep[struct{}, int]{id: "leaf"},
		store: NewStore(),
		run:   runFrom(ctx),
	}

	output, err := execution.complete(ctx, 42)
	if !errors.Is(err, cause) {
		t.Fatalf("complete error = %v; want cancellation cause", err)
	}
	if _, published := output.Lookup(Output("leaf")); published {
		t.Fatal("complete published output after cancellation")
	}
	if keys := execution.run.journal().Keys(); len(keys) != 1 || keys[0].ID != "leaf" {
		t.Fatalf("Journal keys = %+v; want committed leaf checkpoint", keys)
	}
}

func TestJournalCommitErrorsDoNotHideParentCancellation(t *testing.T) {
	cause := errors.New("cancel during journal commit")

	t.Run("leaf", func(t *testing.T) {
		journal := NewJournal()
		if err := journal.Record(JournalKey{ID: "leaf"}, 1); err != nil {
			t.Fatalf("Record: %v", err)
		}
		ctx := withConfig(ctxtest.CancelAtCheck(t.Context(), 1, cause), RunConfig{
			Journal: journal,
		})
		execution := leafExecution[struct{}, int]{
			leaf:  leafStep[struct{}, int]{id: "leaf"},
			store: NewStore(),
			run:   runFrom(ctx),
		}

		if _, err := execution.complete(ctx, 2); !errors.Is(err, cause) {
			t.Fatalf("complete error = %v; want cancellation cause", err)
		}
	})

	t.Run("branch", func(t *testing.T) {
		journal := NewJournal()
		ctx := withConfig(ctxtest.CancelAtCheck(t.Context(), 4, cause), RunConfig{
			Journal: journal,
		})
		step := Branch(BranchConfig{ID: "route", Resolver: flow.NodeFunc[Store, string](func(context.Context, Store) (string, error) {
			if err := journal.Record(JournalKey{ID: "route"}, "case"); err != nil {
				return "", err
			}
			return "case", nil
		}), Cases: map[string]Step{"case": Interrupt("result", nil)}})

		if _, err := step.Run(ctx, NewStore()); !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
	})
}

func TestParallelMerge_resamplesParentCancellation(t *testing.T) {
	cause := errors.New("cancel during parallel merge")
	input := NewStore().WithOutput("seed", 1)
	branch := opaqueTestStepFunc(func(_ context.Context, store Store) (Store, error) {
		return store.WithOutput("result", 2), nil
	})

	t.Run("one branch", func(t *testing.T) {
		ctx := ctxtest.CancelAtCheck(t.Context(), 2, cause)
		output, err := (parallelStep{branches: stepList{branch}}).runOne(ctx, input)
		if !errors.Is(err, cause) {
			t.Fatalf("runOne error = %v; want cancellation cause", err)
		}
		if _, published := output.Lookup(Output("result")); published {
			t.Fatal("runOne published merged output after cancellation")
		}
	})

	t.Run("many branches", func(t *testing.T) {
		for _, cancelAt := range []int{2, 4} {
			t.Run(fmt.Sprintf("check %d", cancelAt), func(t *testing.T) {
				ctx := ctxtest.CancelAtCheck(t.Context(), cancelAt, cause)
				output, err := (parallelStep{branches: stepList{branch, branch}}).runMany(ctx, input)
				if !errors.Is(err, cause) {
					t.Fatalf("runMany error = %v; want cancellation cause", err)
				}
				if _, published := output.Lookup(Output("result")); published {
					t.Fatal("runMany published merged output after cancellation")
				}
			})
		}
	})
}

func TestIterationCollect_resamplesParentCancellation(t *testing.T) {
	cause := errors.New("cancel during iteration collection")
	input := NewStore().WithOutput("seed", 1)
	outcomes := []elementOutcome{{value: 1}}

	for _, cancelAt := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("check %d", cancelAt), func(t *testing.T) {
			ctx := ctxtest.CancelAtCheck(t.Context(), cancelAt, cause)
			execution := iterationExecution{
				iteration: iterationStep{id: "items"},
				input:     input,
			}
			output, err := execution.collect(ctx, outcomes)
			if !errors.Is(err, cause) {
				t.Fatalf("collect error = %v; want cancellation cause", err)
			}
			if _, published := output.Lookup(Output("items")); published {
				t.Fatal("collect published output after cancellation")
			}
		})
	}
}

func TestGuaranteedOutputsModelsEveryBuiltInShape(t *testing.T) {
	produces := interruptStep{id: "value"}
	opaque := opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
		return Store{}, nil
	})
	tests := []struct {
		name  string
		step  Step
		want  []string
		known bool
	}{
		{name: "named output", step: produces, want: []string{"value"}, known: true},
		{name: "named pass through", step: awaitStep{id: "wait"}, known: true},
		{
			name: "step list",
			step: sequenceStep{steps: stepList{awaitStep{id: "wait"}, produces}},
			want: []string{"value"}, known: true,
		},
		{
			name: "opaque step list",
			step: sequenceStep{steps: stepList{produces, opaque}},
		},
		{
			name: "branch intersection",
			step: branchStep{cases: map[string]Step{
				"a": sequenceStep{steps: stepList{produces}},
				"b": sequenceStep{steps: stepList{produces, awaitStep{id: "wait"}}},
			}},
			want: []string{"value"}, known: true,
		},
		{name: "empty branch", step: branchStep{}, known: true},
		{
			name: "opaque branch",
			step: branchStep{cases: map[string]Step{"a": produces, "b": opaque}},
		},
		{
			name: "loop body",
			step: loopStep{config: LoopConfig{Body: produces}},
			want: []string{"value"}, known: true,
		},
		{
			name: "iteration projection",
			step: iterationStep{id: "each"},
			want: []string{"each"}, known: true,
		},
		{
			name: "subgraph projection",
			step: subgraphStep{id: "sub"},
			want: []string{"sub"}, known: true,
		},
		{name: "graph", step: graphStep{}},
		{
			name: "invalid internal kind",
			step: definitionFixture{shape: stepDefinition{kind: definitionKind(255)}},
		},
		{name: "opaque", step: opaque},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outputs := guaranteedOutputs(test.step)
			if outputs.known != test.known || !slices.Equal(slices.Sorted(maps.Keys(outputs.nodes)), test.want) {
				t.Fatalf("guaranteedOutputs = %v, %t; want %v, %t", outputs.nodes, outputs.known, test.want, test.known)
			}
		})
	}
}

func TestSpecGuaranteedOutputsModelsEverySerializableShape(t *testing.T) {
	validator := specValidator{registry: registrySnapshot{
		schemas: map[string]registeredNodeSchema{
			"output": {schema: NodeSchema{Output: TypeAny}},
			"pass":   {schema: NodeSchema{}},
		},
	}}
	output := Spec{Kind: KindLeaf, ID: "value", Type: "output"}
	pass := Spec{Kind: KindLeaf, ID: "wait", Type: "pass"}
	unknown := Spec{Kind: KindLeaf, ID: "unknown", Type: "unknown"}
	tests := []struct {
		name  string
		spec  Spec
		want  []string
		known bool
	}{
		{name: "leaf output", spec: output, want: []string{"value"}, known: true},
		{name: "leaf pass through", spec: pass, known: true},
		{name: "leaf without schema", spec: unknown},
		{
			name: "sequence",
			spec: Spec{Kind: KindSequence, Steps: []Spec{pass, output}},
			want: []string{"value"}, known: true,
		},
		{
			name: "parallel",
			spec: Spec{Kind: KindParallel, Steps: []Spec{output}},
			want: []string{"value"}, known: true,
		},
		{
			name: "step list with unknown leaf",
			spec: Spec{Kind: KindSequence, Steps: []Spec{output, unknown}},
		},
		{
			name: "branch intersection",
			spec: Spec{Kind: KindBranch, Cases: map[string]Spec{
				"a": output,
				"b": {Kind: KindSequence, Steps: []Spec{output, pass}},
			}},
			want: []string{"value"}, known: true,
		},
		{name: "empty branch", spec: Spec{Kind: KindBranch}, known: true},
		{
			name: "branch removes non-common output",
			spec: Spec{Kind: KindBranch, Cases: map[string]Spec{
				"a": output,
				"b": pass,
			}},
			known: true,
		},
		{
			name: "branch with unknown leaf",
			spec: Spec{Kind: KindBranch, Cases: map[string]Spec{
				"a": output,
				"b": unknown,
			}},
		},
		{
			name: "loop body",
			spec: Spec{Kind: KindLoop, Body: &output},
			want: []string{"value"}, known: true,
		},
		{name: "loop without body", spec: Spec{Kind: KindLoop}},
		{
			name: "iteration projection",
			spec: Spec{Kind: KindIteration, ID: "each"},
			want: []string{"each"}, known: true,
		},
		{
			name: "subgraph projection",
			spec: Spec{Kind: KindSubgraph, ID: "sub"},
			want: []string{"sub"}, known: true,
		},
		{name: "invalid kind", spec: Spec{Kind: "invalid"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outputs := validator.guaranteedOutputs(test.spec)
			if outputs.known != test.known || !slices.Equal(slices.Sorted(maps.Keys(outputs.nodes)), test.want) {
				t.Fatalf("guaranteedOutputs = %v, %t; want %v, %t", outputs.nodes, outputs.known, test.want, test.known)
			}
		})
	}
}

func TestJSONDocument_reportsMalformedStructure(t *testing.T) {
	var target any
	if err := jsonDocument(`{`).decode(&target); err == nil {
		t.Fatal("decode unexpectedly accepted a truncated object")
	}
	invalidUTF8 := jsonDocument([]byte{'{', '"', 0xff, '"', ':', '1', '}'})
	if _, err := invalidUTF8.value(); err == nil ||
		!strings.Contains(err.Error(), "invalid UTF-8 at byte 3") {
		t.Fatalf("invalid UTF-8 error = %v; want byte offset", err)
	}
	if _, err := jsonDocument(`1 x`).value(); err == nil {
		t.Fatal("value unexpectedly accepted malformed trailing data")
	}
}

func TestJSONDocument_rejectsUnpairedUnicodeSurrogates(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{name: "plain", data: []byte(`"plain"`)},
		{name: "ordinary escape", data: []byte(`"\u0041"`)},
		{name: "paired surrogates", data: []byte(`"\ud800\udc00"`)},
		{name: "escaped backslash", data: []byte(`"\\ud800"`)},
		{name: "escaped quote", data: []byte(`"\"value"`)},
		{name: "high surrogate", data: []byte(`"\ud800"`), wantErr: "unpaired UTF-16 surrogate"},
		{name: "low surrogate", data: []byte(`"\udc00"`), wantErr: "unpaired UTF-16 surrogate"},
		{name: "high then scalar", data: []byte(`"\ud800\u0041"`), wantErr: "unpaired UTF-16 surrogate"},
		{name: "high then malformed", data: []byte(`"\ud800\uXXXX"`), wantErr: "unpaired UTF-16 surrogate"},
		{name: "short unicode escape", data: []byte(`"\u12"`), wantErr: "invalid character"},
		{name: "unknown escape", data: []byte(`"\q"`), wantErr: "invalid character"},
		{name: "trailing escape", data: []byte{'"', '\\'}, wantErr: "unexpected EOF"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := jsonDocument(test.data).value()
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("value error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("value error = %v; want %q", err, test.wantErr)
			}
		})
	}
}

func TestSchemaInfrastructure_reportsEveryFailureBoundary(t *testing.T) {
	if _, err := (schemaSource{
		url:      "https://example.com/schema.json",
		document: jsonDocument(`{`),
	}).compile(); err == nil {
		t.Fatal("compile unexpectedly accepted malformed schema JSON")
	}
	if _, err := (schemaSource{
		url:      "%",
		document: jsonDocument(`{}`),
	}).compile(); err == nil {
		t.Fatal("compile unexpectedly accepted an invalid resource URL")
	}

	loadErr := errors.New("load schema")
	load := schemaLoader(func() (*compiledSchema, error) {
		return nil, loadErr
	})
	var decoded map[string]any
	if err := load.decode(jsonDocument(`{}`), &decoded); !errors.Is(err, loadErr) {
		t.Fatalf("decode error = %v; want load error", err)
	}

	validateErr := errors.New("validate")
	schema := &compiledSchema{validator: schemaValidatorFunc(func(any) error {
		return validateErr
	})}
	if err := schema.validate(nil); errors.Is(err, validateErr) ||
		!strings.Contains(err.Error(), validateErr.Error()) {
		t.Fatalf("validate error = %v; want diagnostic without backend error identity", err)
	}
}

func TestJSONSchemaError_ordersDeduplicatesAndHidesBackend(t *testing.T) {
	first := &jschema.ValidationError{
		SchemaURL:        "https://example.com/schema.json",
		InstanceLocation: []string{"a"},
		ErrorKind:        &kind.FalseSchema{},
	}
	last := &jschema.ValidationError{
		SchemaURL:        "https://example.com/schema.json",
		InstanceLocation: []string{"z"},
		ErrorKind:        &kind.FalseSchema{},
	}
	makeError := func(causes ...*jschema.ValidationError) *jsonSchemaError {
		return &jsonSchemaError{err: &jschema.ValidationError{
			SchemaURL: "https://example.com/schema.json",
			ErrorKind: &kind.FalseSchema{},
			Causes:    causes,
		}}
	}
	err := makeError(last, first, last)
	message := err.Error()
	if message != makeError(first, last).Error() ||
		strings.Count(message, "at '/a'") != 1 ||
		strings.Count(message, "at '/z'") != 1 ||
		strings.Index(message, "at '/a'") > strings.Index(message, "at '/z'") {
		t.Fatalf("Error = %q; want stable, deduplicated path order", message)
	}
	var backend *jschema.ValidationError
	if errors.As(err, &backend) {
		t.Fatal("JSON Schema backend escaped through the public error chain")
	}
}

func TestSpecCompiler_defendsItsValidatedInputContract(t *testing.T) {
	buildErr := errors.New("build")
	registry := NewRegistry().
		MustRegisterNode("broken", func(NodeSpec) (Step, error) {
			return nil, buildErr
		}).
		MustRegisterResolver("resolver", flow.NodeFunc[Store, string](func(context.Context, Store) (string, error) {
			return "", nil
		})).
		MustRegisterCondition("condition", flow.NodeFunc[Store, bool](func(context.Context, Store) (bool, error) {
			return false, nil
		}))
	compiler := specCompiler{leafCompiler: leafCompiler{registry: registry.snapshot()}}
	broken := Spec{Kind: KindLeaf, ID: "broken", Type: "broken"}

	tests := map[string]Spec{
		"sequence child": {
			Kind: KindSequence, Steps: []Spec{broken},
		},
		"parallel child": {
			Kind: KindParallel, Steps: []Spec{broken},
		},
		"unknown kind": {
			Kind: "unknown",
		},
		"unknown leaf": {
			Kind: KindLeaf, ID: "leaf", Type: "missing",
		},
		"leaf input field": {
			Kind: KindLeaf, ID: "leaf", Type: "broken",
			Input: Output("a"),
		},
		"unknown resolver": {
			Kind: KindBranch, ID: "branch", Resolver: "missing",
		},
		"branch child": {
			Kind: KindBranch, ID: "branch", Resolver: "resolver",
			Cases: map[string]Spec{"case": broken},
		},
		"missing loop body": {
			Kind: KindLoop, ID: "loop", Condition: "condition",
		},
		"unknown condition": {
			Kind: KindLoop, ID: "loop", Body: &Spec{Kind: KindSequence},
			Condition: "missing",
		},
		"loop body": {
			Kind: KindLoop, ID: "loop", Body: &broken,
			Condition: "condition",
		},
		"missing iteration input": {
			Kind: KindIteration, ID: "each",
		},
		"missing iteration body": {
			Kind: KindIteration, ID: "each", Input: Output("items"),
		},
		"missing iteration output": {
			Kind: KindIteration, ID: "each", Input: Output("items"),
			Body: &Spec{Kind: KindSequence},
		},
		"iteration body": {
			Kind: KindIteration, ID: "each", Input: Output("items"),
			Body: &broken, BodyOutput: Output("value"),
		},
		"missing subgraph body": {
			Kind: KindSubgraph, ID: "sub", BodyOutput: Output("value"),
		},
		"missing subgraph output": {
			Kind: KindSubgraph, ID: "sub",
			Body: &Spec{Kind: KindSequence},
		},
		"subgraph body": {
			Kind: KindSubgraph, ID: "sub",
			Body: &broken, BodyOutput: Output("value"),
		},
	}

	for name, spec := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := compiler.compile(spec); err == nil {
				t.Fatal("compile unexpectedly succeeded")
			}
		})
	}
}

func TestStoreInternals_reportStableFallbacks(t *testing.T) {
	for _, value := range []jsonValue{
		{raw: map[string]any{}},
		{raw: bytes.NewBuffer(nil)},
	} {
		if value.kind() == "" {
			t.Fatal("kind returned an empty description")
		}
	}

	changes := make([]storeChange, 0, storeOverlayLimit*2+1)
	base := NewStore()
	for index := range storeOverlayLimit*2 + 1 {
		next := base.WithOutput(fmt.Sprintf("node-%d", index), index)
		changes = append(changes, next.changesSince(base)...)
	}
	merged := base.withChanges(changes)
	if merged.depth != 0 {
		t.Fatalf("withChanges depth = %d; want compacted snapshot", merged.depth)
	}
}

func TestStoreChanges_orderSurvivesCompaction(t *testing.T) {
	base := NewStore()
	first := base.WithOutput("first", 1)
	second := base.WithOutput("second", 2)

	// Merge in the opposite order from the writes themselves. Revision is the
	// Store's write order; overlay linkage is only a representation detail.
	merged := base.merge(second, first)
	want := []Ref{Output("first"), Output("second")}
	for name, store := range map[string]Store{
		"overlay":   merged,
		"compacted": merged.compact(),
	} {
		t.Run(name, func(t *testing.T) {
			changes := store.Changes(base)
			got := make([]Ref, len(changes))
			for index, change := range changes {
				got[index] = change.Ref()
			}
			if !slices.Equal(got, want) {
				t.Fatalf("Changes = %v; want write order %v", got, want)
			}
		})
	}
}

func TestGraphDecorators_preserveDefinitionAndStoreBoundaries(t *testing.T) {
	t.Run("empty graph namespace", func(t *testing.T) {
		store := NewStore().WithOutput("external", 1)
		if got := store.withoutNodes(nil); got != store {
			t.Fatal("empty node set changed the Store")
		}
		nodes := nodeSet{"node": {}}
		if got := (Store{}).withoutNodes(nodes); got != (Store{}) {
			t.Fatalf("empty Store changed to %+v", got)
		}
		if got := store.withoutNodes(nodes); got != store {
			t.Fatal("unrelated node set changed the Store")
		}
		externalSnapshot := store.compact()
		if got := externalSnapshot.withoutNodes(nodes); got != externalSnapshot {
			t.Fatal("unrelated node set changed a snapshotted Store")
		}
		for name, owned := range map[string]Store{
			"overlay":  NewStore().WithOutput("node", 1),
			"snapshot": NewStore().WithOutput("node", 1).compact(),
		} {
			t.Run(name, func(t *testing.T) {
				cleared := owned.withoutNodes(nodes)
				if _, present := cleared.Lookup(Output("node")); present {
					t.Fatal("cleared Store retained the internal node")
				}
				if _, present := owned.merge(cleared).Lookup(Output("node")); present {
					t.Fatal("namespace removal did not survive Store merging")
				}
				if changes := cleared.Changes(owned); len(changes) != 0 {
					t.Fatalf("Changes exposed private namespace removals: %v", changes)
				}
				encoded, err := cleared.MarshalJSON()
				if err != nil || string(encoded) != `{}` {
					t.Fatalf("MarshalJSON = %s, %v; want {}, nil", encoded, err)
				}
			})
		}

		base := NewStore().WithOutput("node", 1)
		cleared := base.
			WithOutput("node", 2).
			WithOutput("node", 3).
			withoutNodes(nodes).
			compact()
		if _, present := base.merge(cleared).Lookup(Output("node")); present {
			t.Fatal("compaction lost a namespace removal's Store lineage")
		}

		unrelated := NewStore().WithOutput("node", 4)
		merged := unrelated.merge(cleared)
		if value, err := Get[int](merged, Output("node")); err != nil || value != 4 {
			t.Fatalf("unrelated Store value = %d, %v; private removal leaked across lineages", value, err)
		}

		removed := base.withoutNodes(nodes)
		written := base.WithOutput("node", 5)
		if value, err := Get[int](base.merge(removed, written), Output("node")); err != nil || value != 5 {
			t.Fatalf("later write = %d, %v; want 5, nil", value, err)
		}
		if value, present := base.merge(written, removed).Lookup(Output("node")); present {
			t.Fatalf("later namespace cleanup left value %v", value)
		}

		large := NewStore()
		for index := range storeOverlayLimit*2 + 1 {
			large = large.WithCell("node", fmt.Sprintf("key-%03d", index), index)
		}
		cleared = large.withoutNodes(nodes)
		if cleared.depth != 0 {
			t.Fatalf("large namespace cleanup depth = %d; want compacted snapshot", cleared.depth)
		}
		if encoded, err := cleared.MarshalJSON(); err != nil || string(encoded) != `{}` {
			t.Fatalf("large cleanup MarshalJSON = %s, %v; want {}, nil", encoded, err)
		}
	})

	t.Run("gated definition depth", func(t *testing.T) {
		var step definedStep = interruptStep{id: "inner"}
		for range MaxNestingDepth + 1 {
			step = Sequence(step).(definedStep)
		}
		step = gated(
			[]compiledGate{{
				Gate:    When("route", "yes"),
				outlets: []string{"yes"},
			}},
			TriggerAll,
			step,
		)
		validator := definitionValidator{}
		if err := validator.validate(step); !errors.Is(err, ErrMaxDepth) {
			t.Fatalf("error = %v; want ErrMaxDepth", err)
		}
	})

	t.Run("bypass reserves execution identity", func(t *testing.T) {
		step := gated(
			[]compiledGate{{
				Gate:    When("route", "yes"),
				outlets: []string{"yes", "no"},
			}},
			TriggerAll,
			interruptStep{id: "target"},
		)
		ctx := withConfig(t.Context(), RunConfig{})
		store := NewStore().WithOutput("route", "no")
		if _, err := step.Run(ctx, store); err != nil {
			t.Fatalf("first bypass: %v", err)
		}
		if _, err := step.Run(ctx, store); !errors.Is(err, ErrDuplicateStep) {
			t.Fatalf("second bypass error = %v; want ErrDuplicateStep", err)
		}
	})
}

func TestGraphCall_doesNotEnterStepAfterCancellation(t *testing.T) {
	calls := 0
	call := graphCall{
		index: 3,
		input: NewStore().WithOutput("seed", 1),
		step: opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
			calls++
			return Store{}, nil
		}),
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	outcome := call.run(ctx)
	if calls != 0 {
		t.Fatalf("step calls = %d; want 0", calls)
	}
	if outcome.index != 3 || !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("outcome = %+v; want index 3 with context cancellation", outcome)
	}
	if value, err := Get[int](outcome.store, Output("seed")); err != nil || value != 1 {
		t.Fatalf("preserved input = %d, %v; want 1, nil", value, err)
	}
}

func TestGraphExecution_doesNotStartReadyNodeAfterCancellation(t *testing.T) {
	calls := 0
	execution := graphExecution{
		graph: graphStep{steps: stepList{opaqueTestStepFunc(
			func(context.Context, Store) (Store, error) {
				calls++
				return Store{}, nil
			},
		)}},
		input:  NewStore(),
		states: make([]graphNodeState, 1),
		ready:  []int{0},
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	outcomes := make(chan graphOutcome, 1)

	execution.startReady(ctx, outcomes, 1)

	if calls != 0 || execution.active != 0 || execution.head != 0 {
		t.Fatalf(
			"calls, active, head = %d, %d, %d; want 0, 0, 0",
			calls,
			execution.active,
			execution.head,
		)
	}
	if execution.states[0] != graphNodePending || len(outcomes) != 0 {
		t.Fatalf(
			"state, outcomes = %d, %d; want pending, 0",
			execution.states[0],
			len(outcomes),
		)
	}
}

func TestCompareBool_ordersFalseBeforeTrue(t *testing.T) {
	if got := compareBool(false, true); got >= 0 {
		t.Fatalf("compareBool(false, true) = %d; want negative", got)
	}
}

func TestValueType_acceptsOnlyPossibleCellPaths(t *testing.T) {
	tests := []struct {
		valueType ValueType
		ref       Ref
		want      bool
	}{
		{valueType: TypeString, ref: Output("source"), want: true},
		{valueType: TypeAny, ref: Output("source").Child("member"), want: true},
		{valueType: TypeObject, ref: Output("source").Child("member"), want: true},
		{valueType: TypeArray, ref: Output("source").Child("0"), want: true},
		{valueType: TypeArray, ref: Output("source").Child("member")},
		{valueType: TypeBool, ref: Output("source").Child("member")},
		{valueType: TypeArray, ref: Ref{NodeID: "source", Path: "output/0"}},
		{valueType: TypeArray, ref: Ref{NodeID: "source", Path: "/other/0"}},
		{valueType: TypeArray, ref: Ref{NodeID: "source", Path: "/output~2/0"}},
		{valueType: TypeArray, ref: Ref{NodeID: "source", Path: "/output/~2"}},
		{valueType: TypeArray, ref: Output("other")},
	}
	for _, test := range tests {
		if got := test.valueType.acceptsCellPath(
			test.ref,
			"source",
			outputKey,
		); got != test.want {
			t.Fatalf("%q accepts %q = %v; want %v", test.valueType, test.ref.Path, got, test.want)
		}
	}
}

// populatedSpec returns a Spec with every field except Kind set to a non-zero,
// JSON-valid value. A field whose type has no sample here fails the test rather
// than quietly escaping the matrix checks below, which is the point: a new Spec
// field must be accounted for deliberately.
func populatedSpec(t *testing.T) Spec {
	t.Helper()
	inner := Spec{Kind: KindLeaf, ID: "inner", Type: "registered"}
	samples := map[reflect.Type]any{
		reflect.TypeFor[string]():          "value",
		reflect.TypeFor[int]():             1,
		reflect.TypeFor[json.RawMessage](): json.RawMessage(`{}`),
		reflect.TypeFor[Ref]():             Output("producer"),
		reflect.TypeFor[Inputs]():          OneInput(Output("producer")),
		reflect.TypeFor[[]Spec]():          []Spec{inner},
		reflect.TypeFor[map[string]Spec](): map[string]Spec{"case": inner},
		reflect.TypeFor[*Spec]():           &inner,
	}

	spec := Spec{Kind: KindLeaf}
	value := reflect.ValueOf(&spec).Elem()
	for index := range value.NumField() {
		field := value.Type().Field(index)
		if field.Name == "Kind" {
			continue
		}
		sample, ok := samples[field.Type]
		if !ok {
			t.Fatalf(
				"Spec field %s has type %s with no sample value; add one so the field matrices are checked against it",
				field.Name,
				field.Type,
			)
		}
		value.Field(index).Set(reflect.ValueOf(sample))
	}
	return spec
}

// specMemberNames returns the JSON member name of every Spec field except kind,
// read from the struct tags. It is the one source of truth the checks below
// compare against.
func specMemberNames(t *testing.T) []string {
	t.Helper()
	specType := reflect.TypeFor[Spec]()
	names := make([]string, 0, specType.NumField())
	for field := range specType.Fields() {
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "" {
			t.Fatalf("Spec field %s has no JSON member name", field.Name)
		}
		if name == fieldKind {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// TestSpecFieldMatricesAgreeWithTheSpecStruct pins the three places that must
// know every Spec field to the struct itself: populatedFields, which decides
// what "populated" means; specKindFields, which decides what each kind may
// carry; and the embedded JSON Schema, which decides the same thing on the wire.
// Nothing else enforces that agreement, and its absence is not inert — a field
// added to Spec but missing from a matrix is accepted by every kind, and one
// missing from the schema makes Spec marshal a document it cannot unmarshal.
func TestSpecFieldMatricesAgreeWithTheSpecStruct(t *testing.T) {
	want := specMemberNames(t)

	t.Run("populatedFields", func(t *testing.T) {
		got := populatedSpec(t).populatedFields()
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("populatedFields() = %v; want every Spec field %v", got, want)
		}
	})

	t.Run("specKindFields union", func(t *testing.T) {
		union := make(map[string]struct{}, len(want))
		for _, fields := range specKindFields {
			for _, field := range fields {
				union[field] = struct{}{}
			}
		}
		if got := slices.Sorted(maps.Keys(union)); !slices.Equal(got, want) {
			t.Fatalf("specKindFields covers %v; want every Spec field %v", got, want)
		}
	})

	t.Run("wire schema per kind", func(t *testing.T) {
		for kind, members := range specSchemaKindMembers(t) {
			got := slices.Sorted(maps.Keys(members))
			expected := slices.Sorted(slices.Values(specKindFields[kind]))
			if !slices.Equal(got, expected) {
				t.Fatalf(
					"schema allows %v for kind %q; specKindFields allows %v",
					got,
					kind,
					expected,
				)
			}
		}
	})
}

// specSchemaKindMembers reads the per-kind member sets out of the embedded Spec
// JSON Schema. Each kind's definition is the object whose kind property is
// pinned to a const, so the set is discovered rather than restated here.
func specSchemaKindMembers(t *testing.T) map[Kind]map[string]any {
	t.Helper()
	var document struct {
		Defs map[string]struct {
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(specSchemaJSON, &document); err != nil {
		t.Fatalf("decode embedded spec schema: %v", err)
	}

	byKind := make(map[Kind]map[string]any, len(specKindFields))
	for _, definition := range document.Defs {
		raw, ok := definition.Properties[fieldKind]
		if !ok {
			continue
		}
		var pinned struct {
			Const string `json:"const"`
		}
		if err := json.Unmarshal(raw, &pinned); err != nil || pinned.Const == "" {
			continue
		}
		members := make(map[string]any, len(definition.Properties))
		for member := range definition.Properties {
			if member != fieldKind {
				members[member] = nil
			}
		}
		byKind[Kind(pinned.Const)] = members
	}
	if len(byKind) != len(specKindFields) {
		t.Fatalf("schema defines %d kinds; specKindFields has %d", len(byKind), len(specKindFields))
	}
	return byKind
}

// TestStoreCells_reportsEveryLiveRecordAndHonorsAnEarlyStop covers the iterator
// contract that withoutNodes, its only caller, does not exercise: a caller may
// stop in either half of the traversal, and stopping must not run the rest.
func TestStoreCells_reportsEveryLiveRecordAndHonorsAnEarlyStop(t *testing.T) {
	const writes = storeOverlayLimit + 3

	// Writing past the limit leaves a snapshot behind, and the writes after that
	// form the overlay, so the fixture has both halves.
	store := NewStore()
	for index := range writes {
		store = store.WithOutput(fmt.Sprintf("node-%02d", index), index)
	}
	// Rewriting one cell makes the overlay shadow a snapshot record.
	store = store.WithOutput("node-00", "rewritten")
	if store.snapshot == nil || store.delta == nil {
		t.Fatalf(
			"fixture has snapshot %t, overlay %t; want both halves",
			store.snapshot != nil,
			store.delta != nil,
		)
	}

	seen := make(map[storeKey]cell, writes)
	for key, record := range store.cells() {
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("cell %v reported twice", key)
		}
		seen[key] = record
	}
	if len(seen) != writes {
		t.Fatalf("cells reported %d records; want %d", len(seen), writes)
	}
	if got := seen[storeKey{nodeID: "node-00", key: outputKey}].value; got != "rewritten" {
		t.Fatalf("shadowed cell = %v; want the overlay value", got)
	}

	for _, test := range []struct {
		name  string
		store Store
		after int
	}{
		{name: "in the overlay", store: store, after: 1},
		{name: "in the snapshot", store: store, after: store.depth + 1},
		{name: "with no overlay", store: store.compact(), after: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			count := 0
			for range test.store.cells() {
				count++
				if count == test.after {
					break
				}
			}
			if count != test.after {
				t.Fatalf("stopped after %d records; want %d", count, test.after)
			}
		})
	}

	for range (Store{}).cells() {
		t.Fatal("the zero Store reported a cell")
	}
}
