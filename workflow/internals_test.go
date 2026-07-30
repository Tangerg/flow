package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	jschema "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

type schemaValidatorFunc func(any) error

func (validate schemaValidatorFunc) Validate(value any) error {
	return validate(value)
}

type opaqueTestStep struct{}

func (opaqueTestStep) Run(_ context.Context, store Store) (Store, error) {
	return store, nil
}

func TestJSONDocument_reportsMalformedStructure(t *testing.T) {
	var target any
	if _, err := jsonDocument(`{`).decodeWithValue(&target); err == nil {
		t.Fatal("decodeWithValue unexpectedly accepted a truncated object")
	}
	if _, err := jsonDocument(`1 x`).value(); err == nil {
		t.Fatal("value unexpectedly accepted malformed trailing data")
	}

	memberReader := jsonReader{
		decoder: json.NewDecoder(strings.NewReader(`"`)),
	}
	if _, err := memberReader.readMemberName(); err == nil {
		t.Fatal("readMemberName unexpectedly accepted a truncated string")
	}

	for name, data := range map[string]string{
		"object": `{"value":1`,
		"member": `{"`,
		"array":  `[1`,
	} {
		t.Run(name, func(t *testing.T) {
			reader := jsonReader{decoder: json.NewDecoder(strings.NewReader(data))}
			if _, err := reader.read(); err == nil {
				t.Fatalf("read unexpectedly accepted %s", data)
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
	if err := load.validate(jsonDocument(`{}`)); !errors.Is(err, loadErr) {
		t.Fatalf("validate error = %v; want load error", err)
	}
	var decoded map[string]any
	if err := load.decode(jsonDocument(`{}`), &decoded); !errors.Is(err, loadErr) {
		t.Fatalf("decode error = %v; want load error", err)
	}

	validateErr := errors.New("validate")
	schema := &compiledSchema{validator: schemaValidatorFunc(func(any) error {
		return validateErr
	})}
	if err := schema.validate(nil); !errors.Is(err, validateErr) {
		t.Fatalf("validate error = %v; want raw validator error", err)
	}
}

func TestJSONSchemaError_deduplicatesAndUnwraps(t *testing.T) {
	leaf := &jschema.ValidationError{
		SchemaURL: "https://example.com/schema.json",
		ErrorKind: &kind.FalseSchema{},
	}
	root := &jschema.ValidationError{
		SchemaURL: "https://example.com/schema.json",
		ErrorKind: &kind.FalseSchema{},
		Causes:    []*jschema.ValidationError{leaf, leaf},
	}
	err := &jsonSchemaError{err: root}
	if message := err.Error(); strings.Contains(message, ";") {
		t.Fatalf("Error = %q; duplicate diagnostics were not removed", message)
	}
	if errors.Unwrap(err) != root {
		t.Fatal("Unwrap did not return the structured validation error")
	}
}

func TestSpecCompiler_defendsItsValidatedInputContract(t *testing.T) {
	buildErr := errors.New("build")
	registry := NewRegistry().
		MustRegisterLeaf("broken", func(LeafSpec) (Step, error) {
			return nil, buildErr
		}).
		MustRegisterResolver("resolver", func(context.Context, Store) (string, error) {
			return "", nil
		}).
		MustRegisterCondition("condition", func(context.Context, int, Store) (bool, error) {
			return false, nil
		})
	compiler := specCompiler{leafCompiler: leafCompiler{registry: registry}}
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
		"duplicate default input": {
			Kind: KindLeaf, ID: "leaf", Type: "broken",
			Input:  Output("a"),
			Inputs: Inputs{DefaultPort: Output("b")},
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
	if _, _, err := (storeJSONDocument{}).firstInvalidValue(); err != nil {
		t.Fatalf("empty document error = %v", err)
	}
	for _, value := range []jsonValue{
		{raw: map[string]any{}},
		{raw: bytes.NewBuffer(nil)},
	} {
		if value.kind() == "" {
			t.Fatal("kind returned an empty description")
		}
	}
}

func TestGraphDecorators_preserveDefinitionAndStoreBoundaries(t *testing.T) {
	t.Run("opaque definition", func(t *testing.T) {
		step := decoratedStep{
			id:   "opaque",
			step: opaqueTestStep{},
		}
		definition := step.workflowDefinition()
		if definition.kind != definitionNamed || definition.id != "opaque" {
			t.Fatalf("definition = %+v; want named opaque", definition)
		}
		if description := step.Describe(); description.ID != "opaque" ||
			description.Kind != "opaque" {
			t.Fatalf("description = %+v; want named opaque", description)
		}
	})

	t.Run("empty graph namespace", func(t *testing.T) {
		store := NewStore().WithOutput("external", 1)
		if got := store.withoutNodes(nil); got != store {
			t.Fatal("empty node set changed the Store")
		}
		nodes := map[string]struct{}{"node": {}}
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
		if got := NewStore().WithOutput("node", 1).withoutNodes(nodes); got != (Store{}) {
			t.Fatalf("removing the only internal node returned %+v", got)
		}
		snapshot := NewStore().WithOutput("node", 1).compact()
		if got := snapshot.withoutNodes(nodes); got != (Store{}) {
			t.Fatalf("removing a snapshotted internal node returned %+v", got)
		}
	})

	t.Run("gated definition depth", func(t *testing.T) {
		var step Step = opaqueTestStep{}
		for range MaxNestingDepth {
			step = Sequence(step)
		}
		step = gated(
			"target",
			[]compiledGate{{
				Gate:    When("route", "yes"),
				outlets: []string{"yes"},
			}},
			TriggerAll,
			step,
		)
		if err := (definitionValidator{}).validate(step); !errors.Is(err, ErrMaxDepth) {
			t.Fatalf("error = %v; want ErrMaxDepth", err)
		}
	})

	t.Run("bypass reserves execution identity", func(t *testing.T) {
		step := gated(
			"target",
			[]compiledGate{{
				Gate:    When("route", "yes"),
				outlets: []string{"yes", "no"},
			}},
			TriggerAll,
			opaqueTestStep{},
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
