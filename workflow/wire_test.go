package workflow_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Tangerg/flow/workflow"
)

// wireSample returns a valid non-zero value for one field of a type with a JSON
// wire format, keyed by "Type.Field" where the field needs more than its type
// implies and by type otherwise. A field with neither fails the test: a newly
// added wire field must be accounted for here, which is what makes the round trip
// below actually cover it.
func wireSample(t *testing.T, owner reflect.Type, field reflect.StructField) reflect.Value {
	t.Helper()
	byField := map[string]any{
		"Ref.Path":          "/output",
		"Suspension.Value":  "waiting on a person",
		"GraphNode.Trigger": workflow.TriggerAny,
	}
	byType := map[reflect.Type]any{
		reflect.TypeFor[string]():          "value",
		reflect.TypeFor[int]():             2,
		reflect.TypeFor[uint64]():          uint64(7),
		reflect.TypeFor[bool]():            true,
		reflect.TypeFor[json.RawMessage](): json.RawMessage(`{"key":"value"}`),
		reflect.TypeFor[workflow.Ref]():    workflow.Output("producer"),
		reflect.TypeFor[workflow.Inputs](): workflow.OneInput(workflow.Output("producer")),
		reflect.TypeFor[[]string]():        []string{"other"},
		reflect.TypeFor[[]workflow.Gate](): []workflow.Gate{workflow.When("router", "yes")},
	}
	if sample, ok := byField[owner.Name()+"."+field.Name]; ok {
		return reflect.ValueOf(sample)
	}
	if sample, ok := byType[field.Type]; ok {
		return reflect.ValueOf(sample)
	}
	// A slice of another wire type carries a fully populated element, so decoding
	// the outer document has to accept every member of the inner one too. This is
	// what makes the Graph case cover GraphNode: decoded alone a GraphNode goes
	// through struct tags both ways and cannot disagree with itself, while inside
	// a Graph it is checked against the embedded schema.
	if field.Type.Kind() == reflect.Slice && field.Type.Elem().Kind() == reflect.Struct {
		return reflect.Append(
			reflect.MakeSlice(field.Type, 0, 1),
			populatedWireValue(t, field.Type.Elem()),
		)
	}
	t.Fatalf(
		"wire type %s field %s of type %s has no sample value; add one so the round trip covers it",
		owner.Name(),
		field.Name,
		field.Type,
	)
	return reflect.Value{}
}

// populatedWireValue returns a value of ownerType with every field set.
func populatedWireValue(t *testing.T, ownerType reflect.Type) reflect.Value {
	t.Helper()
	populated := reflect.New(ownerType).Elem()
	for index := range ownerType.NumField() {
		populated.Field(index).Set(wireSample(t, ownerType, ownerType.Field(index)))
	}
	return populated
}

// TestWireTypesRoundTripEveryPopulatedField checks that each type whose JSON
// member set is stated twice can read back a document it wrote with every field
// populated. Encoding runs off the struct tags while decoding runs off a separate
// statement of the same set — an explicit member list for Ref, ScopeFrame,
// JournalKey, and Suspension, and the embedded JSON Schema for Graph. Those
// second statements earn their place by rejecting unknown, duplicate, and
// case-folded members, which encoding/json cannot, but nothing else keeps them
// agreeing with the struct: a field added to one side alone yields a value that
// marshals to a document it then rejects. Spec is checked separately because its
// members depend on its kind.
func TestWireTypesRoundTripEveryPopulatedField(t *testing.T) {
	for _, prototype := range []any{
		workflow.Ref{},
		workflow.ScopeFrame{},
		workflow.JournalKey{},
		workflow.Suspension{},
		workflow.Gate{},
		workflow.GraphNode{},
		workflow.Graph{},
	} {
		ownerType := reflect.TypeOf(prototype)
		t.Run(ownerType.Name(), func(t *testing.T) {
			original := populatedWireValue(t, ownerType).Interface()

			encoded, err := json.Marshal(original)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			decoded := reflect.New(ownerType)
			if err := json.Unmarshal(encoded, decoded.Interface()); err != nil {
				t.Fatalf("Unmarshal of what Marshal produced: %v\nwire: %s", err, encoded)
			}
			if got := decoded.Elem().Interface(); !reflect.DeepEqual(got, original) {
				t.Fatalf("round trip changed the value:\n got: %+v\nwant: %+v\nwire: %s", got, original, encoded)
			}
		})
	}
}

// A member contract reports what the document lacks. Every decoder here that
// requires a member names it the same way, so one defect in one member does not
// read differently depending on which type decoded it: JournalKey used to leave
// an absent id to its identity check, which called it an empty one instead.
func TestDecodersNameAMissingRequiredMember(t *testing.T) {
	tests := []struct {
		name     string
		document string
		into     json.Unmarshaler
		want     string
	}{
		{
			name:     "ref node ID",
			document: `{"path":"/output"}`,
			into:     new(workflow.Ref),
			want:     `ref field "nodeID" is missing`,
		},
		{
			name:     "ref path",
			document: `{"nodeID":"producer"}`,
			into:     new(workflow.Ref),
			want:     `ref field "path" is missing`,
		},
		{
			name:     "scope frame id",
			document: `{"index":1}`,
			into:     new(workflow.ScopeFrame),
			want:     `scope frame field "id" is missing`,
		},
		{
			name:     "journal key id",
			document: `{"scope":[{"id":"loop"}]}`,
			into:     new(workflow.JournalKey),
			want:     `journal key field "id" is missing`,
		},
		{
			name:     "journal record id",
			document: `{"version":4,"records":[{"value":1}]}`,
			into:     workflow.NewJournal(),
			want:     `record field "id" is missing`,
		},
		{
			name:     "journal record value",
			document: `{"version":4,"records":[{"id":"step"}]}`,
			into:     workflow.NewJournal(),
			want:     `record field "value" is missing`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.into.UnmarshalJSON([]byte(test.document))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("UnmarshalJSON = %v; want a message containing %q", err, test.want)
			}
		})
	}
}

// An empty required member is a different defect from an absent one and must not
// be reported as the same thing.
func TestDecodersDistinguishAnEmptyMemberFromAnAbsentOne(t *testing.T) {
	key := new(workflow.JournalKey)
	err := key.UnmarshalJSON([]byte(`{"id":""}`))
	if err == nil || strings.Contains(err.Error(), "missing") {
		t.Fatalf("UnmarshalJSON of an empty id = %v; want an invalid-identity error", err)
	}
}

// TestSpecRoundTripsEveryKind is the Spec half of the check above, which has to
// be written per kind: a Spec's members depend on its Kind, so no single
// populated value exercises the format. Byte stability is the property that
// matters here — a definition is stored and compared as bytes, so re-encoding
// what was just decoded must not move a member or drop one.
func TestSpecRoundTripsEveryKind(t *testing.T) {
	leaf := workflow.Spec{
		Kind: workflow.KindLeaf, ID: "leaf", Type: "registered",
		Config: json.RawMessage(`{"k":"v"}`),
		Inputs: workflow.OneInput(workflow.Output("seed")),
	}
	kinds := map[workflow.Kind]workflow.Spec{
		workflow.KindLeaf:     leaf,
		workflow.KindSequence: {Kind: workflow.KindSequence, Steps: []workflow.Spec{leaf}},
		workflow.KindParallel: {
			Kind: workflow.KindParallel, Steps: []workflow.Spec{leaf}, Concurrency: 3,
		},
		workflow.KindBranch: {
			Kind: workflow.KindBranch, ID: "branch", Resolver: "pick",
			// Two cases, because their order on the wire comes from sorting rather
			// than from Go's map iteration.
			Cases: map[string]workflow.Spec{"accept": leaf, "reject": leaf},
		},
		workflow.KindLoop: {
			Kind: workflow.KindLoop, ID: "loop", Body: &leaf,
			Condition: "stop", MaxIterations: 7,
		},
		workflow.KindIteration: {
			Kind: workflow.KindIteration, ID: "each", Input: workflow.Output("items"),
			Body: &leaf, BodyOutput: workflow.Output("leaf"), Concurrency: 2,
		},
		workflow.KindSubgraph: {
			Kind: workflow.KindSubgraph, ID: "sub",
			Inputs: workflow.OneInput(workflow.Output("seed")),
			Body:   &leaf, BodyOutput: workflow.Output("leaf"),
		},
	}

	for kind, spec := range kinds {
		t.Run(string(kind), func(t *testing.T) {
			first, err := json.Marshal(spec)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var decoded workflow.Spec
			if err = json.Unmarshal(first, &decoded); err != nil {
				t.Fatalf("Unmarshal of what Marshal produced: %v\nwire: %s", err, first)
			}
			if !reflect.DeepEqual(decoded, spec) {
				t.Fatalf("round trip changed the value:\n got: %+v\nwant: %+v", decoded, spec)
			}
			second, err := json.Marshal(decoded)
			if err != nil {
				t.Fatalf("Marshal of the decoded Spec: %v", err)
			}
			if string(second) != string(first) {
				t.Fatalf("re-encoding moved bytes:\nfirst:  %s\nsecond: %s", first, second)
			}
		})
	}
}

// The nesting limit is shared, but each boundary counts its own unit, so a Spec
// meets the JSON one first: every level spends a step object and a steps array.
// What matters is that the boundary rejects cleanly and that whatever it accepts
// can be read back — a definition that encodes to a document it cannot decode
// would be persisted and then refused on resume.
func TestSpecNestingStopsAtTheJSONBoundaryAndStaysReadable(t *testing.T) {
	nest := func(levels int) workflow.Spec {
		spec := workflow.Spec{Kind: workflow.KindLeaf, ID: "leaf", Type: "registered"}
		for range levels {
			spec = workflow.Spec{Kind: workflow.KindSequence, Steps: []workflow.Spec{spec}}
		}
		return spec
	}

	// Two JSON containers per level, so the limit lands at half of it.
	const deepest = workflow.MaxNestingDepth/2 - 1

	encoded, err := json.Marshal(nest(deepest))
	if err != nil {
		t.Fatalf("Marshal at the deepest accepted nesting: %v", err)
	}
	var decoded workflow.Spec
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal of what Marshal produced: %v", err)
	}
	if !reflect.DeepEqual(decoded, nest(deepest)) {
		t.Fatal("the deepest accepted Spec did not survive its round trip")
	}

	_, err = json.Marshal(nest(deepest + 1))
	if !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("Marshal one level deeper = %v; want ErrMaxDepth", err)
	}
}

// TestTextAndDefinitionChecksDescribeAFieldTheSameWay pins the vocabulary the
// two validators share. A Spec field is checked twice for the same property --
// that its text crosses the JSON boundary unchanged -- once while validating a
// definition and once while encoding one. Naming the field differently would
// make one defect read as two different problems depending on which check ran.
func TestTextAndDefinitionChecksDescribeAFieldTheSameWay(t *testing.T) {
	notUTF8 := string([]byte{0xff})
	leaf := workflow.Spec{Kind: workflow.KindLeaf, ID: "inner", Type: "noop"}
	registry := workflow.NewRegistry().
		MustRegisterNode("noop", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Interrupt(spec.ID, nil), nil
		})

	specs := map[string]workflow.Spec{
		"type": {Kind: workflow.KindLeaf, ID: "a", Type: notUTF8},
		"resolver": {
			Kind: workflow.KindBranch, ID: "b", Resolver: notUTF8,
			Cases: map[string]workflow.Spec{"x": leaf},
		},
		"condition": {
			Kind: workflow.KindLoop, ID: "l", Condition: notUTF8, Body: &leaf,
		},
	}

	for field, spec := range specs {
		t.Run(field, func(t *testing.T) {
			definition := registry.ValidateSpec(spec)
			// MarshalJSON is called directly because encoding/json prefixes its
			// own wrapper, which is not part of what the two checks report.
			_, encoding := spec.MarshalJSON()
			if definition == nil || encoding == nil {
				t.Fatalf("ValidateSpec = %v, MarshalJSON = %v; want both to reject", definition, encoding)
			}
			if definition.Error() != encoding.Error() {
				t.Fatalf("the two checks describe %s differently:\n  definition: %v\n  encoding:   %v",
					field, definition, encoding)
			}
		})
	}
}
