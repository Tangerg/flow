package workflow_test

import (
	"encoding/json"
	"reflect"
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
