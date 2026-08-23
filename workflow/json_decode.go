package workflow

import (
	"fmt"
	"maps"
	"slices"

	"github.com/Tangerg/flow/internal/jsondoc"
)

// jsonDocument keeps workflow's strict JSON boundary private while sharing its
// parser with optional packages. Workflow translates the parser's structural
// depth diagnostic into its stable ErrMaxDepth category here.
type jsonDocument []byte

// jsonObject is one strictly parsed JSON object. Semantic wire types own their
// fields and decode themselves from it; these methods state the shared exact-
// member contract without delegating to encoding/json's case folding.
type jsonObject map[string]any

var strictJSON = jsondoc.Codec{MaxDepth: MaxNestingDepth}

func marshalJSON(value any) ([]byte, error) {
	data, err := strictJSON.Marshal(value)
	return data, translateJSONError(err)
}

func (j jsonDocument) validate() error {
	return translateJSONError(strictJSON.Validate(j))
}

func (j jsonDocument) decode(dst any) error {
	return translateJSONError(strictJSON.Decode(j, dst))
}

func (j jsonDocument) decodeParsed(dst any) error {
	return translateJSONError(strictJSON.DecodeParsed(j, dst))
}

func (j jsonDocument) value() (any, error) {
	value, err := strictJSON.Value(j)
	return value, translateJSONError(err)
}

func (j jsonDocument) object() (map[string]any, error) {
	object, err := strictJSON.Object(j)
	return object, translateJSONError(err)
}

func (j jsonObject) require(kind string, required ...string) error {
	for _, name := range required {
		if _, present := j[name]; !present {
			return fmt.Errorf("%s field %q is missing", kind, name)
		}
	}
	return nil
}

// stringMember reads a member that must be a string. Presence is a separate
// contract — require names an absent member — so this names one that is present
// with the wrong type.
func (j jsonObject) stringMember(name string) (string, error) {
	value, ok := j[name].(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", name)
	}
	return value, nil
}

func (j jsonObject) allow(allowed ...string) error {
	for _, name := range slices.Sorted(maps.Keys(j)) {
		if !slices.Contains(allowed, name) {
			return fmt.Errorf("unknown field %q", name)
		}
	}
	return nil
}

// strictObject is the contract every canonical object in this package shares: it
// must be a JSON object, may hold only the members named here, and must hold the
// required ones. Naming each member once, as required or optional, is what keeps
// a member from being demanded and refused at the same time -- two lists could
// disagree, and a member missing from the allowed one is rejected however it
// arrives.
// what names the object a missing member is missing from, so it is set even
// where every member is currently optional and nothing reads it yet.
type strictObject struct {
	what     string
	required []string
	optional []string
}

// check applies the contract to already-parsed members, which is how a member
// nested inside a larger document arrives.
func (s strictObject) check(object jsonObject) error {
	allowed := slices.Concat(s.required, s.optional)
	if err := object.allow(allowed...); err != nil {
		return err
	}
	return object.require(s.what, s.required...)
}

// parse applies the contract to a complete document and returns its members.
func (s strictObject) parse(data []byte) (jsonObject, error) {
	raw, err := jsonDocument(data).object()
	if err != nil {
		return nil, err
	}
	object := jsonObject(raw)
	if err := s.check(object); err != nil {
		return nil, err
	}
	return object, nil
}

func translateJSONError(err error) error {
	return jsondoc.TranslateDepth(err, ErrMaxDepth)
}

// decodeInto is [jsondoc.DecodeInto] with this package's nil-receiver
// reporting; see it for the boundary all of these methods share.
func (j jsonDocument) decodeInto[T any](
	dst *T,
	decode func([]byte) (T, error),
	wrap func(error) error,
) error {
	return jsondoc.DecodeInto(dst, j, decode, wrap)
}

// unmarshalError names one decoding boundary that has no structured error.
func unmarshalError(what string) func(error) error {
	return func(err error) error {
		return fmt.Errorf("workflow: unmarshal %s: %w", what, err)
	}
}

func graphJSONError(err error) error { return &GraphError{Field: fieldJSON, Err: err} }

func specJSONError(err error) error { return &SpecError{Field: fieldJSON, Err: err} }
