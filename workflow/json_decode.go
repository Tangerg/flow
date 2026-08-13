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

func translateJSONError(err error) error {
	return jsondoc.TranslateDepth(err, ErrMaxDepth)
}
