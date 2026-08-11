package workflow

import (
	"errors"
	"fmt"

	"github.com/Tangerg/flow/internal/jsondoc"
)

// jsonDocument keeps workflow's strict JSON boundary private while sharing its
// parser with optional packages. Workflow translates the parser's structural
// depth diagnostic into its stable ErrMaxDepth category here.
type jsonDocument []byte

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
	value, err := j.value()
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected object, got %s", jsonValue{raw: value}.kind())
	}
	return object, nil
}

func translateJSONError(err error) error {
	var depthErr *jsondoc.DepthError
	if !errors.As(err, &depthErr) {
		return err
	}
	return fmt.Errorf(
		"%w at %s: depth exceeds limit %d",
		ErrMaxDepth,
		depthErr.Path,
		depthErr.Limit,
	)
}
