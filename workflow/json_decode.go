package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// jsonDocument owns the package's strict JSON decoding semantics: one complete
// value, no duplicate object members, preserved numbers, and optional typed
// decoding. Giving those rules one receiver keeps every persistence and DSL
// boundary on the same parser.
type jsonDocument []byte

func (j jsonDocument) decode(dst any) error {
	if _, err := j.value(); err != nil {
		return err
	}
	return j.decodeParsed(dst)
}

// decodeParsed maps a document already accepted by value into a Go type.
func (j jsonDocument) decodeParsed(dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(j))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	return decoder.Decode(dst)
}

// value decodes one JSON value into the ordinary JSON domain while
// rejecting duplicate object members. The standard decoder otherwise silently
// keeps the last value, which would make schema validation observe a different
// document than the caller supplied.
func (j jsonDocument) value() (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(j))
	decoder.UseNumber()
	reader := jsonReader{decoder: decoder}
	value, err := reader.read()
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

// jsonReader owns the cursor and path of a recursive token walk. path always
// identifies the value currently being read.
type jsonReader struct {
	decoder *json.Decoder
	path    []string
	depth   int
}

func (j *jsonReader) read() (any, error) {
	token, err := j.decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	if err := j.enter(); err != nil {
		return nil, err
	}
	defer j.leave()

	// A closing delimiter cannot appear where a value is expected: the top level
	// starts a document, an object member follows its name, and an array element
	// is only read while More reports one. So the delimiter here is '{' or '['.
	if delim == '{' {
		return j.readObject()
	}
	return j.readArray()
}

func (j *jsonReader) enter() error {
	if j.depth >= MaxNestingDepth {
		return fmt.Errorf(
			"%w at %s: depth exceeds limit %d",
			ErrMaxDepth,
			pointerPath(j.path).encode(),
			MaxNestingDepth,
		)
	}
	j.depth++
	return nil
}

func (j *jsonReader) leave() {
	j.depth--
}

func (j *jsonReader) readObject() (map[string]any, error) {
	object := make(map[string]any)
	for j.decoder.More() {
		name, err := j.readMemberName()
		if err != nil {
			return nil, err
		}
		j.path = append(j.path, name)
		if _, duplicate := object[name]; duplicate {
			return nil, fmt.Errorf(
				"duplicate object member %q at %s",
				name,
				pointerPath(j.path).encode(),
			)
		}
		value, err := j.read()
		j.path = j.path[:len(j.path)-1]
		if err != nil {
			return nil, err
		}
		object[name] = value
	}
	if _, err := j.decoder.Token(); err != nil { // }
		return nil, err
	}
	return object, nil
}

func (j *jsonReader) readMemberName() (string, error) {
	token, err := j.decoder.Token()
	if err != nil {
		return "", err
	}
	// More reports true inside an object only when the next token is a member
	// name, so a successful Token call is necessarily a string.
	//
	//nolint:forcetypeassert // Guaranteed by encoding/json's Decoder.More contract.
	return token.(string), nil
}

func (j *jsonReader) readArray() ([]any, error) {
	array := make([]any, 0)
	for index := 0; j.decoder.More(); index++ {
		j.path = append(j.path, strconv.Itoa(index))
		value, err := j.read()
		j.path = j.path[:len(j.path)-1]
		if err != nil {
			return nil, err
		}
		array = append(array, value)
	}
	if _, err := j.decoder.Token(); err != nil { // ]
		return nil, err
	}
	return array, nil
}
