package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

// jsonDocument owns the package's strict JSON semantics: one complete value,
// no duplicate object members, preserved numbers, and optional typed decoding.
// Giving those rules one receiver keeps every persistence and DSL boundary on
// the same parser.
type jsonDocument []byte

func (d jsonDocument) decode(dst any) error {
	if _, err := d.value(); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(d))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

// value decodes one JSON value into the ordinary JSON domain while
// rejecting duplicate object members. The standard decoder otherwise silently
// keeps the last value, which would make schema validation observe a different
// document than the caller supplied.
func (d jsonDocument) value() (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(d))
	decoder.UseNumber()
	reader := jsonReader{decoder: decoder}
	value, err := reader.read()
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
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
}

func (r *jsonReader) read() (any, error) {
	token, err := r.decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}

	switch delim {
	case '{':
		object := make(map[string]any)
		for r.decoder.More() {
			token, err := r.decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := token.(string)
			if !ok {
				return nil, fmt.Errorf("object member name is %T, want string", token)
			}
			r.path = append(r.path, name)
			if _, duplicate := object[name]; duplicate {
				return nil, fmt.Errorf("duplicate object member %q at %s", name, pointerPath(r.path).encode())
			}
			value, err := r.read()
			if err != nil {
				return nil, err
			}
			object[name] = value
			r.path = r.path[:len(r.path)-1]
		}
		if _, err := r.decoder.Token(); err != nil { // }
			return nil, err
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for index := 0; r.decoder.More(); index++ {
			r.path = append(r.path, strconv.Itoa(index))
			value, err := r.read()
			if err != nil {
				return nil, err
			}
			array = append(array, value)
			r.path = r.path[:len(r.path)-1]
		}
		if _, err := r.decoder.Token(); err != nil { // ]
			return nil, err
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
}
