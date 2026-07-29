package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

func decodeStrict(data []byte, dst any) error {
	if err := validateJSONNames(data); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
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

// validateJSONNames rejects duplicate object members before encoding/json or
// the JSON Schema implementation collapses them into one map entry. Accepting
// duplicates would make configuration depend on a decoder's first/last-wins
// policy and leave JSON Schema validating a different document than the caller
// supplied.
func validateJSONNames(data []byte) error {
	_, err := decodeUniqueJSON(data)
	return err
}

// decodeUniqueJSON decodes one JSON value into the ordinary JSON domain while
// rejecting duplicate object members. The standard decoder otherwise silently
// keeps the last value, which would make schema validation observe a different
// document than the caller supplied.
func decodeUniqueJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var path []string
	value, err := readJSONValue(decoder, &path)
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

func readJSONValue(decoder *json.Decoder, path *[]string) (any, error) {
	token, err := decoder.Token()
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
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := token.(string)
			if !ok {
				return nil, fmt.Errorf("object member name is %T, want string", token)
			}
			*path = append(*path, name)
			if _, duplicate := object[name]; duplicate {
				return nil, fmt.Errorf("duplicate object member %q at %s", name, encodePointer(*path))
			}
			value, err := readJSONValue(decoder, path)
			if err != nil {
				return nil, err
			}
			object[name] = value
			*path = (*path)[:len(*path)-1]
		}
		if _, err := decoder.Token(); err != nil { // }
			return nil, err
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for index := 0; decoder.More(); index++ {
			*path = append(*path, strconv.Itoa(index))
			value, err := readJSONValue(decoder, path)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
			*path = (*path)[:len(*path)-1]
		}
		if _, err := decoder.Token(); err != nil { // ]
			return nil, err
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
}
