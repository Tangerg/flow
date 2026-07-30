package workflow

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Ref points at a value in the [Store]: a node ID plus an RFC 6901 JSON Pointer
// under it. The first pointer segment is the key written by that node; further
// segments index into nested data.
type Ref struct {
	NodeID string `json:"nodeID"`
	Path   string `json:"path"`
}

// At returns a reference to key under nodeID, followed by any nested path. Each
// argument is one literal segment; At performs JSON Pointer escaping, so keys
// containing "/" or "~" need no special handling by the caller.
func At(nodeID, key string, segments ...string) Ref {
	var pointer pointerEncoder
	pointer.write(key)
	for _, segment := range segments {
		pointer.write(segment)
	}
	return Ref{NodeID: nodeID, Path: pointer.String()}
}

const (
	outputKey  = "output"
	outputPath = "/" + outputKey
)

// Output returns a reference to a step's conventional output value.
func Output(nodeID string) Ref { return Ref{NodeID: nodeID, Path: outputPath} }

// String returns the reference in nodeID#pointer form.
func (r Ref) String() string { return r.NodeID + "#" + r.Path }

func (r Ref) compare(other Ref) int {
	return cmp.Or(
		strings.Compare(r.NodeID, other.NodeID),
		strings.Compare(r.Path, other.Path),
	)
}

func (r Ref) validate() error {
	if r.NodeID == "" {
		return errors.New("node ID is empty")
	}
	pointer, ok := encodedPointer(r.Path).scan()
	if !ok {
		return fmt.Errorf("path %q is not a non-empty JSON Pointer", r.Path)
	}
	for {
		_, present, valid := pointer.next()
		if !valid {
			return fmt.Errorf("path %q is not a valid JSON Pointer", r.Path)
		}
		if !present {
			return nil
		}
	}
}

// Child returns a reference below r. Each argument is one literal path segment;
// no arguments return r unchanged.
func (r Ref) Child(segments ...string) Ref {
	if len(segments) == 0 {
		return r
	}
	r.Path += pointerPath(segments).encode()
	return r
}

// Get reads the value at ref as a T. A value of exactly T is returned as-is;
// otherwise Get converts it through its JSON representation, which is what makes
// a typed read survive a serialized Store. Reading 42 back as an int works even
// though JSON only has numbers, and the same holds at any path depth and for
// structs and typed slices.
//
// A missing value, nil assigned to a non-nilable T, or a value that cannot be
// converted to T is returned as an error wrapping [ErrNotFound] or
// [ErrTypeMismatch]. Conversion never rounds or reinterprets: reading 42.5 as an
// int fails, as does reading a number as a string.
func Get[T any](store Store, ref Ref) (T, error) {
	var zero T
	target := reflect.TypeFor[T]()
	want := target.String()
	raw, ok := store.Lookup(ref)
	if !ok {
		return zero, &RefError{Ref: ref, Want: want, Err: ErrNotFound}
	}
	if raw == nil {
		switch target.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			return zero, nil
		default:
			return zero, &RefError{Ref: ref, Want: want, Err: ErrTypeMismatch}
		}
	}
	if value, ok := raw.(T); ok {
		return value, nil
	}

	value, err := convert[T](raw)
	if err != nil {
		return zero, &RefError{
			Ref:  ref,
			Want: want,
			Got:  reflect.TypeOf(raw).String(),
			Err:  fmt.Errorf("%w: %w", ErrTypeMismatch, err),
		}
	}
	return value, nil
}

// convert adapts a value to T through JSON. It is the read half of the Store's
// serialization contract: a Store that has been through JSON holds JSON-domain
// values — [json.Number], string, bool, []any, map[string]any — and a typed read
// has to convert rather than assert. Routing every conversion through JSON keeps
// one rule for every depth and shape instead of a table of special cases.
//
// Callers reach this only after an exact type assertion has failed, so the cost
// falls on resumed and deserialized workflows rather than on ordinary runs.
func convert[T any](raw any) (T, error) {
	var zero T
	encoded, err := json.Marshal(raw)
	if err != nil {
		return zero, fmt.Errorf("value is not JSON-representable: %w", err)
	}
	var value T
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return zero, err
	}
	return value, nil
}
