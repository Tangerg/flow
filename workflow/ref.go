package workflow

import (
	"cmp"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// Ref points at a value in the [Store]: a node ID plus an RFC 6901 JSON Pointer
// under it. The first pointer segment is the key written by that node; further
// segments index into nested data.
type Ref struct { //nolint:recvcheck // UnmarshalJSON requires a pointer receiver.
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

// String returns a compact nodeID#pointer display form. It is for diagnostics,
// not parsing or identity: use the structured fields because a node ID and a
// pointer segment may themselves contain '#'.
func (r Ref) String() string { return r.NodeID + "#" + r.Path }

func (r Ref) compare(other Ref) int {
	return cmp.Or(
		strings.Compare(r.NodeID, other.NodeID),
		strings.Compare(r.Path, other.Path),
	)
}

// Validate reports whether r has a non-empty, valid UTF-8 node ID and a
// non-empty, well-formed RFC 6901 JSON Pointer. Caller-defined Binders and Steps
// should use it for definition checks rather than discovering a malformed
// reference as a missing value at run time.
func (r Ref) Validate() error {
	if err := validateName("node ID", r.NodeID); err != nil {
		return err
	}
	if err := validateText("path", r.Path); err != nil {
		return err
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

// validateJSONText checks only the lossless text precondition for encoding.
// Full definition validation remains the caller's responsibility: a zero or
// otherwise incomplete Ref can still be represented faithfully as JSON.
func (r Ref) validateJSONText() error {
	if err := validateText("node ID", r.NodeID); err != nil {
		return err
	}
	return validateText("path", r.Path)
}

// withinOutput reports whether r addresses a conventional output cell or one
// of its JSON descendants. Callers establish which node owns the output before
// applying this path-only check.
func (r Ref) withinOutput() bool {
	return r.Path == outputPath || strings.HasPrefix(r.Path, outputPath+"/")
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
// mutable maps, slices, and pointers are therefore borrowed Store values and
// must not be modified. Otherwise Get converts through the value's JSON
// representation and returns the newly decoded T. This makes a typed read
// survive a serialized Store when T has a faithful JSON round trip. Reading 42
// back as an int works even though JSON only has numbers, and the same holds at
// any path depth and for ordinary structs and typed slices. A type that
// customizes its JSON encoding is responsible for the corresponding decoding
// contract.
//
// A missing value, a value that cannot provide the JSON view needed by a nested
// reference, nil assigned to a non-nilable T, or a value that cannot be
// converted to T is returned as an error wrapping [ErrNotFound] or
// [ErrTypeMismatch]. Built-in scalar conversion never rounds or coerces between
// JSON kinds: reading 42.5 as an int fails, as does reading a number as a
// string. Conversion into an ordinary struct rejects unknown JSON members
// instead of silently discarding data; a type implementing [json.Unmarshaler]
// defines its own decoding contract.
func Get[T any](store Store, ref Ref) (T, error) {
	var zero T
	target := reflect.TypeFor[T]()
	want := target.String()
	raw, ok, err := store.resolve(ref)
	if err != nil {
		got := ""
		if raw != nil {
			got = reflect.TypeOf(raw).String()
		}
		return zero, &RefError{
			Ref:  ref,
			Want: want,
			Got:  got,
			Err: fmt.Errorf(
				"%w: resolve nested value: %w",
				ErrTypeMismatch,
				err,
			),
		}
	}
	if !ok {
		return zero, &RefError{Ref: ref, Want: want, Err: ErrNotFound}
	}
	if raw == nil {
		// These are exactly the Go kinds to which nil is assignable. Keep the
		// classification independent of JSON support: an in-memory Store may hold
		// nil even for a type whose non-nil values cannot be persisted, such as a
		// channel or unsafe.Pointer.
		switch target.Kind() {
		case reflect.Chan,
			reflect.Func,
			reflect.Interface,
			reflect.Map,
			reflect.Pointer,
			reflect.Slice,
			reflect.UnsafePointer:
			return zero, nil
		default:
			return zero, &RefError{Ref: ref, Want: want, Err: ErrTypeMismatch}
		}
	}
	if value, ok := raw.(T); ok {
		return value, nil
	}

	value, err := convertJSON[T](raw)
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

// convertJSON converts a value to T through its JSON representation. It is the
// read half of Store persistence: a decoded Store contains JSON-domain values,
// which typed consumers must convert rather than assert. Get reaches this only
// after an exact type assertion fails, so ordinary in-memory reads stay exact.
func convertJSON[T any](raw any) (T, error) {
	var zero T
	encoded, err := json.Marshal(raw)
	if err != nil {
		return zero, fmt.Errorf("value is not JSON-representable: %w", err)
	}
	var value T
	if err := jsonDocument(encoded).decode(&value); err != nil {
		return zero, err
	}
	return value, nil
}
