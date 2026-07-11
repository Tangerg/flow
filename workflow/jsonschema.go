package workflow

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	jschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	specSchemaURL   = "https://github.com/Tangerg/flow/schema/workflow-spec.json"
	graphSchemaURL  = "https://github.com/Tangerg/flow/schema/workflow-graph.json"
	configSchemaURL = "https://github.com/Tangerg/flow/schema/node-config.json"
)

var (
	//go:embed jsonschema/spec.schema.json
	specSchemaJSON []byte

	//go:embed jsonschema/graph.schema.json
	graphSchemaJSON []byte

	loadSpecSchema = sync.OnceValues(func() (jsonValidator, error) {
		return compileJSONSchema(specSchemaURL, specSchemaJSON)
	})
	loadGraphSchema = sync.OnceValues(func() (jsonValidator, error) {
		return compileJSONSchema(graphSchemaURL, graphSchemaJSON)
	})
)

type jsonValidator interface {
	Validate(any) error
}

// SpecJSONSchema returns the Draft 2020-12 JSON Schema for serialized [Spec]
// values. The returned bytes are a copy and may be modified by the caller.
func SpecJSONSchema() json.RawMessage {
	return bytes.Clone(specSchemaJSON)
}

// GraphJSONSchema returns the Draft 2020-12 JSON Schema for serialized [Graph]
// values. The returned bytes are a copy and may be modified by the caller.
func GraphJSONSchema() json.RawMessage {
	return bytes.Clone(graphSchemaJSON)
}

// ValidateSpecJSON checks that data is one complete JSON value conforming to
// [SpecJSONSchema]. Registry-dependent checks such as node types and config
// schemas are performed by [Registry.ValidateSpec] and compilation.
func ValidateSpecJSON(data []byte) error {
	if err := validateJSON(data, loadSpecSchema); err != nil {
		return &SpecError{Field: "json", Err: fmt.Errorf("%w: %w", ErrInvalidSpec, err)}
	}
	return nil
}

// ValidateGraphJSON checks that data is one complete JSON value conforming to
// [GraphJSONSchema]. Registry-dependent checks such as node types, cycles, and
// config schemas are performed by [Registry.ValidateGraph] and compilation.
func ValidateGraphJSON(data []byte) error {
	if err := validateJSON(data, loadGraphSchema); err != nil {
		return &GraphError{Field: "json", Err: fmt.Errorf("%w: %w", ErrInvalidGraph, err)}
	}
	return nil
}

func compileJSONSchema(resourceURL string, data []byte) (jsonValidator, error) {
	doc, err := jschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("decode JSON Schema: %w", err)
	}
	compiler := jschema.NewCompiler()
	compiler.DefaultDraft(jschema.Draft2020)
	// Schemas must be self-contained. In particular, registering a node must
	// never perform network or filesystem I/O because of an external $ref.
	compiler.UseLoader(jschema.SchemeURLLoader{})
	if err := compiler.AddResource(resourceURL, doc); err != nil {
		return nil, fmt.Errorf("add JSON Schema resource: %w", err)
	}
	schema, err := compiler.Compile(resourceURL)
	if err != nil {
		return nil, fmt.Errorf("compile JSON Schema: %w", err)
	}
	return schema, nil
}

func validateJSON(data []byte, load func() (jsonValidator, error)) error {
	doc, err := jschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return err
	}
	schema, err := load()
	if err != nil {
		return err
	}
	return validateDocument(schema, doc)
}

func compileConfigSchema(data json.RawMessage) (jsonValidator, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	return compileJSONSchema(configSchemaURL, data)
}

func validateConfig(schema jsonValidator, config json.RawMessage) error {
	if schema == nil {
		return nil
	}
	data := config
	if len(bytes.TrimSpace(data)) == 0 {
		data = json.RawMessage(`{}`)
	}
	doc, err := jschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return err
	}
	return validateDocument(schema, doc)
}

func validateDocument(schema jsonValidator, doc any) error {
	err := schema.Validate(doc)
	if err == nil {
		return nil
	}
	var validationErr *jschema.ValidationError
	if errors.As(err, &validationErr) {
		return &jsonSchemaError{err: validationErr}
	}
	return err
}

// jsonSchemaError keeps the validator's structured error in the chain while
// presenting only actionable leaf diagnostics to callers.
type jsonSchemaError struct {
	err *jschema.ValidationError
}

func (e *jsonSchemaError) Error() string {
	leaves := validationLeaves(e.err, nil)
	messages := make([]string, 0, len(leaves))
	seen := make(map[string]struct{}, len(leaves))
	for _, leaf := range leaves {
		message := leaf.Error()
		if _, duplicate := seen[message]; duplicate {
			continue
		}
		seen[message] = struct{}{}
		messages = append(messages, message)
	}
	if len(messages) == 0 {
		return e.err.Error()
	}
	return strings.Join(messages, "; ")
}

func (e *jsonSchemaError) Unwrap() error { return e.err }

func validationLeaves(err *jschema.ValidationError, dst []*jschema.ValidationError) []*jschema.ValidationError {
	if len(err.Causes) == 0 {
		return append(dst, err)
	}
	for _, cause := range err.Causes {
		dst = validationLeaves(cause, dst)
	}
	return dst
}
