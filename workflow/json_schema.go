package workflow

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

	loadSpecSchema = sync.OnceValues(func() (*compiledSchema, error) {
		return (schemaSource{url: specSchemaURL, document: specSchemaJSON}).compile()
	})
	loadGraphSchema = sync.OnceValues(func() (*compiledSchema, error) {
		return (schemaSource{url: graphSchemaURL, document: graphSchemaJSON}).compile()
	})
)

type schemaSource struct {
	url      string
	document jsonDocument
}

type compiledSchema struct {
	validator interface {
		Validate(document any) error
	}
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
// [SpecJSONSchema]. Duplicate object members and values nested beyond
// [MaxNestingDepth] are rejected. Registry-dependent checks such as node types
// and config schemas are performed by [Registry.ValidateSpec] and compilation.
func ValidateSpecJSON(data []byte) error {
	if err := schemaLoader(loadSpecSchema).validate(jsonDocument(data)); err != nil {
		return &SpecError{Field: fieldJSON, Err: fmt.Errorf("%w: %w", ErrInvalidSpec, err)}
	}
	return nil
}

// ValidateGraphJSON checks that data is one complete JSON value conforming to
// [GraphJSONSchema]. Duplicate object members and values nested beyond
// [MaxNestingDepth] are rejected. Registry-dependent checks such as node types,
// cycles, and config schemas are performed by [Registry.ValidateGraph] and
// compilation.
func ValidateGraphJSON(data []byte) error {
	if err := schemaLoader(loadGraphSchema).validate(jsonDocument(data)); err != nil {
		return &GraphError{Field: fieldJSON, Err: fmt.Errorf("%w: %w", ErrInvalidGraph, err)}
	}
	return nil
}

func (s schemaSource) compile() (*compiledSchema, error) {
	doc, err := s.document.value()
	if err != nil {
		return nil, fmt.Errorf("decode JSON Schema: %w", err)
	}
	compiler := jschema.NewCompiler()
	compiler.DefaultDraft(jschema.Draft2020)
	// Schemas must be self-contained. In particular, registering a node must
	// never perform network or filesystem I/O because of an external $ref.
	compiler.UseLoader(jschema.SchemeURLLoader{})
	if err = compiler.AddResource(s.url, doc); err != nil {
		return nil, fmt.Errorf("add JSON Schema resource: %w", err)
	}
	schema, err := compiler.Compile(s.url)
	if err != nil {
		return nil, fmt.Errorf("compile JSON Schema: %w", err)
	}
	return &compiledSchema{validator: schema}, nil
}

// compileOptional reports an absent schema as a nil validator rather than a
// sentinel error, because "this node type declares no config schema" is a
// supported registration and not a condition any caller recovers from.
//
//nolint:nilnil // A nil validator is the documented "no schema" result.
func (s schemaSource) compileOptional() (*compiledSchema, error) {
	if len(bytes.TrimSpace(s.document)) == 0 {
		return nil, nil
	}
	return s.compile()
}

type schemaLoader func() (*compiledSchema, error)

func (s schemaLoader) validate(document jsonDocument) error {
	doc, err := document.value()
	if err != nil {
		return err
	}
	schema, err := s()
	if err != nil {
		return err
	}
	return schema.validate(doc)
}

func (s schemaLoader) decode(document jsonDocument, dst any) error {
	doc, err := document.value()
	if err != nil {
		return err
	}
	schema, err := s()
	if err != nil {
		return err
	}
	if err := schema.validate(doc); err != nil {
		return err
	}
	return document.decodeParsed(dst)
}

func (c *compiledSchema) validateConfig(config json.RawMessage) error {
	data := bytes.TrimSpace(config)
	if len(data) == 0 {
		if c == nil {
			return nil
		}
		data = json.RawMessage(`{}`)
	}
	doc, err := jsonDocument(data).value()
	if err != nil {
		return err
	}
	if c == nil {
		return nil
	}
	return c.validate(doc)
}

func (c *compiledSchema) validate(doc any) error {
	err := c.validator.Validate(doc)
	if err == nil {
		return nil
	}
	if validationErr, ok := errors.AsType[*jschema.ValidationError](err); ok {
		return &jsonSchemaError{err: validationErr}
	}
	return err
}

// jsonSchemaError keeps the validator's structured error in the chain while
// presenting only actionable leaf diagnostics to callers.
type jsonSchemaError struct {
	err *jschema.ValidationError
}

// Error reports the deduplicated leaf diagnostics. leaves always yields at least
// the root error, so the joined message is never empty.
func (j *jsonSchemaError) Error() string {
	return strings.Join(uniqueMessages(j.leaves()), "; ")
}

// uniqueMessages renders each leaf once, keeping the order the validator
// reported. One document error commonly surfaces the same message under several
// sub-schemas, and repeating it buries the part a reader needs. Sorting to let
// slices.Compact do this would destroy that diagnostic order, which is why the
// first position is tracked explicitly.
func uniqueMessages(leaves []*jschema.ValidationError) []string {
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
	return messages
}

func (j *jsonSchemaError) Unwrap() error { return j.err }

func (j *jsonSchemaError) leaves() []*jschema.ValidationError {
	leaves := make([]*jschema.ValidationError, 0)
	pending := []*jschema.ValidationError{j.err}
	for len(pending) > 0 {
		last := len(pending) - 1
		err := pending[last]
		pending = pending[:last]
		if len(err.Causes) == 0 {
			leaves = append(leaves, err)
			continue
		}
		// Push in reverse so the iterative depth-first walk preserves the
		// validator's diagnostic order.
		for _, cause := range slices.Backward(err.Causes) {
			pending = append(pending, cause)
		}
	}
	return leaves
}
