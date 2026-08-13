package workflow

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/Tangerg/flow/internal/jsondoc"
	jschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	specSchemaURL   = "https://github.com/Tangerg/flow/schema/workflow-spec.json"
	graphSchemaURL  = "https://github.com/Tangerg/flow/schema/workflow-graph.json"
	configSchemaURL = "https://github.com/Tangerg/flow/schema/node-config.json"
	draft2020URL    = "https://json-schema.org/draft/2020-12/schema"
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
// [SpecJSONSchema] and representable by [Spec]. Invalid Unicode text (including
// malformed UTF-8 and unpaired UTF-16 surrogate escapes), duplicate object
// members, engine-owned integers outside the current platform's int range, and
// values nested beyond [MaxNestingDepth] are rejected. Registry-dependent checks
// such as node types and config schemas are performed by [Registry.ValidateSpec]
// and compilation.
func ValidateSpecJSON(data []byte) error {
	if _, err := decodeSpecDocument(data); err != nil {
		return specJSONError(err)
	}
	return nil
}

// ValidateGraphJSON checks that data is one complete JSON value conforming to
// [GraphJSONSchema] and representable by [Graph]. Invalid Unicode text
// (including malformed UTF-8 and unpaired UTF-16 surrogate escapes), duplicate
// object members, engine-owned integers outside the current platform's int
// range, and values nested beyond [MaxNestingDepth] are rejected.
// Registry-dependent checks such as node types, cycles, and config schemas are
// performed by [Registry.ValidateGraph] and compilation.
func ValidateGraphJSON(data []byte) error {
	if _, err := decodeGraphDocument(data); err != nil {
		return graphJSONError(err)
	}
	return nil
}

func (s schemaSource) compile() (*compiledSchema, error) {
	doc, err := s.document.value()
	if err != nil {
		return nil, fmt.Errorf("decode JSON Schema: %w", err)
	}
	if dialectErr := new(schemaDialectValidator).validate(doc); dialectErr != nil {
		return nil, fmt.Errorf("validate JSON Schema dialect: %w", dialectErr)
	}
	compiler := jschema.NewCompiler()
	compiler.DefaultDraft(jschema.Draft2020)
	// Schemas must be self-contained. In particular, registering a node must
	// never perform network or filesystem I/O because of an external $ref.
	compiler.UseLoader(jschema.SchemeURLLoader{})
	if err = compiler.AddResource(s.url, doc); err != nil {
		return nil, schemaBackendError{operation: "add JSON Schema resource", message: err.Error()}
	}
	schema, err := compiler.Compile(s.url)
	if err != nil {
		return nil, schemaBackendError{operation: "compile JSON Schema", message: err.Error()}
	}
	return &compiledSchema{validator: schema}, nil
}

// schemaDialectValidator walks only positions that Draft 2020-12 defines as
// subschemas. Instance values carried by const, enum, default, or examples are
// deliberately opaque: an application object may legitimately contain a
// "$schema" member without declaring a schema dialect.
type schemaDialectValidator struct {
	path pointerPath
}

func (s *schemaDialectValidator) validate(value any) error {
	object, ok := value.(map[string]any)
	if !ok {
		// A boolean is the only other valid schema shape. Other values remain the
		// compiler's responsibility so this adapter does not duplicate meta-schema
		// validation.
		return nil
	}
	if dialect, declared := object["$schema"]; declared {
		name, ok := dialect.(string)
		if !ok {
			return fmt.Errorf(
				"$schema at %s must be a string naming %q, got %s",
				s.location(),
				draft2020URL,
				jsondoc.Kind(dialect),
			)
		}
		if !isDraft2020(name) {
			return fmt.Errorf(
				"$schema at %s is %q; want %q",
				s.location(),
				name,
				draft2020URL,
			)
		}
	}

	for _, keyword := range [...]string{
		"not",
		"additionalProperties",
		"items",
		"additionalItems",
		"propertyNames",
		"contains",
		"if",
		"then",
		"else",
		"unevaluatedProperties",
		"unevaluatedItems",
		"contentSchema",
	} {
		if err := s.validateContainer(object[keyword], keyword); err != nil {
			return err
		}
	}
	for _, keyword := range [...]string{"allOf", "anyOf", "oneOf", "prefixItems"} {
		if err := s.validateList(object[keyword], keyword); err != nil {
			return err
		}
	}
	for _, keyword := range [...]string{
		"definitions",
		"properties",
		"patternProperties",
		"dependencies",
		"$defs",
		"dependentSchemas",
	} {
		if err := s.validateMap(object[keyword], keyword); err != nil {
			return err
		}
	}
	return nil
}

func (s *schemaDialectValidator) validateContainer(value any, keyword string) error {
	switch value := value.(type) {
	case map[string]any, bool:
		return s.validateChild(value, keyword)
	case []any:
		return s.validateList(value, keyword)
	default:
		return nil
	}
}

func (s *schemaDialectValidator) validateList(value any, keyword string) error {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	for index, item := range items {
		if err := s.validateChild(item, keyword, strconv.Itoa(index)); err != nil {
			return err
		}
	}
	return nil
}

func (s *schemaDialectValidator) validateMap(value any, keyword string) error {
	entries, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	for _, name := range slices.Sorted(maps.Keys(entries)) {
		if err := s.validateChild(entries[name], keyword, name); err != nil {
			return err
		}
	}
	return nil
}

func (s *schemaDialectValidator) validateChild(value any, segments ...string) error {
	depth := len(s.path)
	s.path = append(s.path, segments...)
	err := s.validate(value)
	s.path = s.path[:depth]
	return err
}

func (s *schemaDialectValidator) location() string {
	if len(s.path) == 0 {
		return "<root>"
	}
	return s.path.encode()
}

func isDraft2020(dialect string) bool {
	dialect = strings.TrimSuffix(dialect, "#")
	return dialect == draft2020URL ||
		dialect == "http://json-schema.org/draft/2020-12/schema"
}

// compileOptional reports an absent schema as a nil validator rather than a
// sentinel error, because "this node type declares no config schema" is a
// supported registration and not a condition any caller recovers from.
//
//nolint:nilnil // A nil validator is the documented "no schema" result.
func (s schemaSource) compileOptional() (*compiledSchema, error) {
	if len(s.document) == 0 {
		return nil, nil
	}
	return s.compile()
}

type schemaLoader func() (*compiledSchema, error)

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

// validateConfig checks a node config against this schema. An absent config
// stands in as the empty object, so a schema with required members rejects it
// rather than skipping the check. A node type that declares no schema still
// requires a config that is a well-formed strict JSON document.
func (c *compiledSchema) validateConfig(config json.RawMessage) error {
	data := config
	if len(data) == 0 {
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
	return schemaBackendError{operation: "validate JSON Schema", message: err.Error()}
}

// schemaBackendError preserves the backend diagnostic without making its
// concrete error vocabulary part of this package's errors.As contract.
type schemaBackendError struct {
	operation string
	message   string
}

func (s schemaBackendError) Error() string { return s.operation + ": " + s.message }

// jsonSchemaError uses the validator's structured tree to present actionable
// leaf diagnostics without exposing that replaceable backend through the
// package's public error chain.
type jsonSchemaError struct {
	err *jschema.ValidationError
}

// Error reports the deduplicated leaf diagnostics. leaves always yields at least
// the root error, so the joined message is never empty.
func (j *jsonSchemaError) Error() string {
	return strings.Join(j.messages(), "; ")
}

// messages renders each leaf once in deterministic diagnostic order. The
// validator aggregates object and schema failures through maps, so its cause
// order is not a public contract and can differ between identical calls. Sort
// only the final text: that keeps backend structure private while making logs,
// editor caches, and tests stable. One document error commonly surfaces the same
// message under several sub-schemas, so duplicates are removed before sorting.
func (j *jsonSchemaError) messages() []string {
	leaves := j.leaves()
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
	slices.Sort(messages)
	return messages
}

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
