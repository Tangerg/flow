package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ValueType describes the shape of a value flowing between nodes. It is used only
// for edit-time connection validation (see [Registry.ValidateGraph]); it is never
// consulted at run time.
type ValueType string

// The value types a port may declare. TypeAny is compatible with any other type.
const (
	TypeAny    ValueType = "any"
	TypeString ValueType = "string"
	TypeNumber ValueType = "number"
	TypeBool   ValueType = "bool"
	TypeArray  ValueType = "array"
	TypeObject ValueType = "object"
)

// NodeSchema describes a registered node type for validation and tooling.
// Input and Output let editors check connections. ConfigSchema, when present,
// is a self-contained Draft 2020-12 JSON Schema for the node's config; an
// omitted config is validated as an empty object. External references are
// rejected.
type NodeSchema struct {
	Input        ValueType       `json:"input"`
	Output       ValueType       `json:"output"`
	ConfigSchema json.RawMessage `json:"configSchema,omitempty"`
}

type registeredNodeSchema struct {
	schema          NodeSchema
	configValidator jsonValidator
}

// RegisterSchema associates a [NodeSchema] with a node type. It compiles
// ConfigSchema once and rejects invalid or external references immediately.
// Node types without a schema accept any connection and config.
func (r *Registry) RegisterSchema(nodeType string, schema NodeSchema) error {
	switch {
	case nodeType == "":
		return &RegistrationError{Kind: "schema", Err: fmt.Errorf("%w: empty node type", ErrInvalidRegistration)}
	case !validValueType(schema.Input) || !validValueType(schema.Output):
		return &RegistrationError{Kind: "schema", Name: nodeType, Err: fmt.Errorf("%w: invalid value type", ErrInvalidRegistration)}
	}

	schema.ConfigSchema = bytes.Clone(schema.ConfigSchema)
	validator, err := compileConfigSchema(schema.ConfigSchema)
	if err != nil {
		return &RegistrationError{
			Kind: "schema",
			Name: nodeType,
			Err:  fmt.Errorf("%w: config JSON Schema: %w", ErrInvalidRegistration, err),
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	_, exists := r.schemas[nodeType]
	if exists {
		return &RegistrationError{Kind: "schema", Name: nodeType, Err: ErrDuplicateRegistration}
	}
	r.schemas[nodeType] = registeredNodeSchema{schema: schema, configValidator: validator}
	return nil
}

// MustRegisterSchema is like [Registry.RegisterSchema] but panics on error.
func (r *Registry) MustRegisterSchema(nodeType string, schema NodeSchema) *Registry {
	if err := r.RegisterSchema(nodeType, schema); err != nil {
		panic(err)
	}
	return r
}

func validValueType(t ValueType) bool {
	switch t {
	case "", TypeAny, TypeString, TypeNumber, TypeBool, TypeArray, TypeObject:
		return true
	default:
		return false
	}
}

// compatible reports whether a value of type out can feed a port of type in.
// An empty or TypeAny type on either side is compatible with anything.
func compatible(out, in ValueType) bool {
	return out == in || out == "" || in == "" || out == TypeAny || in == TypeAny
}

// ValidateGraph checks a Graph without compiling it: unique IDs, known node
// types, config schemas, cycles, and type-compatible Input edges. It is
// intended to power a visual editor's live feedback.
func (r *Registry) ValidateGraph(g Graph) error {
	_, _, err := r.validateGraph(g)
	return err
}

func (r *Registry) validateGraph(g Graph) ([][]string, map[string]NodeSpec, error) {
	layers, byID, err := r.plan(g) // duplicate IDs and cycles
	if err != nil {
		return nil, nil, err
	}

	for _, n := range g.Nodes {
		if _, ok := r.leafFactory(n.Type); !ok {
			return nil, nil, &GraphError{NodeID: n.ID, Field: "type", Err: fmt.Errorf("%w %q", ErrUnknownNodeType, n.Type)}
		}
		if err := validateConfig(r.registeredNodeSchema(n.Type).configValidator, n.Config); err != nil {
			return nil, nil, &GraphError{
				NodeID: n.ID,
				Field:  "config",
				Err:    fmt.Errorf("%w: %w", ErrInvalidGraph, err),
			}
		}
		if n.Input == nil {
			continue
		}
		producer, ok := byID[n.Input.NodeID]
		if !ok {
			continue // external input (the seed Store); nothing to check
		}
		out := r.nodeSchema(producer.Type).Output
		in := r.nodeSchema(n.Type).Input
		if !compatible(out, in) {
			return nil, nil, &GraphError{
				NodeID: n.ID,
				Field:  "input",
				Err: fmt.Errorf("%w: %s.output is %s, want %s",
					ErrIncompatibleType, producer.ID, out, in),
			}
		}
	}
	return layers, byID, nil
}
