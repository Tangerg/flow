package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
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

// Ports declares a node type's input ports and the value type each accepts. It
// is the schema-side counterpart of [Inputs], which wires those ports to
// references.
type Ports map[string]ValueType

// OnePort returns the Ports of a node with a single input on [DefaultPort].
func OnePort(t ValueType) Ports { return Ports{DefaultPort: t} }

// NodeSchema describes a registered node type for validation and tooling.
// Inputs and Output let editors check connections and report incomplete wiring.
// ConfigSchema, when present, is a self-contained Draft 2020-12 JSON Schema for
// the node's config; an omitted config is validated as an empty object. External
// references are rejected.
//
// An empty Inputs declares nothing, so a node's wiring is left unchecked. Use
// [OnePort] for the common single-input node.
type NodeSchema struct {
	Inputs       Ports           `json:"inputs,omitempty"`
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
	case !validValueType(schema.Output):
		return &RegistrationError{Kind: "schema", Name: nodeType, Err: fmt.Errorf("%w: invalid value type", ErrInvalidRegistration)}
	}
	for _, port := range slices.Sorted(maps.Keys(schema.Inputs)) {
		if port == "" {
			return &RegistrationError{Kind: "schema", Name: nodeType, Err: fmt.Errorf("%w: empty port name", ErrInvalidRegistration)}
		}
		if !validValueType(schema.Inputs[port]) {
			return &RegistrationError{
				Kind: "schema",
				Name: nodeType,
				Err:  fmt.Errorf("%w: invalid value type on port %q", ErrInvalidRegistration, port),
			}
		}
	}

	schema.Inputs = maps.Clone(schema.Inputs)
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

// NodeSchema returns the schema registered for nodeType. The bool reports
// whether one was registered; an unregistered node type accepts any wiring and
// config. The returned Inputs and ConfigSchema are copies.
//
// Together with [Registry.NodeTypes] this is what an editor reads to render a
// node palette and to know which ports a node exposes.
func (r *Registry) NodeSchema(nodeType string) (NodeSchema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	registered, ok := r.schemas[nodeType]
	if !ok {
		return NodeSchema{}, false
	}
	schema := registered.schema
	schema.Inputs = maps.Clone(schema.Inputs)
	schema.ConfigSchema = bytes.Clone(schema.ConfigSchema)
	return schema, true
}

// NodeTypes returns the registered leaf node type names in sorted order.
func (r *Registry) NodeTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Sorted(maps.Keys(r.leaves))
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
// types, config schemas, cycles, fully wired input ports, and type-compatible
// edges. It is intended to power a visual editor's live feedback.
func (r *Registry) ValidateGraph(g Graph) error {
	_, _, err := r.validateGraph(g)
	return err
}

func (r *Registry) validateGraph(g Graph) ([][]string, map[string]NodeSpec, error) {
	layers, byID, wiring, err := r.plan(g) // duplicate IDs, cycles, and port wiring
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
		if err := r.validatePorts(n.Type, wiring[n.ID], func(producerID string) (ValueType, bool) {
			producer, ok := byID[producerID]
			if !ok {
				return "", false // external input (the seed Store); nothing to check
			}
			return r.nodeSchema(producer.Type).Output, true
		}); err != nil {
			return nil, nil, &GraphError{NodeID: n.ID, Field: "inputs", Err: err}
		}
	}
	return layers, byID, nil
}

// validatePorts checks a node's wiring against its registered schema: every
// declared port is wired, no undeclared port is wired, and each wired port's
// type is compatible with its producer's output. producerOutput reports a
// producing node's output type, or false when the reference is external.
//
// A node type with no declared ports is left unchecked, so nodes may be
// registered without a schema.
func (r *Registry) validatePorts(nodeType string, inputs Inputs, producerOutput func(string) (ValueType, bool)) error {
	declared := r.nodeSchema(nodeType).Inputs
	if len(declared) == 0 {
		return nil
	}

	for _, port := range slices.Sorted(maps.Keys(declared)) {
		if _, wired := inputs[port]; !wired {
			return fmt.Errorf("%w %q", ErrMissingPort, port)
		}
	}
	for _, port := range inputs.PortNames() {
		want, ok := declared[port]
		if !ok {
			return fmt.Errorf("%w %q", ErrUnknownPort, port)
		}
		ref := inputs[port]
		out, ok := producerOutput(ref.NodeID)
		if !ok {
			continue
		}
		if !compatible(out, want) {
			return fmt.Errorf("%w: port %q reads %s whose output is %s, want %s",
				ErrIncompatibleType, port, ref.NodeID, out, want)
		}
	}
	return nil
}
