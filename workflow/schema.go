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
	configValidator *compiledSchema
}

// RegisterSchema associates a [NodeSchema] with a node type. It compiles
// ConfigSchema once and rejects invalid or external references immediately.
// Node types without a schema accept any connection and any syntactically valid
// JSON config.
func (r *Registry) RegisterSchema(nodeType string, schema NodeSchema) error {
	switch {
	case nodeType == "":
		return &RegistrationError{Kind: "schema", Err: fmt.Errorf("%w: empty node type", ErrInvalidRegistration)}
	case !schema.Output.valid():
		return &RegistrationError{Kind: "schema", Name: nodeType, Err: fmt.Errorf("%w: invalid value type", ErrInvalidRegistration)}
	}
	for _, port := range slices.Sorted(maps.Keys(schema.Inputs)) {
		if port == "" {
			return &RegistrationError{Kind: "schema", Name: nodeType, Err: fmt.Errorf("%w: empty port name", ErrInvalidRegistration)}
		}
		if !schema.Inputs[port].valid() {
			return &RegistrationError{
				Kind: "schema",
				Name: nodeType,
				Err:  fmt.Errorf("%w: invalid value type on port %q", ErrInvalidRegistration, port),
			}
		}
	}

	schema.Inputs = maps.Clone(schema.Inputs)
	schema.ConfigSchema = bytes.Clone(schema.ConfigSchema)
	validator, err := (schemaSource{
		url:      configSchemaURL,
		document: jsonDocument(schema.ConfigSchema),
	}).compileOptional()
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

func (s registeredNodeSchema) validateConfig(config json.RawMessage) error {
	return s.configValidator.validateConfig(config)
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

func (t ValueType) valid() bool {
	switch t {
	case "", TypeAny, TypeString, TypeNumber, TypeBool, TypeArray, TypeObject:
		return true
	default:
		return false
	}
}

// accepts reports whether a value of type out can feed a port of type t. An
// empty or TypeAny type on either side is compatible with anything.
func (t ValueType) accepts(out ValueType) bool {
	return out == t || out == "" || t == "" || out == TypeAny || t == TypeAny
}

// ValidateGraph checks a Graph without compiling it: unique IDs, known node
// types, config schemas, cycles, fully wired input ports, and type-compatible
// edges. It is intended to power a visual editor's live feedback.
func (r *Registry) ValidateGraph(g Graph) error {
	_, err := r.validateGraph(g)
	return err
}

func (r *Registry) validateGraph(g Graph) (graphPlan, error) {
	plan, err := g.plan() // duplicate IDs, cycles, and port wiring
	if err != nil {
		return graphPlan{}, err
	}

	for _, n := range g.Nodes {
		if _, ok := r.leafFactory(n.Type); !ok {
			return graphPlan{}, &GraphError{NodeID: n.ID, Field: "type", Err: fmt.Errorf("%w %q", ErrUnknownNodeType, n.Type)}
		}
		if err := r.registeredNodeSchema(n.Type).validateConfig(n.Config); err != nil {
			return graphPlan{}, &GraphError{
				NodeID: n.ID,
				Field:  "config",
				Err:    fmt.Errorf("%w: %w", ErrInvalidGraph, err),
			}
		}
		if err := r.validatePorts(n.Type, plan.wiring[n.ID], func(ref Ref) (ValueType, bool) {
			producer, ok := plan.byID[ref.NodeID]
			if !ok {
				return "", false // external input (the seed Store); nothing to check
			}
			// NodeSchema describes the conventional output as a whole. Its type
			// says nothing about a nested member or another cell written by a
			// custom step, so treating those references as the whole output
			// produces false type errors.
			if ref.Path != Output(ref.NodeID).Path {
				return TypeAny, true
			}
			return r.nodeSchema(producer.Type).Output, true
		}); err != nil {
			return graphPlan{}, &GraphError{NodeID: n.ID, Field: "inputs", Err: err}
		}
	}
	return plan, nil
}

// validatePorts checks a node's wiring against its registered schema: every
// declared port is wired, no undeclared port is wired, and each wired port's
// type is compatible with its producer's output. producerOutput reports a
// producing node's output type, or false when the reference is external.
//
// A node type with no declared ports is left unchecked, so nodes may be
// registered without a schema.
func (r *Registry) validatePorts(nodeType string, inputs Inputs, producerOutput func(Ref) (ValueType, bool)) error {
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
		out, ok := producerOutput(ref)
		if !ok {
			continue
		}
		if !want.accepts(out) {
			return fmt.Errorf("%w: port %q reads %s whose output is %s, want %s",
				ErrIncompatibleType, port, ref.NodeID, out, want)
		}
	}
	return nil
}
