package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// ValueType describes the shape of a value flowing between nodes. It is used only
// for edit-time connection validation (see [Registry.ValidateGraph]); it is never
// consulted at run time.
type ValueType string

// Supported port value types. TypeAny is compatible with every other type.
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
// Outlets declares every string that a routing node may produce as its ordinary
// output. A non-empty Outlets requires Output to be [TypeString]; an empty slice
// means the node is not declared as a router. ConfigSchema, when present, is a
// self-contained Draft 2020-12 JSON Schema for the node's config; an omitted
// config is validated as an empty object. External references are rejected.
//
// An empty Inputs declares nothing, so a node's wiring is left unchecked. Use
// [OnePort] for the common single-input node.
type NodeSchema struct {
	Inputs       Ports           `json:"inputs,omitempty"`
	Output       ValueType       `json:"output"`
	Outlets      []string        `json:"outlets,omitempty"`
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
	if nodeType == "" {
		return &RegistrationError{
			Kind: "schema",
			Err:  fmt.Errorf("%w: node type is empty", ErrInvalidRegistration),
		}
	}
	registered, err := schema.compile()
	if err != nil {
		return &RegistrationError{
			Kind: "schema",
			Name: nodeType,
			Err:  fmt.Errorf("%w: %w", ErrInvalidRegistration, err),
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	_, exists := r.schemas[nodeType]
	if exists {
		return &RegistrationError{Kind: "schema", Name: nodeType, Err: ErrDuplicateRegistration}
	}
	r.schemas[nodeType] = registered
	return nil
}

func (n NodeSchema) compile() (registeredNodeSchema, error) {
	if err := n.validate(); err != nil {
		return registeredNodeSchema{}, err
	}
	n = n.clone()
	validator, err := (schemaSource{
		url:      configSchemaURL,
		document: jsonDocument(n.ConfigSchema),
	}).compileOptional()
	if err != nil {
		return registeredNodeSchema{}, fmt.Errorf("config JSON Schema: %w", err)
	}
	return registeredNodeSchema{schema: n, configValidator: validator}, nil
}

func (n NodeSchema) validate() error {
	if !n.Output.valid() {
		return fmt.Errorf("output type %q is invalid", n.Output)
	}
	if len(n.Outlets) > 0 && n.Output != TypeString {
		return fmt.Errorf(
			"routing output type is %q; want %q",
			n.Output,
			TypeString,
		)
	}
	outlets := make(map[string]struct{}, len(n.Outlets))
	for _, outlet := range n.Outlets {
		if outlet == "" {
			return errors.New("outlet name is empty")
		}
		if _, duplicate := outlets[outlet]; duplicate {
			return fmt.Errorf("outlet %q is declared more than once", outlet)
		}
		outlets[outlet] = struct{}{}
	}
	for _, port := range slices.Sorted(maps.Keys(n.Inputs)) {
		switch valueType := n.Inputs[port]; {
		case port == "":
			return errors.New("input port name is empty")
		case !valueType.valid():
			return fmt.Errorf("input port %q has invalid type %q", port, valueType)
		}
	}
	return nil
}

func (n NodeSchema) clone() NodeSchema {
	n.Inputs = maps.Clone(n.Inputs)
	n.Outlets = slices.Clone(n.Outlets)
	n.ConfigSchema = bytes.Clone(n.ConfigSchema)
	return n
}

func (r registeredNodeSchema) validateConfig(config json.RawMessage) error {
	return r.configValidator.validateConfig(config)
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
// config. The returned Inputs, Outlets, and ConfigSchema are copies.
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
	return registered.schema.clone(), true
}

// NodeTypes returns the registered leaf node type names in sorted order.
func (r *Registry) NodeTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Sorted(maps.Keys(r.nodes))
}

func (v ValueType) valid() bool {
	switch v {
	case "", TypeAny, TypeString, TypeNumber, TypeBool, TypeArray, TypeObject:
		return true
	default:
		return false
	}
}

// accepts reports whether a value of type out can feed a port of type t. An
// empty or TypeAny type on either side is compatible with anything.
func (v ValueType) accepts(out ValueType) bool {
	return out == v || out == "" || v == "" || out == TypeAny || v == TypeAny
}

// validateInputs checks wiring against the schema: every
// declared port is wired, no undeclared port is wired, and each wired port's
// type is compatible with its producer's output. producerOutput reports a
// producing node's output type, or false when the reference is external.
//
// A node type with no declared ports is left unchecked, so nodes may be
// registered without a schema.
func (n NodeSchema) validateInputs(
	inputs Inputs,
	producerOutput func(Ref) (ValueType, bool),
) error {
	declared := n.Inputs
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
			return fmt.Errorf(
				"%w: input port %q reads %s with type %q; want %q",
				ErrIncompatibleType,
				port,
				ref,
				out,
				want,
			)
		}
	}
	return nil
}
