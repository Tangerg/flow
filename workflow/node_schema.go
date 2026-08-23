package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// ValueType describes the shape of a value flowing between nodes. It is used
// only for edit-time connection validation (see [Registry.ValidateGraph]); it is
// never consulted at run time. Input ports must use one of the declared values;
// use [TypeAny] when the shape is intentionally unrestricted. The zero value is
// reserved for [NodeSchema.Output] to mean that the node has no output.
type ValueType string

// Supported port value types. TypeAny is the wildcard compatible with every
// other type.
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

// OnePort returns the Ports of a node with a single input on [DefaultPort]. It
// is the schema-side counterpart of [OneInput].
func OnePort(t ValueType) Ports { return Ports{DefaultPort: t} }

// NodeSchema describes a registered node type for validation and tooling.
// Inputs and Output let editors check connections and report incomplete wiring.
// Every input type must be explicit. A zero Output declares that the node has
// no conventional output; TypeAny declares an output of unrestricted shape.
// Compilation verifies this output-presence declaration against the built-in
// boundary returned by the registered NodeFactory. Output also constrains
// nested Graph references: scalars have no child paths, an array's first child
// must be a canonical index, and object or TypeAny members remain open-ended.
// Outlets declares every JSON string that a routing node may produce as its
// ordinary output. Comparing the JSON representation keeps routing stable
// across Journal persistence. A non-empty Outlets requires Output to be
// [TypeString]; an empty slice means the node is not declared as a router.
// ConfigSchema, when non-empty, must be one complete, self-contained Draft
// 2020-12 JSON Schema for the node's config; an omitted config is validated as
// an empty object. An omitted $schema uses Draft 2020-12, while an explicit one
// at any subschema resource must use its HTTP or HTTPS dialect URI, optionally
// with an empty fragment. External references and alternate drafts are rejected.
//
// An empty Inputs declares that the node accepts no input ports. A node type
// with no registered NodeSchema remains unchecked. Use [OnePort] for the common
// single-input node.
type NodeSchema struct {
	Inputs       Ports           `json:"inputs,omitempty"`
	Output       ValueType       `json:"output,omitempty"`
	Outlets      []string        `json:"outlets,omitempty"`
	ConfigSchema json.RawMessage `json:"configSchema,omitempty"`
}

type registeredNodeSchema struct {
	schema          NodeSchema
	configValidator *compiledSchema
}

// RegisterSchema associates a [NodeSchema] with a node type. It compiles
// ConfigSchema once and rejects invalid or external references immediately.
// Without a schema, Registry validation imposes no port, output, or config
// metadata rules; the NodeFactory may still reject wiring or config while the
// definition is compiled, and Graph compilation still rejects an internal data
// edge when the concrete factory boundary produces no output.
func (r *Registry) RegisterSchema(nodeType string, schema NodeSchema) error {
	return r.register(&r.schemas, registrationSchema, nodeType, schema.compile)
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

// validate checks the three declarations a schema makes, in the order the type
// documents them: what the node outputs, which outlets that output may name, and
// which ports it reads.
func (n NodeSchema) validate() error {
	if !n.Output.validOutput() {
		return fmt.Errorf("output type %q is invalid", n.Output)
	}
	if err := n.validateOutlets(); err != nil {
		return err
	}
	return n.validateInputPorts()
}

// validateOutlets checks the routing declaration. Outlets are the JSON strings a
// router may produce as its output, so declaring any of them declares the node a
// router and constrains what that output can be.
func (n NodeSchema) validateOutlets() error {
	if len(n.Outlets) > 0 && n.Output != TypeString {
		return fmt.Errorf(
			"routing output type is %q; want %q",
			n.Output,
			TypeString,
		)
	}
	declared := make(map[string]struct{}, len(n.Outlets))
	for _, outlet := range n.Outlets {
		if err := validateName(nameOutlet, outlet); err != nil {
			return err
		}
		if _, duplicate := declared[outlet]; duplicate {
			return fmt.Errorf("outlet %q is declared more than once", outlet)
		}
		declared[outlet] = struct{}{}
	}
	return nil
}

// validateInputPorts checks the wiring declaration: every port names itself
// usably and accepts an explicit type. Ports are walked in name order so a schema
// with several defects reports the same one on every registration.
func (n NodeSchema) validateInputPorts() error {
	for _, port := range slices.Sorted(maps.Keys(n.Inputs)) {
		if err := validateName(nameInputPort, port); err != nil {
			return err
		}
		valueType := n.Inputs[port]
		if !valueType.validPort() {
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

func (r registeredNodeSchema) validateOutput(producesOutput bool) error {
	declaresOutput := r.schema.Output != ""
	switch {
	case declaresOutput && !producesOutput:
		return fmt.Errorf(
			"schema declares output type %q but the factory returned a step with no output",
			r.schema.Output,
		)
	case !declaresOutput && producesOutput:
		return errors.New("schema declares no output but the factory returned an output-producing step")
	default:
		return nil
	}
}

// MustRegisterSchema is like [Registry.RegisterSchema] but panics on error.
func (r *Registry) MustRegisterSchema(nodeType string, schema NodeSchema) *Registry {
	if err := r.RegisterSchema(nodeType, schema); err != nil {
		panic(err)
	}
	return r
}

// NodeSchema returns the schema registered for nodeType. The bool reports
// whether one was registered. An unregistered type has no schema-level wiring
// or config checks, although its NodeFactory may still reject either during
// compilation and the concrete Graph boundary still determines whether an
// output can be referenced. The returned Inputs, Outlets, and ConfigSchema are
// copies.
//
// Together with [Registry.NodeTypes] this is what an editor reads to render a
// node palette and to know which ports a node exposes.
func (r *Registry) NodeSchema(nodeType string) (NodeSchema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	registered, ok := r.schemas.lookup(nodeType)
	if !ok {
		return NodeSchema{}, false
	}
	return registered.schema.clone(), true
}

// NodeTypes returns the registered node type names in sorted order, as a fresh
// slice that does not alias the Registry.
func (r *Registry) NodeTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodes.names()
}

func (v ValueType) validOutput() bool {
	switch v {
	case "", TypeAny, TypeString, TypeNumber, TypeBool, TypeArray, TypeObject:
		return true
	default:
		return false
	}
}

func (v ValueType) validPort() bool { return v != "" && v.validOutput() }

// accepts reports whether a value of type out can feed a port of type t.
// TypeAny on either side is compatible with anything.
func (v ValueType) accepts(out ValueType) bool {
	return out == v || out == TypeAny || v == TypeAny
}

// acceptsCellPath reports whether ref can possibly resolve at or below the
// named cell when that cell has this declared shape. Object members and
// TypeAny remain open-ended; an array's first child must be a canonical index;
// scalars have no children.
func (v ValueType) acceptsCellPath(ref Ref, nodeID, key string) bool {
	if ref.NodeID != nodeID {
		return false
	}
	pointer, ok := encodedPointer(ref.Path).scan()
	if !ok {
		return false
	}
	cell, _, valid := pointer.next()
	if !valid || cell != key {
		return false
	}
	child, present, valid := pointer.next()
	if !valid {
		return false
	}
	if !present {
		// The reference is the cell itself, which every declared shape resolves.
		return true
	}
	switch v {
	case TypeAny, TypeObject:
		return true
	case TypeArray:
		return pointerToken(child).isArrayIndex()
	default:
		return false
	}
}

// validateInputs checks wiring against the schema: every
// declared port is wired, no undeclared port is wired, and each wired port's
// type is compatible with its producer's output. producerOutput reports a
// producing node's output type, or false when the reference is external.
//
// Calling this method means a schema was registered. An empty declaration is
// therefore authoritative and rejects every wired port; callers skip the
// method entirely for a node type with no schema.
func (n NodeSchema) validateInputs(
	inputs Inputs,
	producerOutput func(Ref) (ValueType, bool),
) error {
	declared := n.Inputs
	for _, port := range slices.Sorted(maps.Keys(declared)) {
		if _, wired := inputs[port]; !wired {
			return fmt.Errorf("%w %q", ErrMissingPort, port)
		}
	}
	for port, ref := range inputs.All() {
		want, ok := declared[port]
		if !ok {
			return fmt.Errorf("%w %q", ErrUnknownPort, port)
		}
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
