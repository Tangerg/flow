package workflow

import "fmt"

// ValueType describes the shape of a value flowing between nodes. It is used only
// for edit-time connection validation (see [Registry.Validate]); it is never
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

// Schema declares a node type's primary input and output value types, so a
// visual editor can check connections before running.
type Schema struct {
	Input  ValueType `json:"input"`
	Output ValueType `json:"output"`
}

// RegisterSchema associates a [Schema] with a node type. It returns the Registry
// for chaining. Node types without a schema are treated as accepting and
// producing [TypeAny] (unchecked).
func (r *Registry) RegisterSchema(nodeType string, schema Schema) *Registry {
	r.schemas[nodeType] = schema
	return r
}

// compatible reports whether a value of type out can feed a port of type in.
// An empty or TypeAny type on either side is compatible with anything.
func compatible(out, in ValueType) bool {
	return out == in || out == "" || in == "" || out == TypeAny || in == TypeAny
}

// Validate checks a Graph without building it: unique IDs, known node types, no
// cycles, and — where schemas are registered — type-compatible Input edges. It is
// intended to power a visual editor's live feedback.
func (r *Registry) Validate(g Graph) error {
	_, byID, err := r.plan(g) // duplicate IDs and cycles
	if err != nil {
		return err
	}

	for _, n := range g.Nodes {
		if _, ok := r.leaves[n.Type]; !ok {
			return fmt.Errorf("workflow: node %q: unknown type %q", n.ID, n.Type)
		}
		if n.Input == nil {
			continue
		}
		producer, ok := byID[n.Input.NodeID]
		if !ok {
			continue // external input (the seed Store); nothing to check
		}
		out := r.schemas[producer.Type].Output
		in := r.schemas[n.Type].Input
		if !compatible(out, in) {
			return fmt.Errorf("workflow: edge %s.output (%s) -> %s.input (%s): incompatible types",
				producer.ID, out, n.ID, in)
		}
	}
	return nil
}
