package workflow

import "fmt"

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

// Schema declares a node type's primary input and output value types, so a
// visual editor can check connections before running.
type Schema struct {
	Input  ValueType `json:"input"`
	Output ValueType `json:"output"`
}

// RegisterSchema associates a [Schema] with a node type. Node types without a
// schema are treated as accepting and producing [TypeAny] (unchecked).
func (r *Registry) RegisterSchema(nodeType string, schema Schema) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	_, exists := r.schemas[nodeType]
	switch {
	case nodeType == "":
		return &RegistrationError{Kind: "schema", Err: fmt.Errorf("%w: empty node type", ErrInvalidRegistration)}
	case !validValueType(schema.Input) || !validValueType(schema.Output):
		return &RegistrationError{Kind: "schema", Name: nodeType, Err: fmt.Errorf("%w: invalid value type", ErrInvalidRegistration)}
	case exists:
		return &RegistrationError{Kind: "schema", Name: nodeType, Err: ErrDuplicateRegistration}
	default:
		r.schemas[nodeType] = schema
	}
	return nil
}

// MustRegisterSchema is like [Registry.RegisterSchema] but panics on error.
func (r *Registry) MustRegisterSchema(nodeType string, schema Schema) *Registry {
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

// ValidateGraph checks a Graph without compiling it: unique IDs, known node types, no
// cycles, and — where schemas are registered — type-compatible Input edges. It is
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
		if n.Input == nil {
			continue
		}
		producer, ok := byID[n.Input.NodeID]
		if !ok {
			continue // external input (the seed Store); nothing to check
		}
		out := r.schema(producer.Type).Output
		in := r.schema(n.Type).Input
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
