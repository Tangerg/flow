package workflow

// Description is a node's self-description. Composite nodes include their
// children, so a Description forms a tree that can be walked for introspection
// or rendered for visualization.
type Description struct {
	ID       string        `json:"id,omitempty"`
	Label    string        `json:"label,omitempty"`
	Kind     string        `json:"kind"`
	Children []Description `json:"children,omitempty"`
}

// Describer is implemented by steps that can describe their own structure. Every
// step this package builds (via [Leaf], [Pipe], [Sequence], [Branch], [Loop],
// [Parallel], [Iteration]) implements it.
type Describer interface {
	Describe() Description
}

// Describe returns step's Description, or an opaque leaf for steps that do not
// implement [Describer] (for example a bare flow.NodeFunc supplied by the caller).
func Describe(step Step) Description {
	if d, ok := step.(Describer); ok {
		return d.Describe()
	}
	return Description{Kind: "opaque"}
}

func describeAll(steps []Step) []Description {
	out := make([]Description, len(steps))
	for i, s := range steps {
		out[i] = Describe(s)
	}
	return out
}
