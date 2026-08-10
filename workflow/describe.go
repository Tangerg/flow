package workflow

import "slices"

// Description is a node's self-description. Composite nodes include their
// children, so a Description forms a tree that can be walked for introspection
// or rendered for visualization. A tree returned by [Describe] is owned by the
// caller and may be modified without changing the Step.
type Description struct {
	ID string `json:"id,omitempty"`
	// Label names this node's relationship to its parent when that relationship
	// has a name. Branch uses the selected-case vocabulary here. It is
	// presentation metadata, not node identity or configuration.
	Label    string        `json:"label,omitempty"`
	Kind     Kind          `json:"kind"`
	Children []Description `json:"children,omitempty"`
}

// Describer is implemented by steps that can describe their own structure.
// Every step this package builds, including leaves, waits, composites, and
// compiled graphs, implements it. Describe implementations must return a
// caller-owned tree and be safe for concurrent use when the Step is safe for
// concurrent use.
type Describer interface {
	Describe() Description
}

// Describe returns a caller-owned snapshot of step's Description, or an opaque
// description for steps that do not implement [Describer] (for example a bare
// flow.NodeFunc supplied by the caller). It defensively copies a caller-defined
// Describer's result so one implementation cannot leak mutable description
// storage through a built-in composite.
func Describe(step Step) Description {
	return describe(step)
}

// describe is the ownership boundary for public descriptions. A built-in Step
// constructs fresh storage and recursively reaches every child through this
// function, so its result is already owned. A caller-defined Describer is an
// opaque boundary and may accidentally return borrowed storage; clone it once
// here so both Describe and a built-in composite's own Describer method keep
// the package contract.
func describe(step Step) Description {
	if d, ok := step.(Describer); ok {
		description := d.Describe()
		if _, builtIn := step.(definedStep); builtIn {
			return description
		}
		return description.clone()
	}
	return Description{Kind: KindOpaque}
}

func (d Description) clone() Description {
	d.Children = slices.Clone(d.Children)
	for index := range d.Children {
		d.Children[index] = d.Children[index].clone()
	}
	return d
}
