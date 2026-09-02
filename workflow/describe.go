package workflow

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
// storage through a built-in composite. If that result contains a cycle through
// its Children slices, the repeated node is kept as a leaf so the result remains
// the finite tree Description promises.
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

// descriptionChildren identifies one Children slice by where it starts and how
// far it runs. A caller-defined Describer may hand back a tree whose nodes share
// a slice, which is what a cycle looks like from here, so the walk below keys the
// slices it is currently inside on that identity rather than on their contents.
type descriptionChildren struct {
	first  *Description
	length int
}

// descriptionCloneTask is one step of the copy: a node to write into target, or
// the marker that ends a children slice. The marker is how an iterative walk
// learns it has left a slice, which a recursive one would know by returning.
type descriptionCloneTask struct {
	source   Description
	target   *Description
	children descriptionChildren
	leave    bool
}

func (d Description) clone() Description {
	var clone Description
	pending := []descriptionCloneTask{{source: d, target: &clone}}
	active := make(map[descriptionChildren]struct{})
	for len(pending) > 0 {
		last := len(pending) - 1
		task := pending[last]
		pending = pending[:last]
		if task.leave {
			delete(active, task.children)
			continue
		}

		*task.target = Description{
			ID:    task.source.ID,
			Label: task.source.Label,
			Kind:  task.source.Kind,
		}
		if len(task.source.Children) == 0 {
			continue
		}
		key := descriptionChildren{
			first:  &task.source.Children[0],
			length: len(task.source.Children),
		}
		if _, cyclic := active[key]; cyclic {
			// Description promises a tree and has no error return with which to
			// reject a caller-defined cycle. Preserve the repeated node as a leaf;
			// following its children again would turn a presentation defect into an
			// unbounded clone and hand the same defect to every downstream walker.
			continue
		}
		active[key] = struct{}{}
		task.target.Children = make([]Description, len(task.source.Children))
		pending = append(pending, descriptionCloneTask{children: key, leave: true})
		for index := len(task.source.Children) - 1; index >= 0; index-- {
			pending = append(pending, descriptionCloneTask{
				source: task.source.Children[index],
				target: &task.target.Children[index],
			})
		}
	}
	return clone
}
