package workflow

import (
	"errors"
	"fmt"
	"maps"
	"slices"
)

// DefaultPort is the conventional name of a node's single input. [Factory]
// binds this port.
const DefaultPort = "in"

// Inputs maps a node's input port names to the [Ref] each port reads. A node
// declares its ports through a [NodeSchema]; a graph wires them with Inputs.
//
// Naming every input is what lets the enclosing definition see the whole data
// flow: the flat [Graph] infers dependencies from every wired port, and
// [Registry.ValidateGraph] type-checks each one. A node that smuggles extra
// references through its config is invisible to both.
type Inputs map[string]Ref

// Ref returns the reference wired to port. The bool reports whether the port is
// wired.
func (i Inputs) Ref(port string) (Ref, bool) {
	ref, ok := i[port]
	return ref, ok
}

// Default returns the reference wired to [DefaultPort].
func (i Inputs) Default() (Ref, bool) { return i.Ref(DefaultPort) }

// PortNames returns the wired port names in sorted order.
func (i Inputs) PortNames() []string { return slices.Sorted(maps.Keys(i)) }

// Refs returns the wired references ordered by port name.
func (i Inputs) Refs() []Ref {
	refs := make([]Ref, 0, len(i))
	for _, port := range i.PortNames() {
		refs = append(refs, i[port])
	}
	return refs
}

// validate checks that every port name and wired reference is well formed.
func (i Inputs) validate() error {
	for _, port := range i.PortNames() {
		if port == "" {
			return errors.New("input port name is empty")
		}
		if err := i[port].validate(); err != nil {
			return fmt.Errorf("input port %q: %w", port, err)
		}
	}
	return nil
}
