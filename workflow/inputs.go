package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// DefaultPort is the port name of a node's single unnamed input. The [Spec] and
// [NodeSpec] "input" field is sugar for wiring this port, and [Factory] binds it.
const DefaultPort = "in"

// Inputs maps a node's input port names to the [Ref] each port reads. A node
// declares its ports through a [NodeSchema]; a graph wires them with Inputs.
//
// Naming every input is what lets the layer above see the whole data flow: the
// flat [Graph] infers dependencies from every wired port, and
// [Registry.ValidateGraph] type-checks each one. A node that smuggles extra
// references through its config is invisible to both.
type Inputs map[string]Ref

// Ref returns the reference wired to port. The bool reports whether the port is
// wired.
func (in Inputs) Ref(port string) (Ref, bool) {
	ref, ok := in[port]
	return ref, ok
}

// Default returns the reference wired to [DefaultPort].
func (in Inputs) Default() (Ref, bool) { return in.Ref(DefaultPort) }

// PortNames returns the wired port names in sorted order.
func (in Inputs) PortNames() []string { return slices.Sorted(maps.Keys(in)) }

// Refs returns the wired references ordered by port name.
func (in Inputs) Refs() []Ref {
	refs := make([]Ref, 0, len(in))
	for _, port := range in.PortNames() {
		refs = append(refs, in[port])
	}
	return refs
}

// LeafSpec carries everything a [LeafFactory] needs to build one leaf: the node
// ID it must report in events and Store writes, its wired input ports, and its
// raw JSON config.
type LeafSpec struct {
	ID     string
	Inputs Inputs
	Config json.RawMessage
}

// withDefault merges the single-input "input" sugar into the receiver. It
// reports an error when both spell out the default port, since the intent is
// then ambiguous. The receiver is never mutated.
func (in Inputs) withDefault(input Ref) (Inputs, error) {
	if input == (Ref{}) {
		return maps.Clone(in), nil
	}
	if _, duplicate := in[DefaultPort]; duplicate {
		return nil, fmt.Errorf("%w: %q is set by both input and inputs", ErrDuplicatePort, DefaultPort)
	}
	resolved := make(Inputs, len(in)+1)
	maps.Copy(resolved, in)
	resolved[DefaultPort] = input
	return resolved, nil
}

// validate checks that every port name and wired reference is well formed.
func (in Inputs) validate() error {
	for _, port := range in.PortNames() {
		if port == "" {
			return errors.New("input port name is empty")
		}
		if err := in[port].validate(); err != nil {
			return fmt.Errorf("input port %q: %w", port, err)
		}
	}
	return nil
}
