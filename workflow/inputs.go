package workflow

import (
	"fmt"
	"maps"
	"slices"
)

// DefaultPort is the conventional name of a node's single input. [Factory]
// binds this port.
const DefaultPort = "in"

// Inputs maps names at a data boundary to the [Ref] each name reads. For a
// [GraphNode] or leaf [Spec], names are input ports declared by [NodeSchema].
// For [Subgraph], names are inner seed IDs. Both are the same one-way binding
// shape; the enclosing definition supplies the vocabulary and validation.
//
// Naming every input is what lets the enclosing definition see the whole data
// flow: the flat [Graph] infers dependencies from every wired port, and
// [Registry.ValidateGraph] type-checks each one. A node that smuggles extra
// references through its config is invisible to both.
type Inputs map[string]Ref

// DefaultInput returns an Inputs value with ref wired to [DefaultPort], the
// conventional single input. It is the concise form of the same named-port map,
// not a second wiring shape.
func DefaultInput(ref Ref) Inputs { return Inputs{DefaultPort: ref} }

// Ref returns the reference wired to port. The bool reports whether the port is
// wired.
func (i Inputs) Ref(port string) (Ref, bool) {
	ref, ok := i[port]
	return ref, ok
}

// Default returns the reference wired to [DefaultPort].
func (i Inputs) Default() (Ref, bool) { return i.Ref(DefaultPort) }

// PortNames returns the bound names in sorted order. They are port names for a
// GraphNode or leaf Spec and inner seed IDs for a Subgraph.
func (i Inputs) PortNames() []string { return i.names() }

func (i Inputs) names() []string { return slices.Sorted(maps.Keys(i)) }

// Refs returns the wired references ordered by port name.
func (i Inputs) Refs() []Ref {
	refs := make([]Ref, 0, len(i))
	for _, port := range i.PortNames() {
		refs = append(refs, i[port])
	}
	return refs
}

func (i Inputs) validatePorts() error {
	return i.validateBindings("input port name", "input port", validateName, Ref.Validate)
}

func (i Inputs) validateSeeds() error {
	return i.validateBindings("subgraph seed ID", "subgraph seed", validateName, Ref.Validate)
}

func (i Inputs) validatePortJSONText() error {
	return i.validateBindings(
		"input port name",
		"input port",
		validateText,
		Ref.validateJSONText,
	)
}

func (i Inputs) validateSeedJSONText() error {
	return i.validateBindings(
		"subgraph seed ID",
		"subgraph seed",
		validateText,
		Ref.validateJSONText,
	)
}

// validateBindings is the single validation traversal for Inputs' two named
// data-boundary uses. Only their diagnostic vocabulary differs.
func (i Inputs) validateBindings(
	nameKind string,
	bindingKind string,
	validateBindingName func(string, string) error,
	validateRef func(Ref) error,
) error {
	for _, name := range i.names() {
		if err := validateBindingName(nameKind, name); err != nil {
			return err
		}
		if err := validateRef(i[name]); err != nil {
			return fmt.Errorf("%s %q: %w", bindingKind, name, err)
		}
	}
	return nil
}
