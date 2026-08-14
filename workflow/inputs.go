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

// OneInput returns the Inputs of a node with a single input on [DefaultPort]. It
// is the concise form of the same named-port map, not a second wiring shape, and
// is the wiring-side counterpart of [OnePort].
func OneInput(ref Ref) Inputs { return Inputs{DefaultPort: ref} }

// Ref returns the reference wired to port. The bool reports whether the port is
// wired.
func (i Inputs) Ref(port string) (Ref, bool) {
	ref, ok := i[port]
	return ref, ok
}

// Default returns the reference wired to [DefaultPort].
func (i Inputs) Default() (Ref, bool) { return i.Ref(DefaultPort) }

// PortNames returns the bound names in sorted order, as a fresh slice. They are
// port names for a GraphNode or leaf Spec and inner seed IDs for a Subgraph.
func (i Inputs) PortNames() []string { return i.names() }

func (i Inputs) names() []string { return slices.Sorted(maps.Keys(i)) }

// Refs returns the wired references ordered by port name. The returned slice is
// a copy.
func (i Inputs) Refs() []Ref {
	refs := make([]Ref, 0, len(i))
	for _, port := range i.PortNames() {
		refs = append(refs, i[port])
	}
	return refs
}

// Inputs is validated along two independent axes: which data boundary it binds,
// which supplies the diagnostic vocabulary, and how strict the check is, which
// differs between a definition and the JSON text carrying one. Naming each axis
// value once keeps the four combinations from drifting apart -- a port and a
// seed must not describe themselves differently depending on which check found
// the problem, which TestInputsValidation_namesTheBindingItChecked holds all
// four to.
type bindingVocabulary struct {
	nameKind    string
	bindingKind string
}

type bindingRules struct {
	name func(kind, value string) error
	ref  func(Ref) error
}

var (
	portBinding = bindingVocabulary{nameKind: nameInputPort, bindingKind: "input port"}
	seedBinding = bindingVocabulary{nameKind: nameSubgraphSeed, bindingKind: "subgraph seed"}

	definitionBinding = bindingRules{name: validateName, ref: Ref.Validate}
	jsonTextBinding   = bindingRules{name: validateText, ref: Ref.validateJSONText}
)

func (i Inputs) validatePorts() error {
	return i.validateBindings(portBinding, definitionBinding)
}

func (i Inputs) validateSeeds() error {
	return i.validateBindings(seedBinding, definitionBinding)
}

func (i Inputs) validatePortJSONText() error {
	return i.validateBindings(portBinding, jsonTextBinding)
}

func (i Inputs) validateSeedJSONText() error {
	return i.validateBindings(seedBinding, jsonTextBinding)
}

// validateBindings is the single validation traversal behind all four.
func (i Inputs) validateBindings(vocabulary bindingVocabulary, rules bindingRules) error {
	for _, name := range i.names() {
		if err := rules.name(vocabulary.nameKind, name); err != nil {
			return err
		}
		if err := rules.ref(i[name]); err != nil {
			return fmt.Errorf("%s %q: %w", vocabulary.bindingKind, name, err)
		}
	}
	return nil
}
