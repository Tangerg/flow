package workflow

import (
	"fmt"
	"iter"
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

// All iterates the bindings in name order, yielding each name together with the
// [Ref] it reads. It is the one ordered walk of an Inputs, because a name and the
// reference it binds are one fact: a caller that wants only one half ignores the
// other rather than asking a second method for a projection this walk already
// contains.
//
// The order is canonical, so a check reports the same first offending binding on
// every pass and a rendering lists the same edges in the same places twice. A name
// is an input port for a [GraphNode] or leaf [Spec] and an inner seed ID for a
// [Subgraph]; this walk names neither, so both boundaries can use it and each
// supplies its own vocabulary in diagnostics.
func (i Inputs) All() iter.Seq2[string, Ref] {
	return func(yield func(string, Ref) bool) {
		for _, name := range slices.Sorted(maps.Keys(i)) {
			if !yield(name, i[name]) {
				return
			}
		}
	}
}

// bindingVocabulary and bindingRules are the two independent axes Inputs
// validation varies along: which data boundary it binds, which supplies the
// diagnostic vocabulary, and how strict the check is, which differs between a
// definition and the JSON text carrying one. Naming each axis value once keeps
// the four combinations from drifting apart -- a port and a seed must not
// describe themselves differently depending on which check found the problem,
// which TestInputsValidation_namesTheBindingItChecked holds all four to.
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
	for name, ref := range i.All() {
		if err := rules.name(vocabulary.nameKind, name); err != nil {
			return err
		}
		if err := rules.ref(ref); err != nil {
			return fmt.Errorf("%s %q: %w", vocabulary.bindingKind, name, err)
		}
	}
	return nil
}
