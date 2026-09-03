package workflow

import (
	"fmt"
	"maps"
	"unicode/utf8"
)

// nodeSet is a set of workflow node IDs used by static output analysis and
// engine-owned Store namespace boundaries. Membership is the only question asked
// of it, and it is asked from three places that each named the answer
// differently -- claimed, internal, present -- so the set answers it.
type nodeSet map[string]struct{}

func newNodeSet(ids ...string) nodeSet {
	set := make(nodeSet, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func (n nodeSet) has(id string) bool {
	_, present := n[id]
	return present
}

// definitionIDs owns path-local execution identity during static traversal.
// Branch cases clone the path they inherit, then merge only newly introduced
// IDs after every mutually exclusive case has been checked.
type definitionIDs map[string]struct{}

func newDefinitionIDs(ids ...string) definitionIDs {
	set := make(definitionIDs, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func (d definitionIDs) clone() definitionIDs {
	return maps.Clone(d)
}

func (d definitionIDs) claim(id string) bool {
	if _, duplicate := d[id]; duplicate {
		return false
	}
	d[id] = struct{}{}
	return true
}

func (d definitionIDs) additions(candidate definitionIDs) definitionIDs {
	introduced := newDefinitionIDs()
	for id := range candidate {
		if _, existed := d[id]; !existed {
			introduced[id] = struct{}{}
		}
	}
	return introduced
}

func (d definitionIDs) addAll(other definitionIDs) {
	maps.Copy(d, other)
}

// validateStepID keeps the execution identity usable by every workflow
// boundary. Step IDs travel through definitions, events, Journal keys, and
// JSON; accepting bytes that JSON would replace would make those identities
// change when persisted.
func validateStepID(id string) error {
	switch {
	case id == "":
		return fmt.Errorf("%w: empty", ErrInvalidStepID)
	case !utf8.ValidString(id):
		return fmt.Errorf("%w: not valid UTF-8", ErrInvalidStepID)
	default:
		return nil
	}
}

func validateText(name, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", name)
	}
	return nil
}

// textMember is one serialized member checked as text. It carries both halves of
// its vocabulary: the field a diagnostic points at, and the concept name the
// message states. Reusing the field for both would repeat it -- "field type: type
// is not valid UTF-8" -- and would describe the same member differently from the
// definition check.
type textMember struct {
	field string
	kind  string
	value string
}

// firstInvalidText names the first member whose text would not survive a JSON
// round trip, and is how a document reports which of its members that was. Every
// definition that reaches the wire checks its own members this way, so the order
// -- first failure wins, in declaration order -- is stated here rather than once
// per document type.
func firstInvalidText(members []textMember) (string, error) {
	for _, member := range members {
		if err := validateText(member.kind, member.value); err != nil {
			return member.field, err
		}
	}
	return "", nil
}

// validateName is for names with no error category of their own; a step ID has
// one and keeps its own validator.
func validateName(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is empty", name)
	}
	return validateText(name, value)
}
