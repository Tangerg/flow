package workflow

import (
	"fmt"
	"maps"
	"unicode/utf8"
)

// nodeSet is a set of workflow node IDs used by static output analysis and
// engine-owned Store namespace boundaries.
type nodeSet map[string]struct{}

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

// validateName checks the common contract of serialized names: they are
// required and must cross UTF-8 boundaries unchanged. Concepts with a stronger
// error category, such as step IDs, keep their dedicated validator.
func validateName(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is empty", name)
	}
	return validateText(name, value)
}
