package expr

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"unicode/utf8"

	"github.com/Tangerg/flow/internal/jsondoc"
	"github.com/Tangerg/flow/workflow"
)

var strictJSON = jsondoc.Codec{MaxDepth: workflow.MaxNestingDepth}

type (
	bindingsJSON   Bindings
	switchSpecJSON SwitchSpec
)

// MarshalJSON encodes Bindings without allowing encoding/json to replace
// invalid text. Semantic expression checks remain Register's responsibility.
func (b Bindings) MarshalJSON() ([]byte, error) {
	if err := b.validateJSONText(); err != nil {
		return nil, err
	}
	data, err := strictJSON.Marshal(bindingsJSON(b))
	return data, translateJSONError(err)
}

// UnmarshalJSON applies the same strict object boundary as workflow's Graph and
// Spec: one object, valid Unicode, no duplicate or unknown members, and bounded
// nesting. The receiver is replaced only after the whole document has decoded
// successfully.
func (b *Bindings) UnmarshalJSON(data []byte) error {
	if b == nil {
		return errors.New("expr: bindings JSON: nil receiver")
	}
	var next bindingsJSON
	if err := decodeJSONObject(data, &next); err != nil {
		return fmt.Errorf("expr: bindings JSON: %w", err)
	}
	*b = Bindings(next)
	return nil
}

// MarshalJSON encodes a SwitchSpec without silently changing expression or
// branch text. Switch still owns semantic validation and compilation.
func (s SwitchSpec) MarshalJSON() ([]byte, error) {
	if err := s.validateJSONText(); err != nil {
		return nil, err
	}
	data, err := strictJSON.Marshal(switchSpecJSON(s))
	return data, translateJSONError(err)
}

// UnmarshalJSON strictly and atomically decodes a standalone SwitchSpec JSON
// object.
func (s *SwitchSpec) UnmarshalJSON(data []byte) error {
	if s == nil {
		return errors.New("expr: switch JSON: nil receiver")
	}
	var next switchSpecJSON
	if err := decodeJSONObject(data, &next); err != nil {
		return fmt.Errorf("expr: switch JSON: %w", err)
	}
	*s = SwitchSpec(next)
	return nil
}

func decodeJSONObject(data []byte, dst any) error {
	document, err := strictJSON.Value(data)
	if err != nil {
		return translateJSONError(err)
	}
	if _, ok := document.(map[string]any); !ok {
		return errors.New("document must be an object")
	}
	return translateJSONError(strictJSON.DecodeParsed(data, dst))
}

func translateJSONError(err error) error {
	return jsondoc.TranslateDepth(err, workflow.ErrMaxDepth)
}

func (b Bindings) validateJSONText() error {
	for _, name := range slices.Sorted(maps.Keys(b.Conditions)) {
		if err := validateNamedText("condition", name, b.Conditions[name]); err != nil {
			return err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(b.Resolvers)) {
		if err := validateNamedText("resolver", name, b.Resolvers[name]); err != nil {
			return err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(b.Switches)) {
		if !utf8.ValidString(name) {
			return errors.New("expr: switch name is not valid UTF-8")
		}
		if err := b.Switches[name].validateJSONText(); err != nil {
			return fmt.Errorf("expr: switch %q: %w", name, err)
		}
	}
	return nil
}

func validateNamedText(kind, name, value string) error {
	if !utf8.ValidString(name) {
		return fmt.Errorf("expr: %s name is not valid UTF-8", kind)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("expr: %s %q is not valid UTF-8", kind, name)
	}
	return nil
}

func (s SwitchSpec) validateJSONText() error {
	for index, entry := range s.Cases {
		if !utf8.ValidString(entry.When) {
			return fmt.Errorf("case %d expression is not valid UTF-8", index)
		}
		if !utf8.ValidString(entry.Then) {
			return fmt.Errorf("case %d branch name is not valid UTF-8", index)
		}
	}
	if !utf8.ValidString(s.Fallback) {
		return errors.New("fallback branch name is not valid UTF-8")
	}
	return nil
}
