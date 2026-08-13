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
	return jsondoc.DecodeInto(b, data, decodeBindings, jsonError("bindings"))
}

func decodeBindings(data []byte) (Bindings, error) {
	var next bindingsJSON
	if err := decodeJSONObject(data, &next); err != nil {
		return Bindings{}, err
	}
	return Bindings(next), nil
}

// jsonError names one of this package's JSON boundaries.
func jsonError(what string) func(error) error {
	return func(err error) error {
		return fmt.Errorf("expr: %s JSON: %w", what, err)
	}
}

// MarshalJSON encodes a SwitchSpec without silently changing expression or
// branch text. Switch still owns semantic validation and compilation.
func (s SwitchSpec) MarshalJSON() ([]byte, error) {
	if err := s.validateText(); err != nil {
		return nil, err
	}
	data, err := strictJSON.Marshal(switchSpecJSON(s))
	return data, translateJSONError(err)
}

// UnmarshalJSON strictly and atomically decodes a standalone SwitchSpec JSON
// object.
func (s *SwitchSpec) UnmarshalJSON(data []byte) error {
	return jsondoc.DecodeInto(s, data, decodeSwitchSpec, jsonError("switch"))
}

func decodeSwitchSpec(data []byte) (SwitchSpec, error) {
	var next switchSpecJSON
	if err := decodeJSONObject(data, &next); err != nil {
		return SwitchSpec{}, err
	}
	return SwitchSpec(next), nil
}

func decodeJSONObject(data []byte, dst any) error {
	if _, err := strictJSON.Object(data); err != nil {
		return translateJSONError(err)
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
		if err := b.Switches[name].validateText(); err != nil {
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
