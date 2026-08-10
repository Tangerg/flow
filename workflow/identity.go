package workflow

import (
	"fmt"
	"unicode/utf8"
)

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
