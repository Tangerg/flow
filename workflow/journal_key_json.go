package workflow

import (
	"errors"
	"fmt"
)

type journalKeyJSON JournalKey

// MarshalJSON encodes a validated JournalKey. Unlike encoding/json's default
// string handling, it never replaces bytes inside execution identity.
func (j JournalKey) MarshalJSON() ([]byte, error) {
	if err := j.validate(); err != nil {
		return nil, fmt.Errorf("workflow: marshal journal key: %w", err)
	}
	return marshalJSON(journalKeyJSON(j))
}

// UnmarshalJSON atomically replaces a JournalKey from one strict JSON object.
// Unknown or duplicate members, invalid Unicode, and malformed identity are
// rejected before the receiver changes.
func (j *JournalKey) UnmarshalJSON(data []byte) error {
	if j == nil {
		return errors.New("workflow: unmarshal journal key: nil key")
	}
	document, err := jsonDocument(data).value()
	if err != nil {
		return fmt.Errorf("workflow: unmarshal journal key: %w", err)
	}
	if _, ok := document.(map[string]any); !ok {
		return fmt.Errorf(
			"workflow: unmarshal journal key: expected object, got %s",
			jsonValue{raw: document}.kind(),
		)
	}

	var decoded journalKeyJSON
	if err := jsonDocument(data).decodeParsed(&decoded); err != nil {
		return fmt.Errorf("workflow: unmarshal journal key: %w", err)
	}
	next := JournalKey(decoded)
	if err := next.validate(); err != nil {
		return fmt.Errorf("workflow: unmarshal journal key: %w", err)
	}
	*j = next
	return nil
}
