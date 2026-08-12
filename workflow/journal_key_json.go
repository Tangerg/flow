package workflow

import (
	"errors"
	"fmt"
)

type journalKeyJSON JournalKey

// A persisted key names its own members. Typed decoding alone would fold case,
// letting "ID" satisfy id and, when both spellings appear, letting member order
// decide which one wins.
const (
	keyFieldID    = "id"
	keyFieldScope = "scope"
)

// MarshalJSON encodes a validated JournalKey. Unlike encoding/json's default
// string handling, it never replaces bytes inside execution identity.
func (j JournalKey) MarshalJSON() ([]byte, error) {
	if err := j.validate(); err != nil {
		return nil, fmt.Errorf("workflow: marshal journal key: %w", err)
	}
	return marshalJSON(journalKeyJSON(j))
}

// UnmarshalJSON atomically replaces a JournalKey from one strict JSON object.
// Unknown, duplicate, and noncanonical members, invalid Unicode, and malformed
// identity are rejected before the receiver changes. Only the canonical
// lower-case member names are accepted, so an alternate spelling cannot resume a
// run under a different identity than the one persisted.
func (j *JournalKey) UnmarshalJSON(data []byte) error {
	if j == nil {
		return errors.New("workflow: unmarshal journal key: nil key")
	}
	raw, err := jsonDocument(data).object()
	if err != nil {
		return fmt.Errorf("workflow: unmarshal journal key: %w", err)
	}
	if err := (jsonObject(raw)).allow(keyFieldID, keyFieldScope); err != nil {
		return fmt.Errorf("workflow: unmarshal journal key: %w", err)
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
