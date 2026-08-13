package workflow

import (
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
	return decodeJSONInto(j, data, decodeJournalKey, unmarshalError("journal key"))
}

// decodeJournalKey reads the strict canonical object, each return naming only its
// own condition while UnmarshalJSON owns the context.
func decodeJournalKey(data []byte) (JournalKey, error) {
	raw, err := jsonDocument(data).object()
	if err != nil {
		return JournalKey{}, err
	}

	object := jsonObject(raw)
	if err := object.allow(keyFieldID, keyFieldScope); err != nil {
		return JournalKey{}, err
	}
	// validate below would also reject an absent id, but as an empty one. Naming
	// the missing member says what the document lacks, and matches what every
	// other member contract in this package reports, including the same id read
	// as part of a Journal record.
	if err := object.require("journal key", keyFieldID); err != nil {
		return JournalKey{}, err
	}

	var decoded journalKeyJSON
	if err := jsonDocument(data).decodeParsed(&decoded); err != nil {
		return JournalKey{}, err
	}
	next := JournalKey(decoded)
	if err := next.validate(); err != nil {
		return JournalKey{}, err
	}
	return next, nil
}
