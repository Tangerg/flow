package workflow

import (
	"errors"
	"fmt"
)

type suspensionJSON Suspension

// A persisted wait names its own members. Case folding would be worst here:
// "VALUE" satisfies the application-owned value, so two spellings could collapse
// onto one field and member order would decide which survives.
const (
	suspensionFieldID    = "id"
	suspensionFieldScope = "scope"
	suspensionFieldAwait = "await"
	suspensionFieldValue = "value"
)

// MarshalJSON encodes a Suspension without allowing engine-owned identity to
// change across JSON. Value remains application-owned: encoding/json chooses
// its representation, after which the complete document is checked for
// ambiguity and the engine's recursive-input limit.
func (s Suspension) MarshalJSON() ([]byte, error) {
	if err := s.validateIdentity(); err != nil {
		return nil, fmt.Errorf("workflow: marshal suspension: %w", err)
	}
	encoded, err := marshalJSON(suspensionJSON(s))
	if err != nil {
		return nil, fmt.Errorf("workflow: marshal suspension: %w", err)
	}
	return encoded, nil
}

// UnmarshalJSON atomically replaces a Suspension from one strict JSON object.
// Unknown, duplicate, and noncanonical members, invalid Unicode, excessive
// nesting, and malformed engine identity are rejected. Only the canonical
// lower-case member names are accepted: value is application-owned, so a second
// spelling of it must not be able to replace the persisted payload. Application
// values decode into the lossless JSON domain, including json.Number rather than
// float64.
func (s *Suspension) UnmarshalJSON(data []byte) error {
	if s == nil {
		return errors.New("workflow: unmarshal suspension: nil suspension")
	}
	next, err := decodeSuspension(data)
	if err != nil {
		return fmt.Errorf("workflow: unmarshal suspension: %w", err)
	}
	*s = next
	return nil
}

// decodeSuspension reads the strict canonical object, each return naming only its
// own condition while UnmarshalJSON owns the context.
func decodeSuspension(data []byte) (Suspension, error) {
	raw, err := jsonDocument(data).object()
	if err != nil {
		return Suspension{}, err
	}
	if err := (jsonObject(raw)).allow(
		suspensionFieldID,
		suspensionFieldScope,
		suspensionFieldAwait,
		suspensionFieldValue,
	); err != nil {
		return Suspension{}, err
	}

	var decoded suspensionJSON
	if err := jsonDocument(data).decodeParsed(&decoded); err != nil {
		return Suspension{}, err
	}
	next := Suspension(decoded)
	if err := next.validateIdentity(); err != nil {
		return Suspension{}, err
	}
	return next, nil
}

// validateIdentity checks only fields owned by the engine. An empty ID remains
// valid for the anonymous value returned by Suspend before a Step identifies
// it, but an anonymous wait cannot already have an execution scope. Value
// deliberately has no engine-level schema.
func (s Suspension) validateIdentity() error {
	if s.ID == "" {
		if len(s.Scope) > 0 {
			return errors.New("scope requires an identified step")
		}
	} else if err := validateStepID(s.ID); err != nil {
		return fmt.Errorf("ID: %w", err)
	}
	if err := validateScope(s.Scope); err != nil {
		return fmt.Errorf("scope: %w", err)
	}
	if s.Await != (Ref{}) {
		if err := s.Await.Validate(); err != nil {
			return fmt.Errorf("await: %w", err)
		}
	}
	return nil
}
