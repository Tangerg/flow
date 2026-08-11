package workflow

import (
	"errors"
	"fmt"
)

type suspensionJSON Suspension

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
// Unknown or duplicate members, invalid Unicode, excessive nesting, and
// malformed engine identity are rejected. Application values decode into the
// lossless JSON domain, including json.Number rather than float64.
func (s *Suspension) UnmarshalJSON(data []byte) error {
	if s == nil {
		return errors.New("workflow: unmarshal suspension: nil suspension")
	}
	if _, err := jsonDocument(data).object(); err != nil {
		return fmt.Errorf("workflow: unmarshal suspension: %w", err)
	}

	var decoded suspensionJSON
	if err := jsonDocument(data).decodeParsed(&decoded); err != nil {
		return fmt.Errorf("workflow: unmarshal suspension: %w", err)
	}
	next := Suspension(decoded)
	if err := next.validateIdentity(); err != nil {
		return fmt.Errorf("workflow: unmarshal suspension: %w", err)
	}
	*s = next
	return nil
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
