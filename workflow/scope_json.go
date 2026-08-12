package workflow

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Tangerg/flow/internal/jsonnum"
)

// A scope frame is part of the versioned checkpoint wire contract, so it names
// its own members instead of borrowing the definition diagnostic vocabulary.
const (
	scopeFieldID    = "id"
	scopeFieldIndex = "index"
)

var (
	_ json.Marshaler   = ScopeFrame{}
	_ json.Unmarshaler = (*ScopeFrame)(nil)
)

// scopeFrameJSON is the one canonical wire shape. A nil Index means an
// ordinary namespace; a present pointer, including one to zero, means an
// indexed invocation.
type scopeFrameJSON struct {
	ID    string  `json:"id"`
	Index *uint64 `json:"index,omitempty"`
}

// scopeFrameObject owns strict decoding from the already validated JSON
// domain shared by Journal, JournalKey, Suspension, and standalone frames.
type scopeFrameObject jsonObject

// MarshalJSON encodes a validated frame in its canonical object form. The
// index member is present exactly when Indexed is true, including at index zero.
func (s ScopeFrame) MarshalJSON() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("workflow: marshal scope frame: %w", err)
	}
	wire := scopeFrameJSON{ID: s.ID}
	if s.Indexed {
		wire.Index = &s.Index
	}
	return marshalJSON(wire)
}

// UnmarshalJSON atomically replaces a ScopeFrame from its strict canonical
// object. Unknown, duplicate, or noncanonical members are rejected.
func (s *ScopeFrame) UnmarshalJSON(data []byte) error {
	if s == nil {
		return errors.New("workflow: unmarshal scope frame: nil frame")
	}
	object, err := jsonDocument(data).object()
	if err != nil {
		return fmt.Errorf("workflow: unmarshal scope frame: %w", err)
	}
	next, err := (scopeFrameObject(object)).decode()
	if err != nil {
		return fmt.Errorf("workflow: unmarshal scope frame: %w", err)
	}
	*s = next
	return nil
}

func (s scopeFrameObject) decode() (ScopeFrame, error) {
	object := jsonObject(s)
	if err := object.allow(scopeFieldID, scopeFieldIndex); err != nil {
		return ScopeFrame{}, err
	}
	if err := object.require("scope frame", scopeFieldID); err != nil {
		return ScopeFrame{}, err
	}

	id, ok := object[scopeFieldID].(string)
	if !ok {
		return ScopeFrame{}, errors.New("id must be a string")
	}
	frame := ScopeFrame{ID: id}
	value, indexed := object[scopeFieldIndex]
	if indexed {
		number, ok := value.(json.Number)
		if !ok {
			return ScopeFrame{}, errors.New("index must be an integer")
		}
		parsed, err := jsonnum.ParseInteger(number.String())
		switch {
		case errors.Is(err, jsonnum.ErrRange):
			return ScopeFrame{}, fmt.Errorf("index %s exceeds uint64", number)
		case err != nil || parsed.Negative:
			return ScopeFrame{}, fmt.Errorf(
				"index must be a non-negative integer, got %s",
				number,
			)
		}
		frame.Indexed = true
		frame.Index = parsed.Magnitude
	}
	if err := frame.Validate(); err != nil {
		return ScopeFrame{}, err
	}
	return frame, nil
}
