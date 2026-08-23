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
	return marshalJSON(s.wire())
}

// wire returns the frame's canonical wire shape, stating the Indexed-to-index
// mapping once for both the standalone encoding above and an enclosing document
// that embeds frames directly.
func (s ScopeFrame) wire() scopeFrameJSON {
	frame := scopeFrameJSON{ID: s.ID}
	if s.Indexed {
		frame.Index = &s.Index
	}
	return frame
}

// scopeWire converts a scope whose frames are already known to be valid. A
// document that embeds the result encodes each frame once, where embedding
// [ScopeFrame] values instead would re-validate and re-parse every frame through
// their MarshalJSON. A nil result omits an empty scope member.
func scopeWire(scope []ScopeFrame) []scopeFrameJSON {
	if len(scope) == 0 {
		return nil
	}
	frames := make([]scopeFrameJSON, len(scope))
	for index, frame := range scope {
		frames[index] = frame.wire()
	}
	return frames
}

// UnmarshalJSON atomically replaces a ScopeFrame from its strict canonical
// object. Unknown, duplicate, or noncanonical members are rejected.
func (s *ScopeFrame) UnmarshalJSON(data []byte) error {
	return jsonDocument(data).decodeInto(s, decodeScopeFrame, unmarshalError("scope frame"))
}

func decodeScopeFrame(data []byte) (ScopeFrame, error) {
	object, err := jsonDocument(data).object()
	if err != nil {
		return ScopeFrame{}, err
	}
	return scopeFrameObject(object).decode()
}

func (s scopeFrameObject) decode() (ScopeFrame, error) {
	object := jsonObject(s)
	if err := (strictObject{
		what:     "scope frame",
		required: []string{scopeFieldID},
		optional: []string{scopeFieldIndex},
	}).check(object); err != nil {
		return ScopeFrame{}, err
	}

	id, err := object.stringMember(scopeFieldID)
	if err != nil {
		return ScopeFrame{}, err
	}
	frame := ScopeFrame{ID: id}
	value, indexed := object[scopeFieldIndex]
	if indexed {
		number, ok := value.(json.Number)
		if !ok {
			return ScopeFrame{}, errors.New("index must be an integer")
		}
		parsed, err := jsonnum.ParseInteger(number.String())
		index, fits := parsed.Unsigned()
		switch {
		case errors.Is(err, jsonnum.ErrRange):
			return ScopeFrame{}, fmt.Errorf("index %s exceeds uint64", number)
		case err != nil || !fits:
			return ScopeFrame{}, fmt.Errorf(
				"index must be a non-negative integer, got %s",
				number,
			)
		}
		frame.Indexed = true
		frame.Index = index
	}
	if err := frame.Validate(); err != nil {
		return ScopeFrame{}, err
	}
	return frame, nil
}
