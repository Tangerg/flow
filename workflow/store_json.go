package workflow

import (
	"encoding/json"
	"fmt"
	"iter"
	"maps"
	"slices"
	"unicode/utf8"

	"github.com/Tangerg/flow/internal/jsondoc"
)

var (
	_ json.Marshaler   = Store{}
	_ json.Unmarshaler = (*Store)(nil)
)

// MarshalJSON serializes the Store as nodeID -> key -> value. It reports the
// cell containing a value that encoding/json cannot encode, whose custom JSON
// encoding is malformed or ambiguous, or whose encoded form exceeds
// [MaxNestingDepth]. Application values otherwise follow encoding/json's rules,
// including replacement of invalid UTF-8 in ordinary Go strings. Node IDs and
// cell keys must be valid UTF-8 so their identities survive the JSON boundary
// unchanged.
func (s Store) MarshalJSON() ([]byte, error) {
	encoded, err := s.jsonDocument().encode()
	if err != nil {
		return nil, fmt.Errorf("workflow: marshal store: %w", err)
	}
	return encoded, nil
}

// encode runs the store wire format's three stages in order: names must cross
// the boundary unchanged, application values are marshaled exactly once, and
// the assembled document must stay inside the recursive-input limit.
func (s storeJSONDocument) encode() ([]byte, error) {
	if err := s.validateNames(); err != nil {
		return nil, err
	}
	if err := s.encodeValues(); err != nil {
		return nil, err
	}
	return s.marshal()
}

type storeJSONDocument map[string]map[string]any

// cells iterates the document one cell at a time in the canonical order
// [storeKey.compare] defines. Map iteration order is random, so every pass over
// the wire format walks this order instead: the three encoding stages report the
// same first failing cell on every run, and decoding assigns its revisions in
// the order an encoded document lists them.
func (s storeJSONDocument) cells() iter.Seq2[storeKey, any] {
	return func(yield func(storeKey, any) bool) {
		var identities []storeKey
		for nodeID, values := range s {
			for key := range values {
				identities = append(identities, storeKey{nodeID: nodeID, key: key})
			}
		}
		slices.SortFunc(identities, storeKey.compare)

		for _, identity := range identities {
			if !yield(identity, s[identity.nodeID][identity.key]) {
				return
			}
		}
	}
}

// validateNames rejects identities that would not survive the boundary
// unchanged. A node with no cells cannot reach the document, so checking each
// cell reaches every name.
func (s storeJSONDocument) validateNames() error {
	for identity := range s.cells() {
		if !utf8.ValidString(identity.nodeID) {
			return fmt.Errorf("node ID %q is not valid UTF-8", identity.nodeID)
		}
		if !utf8.ValidString(identity.key) {
			return fmt.Errorf("node %q key %q is not valid UTF-8", identity.nodeID, identity.key)
		}
	}
	return nil
}

func (s Store) jsonDocument() storeJSONDocument {
	document := make(storeJSONDocument)
	for identity, cell := range s.baseCells() {
		document.put(identity, cell)
	}
	s.delta.applyOverlay(document.put)
	return document
}

func (s storeJSONDocument) put(identity storeKey, cell cell) {
	values := s[identity.nodeID]
	if cell.removed {
		delete(values, identity.key)
		if len(values) == 0 {
			delete(s, identity.nodeID)
		}
		return
	}
	if values == nil {
		values = make(map[string]any)
		s[identity.nodeID] = values
	}
	values[identity.key] = cell.value
}

// encodeValues invokes application JSON marshalers exactly once, replacing
// every temporary document value with the resulting immutable JSON fragment.
func (s storeJSONDocument) encodeValues() error {
	for identity, value := range s.cells() {
		encoded, err := json.Marshal(value)
		if err != nil {
			return identity.locate(err)
		}
		s[identity.nodeID][identity.key] = json.RawMessage(encoded)
	}
	return nil
}

// locate names the cell a wire-format failure happened in.
func (s storeKey) locate(err error) error {
	return fmt.Errorf("node %q key %q: %w", s.nodeID, s.key, err)
}

// marshal enforces the same recursive boundary as Store.UnmarshalJSON. Each
// one-cell document has the same maximum depth as the complete document, so
// validating cells independently both identifies failures and proves the final
// assembly readable. Values are RawMessages produced by encodeValues, so the
// structural encodings below cannot invoke application code.
func (s storeJSONDocument) marshal() ([]byte, error) {
	for identity, value := range s.cells() {
		candidate := storeJSONDocument{
			identity.nodeID: {identity.key: value},
		}
		if _, err := marshalJSON(candidate); err != nil {
			return nil, identity.locate(err)
		}
	}
	// The same invariant applies to the complete map, and the per-cell checks
	// above proved the package's recursive-input boundary.
	//
	//nolint:errchkjson // Only validated map keys and RawMessages remain.
	encoded, _ := json.Marshal(s)
	return encoded, nil
}

// UnmarshalJSON atomically replaces the Store from nodeID -> key -> value JSON.
// The top level and each node must be objects; invalid Unicode text, null, and
// duplicate object members are rejected. On failure the receiver is unchanged.
//
// Numbers decode as [json.Number] rather than float64, so a decoded Store loses
// no precision and an int64 beyond float64's exact range survives the round
// trip. Read decoded values with [Get], which converts them to the type a caller
// asks for; a bare [Store.Lookup] returns the JSON-domain value. Decoded cells
// receive fresh write identities, so [Store.Changes] against a Store from a
// different lineage reports every decoded cell.
func (s *Store) UnmarshalJSON(data []byte) error {
	if s == nil {
		return fmt.Errorf("workflow: unmarshal store: %w", jsondoc.ErrNilReceiver)
	}
	raw, err := jsonDocument(data).object()
	if err != nil {
		return fmt.Errorf("workflow: unmarshal store: %w", err)
	}

	// Validating every node before assigning any revision keeps a malformed
	// document from consuming revisions. This pass walks sorted node names so an
	// invalid document reports the same first node on every decode; assembling the
	// document lets the pass below reuse the one canonical cell order.
	document := make(storeJSONDocument, len(raw))
	size := 0
	for _, nodeID := range slices.Sorted(maps.Keys(raw)) {
		value := raw[nodeID]
		values, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf(
				"workflow: unmarshal store node %q: expected object, got %s",
				nodeID,
				jsondoc.Kind(value),
			)
		}
		document[nodeID] = values
		size += len(values)
	}

	nextData := make(storeCells, size)
	for identity, value := range document.cells() {
		revision := revisionCounter.Add(1)
		nextData[identity] = cell{
			value:    value,
			revision: revision,
			lineage:  revision,
		}
	}
	if len(nextData) == 0 {
		*s = Store{}
	} else {
		*s = Store{snapshot: &storeSnapshot{data: nextData}}
	}
	return nil
}
