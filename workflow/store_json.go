package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"unicode/utf8"
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
	document := s.jsonDocument()
	if err := document.validateNames(); err != nil {
		return nil, fmt.Errorf("workflow: marshal store: %w", err)
	}
	if err := document.encodeValues(); err != nil {
		return nil, fmt.Errorf("workflow: marshal store: %w", err)
	}
	encoded, err := document.marshal()
	if err != nil {
		return nil, fmt.Errorf("workflow: marshal store: %w", err)
	}
	return encoded, nil
}

type storeJSONDocument map[string]map[string]any

func (s storeJSONDocument) validateNames() error {
	for _, nodeID := range slices.Sorted(maps.Keys(s)) {
		if !utf8.ValidString(nodeID) {
			return fmt.Errorf("node ID %q is not valid UTF-8", nodeID)
		}
		for _, key := range slices.Sorted(maps.Keys(s[nodeID])) {
			if !utf8.ValidString(key) {
				return fmt.Errorf("node %q key %q is not valid UTF-8", nodeID, key)
			}
		}
	}
	return nil
}

func (s Store) jsonDocument() storeJSONDocument {
	document := make(storeJSONDocument)
	if s.snapshot != nil {
		for identity, cell := range s.snapshot.data {
			document.put(identity, cell)
		}
	}
	for _, delta := range s.deltasOldestFirst() {
		document.put(delta.key, delta.cell)
	}
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
	for _, nodeID := range slices.Sorted(maps.Keys(s)) {
		values := s[nodeID]
		for _, key := range slices.Sorted(maps.Keys(values)) {
			encoded, err := json.Marshal(values[key])
			if err != nil {
				return fmt.Errorf("node %q key %q: %w", nodeID, key, err)
			}
			values[key] = json.RawMessage(encoded)
		}
	}
	return nil
}

// marshal enforces the same recursive boundary as Store.UnmarshalJSON. Each
// one-cell document has the same maximum depth as the complete document, so
// validating cells independently both identifies failures and proves the final
// assembly readable. Values are RawMessages produced by encodeValues, so the
// structural encodings below cannot invoke application code.
func (s storeJSONDocument) marshal() ([]byte, error) {
	for _, nodeID := range slices.Sorted(maps.Keys(s)) {
		values := s[nodeID]
		for _, key := range slices.Sorted(maps.Keys(values)) {
			candidate := storeJSONDocument{
				nodeID: {key: values[key]},
			}
			if _, err := marshalJSON(candidate); err != nil {
				return nil, fmt.Errorf("node %q key %q: %w", nodeID, key, err)
			}
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
		return errors.New("workflow: unmarshal store: nil store")
	}
	document, err := jsonDocument(data).value()
	if err != nil {
		return fmt.Errorf("workflow: unmarshal store: %w", err)
	}
	raw, ok := document.(map[string]any)
	if !ok {
		return fmt.Errorf(
			"workflow: unmarshal store: expected object, got %s",
			jsonValue{raw: document}.kind(),
		)
	}

	// Validating every node before assigning any revision keeps a malformed
	// document from consuming revisions. Map iteration order is random, so both
	// passes walk sorted names: invalid documents report the same first node, and
	// valid documents receive the same revision order on every decode. Holding
	// the converted maps means the second pass needs no repeated assertion.
	nodes := make(map[string]map[string]any, len(raw))
	nodeIDs := slices.Sorted(maps.Keys(raw))
	size := 0
	for _, nodeID := range nodeIDs {
		value := raw[nodeID]
		values, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf(
				"workflow: unmarshal store node %q: expected object, got %s",
				nodeID,
				jsonValue{raw: value}.kind(),
			)
		}
		nodes[nodeID] = values
		size += len(values)
	}

	nextData := make(storeCells, size)
	for _, nodeID := range nodeIDs {
		values := nodes[nodeID]
		for _, key := range slices.Sorted(maps.Keys(values)) {
			revision := revisionCounter.Add(1)
			nextData[storeKey{nodeID: nodeID, key: key}] = cell{
				value:    values[key],
				revision: revision,
				lineage:  revision,
			}
		}
	}
	if len(nextData) == 0 {
		*s = Store{}
	} else {
		*s = Store{snapshot: &storeSnapshot{data: nextData}}
	}
	return nil
}

type jsonValue struct {
	raw any
}

func (j jsonValue) kind() string {
	switch j.raw.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case json.Number:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return fmt.Sprintf("%T", j.raw)
	}
}
