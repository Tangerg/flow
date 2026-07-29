package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

var (
	_ json.Marshaler   = Store{}
	_ json.Unmarshaler = (*Store)(nil)
)

// MarshalJSON serializes the Store as nodeID -> key -> value. It reports the
// cell containing a value that encoding/json cannot encode.
func (s Store) MarshalJSON() ([]byte, error) {
	document := s.jsonDocument()
	encoded, err := json.Marshal(document)
	if err == nil {
		return encoded, nil
	}

	// Keep the successful path to one encoding pass. On failure, isolate the
	// offending cell so callers retain the more useful Store path in the error.
	if nodeID, key, cellErr := document.firstInvalidValue(); cellErr != nil {
		return nil, fmt.Errorf(
			"workflow: marshal store node %q key %q: %w",
			nodeID,
			key,
			cellErr,
		)
	}
	return nil, fmt.Errorf("workflow: marshal store: %w", err)
}

type storeJSONDocument map[string]map[string]any

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

func (document storeJSONDocument) put(identity storeKey, cell cell) {
	values := document[identity.nodeID]
	if values == nil {
		values = make(map[string]any)
		document[identity.nodeID] = values
	}
	values[identity.key] = cell.value
}

func (document storeJSONDocument) firstInvalidValue() (string, string, error) {
	for _, nodeID := range slices.Sorted(maps.Keys(document)) {
		values := document[nodeID]
		for _, key := range slices.Sorted(maps.Keys(values)) {
			if _, err := json.Marshal(values[key]); err != nil {
				return nodeID, key, err
			}
		}
	}
	return "", "", nil
}

// UnmarshalJSON atomically replaces the Store from nodeID -> key -> value JSON.
// The top level and each node must be objects; null and duplicate object members
// are rejected. On failure the receiver is unchanged.
//
// Numbers decode as [json.Number] rather than float64, so a decoded Store loses
// no precision and an int64 beyond float64's exact range survives the round
// trip. Read decoded values with [Get], which converts them to the type a caller
// asks for; a bare [Store.Lookup] returns the JSON-domain value.
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

	nodeIDs := make([]string, 0, len(raw))
	size := 0
	for nodeID, value := range raw {
		values, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf(
				"workflow: unmarshal store node %q: expected object, got %s",
				nodeID,
				jsonValue{raw: value}.kind(),
			)
		}
		nodeIDs = append(nodeIDs, nodeID)
		size += len(values)
	}
	slices.Sort(nodeIDs)

	nextData := make(map[storeKey]cell, size)
	for _, nodeID := range nodeIDs {
		values := raw[nodeID].(map[string]any)
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			nextData[storeKey{nodeID: nodeID, key: key}] = cell{
				value:    values[key],
				revision: revisionCounter.Add(1),
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

func (value jsonValue) kind() string {
	switch value.raw.(type) {
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
		return fmt.Sprintf("%T", value.raw)
	}
}
