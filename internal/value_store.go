package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

type ValueStore interface {
	Set(nodeID, key string, value Value) error
	Get(nodeID, path string) (Value, error)
	Clone() ValueStore
	json.Marshaler
	json.Unmarshaler
}

type InMemoryValueStore struct {
	store map[string]map[string]Value
	mu    sync.RWMutex
}

func NewInMemoryValueStore() *InMemoryValueStore {
	return &InMemoryValueStore{
		store: make(map[string]map[string]Value),
	}
}

func (s *InMemoryValueStore) Set(nodeID, key string, value Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.store[nodeID] == nil {
		s.store[nodeID] = make(map[string]Value)
	}

	s.store[nodeID][key] = value
	return nil
}

func (s *InMemoryValueStore) Get(nodeID, path string) (Value, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodeVars, ok := s.store[nodeID]
	if !ok {
		return nil, fmt.Errorf("node not found: %s", nodeID)
	}

	parts := strings.SplitN(path, ".", 2)
	rootKey := parts[0]

	rootVar, ok := nodeVars[rootKey]
	if !ok {
		return nil, fmt.Errorf("value not found: %s", rootKey)
	}

	if len(parts) == 1 {
		return rootVar, nil
	}

	return getPathRecursive(rootVar, parts[1])
}

func (s *InMemoryValueStore) Clone() ValueStore {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clonedStore := make(map[string]map[string]Value, len(s.store))
	for nodeID, nodeVars := range s.store {
		clonedNodeVars := make(map[string]Value, len(nodeVars))
		for key, value := range nodeVars {
			clonedNodeVars[key] = value.Clone()
		}
		clonedStore[nodeID] = clonedNodeVars
	}

	return &InMemoryValueStore{
		store: clonedStore,
	}
}

func (s *InMemoryValueStore) MarshalJSON() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := make(map[string]map[string]json.RawMessage)

	for nodeID, nodeVars := range s.store {
		data[nodeID] = make(map[string]json.RawMessage)
		for key, value := range nodeVars {
			valueBytes, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal value %s.%s: %w", nodeID, key, err)
			}
			data[nodeID][key] = valueBytes
		}
	}

	return json.Marshal(data)
}

func (s *InMemoryValueStore) UnmarshalJSON(data []byte) error {
	var deserialized map[string]map[string]json.RawMessage
	if err := json.Unmarshal(data, &deserialized); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.store = make(map[string]map[string]Value)

	for nodeID, nodeVars := range deserialized {
		s.store[nodeID] = make(map[string]Value)
		for key, rawBytes := range nodeVars {
			var rawValue any
			if err := json.Unmarshal(rawBytes, &rawValue); err != nil {
				return fmt.Errorf("failed to unmarshal value %s.%s: %w", nodeID, key, err)
			}

			value, err := InferValue(rawValue)
			if err != nil {
				return fmt.Errorf("failed to infer value %s.%s: %w", nodeID, key, err)
			}

			s.store[nodeID][key] = value
		}
	}

	return nil
}
