package workflow

import (
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync/atomic"
)

// Store is a persistent variable pool: a two-level map of nodeID -> key ->
// value. Every write returns a new Store that shares untouched cells with the
// original, so the Store structure is immutable and each intermediate state is
// a cheap snapshot.
//
// Values are held and returned as-is (any). Callers must treat mutable values
// such as maps, slices, and pointers as immutable after insertion; mutating one
// would mutate every Store snapshot that shares it and may introduce a data
// race. Lookup walks into map[string]any and []any, so JSON-shaped data can be
// addressed by path such as "result.items.0".
//
// The zero Store is empty and ready to use; prefer [NewStore] for clarity.
type Store struct {
	data map[string]map[string]cell
}

// revisionCounter gives each write an identity. Parallel uses it to distinguish
// a branch's writes from cells merely inherited from its input snapshot,
// without comparing arbitrary values.
var revisionCounter atomic.Uint64

type cell struct {
	value    any
	revision uint64
}

var (
	_ json.Marshaler   = Store{}
	_ json.Unmarshaler = (*Store)(nil)
)

// NewStore returns an empty Store.
func NewStore() Store {
	return Store{}
}

// With returns a copy of the Store with value written at (nodeID, key). The
// receiver is not modified; untouched node maps are shared with the copy. Value
// is not cloned and must not be mutated after insertion.
func (s Store) With(nodeID, key string, value any) Store {
	outer := maps.Clone(s.data)
	if outer == nil {
		outer = make(map[string]map[string]cell, 1)
	}
	inner := maps.Clone(outer[nodeID])
	if inner == nil {
		inner = make(map[string]cell, 1)
	}
	inner[key] = cell{value: value, revision: revisionCounter.Add(1)}
	outer[nodeID] = inner
	return Store{data: outer}
}

// WithOutput returns a copy of the Store with value written to the conventional
// output key for nodeID.
func (s Store) WithOutput(nodeID string, value any) Store {
	return s.With(nodeID, OutputKey, value)
}

// Lookup returns the value at ref. The path's first segment is the key under the
// node; remaining segments walk into nested map[string]any and []any values. The
// bool reports whether the reference resolved. Returned mutable values are
// borrowed views and must not be mutated.
func (s Store) Lookup(ref Ref) (any, bool) {
	inner, ok := s.data[ref.NodeID]
	if !ok {
		return nil, false
	}
	key, rest, _ := strings.Cut(ref.Path, ".")
	c, ok := inner[key]
	if !ok {
		return nil, false
	}
	return walk(c.value, rest)
}

// MarshalJSON serializes the Store as nodeID -> key -> value. It reports the
// cell containing a value that encoding/json cannot encode.
func (s Store) MarshalJSON() ([]byte, error) {
	raw := make(map[string]map[string]json.RawMessage, len(s.data))
	for nodeID, inner := range s.data {
		rawInner := make(map[string]json.RawMessage, len(inner))
		for key, c := range inner {
			data, err := json.Marshal(c.value)
			if err != nil {
				return nil, fmt.Errorf("workflow: marshal store %s.%s: %w", nodeID, key, err)
			}
			rawInner[key] = data
		}
		raw[nodeID] = rawInner
	}
	return json.Marshal(raw)
}

// UnmarshalJSON atomically replaces the Store from nodeID -> key -> value JSON.
// On failure the receiver is unchanged. Values follow encoding/json's standard
// representation, including float64 for JSON numbers.
func (s *Store) UnmarshalJSON(data []byte) error {
	var raw map[string]map[string]json.RawMessage
	if err := decodeStrict(data, &raw); err != nil {
		return fmt.Errorf("workflow: unmarshal store: %w", err)
	}

	next := Store{}
	for nodeID, inner := range raw {
		for key, encoded := range inner {
			var value any
			if err := json.Unmarshal(encoded, &value); err != nil {
				return fmt.Errorf("workflow: unmarshal store %s.%s: %w", nodeID, key, err)
			}
			next = next.With(nodeID, key, value)
		}
	}
	*s = next
	return nil
}

// walk descends into v following a dot-separated path over map[string]any and
// []any values.
func walk(v any, path string) (any, bool) {
	if path == "" {
		return v, true
	}
	key, rest, _ := strings.Cut(path, ".")
	switch c := v.(type) {
	case map[string]any:
		next, ok := c[key]
		if !ok {
			return nil, false
		}
		return walk(next, rest)
	case []any:
		i, err := strconv.Atoi(key)
		if err != nil || i < 0 || i >= len(c) {
			return nil, false
		}
		return walk(c[i], rest)
	default:
		return nil, false
	}
}
