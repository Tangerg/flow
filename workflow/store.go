package workflow

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

// Store is a persistent variable pool: a two-level map of nodeID -> key ->
// value. Every write returns a new Store that shares its base snapshot with the
// original and records the change in a bounded overlay. The Store structure is
// immutable; overlays are periodically compacted to keep lookups bounded.
//
// Values are held and returned as-is (any). Callers must treat mutable values
// such as maps, slices, and pointers as immutable after insertion; mutating one
// would mutate every Store snapshot that shares it and may introduce a data
// race. Lookup walks into map[string]any and []any, so JSON-shaped data can be
// addressed by path such as "result.items.0".
//
// The zero Store is empty and ready to use; prefer [NewStore] for clarity.
type Store struct {
	snapshot *storeSnapshot
	delta    *storeDelta
	depth    int
}

const storeOverlayLimit = 64

type storeSnapshot struct {
	data map[storeKey]cell
}

type storeDelta struct {
	parent *storeDelta
	key    storeKey
	cell   cell
}

type storeKey struct {
	nodeID string
	key    string
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
// receiver is not modified. Most writes add a constant-size overlay; after a
// bounded number of writes the overlays are compacted into a new snapshot.
// Value is not cloned and must not be mutated after insertion.
func (s Store) With(nodeID, key string, value any) Store {
	next := cell{value: value, revision: revisionCounter.Add(1)}
	identity := storeKey{nodeID: nodeID, key: key}
	if s.depth < storeOverlayLimit {
		return s.withDelta(identity, next)
	}

	data := s.materialize()
	data[identity] = next
	return Store{snapshot: &storeSnapshot{data: data}}
}

func (s Store) withDelta(key storeKey, value cell) Store {
	return Store{
		snapshot: s.snapshot,
		delta:    &storeDelta{parent: s.delta, key: key, cell: value},
		depth:    s.depth + 1,
	}
}

func (s Store) compact() Store {
	if s.delta == nil {
		return s
	}
	return Store{snapshot: &storeSnapshot{data: s.materialize()}}
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
	key, rest, _ := strings.Cut(ref.Path, ".")
	c, ok := s.lookupCell(ref.NodeID, key)
	if !ok {
		return nil, false
	}
	return walk(c.value, rest)
}

// MarshalJSON serializes the Store as nodeID -> key -> value. It reports the
// cell containing a value that encoding/json cannot encode.
func (s Store) MarshalJSON() ([]byte, error) {
	raw := make(map[string]map[string]any)
	put := func(identity storeKey, c cell) {
		inner := raw[identity.nodeID]
		if inner == nil {
			inner = make(map[string]any)
			raw[identity.nodeID] = inner
		}
		inner[identity.key] = c.value
	}
	if s.snapshot != nil {
		for identity, c := range s.snapshot.data {
			put(identity, c)
		}
	}
	for _, delta := range s.deltasOldestFirst() {
		put(delta.key, delta.cell)
	}

	encoded, err := json.Marshal(raw)
	if err == nil {
		return encoded, nil
	}

	// Keep the successful path to one encoding pass. On failure, isolate the
	// offending cell so callers retain the more useful Store path in the error.
	for nodeID, inner := range raw {
		for key, value := range inner {
			if _, cellErr := json.Marshal(value); cellErr != nil {
				return nil, fmt.Errorf("workflow: marshal store %s.%s: %w", nodeID, key, cellErr)
			}
		}
	}
	return nil, fmt.Errorf("workflow: marshal store: %w", err)
}

func (s Store) lookupCell(nodeID, key string) (cell, bool) {
	identity := storeKey{nodeID: nodeID, key: key}
	for delta := s.delta; delta != nil; delta = delta.parent {
		if delta.key == identity {
			return delta.cell, true
		}
	}
	if s.snapshot == nil {
		return cell{}, false
	}
	c, ok := s.snapshot.data[identity]
	return c, ok
}

// materialize returns a mutable copy of the Store's complete flat cell map.
func (s Store) materialize() map[storeKey]cell {
	capacity := 0
	if s.snapshot != nil {
		capacity = len(s.snapshot.data)
	}
	data := make(map[storeKey]cell, capacity+s.depth)
	if s.snapshot != nil {
		for key, value := range s.snapshot.data {
			data[key] = value
		}
	}

	for _, write := range s.deltasOldestFirst() {
		data[write.key] = write.cell
	}
	return data
}

func (s Store) deltasOldestFirst() []*storeDelta {
	writes := make([]*storeDelta, 0, s.depth)
	for delta := s.delta; delta != nil; delta = delta.parent {
		writes = append(writes, delta)
	}
	for left, right := 0, len(writes)-1; left < right; left, right = left+1, right-1 {
		writes[left], writes[right] = writes[right], writes[left]
	}
	return writes
}

// UnmarshalJSON atomically replaces the Store from nodeID -> key -> value JSON.
// On failure the receiver is unchanged. Values follow encoding/json's standard
// representation, including float64 for JSON numbers.
func (s *Store) UnmarshalJSON(data []byte) error {
	var raw map[string]map[string]json.RawMessage
	if err := decodeStrict(data, &raw); err != nil {
		return fmt.Errorf("workflow: unmarshal store: %w", err)
	}

	size := 0
	for _, inner := range raw {
		size += len(inner)
	}
	nextData := make(map[storeKey]cell, size)
	for nodeID, inner := range raw {
		for key, encoded := range inner {
			var value any
			if err := json.Unmarshal(encoded, &value); err != nil {
				return fmt.Errorf("workflow: unmarshal store %s.%s: %w", nodeID, key, err)
			}
			nextData[storeKey{nodeID: nodeID, key: key}] = cell{
				value:    value,
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
