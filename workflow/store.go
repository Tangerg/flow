package workflow

import (
	"maps"
	"strconv"
	"strings"
)

// Store is an immutable variable pool: a two-level map of nodeID -> key -> value.
// Every write returns a new Store that shares the untouched data with the
// original, so a Store is safe to read from many goroutines at once and every
// intermediate value is a cheap snapshot.
//
// Values are held as-is (any). Get walks into map[string]any and []any, so
// JSON-shaped data can be addressed by path such as "result.items.0".
//
// The zero Store is empty and ready to use; prefer [NewStore] for clarity.
type Store struct {
	data map[string]map[string]any
}

// NewStore returns an empty Store.
func NewStore() Store {
	return Store{}
}

// With returns a copy of the Store with value written at (nodeID, key). The
// receiver is not modified; untouched node maps are shared with the copy.
func (s Store) With(nodeID, key string, value any) Store {
	outer := maps.Clone(s.data)
	if outer == nil {
		outer = make(map[string]map[string]any, 1)
	}
	inner := maps.Clone(outer[nodeID])
	if inner == nil {
		inner = make(map[string]any, 1)
	}
	inner[key] = value
	outer[nodeID] = inner
	return Store{data: outer}
}

// Get looks up a value by node ID and path. The path's first segment is the key
// under the node; any remaining segments walk into nested map[string]any and
// []any values (for example "result.items.0"). The bool reports whether the path
// resolved.
func (s Store) Get(nodeID, path string) (any, bool) {
	inner, ok := s.data[nodeID]
	if !ok {
		return nil, false
	}
	key, rest, _ := strings.Cut(path, ".")
	v, ok := inner[key]
	if !ok {
		return nil, false
	}
	return walk(v, rest)
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
