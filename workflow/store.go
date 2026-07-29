package workflow

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
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
// race. Lookup walks through a value's JSON representation, so maps, slices,
// arrays, structs, pointers, and custom JSON marshalers have the same path
// semantics before and after Store serialization. Ref paths are JSON Pointers,
// so every possible object key is representable without ambiguity.
//
// A Store holds arbitrary Go values, but a Store that has been serialized holds
// JSON-domain values: [json.Number], string, bool, nil, []any, and
// map[string]any. Reading with [Get] hides that difference — it converts to the
// type asked for — so a typed step works the same on a fresh Store and on one
// restored from JSON. Reading with Lookup does not: it returns whatever is
// stored, which after a round trip is the JSON-domain value.
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

type storeWrite struct {
	key  storeKey
	cell cell
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
// receiver is not modified. Most writes add a constant-size overlay, which is
// periodically compacted. Value is not cloned and must not be mutated after
// insertion.
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
	return s.With(nodeID, outputKey, value)
}

// Lookup returns the value at ref. The path's first segment is the key under the
// node; remaining segments walk through the value's JSON representation. The
// bool reports whether the reference resolved. A whole-cell lookup returns the
// stored Go value as-is; a nested lookup into a typed Go value returns the
// corresponding JSON-domain value. Returned mutable values are borrowed views
// and must not be mutated.
func (s Store) Lookup(ref Ref) (any, bool) {
	var conventionalKey string
	switch ref.Path {
	case outputPath:
		conventionalKey = outputKey
	case itemPath:
		conventionalKey = itemKey
	case indexPath:
		conventionalKey = indexKey
	}
	if conventionalKey != "" {
		c, ok := s.lookupCell(ref.NodeID, conventionalKey)
		if !ok {
			return nil, false
		}
		return c.value, true
	}

	pointer, ok := encodedPointer(ref.Path).scan()
	if !ok {
		return nil, false
	}
	key, present, valid := pointer.next()
	if !present || !valid {
		return nil, false
	}
	c, ok := s.lookupCell(ref.NodeID, key)
	if !ok {
		return nil, false
	}
	return pointer.lookup(c.value)
}

// Write records one Store write: the cell it landed in and the value it wrote.
type Write struct {
	NodeID string
	Key    string
	Value  any
}

// Ref returns a reference to the written cell.
func (w Write) Ref() Ref { return At(w.NodeID, w.Key) }

// Changes returns the writes that distinguish s from base, oldest first, keeping
// only the final write to each cell. It is the delta an audit log or an external
// persister records instead of a whole snapshot; pair it with the [Store] on an
// [EventCompleted] event.
//
// Cells that base holds and s does not are not reported: a Store has no delete.
// Values are borrowed views and must not be mutated.
func (s Store) Changes(base Store) []Write {
	if writes, ok := s.deltaSince(base); ok {
		changes := make([]Write, 0, len(writes))
		for _, write := range writes {
			changes = append(changes, Write{NodeID: write.key.nodeID, Key: write.key.key, Value: write.cell.value})
		}
		return changes
	}

	// s may be unrelated to base or may have compacted a long overlay. Compare
	// write identities, then restore write order from the revision counter.
	changed := s.changedWrites(base.materialize())
	changes := make([]Write, 0, len(changed))
	for _, write := range changed {
		changes = append(changes, Write{
			NodeID: write.key.nodeID,
			Key:    write.key.key,
			Value:  write.cell.value,
		})
	}
	return changes
}

// changedWrites returns the receiver's cells that do not share the
// corresponding identity in base. Revisions preserve write order; the key is a
// deterministic tie-breaker for data restored from an external snapshot.
func (s Store) changedWrites(base map[storeKey]cell) []storeWrite {
	candidate := s.materialize()
	changed := make([]storeWrite, 0, len(candidate))
	for identity, next := range candidate {
		if current, ok := base[identity]; ok && next.revision == current.revision {
			continue
		}
		changed = append(changed, storeWrite{key: identity, cell: next})
	}
	slices.SortFunc(changed, func(a, b storeWrite) int {
		if order := cmp.Compare(a.cell.revision, b.cell.revision); order != 0 {
			return order
		}
		if order := cmp.Compare(a.key.nodeID, b.key.nodeID); order != 0 {
			return order
		}
		return cmp.Compare(a.key.key, b.key.key)
	})
	return changed
}

// deltaSince returns the final write to each cell changed in s after base. It
// succeeds when both Stores share a snapshot and s's overlay descends from
// base's overlay.
func (s Store) deltaSince(base Store) ([]*storeDelta, bool) {
	if s.snapshot != base.snapshot {
		return nil, false
	}

	var writes []*storeDelta
	for delta := s.delta; delta != base.delta; delta = delta.parent {
		if delta == nil {
			return nil, false
		}
		seen := false
		for _, write := range writes {
			if write.key == delta.key {
				seen = true
				break
			}
		}
		if !seen {
			writes = append(writes, delta)
		}
	}
	slices.Reverse(writes)
	return writes, true
}

// merge returns a Store containing base plus each supplied Store's writes. On a
// same-cell conflict a later Store wins.
func (base Store) merge(others ...Store) Store {
	out := base
	var baseData map[storeKey]cell
	for _, other := range others {
		if other.snapshot == base.snapshot && other.delta != nil && other.delta.parent == base.delta {
			if out.snapshot == base.snapshot && out.delta == base.delta {
				out = other
			} else {
				out = out.withDelta(other.delta.key, other.delta.cell)
			}
			continue
		}
		if writes, ok := other.deltaSince(base); ok {
			for _, write := range writes {
				out = out.withDelta(write.key, write.cell)
			}
			continue
		}

		// A branch may return a Store unrelated to its input or compact a long
		// overlay. Fall back to revision comparison in that uncommon case.
		if baseData == nil {
			baseData = base.materialize()
		}
		for _, write := range other.changedWrites(baseData) {
			out = out.withDelta(write.key, write.cell)
		}
	}
	if out.depth > storeOverlayLimit*2 {
		return out.compact()
	}
	return out
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
	nodeIDs := make([]string, 0, len(raw))
	for nodeID := range raw {
		nodeIDs = append(nodeIDs, nodeID)
	}
	slices.Sort(nodeIDs)
	for _, nodeID := range nodeIDs {
		inner := raw[nodeID]
		keys := make([]string, 0, len(inner))
		for key := range inner {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			value := inner[key]
			if _, cellErr := json.Marshal(value); cellErr != nil {
				return nil, fmt.Errorf(
					"workflow: marshal store node %q key %q: %w",
					nodeID, key, cellErr,
				)
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
// The top level and each node must be objects; null and duplicate object members
// are rejected. On failure the receiver is unchanged.
//
// Numbers decode as [json.Number] rather than float64, so a decoded Store loses
// no precision and an int64 beyond float64's exact range survives the round
// trip. Read decoded values with [Get], which converts them to the type a caller
// asks for; a bare [Store.Lookup] returns the JSON-domain value.
func (s *Store) UnmarshalJSON(data []byte) error {
	document, err := jsonDocument(data).value()
	if err != nil {
		return fmt.Errorf("workflow: unmarshal store: %w", err)
	}
	raw, ok := document.(map[string]any)
	if !ok {
		return fmt.Errorf("workflow: unmarshal store: expected object, got %s", (jsonValue{raw: document}).kind())
	}

	nodeIDs := make([]string, 0, len(raw))
	size := 0
	for nodeID, value := range raw {
		inner, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf(
				"workflow: unmarshal store node %q: expected object, got %s",
				nodeID, (jsonValue{raw: value}).kind(),
			)
		}
		nodeIDs = append(nodeIDs, nodeID)
		size += len(inner)
	}
	slices.Sort(nodeIDs)
	nextData := make(map[storeKey]cell, size)
	for _, nodeID := range nodeIDs {
		inner := raw[nodeID].(map[string]any)
		keys := make([]string, 0, len(inner))
		for key := range inner {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			nextData[storeKey{nodeID: nodeID, key: key}] = cell{
				value:    inner[key],
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

func (v jsonValue) kind() string {
	switch v.raw.(type) {
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
		return fmt.Sprintf("%T", v.raw)
	}
}

// lookup descends through the receiver without first materializing its segments.
// JSON-domain maps and arrays remain allocation-free. A typed Go value is
// converted through JSON at most once, after which the rest of the walk stays
// in the JSON domain.
func (pointer *pointerScanner) lookup(value any) (any, bool) {
	jsonDomain := false
	for {
		key, present, valid := pointer.next()
		if !valid {
			return nil, false
		}
		if !present {
			return value, true
		}

		switch current := value.(type) {
		case map[string]any:
			next, ok := current[key]
			if !ok {
				return nil, false
			}
			value = next
		case []any:
			index, ok := pointerToken(key).index(len(current))
			if !ok {
				return nil, false
			}
			value = current[index]
		default:
			if jsonDomain {
				return nil, false
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				return nil, false
			}
			value, err = jsonDocument(encoded).value()
			if err != nil {
				return nil, false
			}
			jsonDomain = true

			// Reprocess this segment against the converted value.
			switch current := value.(type) {
			case map[string]any:
				next, ok := current[key]
				if !ok {
					return nil, false
				}
				value = next
			case []any:
				index, ok := pointerToken(key).index(len(current))
				if !ok {
					return nil, false
				}
				value = current[index]
			default:
				return nil, false
			}
		}
	}
}

type pointerToken string

// index implements RFC 6901's array-index grammar. strconv.Atoi alone
// would incorrectly accept tokens such as "+1" and "01", which are object keys
// but not canonical array indexes.
func (token pointerToken) index(length int) (int, bool) {
	if token == "" || (len(token) > 1 && token[0] == '0') {
		return 0, false
	}
	for i := range len(token) {
		if token[i] < '0' || token[i] > '9' {
			return 0, false
		}
	}
	index, err := strconv.Atoi(string(token))
	return index, err == nil && index < length
}
