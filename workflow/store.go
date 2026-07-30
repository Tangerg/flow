package workflow

import (
	"cmp"
	"maps"
	"slices"
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
// JSON-domain values: json.Number, string, bool, nil, []any, and
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

// compact flattens a Store that carries an overlay. Callers reach it only for a
// Store whose depth is above a threshold, which implies a non-nil delta; calling
// it on a delta-free Store would copy the snapshot to no effect.
func (s Store) compact() Store {
	return Store{snapshot: &storeSnapshot{data: s.materialize()}}
}

// withoutNodes returns a snapshot without cells owned by nodeIDs. A compiled
// Graph uses it at its execution boundary so stale outputs from an earlier run
// cannot satisfy current dependencies or conditional merges. The Journal then
// restores exactly the internal values that belong to the current definition.
func (s Store) withoutNodes(nodeIDs map[string]struct{}) Store {
	if len(nodeIDs) == 0 || !s.hasNode(nodeIDs) {
		return s
	}
	data := s.materialize()
	for key := range data {
		if _, owned := nodeIDs[key.nodeID]; owned {
			delete(data, key)
		}
	}
	if len(data) == 0 {
		return Store{}
	}
	return Store{snapshot: &storeSnapshot{data: data}}
}

func (s Store) hasNode(nodeIDs map[string]struct{}) bool {
	for delta := s.delta; delta != nil; delta = delta.parent {
		if _, found := nodeIDs[delta.key.nodeID]; found {
			return true
		}
	}
	if s.snapshot == nil {
		return false
	}
	for key := range s.snapshot.data {
		if _, found := nodeIDs[key.nodeID]; found {
			return true
		}
	}
	return false
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
// corresponding identity in base. Every write takes a globally unique revision,
// so ordering by revision reproduces write order and needs no tie-breaker. A
// Store restored from an external snapshot has no original write order to
// recover; [Store.UnmarshalJSON] assigns its revisions by sorted cell, which
// makes the resulting order deterministic rather than chronological.
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
		return cmp.Compare(a.cell.revision, b.cell.revision)
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
func (s Store) merge(others ...Store) Store {
	merger := storeMerger{base: s, result: s}
	for _, other := range others {
		merger.add(other)
	}
	if merger.result.depth > storeOverlayLimit*2 {
		return merger.result.compact()
	}
	return merger.result
}

// storeMerger owns the lazy fallback state needed while combining branches.
// The common descendant-overlay path never materializes the base Store.
type storeMerger struct {
	base     Store
	result   Store
	baseData map[storeKey]cell
}

func (s *storeMerger) add(other Store) {
	if s.addDirectChild(other) {
		return
	}
	if writes, ok := other.deltaSince(s.base); ok {
		s.addWrites(writes)
		return
	}

	// A branch may return a Store unrelated to its input or compact a long
	// overlay. Fall back to revision comparison in that uncommon case.
	if s.baseData == nil {
		s.baseData = s.base.materialize()
	}
	for _, write := range other.changedWrites(s.baseData) {
		s.result = s.result.withDelta(write.key, write.cell)
	}
}

func (s *storeMerger) addDirectChild(other Store) bool {
	delta := other.delta
	if other.snapshot != s.base.snapshot ||
		delta == nil ||
		delta.parent != s.base.delta {
		return false
	}
	if s.result.snapshot == s.base.snapshot &&
		s.result.delta == s.base.delta {
		s.result = other
	} else {
		s.result = s.result.withDelta(delta.key, delta.cell)
	}
	return true
}

func (s *storeMerger) addWrites(writes []*storeDelta) {
	for _, write := range writes {
		s.result = s.result.withDelta(write.key, write.cell)
	}
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
		maps.Copy(data, s.snapshot.data)
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
