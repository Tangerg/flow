package workflow

import (
	"cmp"
	"iter"
	"maps"
	"slices"
	"sync/atomic"
)

// Store is an immutable-snapshot variable pool: a two-level map of nodeID ->
// key -> value. Every write returns a new Store that shares its base snapshot
// with the original and records the change in a bounded overlay. The Store is
// immutable; overlays are periodically compacted to keep lookups bounded.
//
// Values are held and returned as-is (any). Callers must treat mutable values
// such as maps, slices, and pointers as immutable after insertion; mutating one
// would mutate every Store snapshot that shares it and may introduce a data
// race. When a Ref descends below a cell, Lookup walks through the value's JSON
// representation, so maps, slices, arrays, structs, pointers, and custom JSON
// marshalers have the same path semantics before and after Store serialization.
// A whole-cell lookup still returns the stored Go value as-is. Ref paths are
// JSON Pointers, so every possible object key is representable without
// ambiguity.
//
// A Store can hold arbitrary Go values in memory, but only values with a
// faithful JSON representation can be persisted and restored. A deserialized
// Store holds JSON-domain values: json.Number, string, bool, nil, []any, and
// map[string]any. [Get] converts those values to the requested type, so a typed
// step sees the same value before and after persistence when that type has a
// faithful JSON round trip. A type with only a custom json.Marshaler, for
// example, must also define the corresponding decoding contract. [Store.Lookup]
// does not convert: it returns whatever is stored, which after a round trip is
// the JSON-domain value.
//
// The zero Store is empty and ready to use; prefer [NewStore] for clarity.
// Store does not distinguish seed values from workflow outputs. Start a new
// logical run from a freshly assembled seed Store unless retaining prior cells
// is intentional. A compiled [Graph] is the exception with explicit ownership:
// it removes cells named by its GraphNode IDs before rebuilding them.
//
// Every method takes a value receiver so a Store cannot be mutated through a
// copy. UnmarshalJSON is the one exception: json.Unmarshaler requires a pointer,
// and replacing a Store wholesale is the only way to decode one.
//
//nolint:recvcheck // UnmarshalJSON must be a pointer method to satisfy json.Unmarshaler.
type Store struct {
	snapshot *storeSnapshot
	delta    *storeDelta
	depth    int
}

// storeOverlayLimit is the longest overlay a Store hands back to a caller. A
// lookup scans the overlay before consulting the snapshot, so the constant
// trades that bounded scan against copying the snapshot map: a write is cheap
// and a flattening is proportional to the whole Store, which is worth paying
// once per limit writes rather than on every write. Every path that extends an
// overlay ends at [Store.bounded], so the bound is a Store invariant rather
// than a rule each path restates.
const storeOverlayLimit = 64

// storeSnapshot gives a flattened cell map a comparable identity. Two Stores
// that share one are known to agree on everything outside their overlays, which
// is what lets change detection compare overlays instead of values.
type storeSnapshot struct {
	data storeCells
}

// storeDelta is one write in the linked overlay that a Store carries over its
// snapshot, newest first.
type storeDelta struct {
	parent *storeDelta
	key    storeKey
	cell   cell
}

// storeKey identifies a cell: the node that owns it and the key under that node.
type storeKey struct {
	nodeID string
	key    string
}

type storeCells map[storeKey]cell

// revisionCounter gives each internal mutation an identity. Parallel uses it to
// distinguish a branch's changes from cells merely inherited from its input
// snapshot, without comparing arbitrary values.
var revisionCounter atomic.Uint64

type cell struct {
	value    any
	revision uint64
	// lineage identifies the first version of this cell in one Store ancestry.
	// It lets a compacted removal target its own input without affecting an
	// unrelated Store that happens to use the same node ID and key.
	lineage uint64
	removed bool
}

// storeChange is one identity-preserving internal mutation. A removed cell is
// an engine-owned namespace change, not a public Store operation; carrying it
// through composition lets a Graph's cleanup survive Parallel merging.
type storeChange struct {
	key  storeKey
	cell cell
}

// NewStore returns an empty Store.
func NewStore() Store {
	return Store{}
}

// WithCell returns a copy of the Store with value written at (nodeID, key). The
// receiver is not modified. Most writes add a constant-size overlay, which is
// periodically compacted. Value is not cloned and must not be mutated after
// insertion.
func (s Store) WithCell(nodeID, key string, value any) Store {
	identity := storeKey{nodeID: nodeID, key: key}
	revision := revisionCounter.Add(1)
	lineage := revision
	if current, ok := s.lookupRecord(identity); ok {
		lineage = current.lineage
	}
	next := cell{value: value, revision: revision, lineage: lineage}
	return s.withDelta(identity, next).bounded()
}

func (s Store) withDelta(key storeKey, value cell) Store {
	return Store{
		snapshot: s.snapshot,
		delta:    &storeDelta{parent: s.delta, key: key, cell: value},
		depth:    s.depth + 1,
	}
}

// bounded enforces [storeOverlayLimit]. It is the single exit of every path that
// extends an overlay, whether by one write or by a batch of merged changes, so no
// Store leaves this package's internals carrying a longer one.
func (s Store) bounded() Store {
	if s.depth <= storeOverlayLimit {
		return s
	}
	return s.compact()
}

// sharedBase returns the Store to hand to concurrent derivers: parallel
// branches, iteration elements, or graph nodes. A Store whose overlay already
// reaches [storeOverlayLimit] would make every deriver's first write flatten it
// separately, so a fan-out of n pays n snapshot copies instead of one, and each
// deriver ends up owning an unrelated snapshot — which also drops a later merge
// onto its whole-Store fallback. Flattening once here leaves every deriver
// sharing one snapshot with a full overlay budget to extend. A shorter overlay
// is left alone: copying the snapshot would cost more than the scans it saves.
//
// This bounds what the shared input forces on all derivers at once. A deriver
// that goes on to make enough writes of its own still crosses the limit, which
// is the ordinary amortized cost of writing.
func (s Store) sharedBase() Store {
	if s.depth < storeOverlayLimit {
		return s
	}
	return s.compact()
}

// compact flattens a Store that carries an overlay. Callers reach it through
// [Store.bounded] or [Store.sharedBase] for a Store at or past the limit, which
// implies a non-nil delta; calling it on a delta-free Store would copy the
// snapshot to no effect.
func (s Store) compact() Store {
	return Store{snapshot: &storeSnapshot{data: s.materialize()}}
}

// withoutNodes returns a Store without cells owned by nodeIDs. A compiled Graph
// uses it at its execution boundary so stale outputs from an earlier run cannot
// satisfy current dependencies or conditional merges. Removals retain their
// private change identity, allowing an enclosing Parallel to preserve the
// Graph's namespace ownership when it merges branches. Execution then rebuilds
// those cells, replaying whatever boundaries the Journal recorded.
func (s Store) withoutNodes(nodeIDs nodeSet) Store {
	if len(nodeIDs) == 0 {
		return s
	}
	// A node conventionally owns its Output cell, so the node count is the natural
	// estimate — closer than the whole Store, which a graph rarely owns entirely.
	owned := make([]storeChange, 0, len(nodeIDs))
	for key, current := range s.cells() {
		if _, claimed := nodeIDs[key.nodeID]; claimed && !current.removed {
			owned = append(owned, storeChange{key: key, cell: current})
		}
	}
	if len(owned) == 0 {
		return s
	}
	slices.SortFunc(owned, func(left, right storeChange) int {
		return left.key.compare(right.key)
	})
	for _, current := range owned {
		s = s.withDelta(current.key, cell{
			revision: revisionCounter.Add(1),
			lineage:  current.cell.lineage,
			removed:  true,
		})
	}
	return s.bounded()
}

// WithOutput returns a copy of the Store with value written to the conventional
// output key for nodeID.
func (s Store) WithOutput(nodeID string, value any) Store {
	return s.WithCell(nodeID, outputKey, value)
}

// Lookup returns the value at ref. The path's first segment is the key under the
// node; remaining segments walk through the value's JSON representation. The
// bool reports whether the reference resolved. A whole-cell lookup returns the
// stored Go value as-is; a nested lookup into a typed Go value returns the
// corresponding JSON-domain value. If that conversion fails, Lookup reports an
// unresolved reference; use [Get] when the error matters. Returned mutable
// values are borrowed views and must not be mutated.
func (s Store) Lookup(ref Ref) (any, bool) {
	value, found, _ := s.resolve(ref)
	return value, found
}

// resolve is the error-preserving form of Lookup used by Get. On a nested-value
// conversion error it returns the containing cell as value, so Get can report
// its concrete type without traversing or encoding application data twice.
func (s Store) resolve(ref Ref) (value any, found bool, err error) {
	pointer, ok := encodedPointer(ref.Path).scan()
	if !ok {
		return nil, false, nil
	}
	key, present, valid := pointer.next()
	if !present || !valid {
		return nil, false, nil
	}
	c, ok := s.lookupCell(ref.NodeID, key)
	if !ok {
		return nil, false, nil
	}
	value, found, err = pointer.lookup(c.value)
	if err != nil {
		return c.value, false, err
	}
	return value, found, nil
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
// only the final write to each cell. It is the write set an audit log records;
// pair it with the [Store] on an [EventCompleted] event.
//
// Cells that base holds and s does not are not reported: Changes describes
// writes, not engine-owned namespace cleanup. It is therefore not a general
// snapshot-synchronization protocol; persist s itself when the exact resulting
// state matters.
// Change identity comes from Store lineage rather than value equality. If s is
// unrelated to base — including a separately decoded snapshot — every cell in s
// has a distinct write identity and is reported. Values are borrowed views and
// must not be mutated.
func (s Store) Changes(base Store) []Write {
	changed := s.changesSince(base)
	changes := make([]Write, 0, len(changed))
	for _, change := range changed {
		if change.cell.removed {
			continue
		}
		changes = append(changes, Write{
			NodeID: change.key.nodeID,
			Key:    change.key.key,
			Value:  change.cell.value,
		})
	}
	return changes
}

// changesSince is the identity-preserving form used by Store composition. It
// includes private namespace removals that the public Changes method omits. It
// takes the overlay fast path when possible and falls back to revision
// comparison for an unrelated or compacted Store.
func (s Store) changesSince(base Store) []storeChange {
	if deltas, ok := s.deltaSince(base); ok {
		changes := make([]storeChange, 0, len(deltas))
		for _, delta := range deltas {
			changes = append(changes, storeChange{key: delta.key, cell: delta.cell})
		}
		return changes
	}
	return s.changedCells(base.materialize())
}

// changedCells returns the receiver's cell records that do not share the
// corresponding identity in base. Every mutation takes a globally unique
// revision, so ordering by revision reproduces change order and needs no
// tie-breaker. A Store restored from an external snapshot has no original write
// order to recover; [Store.UnmarshalJSON] assigns its revisions by sorted cell,
// which makes the resulting order deterministic rather than chronological.
// Reading the receiver needs no copy, and the result carries no useful capacity
// hint: a related Store contributes a handful of changes while an unrelated one
// contributes every cell, so sizing for the latter would over-allocate the case
// that actually happens.
func (s Store) changedCells(base storeCells) []storeChange {
	var changed []storeChange
	for identity, next := range s.cells() {
		if current, ok := base[identity]; ok && next.revision == current.revision {
			continue
		}
		if next.removed {
			current, ok := base[identity]
			if !ok || current.removed || current.lineage != next.lineage {
				continue
			}
		}
		changed = append(changed, storeChange{key: identity, cell: next})
	}
	slices.SortFunc(changed, func(a, b storeChange) int {
		return cmp.Compare(a.cell.revision, b.cell.revision)
	})
	return changed
}

// deltaSince returns the final change to each cell changed in s after base. It
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
	slices.SortFunc(writes, func(left, right *storeDelta) int {
		return cmp.Compare(left.cell.revision, right.cell.revision)
	})
	return writes, true
}

// merge returns a Store containing base plus each supplied Store's changes. On
// a same-cell conflict a later Store wins.
func (s Store) merge(others ...Store) Store {
	merger := storeMerger{base: s, result: s}
	for _, other := range others {
		merger.add(other)
	}
	return merger.result.bounded()
}

// withChanges applies identity-preserving changes in order. It is the low-level
// counterpart of merge for a caller that already isolated each branch's delta.
func (s Store) withChanges(changes []storeChange) Store {
	for _, change := range changes {
		s = s.withDelta(change.key, change.cell)
	}
	return s.bounded()
}

// storeMerger owns the lazy fallback state needed while combining branches.
// The common descendant-overlay path never materializes the base Store.
type storeMerger struct {
	base     Store
	result   Store
	baseData storeCells
}

func (s *storeMerger) add(other Store) {
	if s.addDirectChild(other) {
		return
	}
	if changes, ok := other.deltaSince(s.base); ok {
		s.addChanges(changes)
		return
	}

	// A branch may return a Store unrelated to its input or compact a long
	// overlay. Fall back to revision comparison in that uncommon case.
	if s.baseData == nil {
		s.baseData = s.base.materialize()
	}
	for _, change := range other.changedCells(s.baseData) {
		s.result = s.result.withDelta(change.key, change.cell)
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

func (s *storeMerger) addChanges(changes []*storeDelta) {
	for _, change := range changes {
		s.result = s.result.withDelta(change.key, change.cell)
	}
}

func (s Store) lookupCell(nodeID, key string) (cell, bool) {
	identity := storeKey{nodeID: nodeID, key: key}
	c, ok := s.lookupRecord(identity)
	if !ok || c.removed {
		return cell{}, false
	}
	return c, true
}

func (s Store) lookupRecord(identity storeKey) (cell, bool) {
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

// cells iterates the Store's complete cell records, newest overlay write first
// and shadowing the snapshot, including the private removal markers that
// [Store.materialize] also reports. Use it to read the whole Store without
// copying it: the only set it allocates is bounded by the overlay length rather
// than by the number of cells.
func (s Store) cells() iter.Seq2[storeKey, cell] {
	return func(yield func(storeKey, cell) bool) {
		if s.snapshot == nil && s.delta == nil {
			return
		}
		if s.delta == nil {
			// Nothing shadows the snapshot, so no tracking set is needed.
			for key, record := range s.snapshot.data {
				if !yield(key, record) {
					return
				}
			}
			return
		}

		shadowed := make(map[storeKey]struct{}, s.depth)
		for delta := s.delta; delta != nil; delta = delta.parent {
			if _, seen := shadowed[delta.key]; seen {
				continue
			}
			shadowed[delta.key] = struct{}{}
			if !yield(delta.key, delta.cell) {
				return
			}
		}
		if s.snapshot == nil {
			return
		}
		for key, record := range s.snapshot.data {
			if _, seen := shadowed[key]; seen {
				continue
			}
			if !yield(key, record) {
				return
			}
		}
	}
}

// materialize returns a mutable copy of the Store's complete flat cell-record
// map, including private removal markers needed by enclosing composites.
func (s Store) materialize() storeCells {
	capacity := 0
	if s.snapshot != nil {
		capacity = len(s.snapshot.data)
	}
	data := make(storeCells, capacity+s.depth)
	if s.snapshot != nil {
		maps.Copy(data, s.snapshot.data)
	}

	s.delta.applyOverlay(func(key storeKey, record cell) {
		data[key] = record
	})
	return data
}

func (s storeKey) compare(other storeKey) int {
	if order := cmp.Compare(s.nodeID, other.nodeID); order != 0 {
		return order
	}
	return cmp.Compare(s.key, other.key)
}

// applyOverlay replays the overlay in write order, so the newest write to a cell
// lands last and wins. The chain runs newest first, so reaching the oldest write
// means recursing before writing; [storeOverlayLimit] bounds the chain and the
// recursion with it. A nil receiver is the empty overlay.
func (d *storeDelta) applyOverlay(write func(storeKey, cell)) {
	if d == nil {
		return
	}
	d.parent.applyOverlay(write)
	write(d.key, d.cell)
}
