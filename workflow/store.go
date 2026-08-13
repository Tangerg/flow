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
// snapshot, without comparing arbitrary values. Its first value is 1, so
// revision and lineage 0 belong to the zero cell alone: a reader may take the
// cell a Store lacks as that zero value instead of testing for presence.
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

// removedBy reports whether next, a removal marker, removes this cell: a live
// cell of the same lineage. A cell that merely shares a node ID and key carries
// a different lineage, and the zero cell carries none.
func (c cell) removedBy(next cell) bool {
	return !c.removed && c.lineage == next.lineage
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
		if nodeIDs.has(key.nodeID) && !current.removed {
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
	// A scanned pointer always has a first segment, so only its escaping can be
	// wrong here.
	key, _, valid := pointer.next()
	if !valid {
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
//
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
	if changes, ok := s.deltaSince(base); ok {
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
		current := base[identity]
		if next.revision == current.revision {
			continue
		}
		// A removal says something only about a cell base still holds. One base
		// never held reads as the zero cell, which no lineage can match.
		if next.removed && !current.removedBy(next) {
			continue
		}
		changed = append(changed, storeChange{key: identity, cell: next})
	}
	slices.SortFunc(changed, byRevision)
	return changed
}

// byRevision orders changes as they were written. Every mutation takes a
// globally unique revision, so the order needs no tie-breaker.
func byRevision(left, right storeChange) int {
	return cmp.Compare(left.cell.revision, right.cell.revision)
}

// deltaSince returns the final change to each cell changed in s after base. It
// succeeds when both Stores share a snapshot and s's overlay descends from
// base's overlay, which is what a Store handed to a deriver and returned with
// its own writes on top looks like.
func (s Store) deltaSince(base Store) ([]storeChange, bool) {
	if s.snapshot != base.snapshot {
		return nil, false
	}

	var writes []storeChange
	for delta := s.delta; delta != base.delta; delta = delta.parent {
		if delta == nil {
			return nil, false
		}
		if slices.ContainsFunc(writes, func(write storeChange) bool {
			return write.key == delta.key
		}) {
			continue
		}
		writes = append(writes, storeChange{key: delta.key, cell: delta.cell})
	}
	slices.SortFunc(writes, byRevision)
	return writes, true
}

// merge returns a Store containing base plus each supplied Store's changes. On
// a same-cell conflict a later Store wins.
func (s Store) merge(others ...Store) Store {
	merger := storeMerger{base: s, result: s}
	for _, other := range others {
		merger.add(other)
	}
	return merger.result
}

// withChanges applies identity-preserving changes in order. Every batch of
// changes lands here, whether merge isolated it from a branch or a graph node
// replayed it from a recorded dependency.
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
	changes, ok := other.deltaSince(s.base)
	if !ok {
		// A branch may return a Store unrelated to its input or compact a long
		// overlay. Fall back to revision comparison in that uncommon case, over one
		// materialized base shared by every branch that needs it.
		if s.baseData == nil {
			s.baseData = s.base.materialize()
		}
		changes = other.changedCells(s.baseData)
	}
	s.result = s.result.withChanges(changes)
}

func (s Store) lookupCell(nodeID, key string) (cell, bool) {
	identity := storeKey{nodeID: nodeID, key: key}
	c, ok := s.lookupRecord(identity)
	if !ok || c.removed {
		return cell{}, false
	}
	return c, true
}

// base returns the cell records under the overlay. A Store that has never been
// flattened carries no snapshot; that is the same Store as one carrying an empty
// snapshot, so this reports the empty map for both and readers below index,
// range, and copy it without asking which they hold.
func (s Store) baseCells() storeCells {
	if s.snapshot == nil {
		return nil
	}
	return s.snapshot.data
}

func (s Store) lookupRecord(identity storeKey) (cell, bool) {
	for delta := s.delta; delta != nil; delta = delta.parent {
		if delta.key == identity {
			return delta.cell, true
		}
	}
	c, ok := s.baseCells()[identity]
	return c, ok
}

// cells iterates the Store's complete cell records, newest overlay write first
// and shadowing the snapshot, including the private removal markers that
// [Store.materialize] also reports. Use it to read the whole Store without
// copying it: the only set it allocates is bounded by the overlay length rather
// than by the number of cells.
func (s Store) cells() iter.Seq2[storeKey, cell] {
	return func(yield func(storeKey, cell) bool) {
		if s.delta == nil {
			// Nothing shadows the snapshot, so no tracking set is needed.
			for key, record := range s.baseCells() {
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
		for key, record := range s.baseCells() {
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
	data := make(storeCells, len(s.baseCells())+s.depth)
	maps.Copy(data, s.baseCells())

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
