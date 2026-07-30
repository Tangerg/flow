package workflow

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// Journal records what a run already did, so a later run can pick up where it
// stopped instead of starting over. Pass one to [Run] through [RunConfig]; every
// [Leaf] then records its output as it completes and skips any step the Journal
// already holds.
//
// Records are keyed by scope path and step ID, not by step ID alone. That is what
// makes resumption correct inside [Loop] and [Iteration], where the same step
// runs many times: element 2 is not mistaken for element 1. It is also why a
// resumed [Parallel] recovers every branch that finished, whatever the branch
// that suspended returned.
//
// A Journal belongs to one workflow definition. Replaying it against a changed
// graph would skip steps that never ran under it, or restore a value a renamed
// step no longer produces, so store a Journal with the version of the definition
// that produced it and discard it when that definition changes — the same
// discipline a schema migration needs. [Journal.Record] supplies an external
// [Interrupt] result, [Journal.Reset] starts a run over, and [Journal.Forget]
// retries one step.
//
// A Journal is safe for concurrent use within one logical workflow execution;
// concurrent branches record into the same one. Do not share one Journal between
// unrelated executions, because records intentionally have no run ID. Records are
// append-only until Forget or Reset: every duplicate completion reports
// [ErrJournalConflict]. Each [Run] replays only records that existed when that
// run began, so a duplicate identity created during a run cannot masquerade as
// historical work. The zero Journal is empty and ready to use. A Journal must
// not be copied after first use.
type Journal struct {
	mu       sync.RWMutex
	root     journalNode
	count    int
	revision uint64
}

// JournalKey identifies one recorded execution of a step. ID is the step ID;
// Path is the enclosing repeated scopes, outermost first, and may contain at
// most [MaxNestingDepth] segments. Construct JournalKey values with keyed fields
// so the type can grow without breaking callers.
type JournalKey struct {
	ID   string   `json:"id"`
	Path []string `json:"path,omitempty"`
}

func (j JournalKey) compare(other JournalKey) int {
	if order := slices.Compare(j.Path, other.Path); order != 0 {
		return order
	}
	return cmp.Compare(j.ID, other.ID)
}

// journalNode is a trie over scope segments. Keeping the path structured avoids
// delimiter escaping entirely: every possible segment and step ID has one
// unambiguous place in the tree.
type journalNode struct {
	records  map[string]journalValue
	children map[string]*journalNode
}

type journalValue struct {
	value    any
	revision uint64
}

// NewJournal returns an empty Journal.
func NewJournal() *Journal { return &Journal{} }

// Record marks one step execution as completed with value as its result. It is
// the resumption boundary for [Interrupt] and for a journaled boundary whose
// [Suspend] represents an externally supplied result: record the response under
// the suspension's [Suspension.Key], then run the same workflow again.
//
// Record rejects an empty step ID, a path deeper than [MaxNestingDepth], and an
// identity already present in the Journal. A recorded value is held as-is;
// mutable values must not be modified afterward. The method is safe for
// concurrent use.
func (j *Journal) Record(key JournalKey, value any) error {
	if j == nil {
		return errors.New("workflow: record journal: nil journal")
	}
	return j.insert(key, value)
}

// record stores a step's output. A nil Journal discards it, so callers need not
// check whether resumption is enabled.
func (j *Journal) record(path []string, id string, value any) error {
	if j == nil {
		return nil
	}
	return j.insert(JournalKey{ID: id, Path: path}, value)
}

func (j *Journal) insert(key JournalKey, value any) error {
	if err := key.validate(); err != nil {
		return fmt.Errorf("workflow: record journal: %w", err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, exists := j.root.lookup(key.Path, key.ID); exists {
		return fmt.Errorf("workflow: record journal step %q at %q: %w",
			key.ID, key.Path, ErrJournalConflict)
	}
	j.revision++
	j.root.record(key.Path, key.ID, journalValue{
		value:    value,
		revision: j.revision,
	})
	j.count++
	return nil
}

func (j JournalKey) validate() error {
	switch {
	case j.ID == "":
		return ErrInvalidStepID
	case len(j.Path) > MaxNestingDepth:
		return fmt.Errorf(
			"%w: scope path depth %d exceeds limit %d",
			ErrMaxDepth,
			len(j.Path),
			MaxNestingDepth,
		)
	default:
		return nil
	}
}

func (j *journalNode) record(path []string, id string, value journalValue) bool {
	for _, segment := range path {
		if j.children == nil {
			j.children = make(map[string]*journalNode)
		}
		child := j.children[segment]
		if child == nil {
			child = new(journalNode)
			j.children[segment] = child
		}
		j = child
	}
	if j.records == nil {
		j.records = make(map[string]journalValue)
	}
	if _, exists := j.records[id]; exists {
		return false
	}
	j.records[id] = value
	return true
}

// lookupAt returns a record no newer than revision, which is how a run replays
// only work that predates it. The receiver is never nil: [runState.replay]
// resolves a nil Journal to "no record" before calling.
func (j *Journal) lookupAt(path []string, id string, revision uint64) (any, bool) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	value, ok := j.root.lookup(path, id)
	if !ok || value.revision > revision {
		return nil, false
	}
	return value.value, true
}

func (j *Journal) snapshotRevision() uint64 {
	if j == nil {
		return 0
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.revision
}

func (j *journalNode) lookup(path []string, id string) (journalValue, bool) {
	for _, segment := range path {
		j = j.children[segment]
		if j == nil {
			return journalValue{}, false
		}
	}
	value, ok := j.records[id]
	return value, ok
}

// Len returns the number of recorded steps.
func (j *Journal) Len() int {
	if j == nil {
		return 0
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.count
}

// Keys returns the identities of the recorded steps in path and ID order. Every
// Path is a copy.
func (j *Journal) Keys() []JournalKey {
	if j == nil {
		return nil
	}
	j.mu.RLock()
	defer j.mu.RUnlock()

	keys := make([]JournalKey, 0, j.count)
	j.root.appendKeys(nil, &keys)
	slices.SortFunc(keys, JournalKey.compare)
	return keys
}

func (j *journalNode) appendKeys(path []string, keys *[]JournalKey) {
	for id := range j.records {
		*keys = append(*keys, JournalKey{Path: slices.Clone(path), ID: id})
	}
	for segment, child := range j.children {
		path = append(path, segment)
		child.appendKeys(path, keys)
		path = path[:len(path)-1]
	}
}

// Reset removes every record, so the next run starts from the beginning.
func (j *Journal) Reset() {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.root = journalNode{}
	j.count = 0
	j.revision++
}

// Forget removes one recorded step, so the next run repeats it.
func (j *Journal) Forget(key JournalKey) {
	if j == nil || key.validate() != nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.root.forget(key.Path, key.ID) {
		j.count--
		j.revision++
	}
}

// forget reports whether it removed a record and prunes empty scope nodes on
// the way back up.
func (j *journalNode) forget(path []string, id string) bool {
	if len(path) == 0 {
		if _, ok := j.records[id]; !ok {
			return false
		}
		delete(j.records, id)
		if len(j.records) == 0 {
			j.records = nil
		}
		return true
	}

	child := j.children[path[0]]
	if child == nil || !child.forget(path[1:], id) {
		return false
	}
	if child.empty() {
		delete(j.children, path[0])
		if len(j.children) == 0 {
			j.children = nil
		}
	}
	return true
}

func (j *journalNode) empty() bool {
	return len(j.records) == 0 && len(j.children) == 0
}
