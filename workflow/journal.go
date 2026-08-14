package workflow

import (
	"cmp"
	"fmt"
	"slices"
	"sync"

	"github.com/Tangerg/flow/internal/jsondoc"
)

// Journal records what a run already did, so a later run can pick up where it
// stopped instead of starting over. Pass one to [Run] through [RunConfig]; every
// [Leaf] then records its output as it completes and skips any step the Journal
// already holds.
//
// Records are keyed by scope and step ID, not by step ID alone. That is what
// makes resumption correct inside [Loop] and [Iteration], where the same step
// runs many times: element 2 is not mistaken for element 1. [Subgraph] uses the
// same structured scope to isolate its inner identities. It is also why a
// resumed [Parallel] recovers every branch that finished, whatever the branch
// that suspended returned.
//
// A Journal belongs to one workflow definition. Replaying it against a changed
// graph would skip steps that never ran under it, or restore a value a renamed
// step no longer produces, so store a Journal with the version of the definition
// that produced it and discard it when that definition changes — the same
// discipline a schema migration needs. [Journal.Record] supplies an external
// [Interrupt] result, [Journal.Reset] starts a run over, and [Journal.Forget]
// removes one exact checkpoint.
//
// A Journal is safe for concurrent use within one logical workflow execution;
// concurrent branches record into the same one. It does not serialize separate
// [Run] calls. The host must admit at most one Run for a logical execution at a
// time, or two calls may both perform work before one encounters a record
// conflict. Do not share one Journal between unrelated executions, because
// records intentionally have no run ID.
//
// Records are append-only until Forget or Reset: every duplicate completion
// reports [ErrJournalConflict]. Each Run replays only records that existed when
// that run began, so a duplicate identity created during a run cannot masquerade
// as historical work. Call Forget and Reset only between runs; although
// synchronized against concurrent access, they intentionally remove replay
// history.
//
// A checkpoint may be newer than the Store returned with an error. For example,
// a leaf can commit just before parent cancellation is observed, or a parallel
// sibling can complete before another sibling fails. This is intentional:
// replay restores that completed boundary on the next run. Persist the returned
// Store and Journal together rather than deriving one from the other. The zero
// Journal is empty and ready to use. A Journal must not be copied after first
// use.
type Journal struct {
	mu    sync.RWMutex
	root  journalNode
	count int
	// revision numbers records, not mutations. Every record carries the revision
	// it was stored at, and a run replays only records stored at or before it
	// began, so what has to hold is that a record stored after a run started
	// outranks that run's snapshot -- which insert guarantees by taking the next
	// revision for itself. Removing records stores nothing and leaves this alone;
	// a tick there would advance a Journal that holds nothing newer.
	revision uint64
}

// JournalKey identifies one recorded execution of a step. ID is the step ID;
// Scope contains the enclosing execution scopes, outermost first, and may
// contain at most [MaxNestingDepth] frames. Frame structure is identity: an
// indexed frame is distinct from an ordinary frame whose ID merely looks like
// its display form. Construct JournalKey values with keyed fields so the type
// can grow without breaking callers.
// JournalKey's standalone JSON representation is strict, lossless, and
// failure-atomic so a persisted callback correlation key cannot be renamed by
// Unicode replacement or ambiguous object members.
type JournalKey struct { //nolint:recvcheck // UnmarshalJSON requires a pointer receiver.
	ID    string       `json:"id"`
	Scope []ScopeFrame `json:"scope,omitempty"`
}

func (j JournalKey) compare(other JournalKey) int {
	return cmp.Or(
		compareScope(j.Scope, other.Scope),
		cmp.Compare(j.ID, other.ID),
	)
}

// journalNode is a trie over scope frames. Keeping the scope structured avoids
// rendering-based identity: an indexed invocation and an ordinary scope whose
// text happens to resemble it always occupy different nodes.
type journalNode struct {
	records  map[string]journalValue
	children map[ScopeFrame]*journalNode
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
// Record rejects an empty or non-UTF-8 step ID, an invalid or excessively deep
// scope, and an identity already present in the Journal. A recorded value is
// held as-is; mutable values must not be modified afterward. The method is safe
// for concurrent use.
func (j *Journal) Record(key JournalKey, value any) error {
	if j == nil {
		return fmt.Errorf("workflow: record journal: %w", jsondoc.ErrNilReceiver)
	}
	if err := j.insert(key, value); err != nil {
		return fmt.Errorf("workflow: record journal: %w", err)
	}
	return nil
}

// record stores a step's output. A nil Journal discards it, so callers need not
// check whether resumption is enabled.
func (j *Journal) record(key JournalKey, value any) error {
	if j == nil {
		return nil
	}
	return j.insert(key, value)
}

// insert is the write path both Record and record share. Its errors locate a
// failure by scope alone, because the step ID and this package are context each
// caller already supplies: Record from the key it was handed, an internal
// caller from the [StepError] that wraps it.
func (j *Journal) insert(key JournalKey, value any) error {
	if err := key.validate(); err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	nextRevision := j.revision + 1
	if inserted := j.root.record(key.Scope, key.ID, journalValue{
		value:    value,
		revision: nextRevision,
	}); !inserted {
		return fmt.Errorf("%w at %q", ErrJournalConflict, formatScope(key.Scope))
	}
	j.revision = nextRevision
	j.count++
	return nil
}

func (j JournalKey) validate() error {
	if err := validateStepID(j.ID); err != nil {
		return err
	}
	return validateScope(j.Scope)
}

// record inserts value and reports whether the identity was new. It takes the two
// halves of a [JournalKey] rather than the key, because this is the layer where an
// identity stops being one value: the scope is a path this trie descends and the ID
// is a member it acts on, and forget consumes that path one frame at a time.
// Everything above here passes the key whole.
func (j *journalNode) record(scope []ScopeFrame, id string, value journalValue) bool {
	for _, frame := range scope {
		if j.children == nil {
			j.children = make(map[ScopeFrame]*journalNode)
		}
		child := j.children[frame]
		if child == nil {
			child = new(journalNode)
			j.children[frame] = child
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
func (j *Journal) lookupAt(key JournalKey, revision uint64) (any, bool) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	value, ok := j.root.lookup(key.Scope, key.ID)
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

func (j *journalNode) lookup(scope []ScopeFrame, id string) (journalValue, bool) {
	for _, frame := range scope {
		j = j.children[frame]
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

// Keys returns the identities of the recorded steps in scope and ID order. Every
// Scope is a copy.
func (j *Journal) Keys() []JournalKey {
	if j == nil {
		return nil
	}
	j.mu.RLock()
	defer j.mu.RUnlock()

	keys := make([]JournalKey, 0, j.count)
	j.root.walk(nil, func(key JournalKey, _ journalValue) {
		keys = append(keys, key)
	})
	slices.SortFunc(keys, JournalKey.compare)
	return keys
}

// walk visits every record in the subtree under the scope it sits in, outermost
// frame first. Each visit receives a complete key that owns its scope, which is
// what both callers need: one keeps it as a key and the other as a wire record.
// Map iteration order is random, so a caller that needs an order imposes it.
func (j *journalNode) walk(scope []ScopeFrame, visit func(JournalKey, journalValue)) {
	for id, value := range j.records {
		visit(JournalKey{Scope: slices.Clone(scope), ID: id}, value)
	}
	for frame, child := range j.children {
		scope = append(scope, frame)
		child.walk(scope, visit)
		scope = scope[:len(scope)-1]
	}
}

// Reset removes every record, so the next run starts from the beginning. Call it
// between runs, not while a Run is using the Journal.
func (j *Journal) Reset() {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.root = journalNode{}
	j.count = 0
}

// Forget removes exactly one recorded step, so the next run repeats that
// boundary. It does not invalidate later records that may depend on the removed
// result: Journal deliberately has no workflow-definition or dependency
// knowledge. Forget the complete dependent closure yourself, or use [Journal.Reset],
// when recomputation may change a value consumed downstream.
//
// A missing record is already forgotten and succeeds. Invalid keys and a nil
// Journal are reported rather than silently doing nothing. Call Forget between
// runs, not while a Run is using the Journal.
func (j *Journal) Forget(key JournalKey) error {
	if j == nil {
		return fmt.Errorf("workflow: forget journal: %w", jsondoc.ErrNilReceiver)
	}
	if err := key.validate(); err != nil {
		return fmt.Errorf("workflow: forget journal: %w", err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.root.forget(key.Scope, key.ID) {
		j.count--
	}
	return nil
}

// forget reports whether it removed a record and prunes empty scope nodes on
// the way back up.
func (j *journalNode) forget(scope []ScopeFrame, id string) bool {
	if len(scope) == 0 {
		if _, ok := j.records[id]; !ok {
			return false
		}
		delete(j.records, id)
		if len(j.records) == 0 {
			j.records = nil
		}
		return true
	}

	child := j.children[scope[0]]
	if child == nil || !child.forget(scope[1:], id) {
		return false
	}
	if child.empty() {
		delete(j.children, scope[0])
		if len(j.children) == 0 {
			j.children = nil
		}
	}
	return true
}

func (j *journalNode) empty() bool {
	return len(j.records) == 0 && len(j.children) == 0
}
