package workflow

import (
	"cmp"
	"encoding/json"
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
// append-only until Forget or Reset: an internal duplicate completion keeps the
// first value, while Record reports ErrJournalConflict. The zero Journal is empty
// and ready to use. A Journal must not be copied after first use.
type Journal struct {
	mu    sync.RWMutex
	root  journalNode
	count int
}

// JournalKey identifies one recorded execution of a step. ID is the step ID;
// Path is the enclosing repeated scopes, outermost first. Construct JournalKey
// values with keyed fields so the type can grow without breaking callers.
type JournalKey struct {
	ID   string   `json:"id"`
	Path []string `json:"path,omitempty"`
}

func (k JournalKey) compare(other JournalKey) int {
	if order := slices.Compare(k.Path, other.Path); order != 0 {
		return order
	}
	return cmp.Compare(k.ID, other.ID)
}

type journalPath []string

func (p journalPath) child(segment string) journalPath {
	next := make(journalPath, len(p)+1)
	copy(next, p)
	next[len(p)] = segment
	return next
}

// journalNode is a trie over scope segments. Keeping the path structured avoids
// delimiter escaping entirely: every possible segment and step ID has one
// unambiguous place in the tree.
type journalNode struct {
	records  map[string]any
	children map[string]*journalNode
}

var (
	_ json.Marshaler   = (*Journal)(nil)
	_ json.Unmarshaler = (*Journal)(nil)
)

// NewJournal returns an empty Journal.
func NewJournal() *Journal { return &Journal{} }

// Record marks one step execution as completed with value as its result. It is
// the resumption boundary for [Interrupt] and for a journaled boundary whose
// [Suspend] represents an externally supplied result: record the response under
// the suspension's [Suspension.Key], then run the same workflow again.
//
// Record rejects an empty step ID and an identity already present in the
// Journal. A recorded value is held as-is; mutable values must not be modified
// afterward. The method is safe for concurrent use.
func (j *Journal) Record(key JournalKey, value any) error {
	switch {
	case j == nil:
		return errors.New("workflow: record journal: nil Journal")
	case key.ID == "":
		return fmt.Errorf("workflow: record journal: %w", ErrInvalidStepID)
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	if _, exists := j.root.lookup(key.Path, key.ID); exists {
		return fmt.Errorf("workflow: record journal step %q at %q: %w",
			key.ID, key.Path, ErrJournalConflict)
	}
	j.root.record(key.Path, key.ID, value)
	j.count++
	return nil
}

// record stores a step's output. A nil Journal discards it, so callers need not
// check whether resumption is enabled.
func (j *Journal) record(path []string, id string, value any) {
	if j == nil || id == "" {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.root.record(path, id, value) {
		j.count++
	}
}

func (n *journalNode) record(path []string, id string, value any) bool {
	for _, segment := range path {
		if n.children == nil {
			n.children = make(map[string]*journalNode)
		}
		child := n.children[segment]
		if child == nil {
			child = new(journalNode)
			n.children[segment] = child
		}
		n = child
	}
	if n.records == nil {
		n.records = make(map[string]any)
	}
	if _, exists := n.records[id]; exists {
		return false
	}
	n.records[id] = value
	return true
}

func (j *Journal) lookup(path []string, id string) (any, bool) {
	if j == nil {
		return nil, false
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.root.lookup(path, id)
}

func (n *journalNode) lookup(path []string, id string) (any, bool) {
	for _, segment := range path {
		n = n.children[segment]
		if n == nil {
			return nil, false
		}
	}
	value, ok := n.records[id]
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

func (n *journalNode) appendKeys(path []string, keys *[]JournalKey) {
	for id := range n.records {
		*keys = append(*keys, JournalKey{Path: slices.Clone(path), ID: id})
	}
	for segment, child := range n.children {
		child.appendKeys(journalPath(path).child(segment), keys)
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
}

// Forget removes one recorded step, so the next run repeats it.
func (j *Journal) Forget(key JournalKey) {
	if j == nil || key.ID == "" {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.root.forget(key.Path, key.ID) {
		j.count--
	}
}

// forget reports whether it removed a record and prunes empty scope nodes on
// the way back up.
func (n *journalNode) forget(path []string, id string) bool {
	if len(path) == 0 {
		if _, ok := n.records[id]; !ok {
			return false
		}
		delete(n.records, id)
		if len(n.records) == 0 {
			n.records = nil
		}
		return true
	}

	child := n.children[path[0]]
	if child == nil || !child.forget(path[1:], id) {
		return false
	}
	if child.empty() {
		delete(n.children, path[0])
		if len(n.children) == 0 {
			n.children = nil
		}
	}
	return true
}

func (n *journalNode) empty() bool {
	return len(n.records) == 0 && len(n.children) == 0
}

const journalJSONVersion = 1

type journalDocument struct {
	Version int                 `json:"version"`
	Records []journalJSONRecord `json:"records"`
}

type journalJSONRecord struct {
	Path  []string        `json:"path,omitempty"`
	ID    string          `json:"id"`
	Value json.RawMessage `json:"value"`
}

type journalEntry struct {
	key   JournalKey
	value any
}

// MarshalJSON serializes the Journal as a versioned list of structured records.
// It reports the record holding a value that encoding/json cannot encode.
func (j *Journal) MarshalJSON() ([]byte, error) {
	j.mu.RLock()
	entries := make([]journalEntry, 0, j.count)
	j.root.appendEntries(nil, &entries)
	j.mu.RUnlock()

	slices.SortFunc(entries, func(a, b journalEntry) int {
		return a.key.compare(b.key)
	})
	records := make([]journalJSONRecord, 0, len(entries))
	for _, entry := range entries {
		encoded, err := json.Marshal(entry.value)
		if err != nil {
			return nil, fmt.Errorf("workflow: marshal journal record %q at %q: %w",
				entry.key.ID, entry.key.Path, err)
		}
		records = append(records, journalJSONRecord{
			Path:  entry.key.Path,
			ID:    entry.key.ID,
			Value: encoded,
		})
	}
	return json.Marshal(journalDocument{Version: journalJSONVersion, Records: records})
}

func (n *journalNode) appendEntries(path []string, entries *[]journalEntry) {
	for id, value := range n.records {
		*entries = append(*entries, journalEntry{
			key:   JournalKey{Path: slices.Clone(path), ID: id},
			value: value,
		})
	}
	for segment, child := range n.children {
		child.appendEntries(journalPath(path).child(segment), entries)
	}
}

// UnmarshalJSON atomically replaces the Journal's records. On failure the
// receiver is unchanged.
//
// As in a [Store], numbers decode as [json.Number] so nothing is rounded, and a
// skipped step's recorded value is read back through [Get], which converts it to
// the type the reading step asks for.
func (j *Journal) UnmarshalJSON(data []byte) error {
	var document journalDocument
	if err := jsonDocument(data).decode(&document); err != nil {
		return fmt.Errorf("workflow: unmarshal journal: %w", err)
	}
	if document.Version != journalJSONVersion {
		return fmt.Errorf("workflow: unmarshal journal: unsupported version %d", document.Version)
	}

	var root journalNode
	count := 0
	for i, record := range document.Records {
		if record.ID == "" {
			return fmt.Errorf("workflow: unmarshal journal record %d: empty step ID", i)
		}
		if len(record.Value) == 0 {
			return fmt.Errorf("workflow: unmarshal journal record %d: missing value", i)
		}
		value, err := jsonDocument(record.Value).value()
		if err != nil {
			return fmt.Errorf("workflow: unmarshal journal record %d (%q at %q): %w",
				i, record.ID, record.Path, err)
		}
		if inserted := root.record(record.Path, record.ID, value); !inserted {
			return fmt.Errorf("workflow: unmarshal journal record %d: duplicate %q at %q",
				i, record.ID, record.Path)
		}
		count++
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	j.root = root
	j.count = count
	return nil
}
