package workflow

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
)

// Journal records what a run already did, so a later run can pick up where it
// stopped instead of starting over. Attach one through [RunConfig]; every [Leaf]
// then records its output as it completes and skips any step the Journal already
// holds.
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
// discipline a schema migration needs. [Journal.Reset] starts a run over and
// [Journal.Forget] retries one step.
//
// A Journal is safe for concurrent use; concurrent branches record into the same
// one. The zero Journal is empty and ready to use.
type Journal struct {
	mu      sync.RWMutex
	records map[string]any
}

var (
	_ json.Marshaler   = (*Journal)(nil)
	_ json.Unmarshaler = (*Journal)(nil)
)

// NewJournal returns an empty Journal.
func NewJournal() *Journal { return &Journal{} }

// recordKey joins a scope path and a step ID into one key. The path segments
// already carry brackets, so "/" cannot be produced by an index and only
// separates scopes.
func recordKey(path []string, id string) string {
	if len(path) == 0 {
		return id
	}
	return strings.Join(path, "/") + "/" + id
}

// record stores a step's output. A nil Journal discards it, so callers need not
// check whether resumption is enabled.
func (j *Journal) record(path []string, id string, value any) {
	if j == nil || id == "" {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.records == nil {
		j.records = make(map[string]any)
	}
	j.records[recordKey(path, id)] = value
}

func (j *Journal) lookup(path []string, id string) (any, bool) {
	if j == nil {
		return nil, false
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	value, ok := j.records[recordKey(path, id)]
	return value, ok
}

// Len returns the number of recorded steps.
func (j *Journal) Len() int {
	if j == nil {
		return 0
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	return len(j.records)
}

// Keys returns the recorded step keys in sorted order, each a scope path and step
// ID joined by "/". It is meant for diagnostics and tests.
func (j *Journal) Keys() []string {
	if j == nil {
		return nil
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	return slices.Sorted(maps.Keys(j.records))
}

// Reset removes every record, so the next run starts from the beginning.
func (j *Journal) Reset() {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.records = nil
}

// Forget removes the record for one step in one scope, so the next run repeats
// it. Use it to retry a single step without discarding the rest of the run.
func (j *Journal) Forget(path []string, id string) {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.records, recordKey(path, id))
}

// MarshalJSON serializes the Journal as key -> recorded value. It reports the
// record holding a value that encoding/json cannot encode.
func (j *Journal) MarshalJSON() ([]byte, error) {
	j.mu.RLock()
	records := maps.Clone(j.records)
	j.mu.RUnlock()

	if records == nil {
		records = map[string]any{}
	}
	encoded, err := json.Marshal(records)
	if err == nil {
		return encoded, nil
	}
	for _, key := range slices.Sorted(maps.Keys(records)) {
		if _, recordErr := json.Marshal(records[key]); recordErr != nil {
			return nil, fmt.Errorf("workflow: marshal journal %s: %w", key, recordErr)
		}
	}
	return nil, fmt.Errorf("workflow: marshal journal: %w", err)
}

// UnmarshalJSON atomically replaces the Journal's records. On failure the
// receiver is unchanged.
//
// As in a [Store], numbers decode as [json.Number] so nothing is rounded, and a
// skipped step's recorded value is read back through [Get], which converts it to
// the type the reading step asks for.
func (j *Journal) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := decodeStrict(data, &raw); err != nil {
		return fmt.Errorf("workflow: unmarshal journal: %w", err)
	}

	records := make(map[string]any, len(raw))
	for key, encoded := range raw {
		value, err := decodeValue(encoded)
		if err != nil {
			return fmt.Errorf("workflow: unmarshal journal %s: %w", key, err)
		}
		records[key] = value
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	j.records = records
	return nil
}
