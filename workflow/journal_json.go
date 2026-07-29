package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

var (
	_ json.Marshaler   = (*Journal)(nil)
	_ json.Unmarshaler = (*Journal)(nil)
)

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
// It reports the record holding a value that encoding/json cannot encode. A nil
// Journal is encoded as an empty Journal, matching [Journal.Len] and
// [Journal.Keys].
func (j *Journal) MarshalJSON() ([]byte, error) {
	if j == nil {
		return json.Marshal(journalDocument{
			Version: journalJSONVersion,
			Records: []journalJSONRecord{},
		})
	}

	j.mu.RLock()
	entries := make([]journalEntry, 0, j.count)
	j.root.appendEntries(nil, &entries)
	j.mu.RUnlock()

	slices.SortFunc(entries, func(left, right journalEntry) int {
		return left.key.compare(right.key)
	})
	records := make([]journalJSONRecord, 0, len(entries))
	for _, entry := range entries {
		encoded, err := json.Marshal(entry.value)
		if err != nil {
			return nil, fmt.Errorf(
				"workflow: marshal journal record %q at path %q: %w",
				entry.key.ID,
				entry.key.Path,
				err,
			)
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
	if j == nil {
		return errors.New("workflow: unmarshal journal: nil journal")
	}

	var document journalDocument
	if err := jsonDocument(data).decode(&document); err != nil {
		return fmt.Errorf("workflow: unmarshal journal: %w", err)
	}
	if document.Version != journalJSONVersion {
		return fmt.Errorf(
			"workflow: unmarshal journal: unsupported version %d; want %d",
			document.Version,
			journalJSONVersion,
		)
	}

	var root journalNode
	count := 0
	for index, record := range document.Records {
		if record.ID == "" {
			return fmt.Errorf("workflow: unmarshal journal record %d: step ID is empty", index)
		}
		if len(record.Value) == 0 {
			return fmt.Errorf("workflow: unmarshal journal record %d: value is missing", index)
		}
		value, err := jsonDocument(record.Value).value()
		if err != nil {
			return fmt.Errorf(
				"workflow: unmarshal journal record %d (%q at path %q): %w",
				index,
				record.ID,
				record.Path,
				err,
			)
		}
		if inserted := root.record(record.Path, record.ID, value); !inserted {
			return fmt.Errorf(
				"workflow: unmarshal journal record %d: duplicate step %q at path %q",
				index,
				record.ID,
				record.Path,
			)
		}
		count++
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	j.root = root
	j.count = count
	return nil
}
