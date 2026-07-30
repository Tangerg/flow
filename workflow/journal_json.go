package workflow

import (
	"bytes"
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

// journalDocument is the shape [Journal.MarshalJSON] writes: each value arrives
// already encoded, so the document is assembled without re-encoding.
type journalDocument struct {
	Version int                 `json:"version"`
	Records []journalJSONRecord `json:"records"`
}

type journalJSONRecord struct {
	Path  []string        `json:"path,omitempty"`
	ID    string          `json:"id"`
	Value json.RawMessage `json:"value"`
}

// journalDecodedDocument reads that same shape. It exists separately from
// journalDocument because the two directions need different value
// representations, and deriving one from the other would mean decoding the
// document twice — once for its typed fields and once for its raw values — which
// only works while both views agree on how a member name is spelled.
type journalDecodedDocument struct {
	Version int                    `json:"version"`
	Records []journalDecodedRecord `json:"records"`
}

type journalDecodedRecord struct {
	Path  []string            `json:"path,omitempty"`
	ID    string              `json:"id"`
	Value journalDecodedValue `json:"value"`
}

// journalDecodedValue remembers that a "value" member appeared, which is what
// separates a step that recorded nil from a record that omitted its result
// entirely. An omitted member never reaches UnmarshalJSON, while an explicit
// null does.
type journalDecodedValue struct {
	value   any
	present bool
}

func (j *journalDecodedValue) UnmarshalJSON(data []byte) error {
	j.present = true
	// The enclosing decoder's UseNumber setting does not reach a custom
	// Unmarshaler, so this repeats it: a recorded number must survive a round
	// trip without being widened to float64.
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(&j.value)
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

func (j *journalNode) appendEntries(path []string, entries *[]journalEntry) {
	for id, value := range j.records {
		*entries = append(*entries, journalEntry{
			key:   JournalKey{Path: slices.Clone(path), ID: id},
			value: value.value,
		})
	}
	for segment, child := range j.children {
		path = append(path, segment)
		child.appendEntries(path, entries)
		path = path[:len(path)-1]
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

	var document journalDecodedDocument
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
		key := JournalKey{ID: record.ID, Path: record.Path}
		if err := key.validate(); err != nil {
			return fmt.Errorf("workflow: unmarshal journal record %d: %w", index, err)
		}
		if !record.Value.present {
			return fmt.Errorf("workflow: unmarshal journal record %d: value is missing", index)
		}
		if inserted := root.record(
			record.Path,
			record.ID,
			journalValue{value: record.Value.value},
		); !inserted {
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
	j.revision++
	root.setRevision(j.revision)
	j.root = root
	j.count = count
	return nil
}

func (j *journalNode) setRevision(revision uint64) {
	for id, value := range j.records {
		value.revision = revision
		j.records[id] = value
	}
	for _, child := range j.children {
		child.setRevision(revision)
	}
}
