package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
)

var (
	_ json.Marshaler   = (*Journal)(nil)
	_ json.Unmarshaler = (*Journal)(nil)
)

const (
	journalJSONVersion = 2

	journalFieldID      = "id"
	journalFieldRecords = "records"
	journalFieldScope   = "scope"
	journalFieldValue   = "value"
	journalFieldVersion = "version"
)

// journalDocument is the shape [Journal.MarshalJSON] writes: each value arrives
// already encoded, so the document is assembled without re-encoding.
type journalDocument struct {
	Version int                 `json:"version"`
	Records []journalJSONRecord `json:"records"`
}

type journalJSONRecord struct {
	Scope []string        `json:"scope,omitempty"`
	ID    string          `json:"id"`
	Value json.RawMessage `json:"value"`
}

type journalEntry struct {
	key   JournalKey
	value any
}

// journalDecoder owns the wire contract of Journal version 2. It reads the
// ordinary JSON domain produced by jsonDocument exactly once, so member names
// retain JSON's case-sensitive meaning instead of being folded by struct
// decoding. A versioned checkpoint accepts only its canonical field names:
// ambiguous documents must fail rather than silently change recorded work.
type journalDecoder struct {
	root  journalNode
	count int
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
				"workflow: marshal journal record %q in scope %q: %w",
				entry.key.ID,
				entry.key.Scope,
				err,
			)
		}
		records = append(records, journalJSONRecord{
			Scope: entry.key.Scope,
			ID:    entry.key.ID,
			Value: encoded,
		})
	}
	return json.Marshal(journalDocument{Version: journalJSONVersion, Records: records})
}

func (j *journalNode) appendEntries(scope []string, entries *[]journalEntry) {
	for id, value := range j.records {
		*entries = append(*entries, journalEntry{
			key:   JournalKey{Scope: slices.Clone(scope), ID: id},
			value: value.value,
		})
	}
	for segment, child := range j.children {
		scope = append(scope, segment)
		child.appendEntries(scope, entries)
		scope = scope[:len(scope)-1]
	}
}

// UnmarshalJSON atomically replaces the Journal's records. On failure the
// receiver is unchanged. Version 2 accepts only the canonical lower-case member
// names written by [Journal.MarshalJSON]; alternate casing is rejected so two
// spellings cannot be folded onto one field.
//
// As in a [Store], numbers decode as [json.Number] so nothing is rounded, and a
// skipped step's recorded value is read back through [Get], which converts it to
// the type the reading step asks for. Call UnmarshalJSON between runs, not while
// a Run is using the Journal.
func (j *Journal) UnmarshalJSON(data []byte) error {
	if j == nil {
		return errors.New("workflow: unmarshal journal: nil journal")
	}

	var decoded journalDecoder
	if err := decoded.decode(data); err != nil {
		return fmt.Errorf("workflow: unmarshal journal: %w", err)
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	j.revision++
	decoded.root.setRevision(j.revision)
	j.root = decoded.root
	j.count = decoded.count
	return nil
}

func (j *journalDecoder) decode(data []byte) error {
	value, err := jsonDocument(data).value()
	if err != nil {
		return err
	}
	document, ok := value.(map[string]any)
	if !ok {
		return errors.New("document must be an object")
	}
	if fieldErr := j.allowFields(document, journalFieldVersion, journalFieldRecords); fieldErr != nil {
		return fieldErr
	}
	if fieldErr := j.requireFields(
		document,
		"document",
		journalFieldVersion,
		journalFieldRecords,
	); fieldErr != nil {
		return fieldErr
	}

	versionNumber, ok := document[journalFieldVersion].(json.Number)
	if !ok {
		return errors.New("version must be an integer")
	}
	version, err := strconv.Atoi(versionNumber.String())
	if err != nil {
		return fmt.Errorf("version must be an integer: %w", err)
	}
	if version != journalJSONVersion {
		return fmt.Errorf(
			"unsupported version %d; want %d",
			version,
			journalJSONVersion,
		)
	}

	records, ok := document[journalFieldRecords].([]any)
	if !ok {
		return errors.New("records must be an array")
	}
	for index, value := range records {
		if err := j.decodeRecord(value); err != nil {
			return fmt.Errorf("record %d: %w", index, err)
		}
	}
	return nil
}

func (j *journalDecoder) decodeRecord(value any) error {
	record, ok := value.(map[string]any)
	if !ok {
		return errors.New("must be an object")
	}
	if err := j.allowFields(record, journalFieldScope, journalFieldID, journalFieldValue); err != nil {
		return err
	}
	if err := j.requireFields(record, "record", journalFieldID, journalFieldValue); err != nil {
		return err
	}

	id, ok := record[journalFieldID].(string)
	if !ok {
		return errors.New("id must be a string")
	}
	scope, err := j.decodeScope(record)
	if err != nil {
		return err
	}
	key := JournalKey{ID: id, Scope: scope}
	if err := key.validate(); err != nil {
		return err
	}
	if inserted := j.root.record(
		scope,
		id,
		journalValue{value: record[journalFieldValue]},
	); !inserted {
		return fmt.Errorf("duplicate step %q in scope %q", id, scope)
	}
	j.count++
	return nil
}

func (j *journalDecoder) decodeScope(record map[string]any) ([]string, error) {
	value, present := record[journalFieldScope]
	if !present {
		return nil, nil
	}
	values, ok := value.([]any)
	if !ok {
		return nil, errors.New("scope must be an array")
	}
	scope := make([]string, len(values))
	for index, value := range values {
		segment, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("scope segment %d must be a string", index)
		}
		scope[index] = segment
	}
	return scope, nil
}

func (j *journalDecoder) requireFields(
	object map[string]any,
	kind string,
	required ...string,
) error {
	for _, name := range required {
		if _, present := object[name]; !present {
			return fmt.Errorf("%s field %q is missing", kind, name)
		}
	}
	return nil
}

func (*journalDecoder) allowFields(object map[string]any, allowed ...string) error {
	for _, name := range slices.Sorted(maps.Keys(object)) {
		if !slices.Contains(allowed, name) {
			return fmt.Errorf("unknown field %q", name)
		}
	}
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
