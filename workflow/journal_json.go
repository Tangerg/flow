package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/Tangerg/flow/internal/jsonnum"
)

var (
	_ json.Marshaler   = (*Journal)(nil)
	_ json.Unmarshaler = (*Journal)(nil)
)

const (
	journalJSONVersion = 3

	journalFieldID      = "id"
	journalFieldIndex   = "index"
	journalFieldIndexed = "indexed"
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
	Scope []ScopeFrame    `json:"scope,omitempty"`
	ID    string          `json:"id"`
	Value json.RawMessage `json:"value"`
}

type journalEntry struct {
	key   JournalKey
	value any
}

// journalDecoder owns the wire contract of Journal version 3. It reads the
// ordinary JSON domain produced by jsonDocument exactly once, so member names
// retain JSON's case-sensitive meaning instead of being folded by struct
// decoding. A versioned checkpoint accepts only its canonical field names:
// ambiguous documents must fail rather than silently change recorded work.
type journalDecoder struct {
	root  journalNode
	count int
}

// MarshalJSON serializes the Journal as a versioned list of structured records.
// It reports the record holding a value that encoding/json cannot encode, whose
// custom JSON encoding is malformed or ambiguous, or whose encoded form exceeds
// [MaxNestingDepth]. Application values otherwise follow encoding/json's rules,
// including replacement of invalid UTF-8 in ordinary Go strings. Step and scope
// IDs must be valid UTF-8 so their identities survive the JSON boundary
// unchanged. A nil *Journal encodes as null, matching encoding/json's nil
// pointer behavior and representing that resumption is disabled. Records are
// emitted in [Journal.Keys] order, so equal record values produce stable bytes.
// Allocate a zero Journal when an empty, versioned checkpoint is required.
func (j *Journal) MarshalJSON() ([]byte, error) {
	if j == nil {
		return []byte("null"), nil
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
				formatScope(entry.key.Scope),
				err,
			)
		}
		records = append(records, journalJSONRecord{
			Scope: entry.key.Scope,
			ID:    entry.key.ID,
			Value: encoded,
		})
	}
	for index, record := range records {
		if _, err := (journalDocument{
			Version: journalJSONVersion,
			Records: []journalJSONRecord{record},
		}).marshal(); err != nil {
			entry := entries[index]
			return nil, fmt.Errorf(
				"workflow: marshal journal record %q in scope %q: %w",
				entry.key.ID,
				formatScope(entry.key.Scope),
				err,
			)
		}
	}
	// Every record has crossed the complete wire boundary independently. Adding
	// siblings to the records array cannot increase nesting or introduce a
	// duplicate object member, so the assembled document is readable too.
	return (journalDocument{
		Version: journalJSONVersion,
		Records: records,
	}).encode(), nil
}

// marshal enforces the same recursive boundary as Journal.UnmarshalJSON, so a
// successful encoding is always structurally readable by this package.
func (j journalDocument) marshal() ([]byte, error) {
	encoded := j.encode()
	if err := jsonDocument(encoded).validate(); err != nil {
		return nil, err
	}
	return encoded, nil
}

// encode assembles a document from JSON fragments produced by json.Marshal.
// It cannot invoke application code or fail.
func (j journalDocument) encode() []byte {
	// Every RawMessage was just produced by json.Marshal, and the remaining
	// fields contain only strings, booleans, and integers, so this structural
	// encoding cannot fail.
	//
	//nolint:errchkjson // The construction invariant above excludes invalid JSON.
	encoded, _ := json.Marshal(j)
	return encoded
}

func (j *journalNode) appendEntries(scope []ScopeFrame, entries *[]journalEntry) {
	for id, value := range j.records {
		*entries = append(*entries, journalEntry{
			key:   JournalKey{Scope: slices.Clone(scope), ID: id},
			value: value.value,
		})
	}
	for frame, child := range j.children {
		scope = append(scope, frame)
		child.appendEntries(scope, entries)
		scope = scope[:len(scope)-1]
	}
}

// UnmarshalJSON atomically replaces the Journal's records. On failure the
// receiver is unchanged. Version 3 accepts only the canonical lower-case member
// names written by [Journal.MarshalJSON]; alternate casing is rejected so two
// spellings cannot be folded onto one field. Invalid Unicode text is rejected
// rather than silently replaced.
//
// As in a [Store], numbers decode as [json.Number] so nothing is rounded, and a
// skipped step's recorded value is read back through [Get], which converts it to
// the type the reading step asks for when that type has a faithful JSON round
// trip. Version and scope-index fields use mathematical JSON integer semantics:
// equivalent decimal and exponent spellings are accepted, while an index must
// fit uint64. Call UnmarshalJSON between runs, not while a Run is using the
// Journal.
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
	version, err := jsonnum.ParseInteger(versionNumber.String())
	if errors.Is(err, jsonnum.ErrFractional) {
		return fmt.Errorf("version must be an integer, got %s", versionNumber)
	}
	if err != nil || version.Negative || version.Magnitude != journalJSONVersion {
		return fmt.Errorf(
			"unsupported version %s; want %d",
			versionNumber,
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
		return fmt.Errorf("duplicate step %q in scope %q", id, formatScope(scope))
	}
	j.count++
	return nil
}

func (j *journalDecoder) decodeScope(record map[string]any) ([]ScopeFrame, error) {
	value, present := record[journalFieldScope]
	if !present {
		return nil, nil
	}
	values, ok := value.([]any)
	if !ok {
		return nil, errors.New("scope must be an array")
	}
	if err := validateScopeDepth(len(values)); err != nil {
		return nil, err
	}
	scope := make([]ScopeFrame, len(values))
	for index, value := range values {
		frame, err := j.decodeScopeFrame(value)
		if err != nil {
			return nil, fmt.Errorf("scope frame %d: %w", index, err)
		}
		scope[index] = frame
	}
	return scope, nil
}

func (j *journalDecoder) decodeScopeFrame(value any) (ScopeFrame, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return ScopeFrame{}, errors.New("must be an object")
	}
	if err := j.allowFields(
		object,
		journalFieldID,
		journalFieldIndexed,
		journalFieldIndex,
	); err != nil {
		return ScopeFrame{}, err
	}
	if err := j.requireFields(object, "scope frame", journalFieldID); err != nil {
		return ScopeFrame{}, err
	}

	id, ok := object[journalFieldID].(string)
	if !ok {
		return ScopeFrame{}, errors.New("id must be a string")
	}
	frame := ScopeFrame{ID: id}
	if value, present := object[journalFieldIndexed]; present {
		indexed, ok := value.(bool)
		if !ok {
			return ScopeFrame{}, errors.New("indexed must be a boolean")
		}
		frame.Indexed = indexed
	}
	if value, present := object[journalFieldIndex]; present {
		if !frame.Indexed {
			return ScopeFrame{}, errors.New("index requires indexed")
		}
		number, ok := value.(json.Number)
		if !ok {
			return ScopeFrame{}, errors.New("index must be an integer")
		}
		parsed, err := jsonnum.ParseInteger(number.String())
		switch {
		case errors.Is(err, jsonnum.ErrRange):
			return ScopeFrame{}, fmt.Errorf("index %s exceeds uint64", number)
		case err != nil || parsed.Negative:
			return ScopeFrame{}, fmt.Errorf("index must be a non-negative integer, got %s", number)
		}
		frame.Index = parsed.Magnitude
	}
	if err := frame.Validate(); err != nil {
		return ScopeFrame{}, err
	}
	return frame, nil
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
