// Package jsondoc implements the repository's strict JSON document boundary.
// It is internal so public packages can share parsing semantics without making
// a second JSON API part of flow's compatibility surface.
package jsondoc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	unicodeEscapePrefix = 2
	unicodeEscapeDigits = 4
	unicodeEscapeLength = unicodeEscapePrefix + unicodeEscapeDigits
)

// Codec applies one strict document contract in both directions: one complete
// value, valid Unicode text, no duplicate object members, preserved numbers,
// bounded nesting, and optional typed decoding. MaxDepth counts nested JSON
// containers.
type Codec struct {
	MaxDepth int
}

// DepthError reports the path at which a JSON container exceeded MaxDepth.
// Path is an RFC 6901 JSON Pointer and Limit is the configured maximum.
type DepthError struct {
	Path  string
	Limit int
}

func (d *DepthError) Error() string {
	return fmt.Sprintf("JSON nesting at %s exceeds limit %d", d.Path, d.Limit)
}

// TranslateDepth rewrites a [DepthError] as sentinel, keeping the failing path
// and limit, and returns any other error unchanged. The caller supplies the
// sentinel so the message carries its own package's prefix: this package is an
// implementation detail and must not appear in a public error. Stating the
// rewrite here keeps every boundary that shares one depth limit from also
// sharing a copy of its diagnostic.
func TranslateDepth(err error, sentinel error) error {
	var depthErr *DepthError
	if !errors.As(err, &depthErr) {
		return err
	}
	return fmt.Errorf(
		"%w at %s: depth exceeds limit %d",
		sentinel,
		depthErr.Path,
		depthErr.Limit,
	)
}

// Validate checks data without retaining its decoded value.
func (c Codec) Validate(data []byte) error {
	_, err := c.Value(data)
	return err
}

// Marshal encodes value and then validates the complete resulting document.
// Validation reads only the encoded bytes, so it does not invoke a custom
// MarshalJSON method a second time for any encoded occurrence. Callers that
// attach identity semantics to Go strings must validate those strings before
// calling Marshal because encoding/json replaces invalid UTF-8 by design.
func (c Codec) Marshal(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if err := c.Validate(data); err != nil {
		return nil, err
	}
	return data, nil
}

// Decode validates data and then maps it into dst. Unknown struct fields are
// rejected. dst is not guaranteed to remain unchanged; callers that require an
// atomic update must decode into a temporary value and assign after success.
func (c Codec) Decode(data []byte, dst any) error {
	if err := c.Validate(data); err != nil {
		return err
	}
	return c.DecodeParsed(data, dst)
}

// DecodeParsed maps data already accepted by Value into dst.
func (Codec) DecodeParsed(data []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	return decoder.Decode(dst)
}

// Value decodes data into the ordinary JSON domain while enforcing the strict
// document contract. Object members are retained exactly once, so schema and
// typed decoding cannot observe a different document from the caller.
func (c Codec) Value(data []byte) (any, error) {
	return c.valueAt(data, nil)
}

// ValidateFragment checks data as if it appeared at the JSON Pointer path named
// by at, whose segments are the containers already entered to reach it. Nesting
// is therefore counted from that position and reported against the same MaxDepth
// as a whole document, so validating a fragment on its own yields the depth and
// path a caller would see had the assembled document been validated instead.
func (c Codec) ValidateFragment(data []byte, at ...string) error {
	_, err := c.valueAt(data, at)
	return err
}

func (c Codec) valueAt(data []byte, at []string) (any, error) {
	if err := validateUTF8(data); err != nil {
		return nil, err
	}
	if err := (&unicodeEscapeValidator{data: data}).validate(); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	reader := reader{
		decoder: decoder,
		// Capping the capacity keeps the walk's appends from writing into the
		// caller's array beyond the prefix it passed.
		path:     at[:len(at):len(at)],
		depth:    len(at),
		maxDepth: c.MaxDepth,
	}
	value, err := reader.read()
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

// unicodeEscapeValidator owns the cursor used to inspect JSON strings for lone
// UTF-16 surrogate escapes. The JSON decoder remains responsible for ordinary
// syntax; this pass rejects the one otherwise-valid shape encoding/json would
// silently replace with U+FFFD and thereby change identity-bearing text.
type unicodeEscapeValidator struct {
	data   []byte
	offset int
}

func (u *unicodeEscapeValidator) validate() error {
	for u.offset = 0; u.offset < len(u.data); u.offset++ {
		if u.data[u.offset] != '"' {
			continue
		}
		if err := u.validateString(); err != nil {
			return err
		}
	}
	return nil
}

func (u *unicodeEscapeValidator) validateString() error {
	for u.offset++; u.offset < len(u.data) && u.data[u.offset] != '"'; u.offset++ {
		if u.data[u.offset] != '\\' {
			continue
		}
		if err := u.validateEscape(); err != nil {
			return err
		}
	}
	return nil
}

func (u *unicodeEscapeValidator) validateEscape() error {
	escapeOffset := u.offset
	u.offset++
	if u.offset >= len(u.data) || u.data[u.offset] != 'u' {
		return nil
	}
	code, ok := u.codeUnit(u.offset + 1)
	if !ok {
		return nil // The JSON decoder reports the malformed escape.
	}
	u.offset += unicodeEscapeDigits
	switch {
	case code >= 0xD800 && code <= 0xDBFF:
		low, paired := u.surrogatePair(u.offset + 1)
		if !paired || low < 0xDC00 || low > 0xDFFF {
			return unpairedSurrogateError(escapeOffset)
		}
		u.offset += unicodeEscapeLength
	case code >= 0xDC00 && code <= 0xDFFF:
		return unpairedSurrogateError(escapeOffset)
	}
	return nil
}

func (u *unicodeEscapeValidator) surrogatePair(offset int) (uint64, bool) {
	if len(u.data)-offset < unicodeEscapeLength ||
		u.data[offset] != '\\' ||
		u.data[offset+1] != 'u' {
		return 0, false
	}
	return u.codeUnit(offset + unicodeEscapePrefix)
}

func (u *unicodeEscapeValidator) codeUnit(offset int) (uint64, bool) {
	if len(u.data)-offset < unicodeEscapeDigits {
		return 0, false
	}
	value, err := strconv.ParseUint(
		string(u.data[offset:offset+unicodeEscapeDigits]),
		16,
		16,
	)
	return value, err == nil
}

func unpairedSurrogateError(offset int) error {
	return fmt.Errorf("unpaired UTF-16 surrogate escape at byte %d", offset+1)
}

func validateUTF8(data []byte) error {
	for offset := 0; offset < len(data); {
		_, size := utf8.DecodeRune(data[offset:])
		if size == 1 && data[offset] >= utf8.RuneSelf {
			return fmt.Errorf("invalid UTF-8 at byte %d", offset+1)
		}
		offset += size
	}
	return nil
}

// reader owns the cursor and path of a recursive token walk. path always
// identifies the value currently being read.
type reader struct {
	decoder  *json.Decoder
	path     []string
	depth    int
	maxDepth int
}

func (r *reader) read() (any, error) {
	token, err := r.decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	if err := r.enter(); err != nil {
		return nil, err
	}
	defer r.leave()

	// A closing delimiter cannot appear where a value is expected: the top
	// level starts a document, an object member follows its name, and an array
	// element is read only while More reports one. delim is therefore '{' or '['.
	if delim == '{' {
		return r.readObject()
	}
	return r.readArray()
}

func (r *reader) enter() error {
	if r.depth >= r.maxDepth {
		return &DepthError{Path: encodePointer(r.path), Limit: r.maxDepth}
	}
	r.depth++
	return nil
}

func (r *reader) leave() { r.depth-- }

func (r *reader) readObject() (map[string]any, error) {
	object := make(map[string]any)
	for r.decoder.More() {
		name, err := r.readMemberName()
		if err != nil {
			return nil, err
		}
		r.path = append(r.path, name)
		if _, duplicate := object[name]; duplicate {
			return nil, fmt.Errorf(
				"duplicate object member %q at %s",
				name,
				encodePointer(r.path),
			)
		}
		value, err := r.read()
		r.path = r.path[:len(r.path)-1]
		if err != nil {
			return nil, err
		}
		object[name] = value
	}
	if _, err := r.decoder.Token(); err != nil { // }
		return nil, err
	}
	return object, nil
}

func (r *reader) readMemberName() (string, error) {
	token, err := r.decoder.Token()
	if err != nil {
		return "", err
	}
	// More reports true inside an object only when the next token is a member
	// name, so a successful Token call is necessarily a string.
	return token.(string), nil //nolint:forcetypeassert // encoding/json contract.
}

func (r *reader) readArray() ([]any, error) {
	array := make([]any, 0)
	for index := 0; r.decoder.More(); index++ {
		r.path = append(r.path, strconv.Itoa(index))
		value, err := r.read()
		r.path = r.path[:len(r.path)-1]
		if err != nil {
			return nil, err
		}
		array = append(array, value)
	}
	if _, err := r.decoder.Token(); err != nil { // ]
		return nil, err
	}
	return array, nil
}

func encodePointer(path []string) string {
	var encoded strings.Builder
	for _, segment := range path {
		encoded.WriteByte('/')
		for index := range len(segment) {
			switch character := segment[index]; character {
			case '~':
				encoded.WriteString("~0")
			case '/':
				encoded.WriteString("~1")
			default:
				encoded.WriteByte(character)
			}
		}
	}
	return encoded.String()
}
