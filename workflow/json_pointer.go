package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
)

// pointerPath is an unescaped sequence of RFC 6901 path segments.
type pointerPath []string

func (p pointerPath) encode() string {
	var encoder pointerEncoder
	for _, segment := range p {
		encoder.write(segment)
	}
	return encoder.String()
}

type pointerEncoder struct {
	strings.Builder
}

//nolint:gosec // strings.Builder's writes are documented never to fail.
func (p *pointerEncoder) write(segment string) {
	p.WriteByte('/')
	for index := range len(segment) {
		character := segment[index]
		switch character {
		case '~':
			p.WriteString("~0")
		case '/':
			p.WriteString("~1")
		default:
			// JSON Pointer escaping only gives the two ASCII delimiter bytes
			// special forms. Preserving every other byte also preserves an
			// in-memory Store key that is not valid UTF-8; ranging over runes
			// would silently replace it with U+FFFD.
			p.WriteByte(character)
		}
	}
}

type encodedPointer string

// scan starts a walk over an RFC 6901 pointer, reporting whether e is one. A
// successful scanner always yields at least one segment: "/" is the pointer to
// the member named "", so the first [pointerScanner.next] never reports absence.
func (e encodedPointer) scan() (pointerScanner, bool) {
	if e == "" || e[0] != '/' {
		return pointerScanner{}, false
	}
	return pointerScanner{rest: string(e[1:]), more: true}, true
}

type pointerScanner struct {
	rest string
	more bool
}

// next returns one decoded pointer segment, whether one was present, and
// whether its escaping was valid. Segments without "~" borrow the original
// pointer string and allocate nothing.
func (p *pointerScanner) next() (segment string, present, valid bool) {
	if !p.more {
		return "", false, true
	}
	encoded := p.rest
	if slash := strings.IndexByte(encoded, '/'); slash >= 0 {
		encoded, p.rest = encoded[:slash], encoded[slash+1:]
	} else {
		p.rest, p.more = "", false
	}
	if !strings.Contains(encoded, "~") {
		return encoded, true, true
	}

	var decoded strings.Builder
	decoded.Grow(len(encoded))
	for index := 0; index < len(encoded); index++ {
		if encoded[index] != '~' {
			decoded.WriteByte(encoded[index])
			continue
		}
		if index+1 == len(encoded) {
			return "", true, false
		}
		index++
		switch encoded[index] {
		case '0':
			decoded.WriteByte('~')
		case '1':
			decoded.WriteByte('/')
		default:
			return "", true, false
		}
	}
	return decoded.String(), true, true
}

// lookup descends through value without first materializing the pointer's
// segments. JSON-domain maps and arrays remain allocation-free. A typed Go
// value is converted through JSON at most once, after which the rest of the
// walk stays in the JSON domain.
func (p *pointerScanner) lookup(value any) (any, bool, error) {
	cursor := jsonCursor{value: value}
	for {
		segment, present, valid := p.next()
		if !valid {
			return nil, false, nil
		}
		if !present {
			return cursor.value, true, nil
		}
		found, err := cursor.descend(segment)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, nil
		}
	}
}

// jsonCursor owns one walk through a possibly typed Go value. Conversion is
// delayed until the first segment that cannot be resolved in the JSON domain,
// and then performed at most once.
type jsonCursor struct {
	value     any
	converted bool
}

func (j *jsonCursor) descend(segment string) (bool, error) {
	switch current := j.value.(type) {
	case map[string]any:
		next, ok := current[segment]
		if ok {
			j.value = next
		}
		return ok, nil
	case []any:
		index, ok := pointerToken(segment).index(len(current))
		if ok {
			j.value = current[index]
		}
		return ok, nil
	default:
		if j.converted {
			return false, nil
		}
		if err := j.convert(); err != nil {
			return false, err
		}
		return j.descend(segment)
	}
}

func (j *jsonCursor) convert() error {
	encoded, err := json.Marshal(j.value)
	if err != nil {
		return fmt.Errorf("encode value as JSON: %w", err)
	}
	j.value, err = jsonDocument(encoded).value()
	if err != nil {
		return fmt.Errorf("read encoded JSON value: %w", err)
	}
	j.converted = true
	return nil
}

type pointerToken string

func (p pointerToken) isArrayIndex() bool {
	if p == "" || (len(p) > 1 && p[0] == '0') {
		return false
	}
	for offset := range len(p) {
		if p[offset] < '0' || p[offset] > '9' {
			return false
		}
	}
	return true
}

// index implements RFC 6901's array-index grammar without depending on the
// machine word size. Tokens such as "+1" and "01" are object keys, not
// canonical array indexes.
func (p pointerToken) index(length int) (int, bool) {
	const decimalRadix = 10

	if !p.isArrayIndex() || length <= 0 {
		return 0, false
	}
	var value int
	for offset := range len(p) {
		digit := int(p[offset] - '0')
		// Once an index reaches length, appending another decimal digit cannot
		// make it valid. Checking before multiplication also prevents overflow.
		limit := length - 1
		if digit > limit || value > (limit-digit)/decimalRadix {
			return 0, false
		}
		value = value*decimalRadix + digit
	}
	return value, true
}
