package workflow

import (
	"encoding/json"
	"strconv"
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

func (p *pointerEncoder) write(segment string) {
	p.WriteByte('/')
	for _, character := range segment {
		switch character {
		case '~':
			p.WriteString("~0")
		case '/':
			p.WriteString("~1")
		default:
			p.WriteRune(character)
		}
	}
}

type encodedPointer string

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
func (p *pointerScanner) lookup(value any) (any, bool) {
	cursor := jsonCursor{value: value}
	for {
		segment, present, valid := p.next()
		if !valid {
			return nil, false
		}
		if !present {
			return cursor.value, true
		}
		if !cursor.descend(segment) {
			return nil, false
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

func (j *jsonCursor) descend(segment string) bool {
	switch current := j.value.(type) {
	case map[string]any:
		next, ok := current[segment]
		if ok {
			j.value = next
		}
		return ok
	case []any:
		index, ok := pointerToken(segment).index(len(current))
		if ok {
			j.value = current[index]
		}
		return ok
	default:
		if !j.convert() {
			return false
		}
		return j.descend(segment)
	}
}

func (j *jsonCursor) convert() bool {
	if j.converted {
		return false
	}
	encoded, err := json.Marshal(j.value)
	if err != nil {
		return false
	}
	j.value, err = jsonDocument(encoded).value()
	if err != nil {
		return false
	}
	j.converted = true
	return true
}

type pointerToken string

// index implements RFC 6901's array-index grammar. strconv.Atoi alone would
// incorrectly accept tokens such as "+1" and "01", which are object keys but
// not canonical array indexes.
func (p pointerToken) index(length int) (int, bool) {
	if p == "" || (len(p) > 1 && p[0] == '0') {
		return 0, false
	}
	for index := range len(p) {
		if p[index] < '0' || p[index] > '9' {
			return 0, false
		}
	}
	index, err := strconv.Atoi(string(p))
	return index, err == nil && index < length
}
