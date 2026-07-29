package workflow

import (
	"encoding/json"
	"strconv"
	"strings"
)

// pointerPath is an unescaped sequence of RFC 6901 path segments.
type pointerPath []string

func (path pointerPath) encode() string {
	var encoder pointerEncoder
	for _, segment := range path {
		encoder.write(segment)
	}
	return encoder.String()
}

type pointerEncoder struct {
	strings.Builder
}

func (encoder *pointerEncoder) write(segment string) {
	encoder.WriteByte('/')
	for _, character := range segment {
		switch character {
		case '~':
			encoder.WriteString("~0")
		case '/':
			encoder.WriteString("~1")
		default:
			encoder.WriteRune(character)
		}
	}
}

type encodedPointer string

func (pointer encodedPointer) scan() (pointerScanner, bool) {
	if pointer == "" || pointer[0] != '/' {
		return pointerScanner{}, false
	}
	return pointerScanner{rest: string(pointer[1:]), more: true}, true
}

type pointerScanner struct {
	rest string
	more bool
}

// next returns one decoded pointer segment, whether one was present, and
// whether its escaping was valid. Segments without "~" borrow the original
// pointer string and allocate nothing.
func (scanner *pointerScanner) next() (segment string, present, valid bool) {
	if !scanner.more {
		return "", false, true
	}
	encoded := scanner.rest
	if slash := strings.IndexByte(encoded, '/'); slash >= 0 {
		encoded, scanner.rest = encoded[:slash], encoded[slash+1:]
	} else {
		scanner.rest, scanner.more = "", false
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
func (scanner *pointerScanner) lookup(value any) (any, bool) {
	cursor := jsonCursor{value: value}
	for {
		segment, present, valid := scanner.next()
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

func (cursor *jsonCursor) descend(segment string) bool {
	switch current := cursor.value.(type) {
	case map[string]any:
		next, ok := current[segment]
		if ok {
			cursor.value = next
		}
		return ok
	case []any:
		index, ok := pointerToken(segment).index(len(current))
		if ok {
			cursor.value = current[index]
		}
		return ok
	default:
		if !cursor.convert() {
			return false
		}
		return cursor.descend(segment)
	}
}

func (cursor *jsonCursor) convert() bool {
	if cursor.converted {
		return false
	}
	encoded, err := json.Marshal(cursor.value)
	if err != nil {
		return false
	}
	cursor.value, err = jsonDocument(encoded).value()
	if err != nil {
		return false
	}
	cursor.converted = true
	return true
}

type pointerToken string

// index implements RFC 6901's array-index grammar. strconv.Atoi alone would
// incorrectly accept tokens such as "+1" and "01", which are object keys but
// not canonical array indexes.
func (token pointerToken) index(length int) (int, bool) {
	if token == "" || (len(token) > 1 && token[0] == '0') {
		return 0, false
	}
	for index := range len(token) {
		if token[index] < '0' || token[index] > '9' {
			return 0, false
		}
	}
	index, err := strconv.Atoi(string(token))
	return index, err == nil && index < length
}
