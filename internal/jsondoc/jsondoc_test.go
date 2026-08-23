package jsondoc

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

type countingMarshaler struct {
	calls *int
	data  string
}

func (m countingMarshaler) MarshalJSON() ([]byte, error) {
	(*m.calls)++
	return []byte(m.data), nil
}

func TestCodec(t *testing.T) {
	codec := Codec{MaxDepth: 4}
	var target struct {
		Number json.Number `json:"number"`
	}
	if err := codec.Decode([]byte(`{"number":1.0}`), &target); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if target.Number != "1.0" {
		t.Fatalf("number = %q; want 1.0", target.Number)
	}
	if err := codec.Validate([]byte(`[true,null,{"value":"ok"}]`)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	// Value returns what a caller reads, so the document has to come back as
	// itself. Numbers arrive as json.Number because that is the only form a
	// re-encoding can reproduce exactly. Checking the whole value is what says the
	// containers have the members they were given and no others -- an
	// error-only check cannot see an array that came back one element longer.
	value, err := codec.Value([]byte(`{"a":[1,"two",[3]],"b":null}`))
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	want := map[string]any{
		"a": []any{json.Number("1"), "two", []any{json.Number("3")}},
		"b": nil,
	}
	if !reflect.DeepEqual(value, want) {
		t.Fatalf("Value = %#v; want %#v", value, want)
	}
	// A destination that takes any value must receive the number as text. Only an
	// untyped field says so: a json.Number field is filled from the document either
	// way, and float64 is where an integer too large for one loses digits.
	var untyped struct {
		Value any `json:"value"`
	}
	if err := codec.Decode([]byte(`{"value":9007199254740993}`), &untyped); err != nil {
		t.Fatalf("Decode untyped: %v", err)
	}
	if untyped.Value != json.Number("9007199254740993") {
		t.Fatalf("untyped value = %#v; want json.Number", untyped.Value)
	}
	if err := codec.Decode([]byte(`{"unknown":1}`), &target); err == nil {
		t.Fatal("Decode accepted an unknown field")
	}
	if err := codec.Decode([]byte(`{`), &target); err == nil {
		t.Fatal("Decode accepted malformed JSON")
	}
	if err := codec.DecodeParsed([]byte(`{`), &target); err == nil {
		t.Fatal("DecodeParsed accepted malformed JSON")
	}
}

func TestCodecMarshal(t *testing.T) {
	codec := Codec{MaxDepth: 4}
	calls := 0
	data, err := codec.Marshal(countingMarshaler{calls: &calls, data: `{"value":1}`})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(data) != `{"value":1}` || calls != 1 {
		t.Fatalf("Marshal = %s with %d calls; want one exact call", data, calls)
	}

	if _, err := codec.Marshal(make(chan int)); err == nil {
		t.Fatal("Marshal accepted an unsupported value")
	}
	if _, err := codec.Marshal(countingMarshaler{
		calls: &calls,
		data:  `{"same":1,"same":2}`,
	}); err == nil || !strings.Contains(err.Error(), "duplicate object member") {
		t.Fatalf("Marshal duplicate error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("MarshalJSON calls = %d; want exactly one per invocation", calls)
	}
}

func TestCodec_rejectsAmbiguousOrLossyDocuments(t *testing.T) {
	codec := Codec{MaxDepth: 8}
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{name: "duplicate", data: []byte(`{"a/b":{"~key":1,"~key":2}}`), want: `duplicate object member "~key" at /a~1b/~0key`},
		// An array contributes its index to the path, counted from zero as RFC 6901
		// requires. Only a failure inside an array reports one.
		{name: "duplicate in an array", data: []byte(`[0,{"k":1,"k":2}]`), want: `duplicate object member "k" at /1/k`},
		{name: "multiple", data: []byte(`1 2`), want: "multiple JSON values"},
		{name: "trailing syntax", data: []byte(`1 x`), want: "invalid character"},
		{name: "invalid UTF-8", data: []byte{'"', 0xff, '"'}, want: "invalid UTF-8 at byte 2"},
		{name: "high surrogate", data: []byte(`"\ud800"`), want: "unpaired UTF-16 surrogate"},
		{name: "low surrogate", data: []byte(`"\udc00"`), want: "unpaired UTF-16 surrogate"},
		{name: "high then scalar", data: []byte(`"\ud800\u0041"`), want: "unpaired UTF-16 surrogate"},
		{name: "high then malformed", data: []byte(`"\ud800\uXXXX"`), want: "unpaired UTF-16 surrogate"},
		{name: "short escape", data: []byte(`"\u12"`)},
		{name: "unknown escape", data: []byte(`"\q"`)},
		{name: "trailing escape", data: []byte{'"', '\\'}, want: "unexpected EOF"},
		// Each surrogate range ends where the other begins, so its last code unit
		// is the one a boundary can misplace: 0xDBFF is still a high surrogate and
		// 0xDFFF is still a low one.
		{name: "highest high surrogate", data: []byte(`"\udbff"`), want: "unpaired UTF-16 surrogate"},
		{name: "highest low surrogate", data: []byte(`"\udfff"`), want: "unpaired UTF-16 surrogate"},
		// A low surrogate is never the first unit of a pair, so one followed by
		// another is two unpaired escapes rather than a pair -- the case that says
		// which range the first unit was matched against.
		{name: "low then low", data: []byte(`"\udc00\udc00"`), want: "unpaired UTF-16 surrogate"},
		// 0xE000 is the first code unit above the surrogate range: too high to
		// complete a pair after a high surrogate, and valid on its own.
		{name: "high then above the range", data: []byte(`"\ud800\ue000"`), want: "unpaired UTF-16 surrogate"},
		// Every other case opens its string with the escape, where a scan that read
		// only the first character would still find it. Ordinary text in front is
		// what says the scan reads on through a string rather than just into it.
		{name: "escape after text", data: []byte(`"ab\ud800"`), want: "unpaired UTF-16 surrogate"},
		// The offset is 1-based and names the backslash that opened the escape, not
		// the code unit inside it.
		{name: "reported position", data: []byte(`{"k":"\ud800"}`), want: "unpaired UTF-16 surrogate escape at byte 7"},
		// 0x80 is the first byte that cannot stand alone, so it is the one an
		// invalid-UTF-8 bound can let through.
		{name: "lowest continuation byte", data: []byte{'"', 0x80, '"'}, want: "invalid UTF-8 at byte 2"},
		// This scan starts at the first byte, which only a document whose very first
		// byte is invalid can say. Such a document is not JSON either, so the message
		// is what distinguishes the scan from the decoder behind it.
		{name: "invalid first byte", data: []byte{0xff}, want: "invalid UTF-8 at byte 1"},
		// This pass runs before the JSON decoder, so it reads whatever a caller
		// passed. Each input below ends inside the escape the scan is examining,
		// which is the only way to reach a bound with nothing behind it.
		{name: "unterminated string", data: []byte(`"abc`), want: "unexpected EOF"},
		{name: "escape ends the input", data: []byte(`"\ud800`), want: "unpaired UTF-16 surrogate"},
		{name: "backslash ends the input", data: []byte(`"\ud800\`), want: "unpaired UTF-16 surrogate"},
		// A high surrogate, then an escape that is not \u, then four characters
		// that would read as a low surrogate if the scan looked past the escape.
		{name: "escape that is not a code unit", data: []byte(`"\ud800\ndc00"`), want: "unpaired UTF-16 surrogate"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := codec.Value(test.data)
			if err == nil {
				t.Fatal("Value accepted an invalid document")
			}
			if test.want != "" && !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Value error = %v; want %q", err, test.want)
			}
		})
	}
	for _, data := range [][]byte{
		[]byte(`"plain"`),
		[]byte(`"\u0041"`),
		[]byte(`"\ud800\udc00"`),
		// The last low surrogate still completes a pair.
		[]byte(`"\ud800\udfff"`),
		// The first code unit above the surrogate range stands alone.
		[]byte(`"\ue000"`),
		[]byte(`"\\ud800"`),
		[]byte(`"\"value"`),
	} {
		if _, err := codec.Value(data); err != nil {
			t.Fatalf("Value(%s): %v", data, err)
		}
	}

	// A complete pair that ends the input is still a pair: the document is
	// malformed for want of a closing quote, not for an unpaired escape.
	if _, err := codec.Value([]byte(`"\ud800\udc00`)); err == nil ||
		!strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("Value of a pair at end of input = %v; want a syntax error", err)
	}
}

func TestCodec_depth(t *testing.T) {
	_, err := (Codec{MaxDepth: 2}).Value([]byte(`{"a":[[]]}`))
	var depthErr *DepthError
	if !errors.As(err, &depthErr) {
		t.Fatalf("Value error = %v; want DepthError", err)
	}
	if depthErr.Path != "/a/0" || depthErr.Limit != 2 ||
		depthErr.Error() != "JSON nesting at /a/0 exceeds limit 2" {
		t.Fatalf("DepthError = %#v, %q", depthErr, depthErr.Error())
	}
}

// MaxDepth bounds how deeply a document nests, never how much it holds. The two
// are the same measurement only while every container entered is also left, so a
// walk that tracked depth apart from the path it reports could silently turn the
// limit into a cap on a document's size. Each member here sits at the limit, and
// there are more of them than the limit allows.
func TestCodec_boundsNestingNotBreadth(t *testing.T) {
	codec := Codec{MaxDepth: 2}
	document := []byte(`{"a":[1],"b":{"c":2},"d":[3],"e":{"f":4},"g":[5]}`)
	value, err := codec.Value(document)
	if err != nil {
		t.Fatalf("Value of a wide document within the depth limit: %v", err)
	}
	if members, ok := value.(map[string]any); !ok || len(members) != 5 {
		t.Fatalf("Value = %#v; want all five members", value)
	}
}

func TestReader_reportsMalformedContainers(t *testing.T) {
	for name, data := range map[string]string{
		"token":        ``,
		"object close": `{`,
		"object value": `{"value":1`,
		"member":       `{"`,
		"array close":  `[`,
		"array value":  `[1`,
	} {
		t.Run(name, func(t *testing.T) {
			decoder := json.NewDecoder(strings.NewReader(data))
			decoder.UseNumber()
			if _, err := (&reader{decoder: decoder, maxDepth: 8}).read(); err == nil {
				t.Fatalf("read accepted %q", data)
			}
		})
	}

	memberReader := reader{decoder: json.NewDecoder(strings.NewReader(`"`))}
	if _, err := memberReader.readMemberName(); err == nil {
		t.Fatal("readMemberName accepted a truncated string")
	}
}

func TestTranslateDepth(t *testing.T) {
	sentinel := errors.New("caller: too deep")

	depth := &DepthError{Path: "/a/b", Limit: 3}
	translated := TranslateDepth(depth, sentinel)
	if !errors.Is(translated, sentinel) {
		t.Fatalf("translated = %v; want the caller's sentinel", translated)
	}
	// The caller's prefix survives, and this package's name does not appear.
	for _, want := range []string{"caller: too deep", "/a/b", "limit 3"} {
		if !strings.Contains(translated.Error(), want) {
			t.Fatalf("message %q lacks %q", translated, want)
		}
	}
	if strings.Contains(translated.Error(), "jsondoc") {
		t.Fatalf("message %q names this internal package", translated)
	}

	// A wrapped DepthError is still recognized.
	if wrapped := TranslateDepth(fmt.Errorf("outer: %w", depth), sentinel); !errors.Is(wrapped, sentinel) {
		t.Fatalf("wrapped = %v; want the caller's sentinel", wrapped)
	}

	// Any other error passes through untouched.
	other := errors.New("unrelated")
	if got := TranslateDepth(other, sentinel); !errors.Is(got, other) || errors.Is(got, sentinel) {
		t.Fatalf("passthrough = %v; want the original error", got)
	}
	if TranslateDepth(nil, sentinel) != nil {
		t.Fatal("nil error did not pass through")
	}
}

// ValidateFragment exists so a caller that already assembled a document from
// fragments can check one fragment and still report the depth and path the whole
// document would have produced. Both are checked here, because reporting a
// reduced limit or a path relative to the fragment would leak the caller's
// envelope arithmetic into a public message.
func TestValidateFragmentReportsWholeDocumentDepthAndPath(t *testing.T) {
	codec := Codec{MaxDepth: 6}
	nested := func(depth int) []byte {
		document := "1"
		for range depth {
			document = "[" + document + "]"
		}
		return []byte(document)
	}

	// Three containers already entered leave three of the six for the fragment.
	at := []string{"records", "1", "value"}
	if err := codec.ValidateFragment(nested(3), at...); err != nil {
		t.Fatalf("fragment at the remaining budget: %v", err)
	}

	err := codec.ValidateFragment(nested(4), at...)
	var depthErr *DepthError
	if !errors.As(err, &depthErr) {
		t.Fatalf("fragment past the budget = %v; want a DepthError", err)
	}
	if depthErr.Limit != codec.MaxDepth {
		t.Fatalf("Limit = %d; want the document limit %d", depthErr.Limit, codec.MaxDepth)
	}
	if want := "/records/1/value/0/0/0"; depthErr.Path != want {
		t.Fatalf("Path = %q; want %q", depthErr.Path, want)
	}

	// The prefix is not written past its own length, so a caller may reuse it.
	reusable := make([]string, 3, 8)
	copy(reusable, at)
	if err := codec.ValidateFragment(nested(3), reusable...); err != nil {
		t.Fatalf("fragment with a spare-capacity prefix: %v", err)
	}
	if !slices.Equal(reusable, at) {
		t.Fatalf("prefix was modified to %v; want %v", reusable, at)
	}

	// An empty prefix is the whole-document case Value takes.
	if err := codec.ValidateFragment(nested(6)); err != nil {
		t.Fatalf("whole document at the limit: %v", err)
	}
}

// TestKindNamesEveryDecodedShape covers the vocabulary every boundary built on
// this package uses to report a wrong shape, including the fallback for a value
// that never came from Value.
func TestKindNamesEveryDecodedShape(t *testing.T) {
	codec := Codec{MaxDepth: 8}
	documents := map[string]string{
		"null":    `null`,
		"boolean": `true`,
		"number":  `1`,
		"string":  `"s"`,
		"array":   `[]`,
		"object":  `{}`,
	}
	for want, document := range documents {
		t.Run(want, func(t *testing.T) {
			value, err := codec.Value([]byte(document))
			if err != nil {
				t.Fatalf("Value: %v", err)
			}
			if got := Kind(value); got != want {
				t.Fatalf("Kind(%s) = %q; want %q", document, got, want)
			}
		})
	}
	if got := Kind(strings.NewReader("")); got == "" {
		t.Fatal("Kind returned an empty description for a value outside the JSON domain")
	}
}

func TestObjectRequiresOneJSONObject(t *testing.T) {
	codec := Codec{MaxDepth: 8}
	object, err := codec.Object([]byte(`{"a":1}`))
	if err != nil || len(object) != 1 {
		t.Fatalf("Object = %v, %v; want one member", object, err)
	}
	if _, err := codec.Object([]byte(`[]`)); err == nil ||
		!strings.Contains(err.Error(), "expected object, got array") {
		t.Fatalf("Object([]) error = %v; want a wrong-shape report", err)
	}
	if _, err := codec.Object([]byte(`{`)); err == nil {
		t.Fatal("Object accepted a malformed document")
	}
}

func TestDecodeIntoIsFailureAtomicAndReportsANilDestination(t *testing.T) {
	decode := func(data []byte) (int, error) {
		if len(data) == 0 {
			return 0, errors.New("empty")
		}
		return len(data), nil
	}
	wrapped := errors.New("boundary")
	wrap := func(err error) error { return fmt.Errorf("%w: %w", wrapped, err) }

	value := 7
	if err := DecodeInto(&value, []byte("abc"), decode, wrap); err != nil || value != 3 {
		t.Fatalf("DecodeInto = %v, value %d; want 3", err, value)
	}
	if err := DecodeInto(&value, nil, decode, wrap); !errors.Is(err, wrapped) || value != 3 {
		t.Fatalf("DecodeInto = %v, value %d; want the wrapped failure and no assignment", err, value)
	}
	err := DecodeInto((*int)(nil), []byte("abc"), decode, wrap)
	if !errors.Is(err, ErrNilReceiver) || !errors.Is(err, wrapped) {
		t.Fatalf("DecodeInto on a nil destination = %v; want a wrapped ErrNilReceiver", err)
	}
}
