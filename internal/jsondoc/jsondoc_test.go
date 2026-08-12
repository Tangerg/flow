package jsondoc

import (
	"encoding/json"
	"errors"
	"fmt"
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
		{name: "multiple", data: []byte(`1 2`), want: "multiple JSON values"},
		{name: "trailing syntax", data: []byte(`1 x`), want: "invalid character"},
		{name: "invalid UTF-8", data: []byte{'"', 0xff, '"'}, want: "invalid UTF-8 at byte 2"},
		{name: "high surrogate", data: []byte(`"\ud800"`), want: "unpaired UTF-16 surrogate"},
		{name: "low surrogate", data: []byte(`"\udc00"`), want: "unpaired UTF-16 surrogate"},
		{name: "high then scalar", data: []byte(`"\ud800\u0041"`), want: "unpaired UTF-16 surrogate"},
		{name: "high then malformed", data: []byte(`"\ud800\uXXXX"`), want: "unpaired UTF-16 surrogate"},
		{name: "short escape", data: []byte(`"\u12"`), want: "invalid character"},
		{name: "unknown escape", data: []byte(`"\q"`), want: "invalid character"},
		{name: "trailing escape", data: []byte{'"', '\\'}, want: "unexpected EOF"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := codec.Value(test.data)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Value error = %v; want %q", err, test.want)
			}
		})
	}
	for _, data := range [][]byte{
		[]byte(`"plain"`),
		[]byte(`"\u0041"`),
		[]byte(`"\ud800\udc00"`),
		[]byte(`"\\ud800"`),
		[]byte(`"\"value"`),
	} {
		if _, err := codec.Value(data); err != nil {
			t.Fatalf("Value(%s): %v", data, err)
		}
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

func TestReader_reportsMalformedContainers(t *testing.T) {
	for name, data := range map[string]string{
		"token":  ``,
		"object": `{"value":1`,
		"member": `{"`,
		"array":  `[1`,
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
