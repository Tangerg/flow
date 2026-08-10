package jsondoc

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestDecoder(t *testing.T) {
	decoder := Decoder{MaxDepth: 4}
	var target struct {
		Number json.Number `json:"number"`
	}
	if err := decoder.Decode([]byte(`{"number":1.0}`), &target); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if target.Number != "1.0" {
		t.Fatalf("number = %q; want 1.0", target.Number)
	}
	if err := decoder.Validate([]byte(`[true,null,{"value":"ok"}]`)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := decoder.Decode([]byte(`{"unknown":1}`), &target); err == nil {
		t.Fatal("Decode accepted an unknown field")
	}
	if err := decoder.Decode([]byte(`{`), &target); err == nil {
		t.Fatal("Decode accepted malformed JSON")
	}
	if err := decoder.DecodeParsed([]byte(`{`), &target); err == nil {
		t.Fatal("DecodeParsed accepted malformed JSON")
	}
}

func TestDecoder_rejectsAmbiguousOrLossyDocuments(t *testing.T) {
	decoder := Decoder{MaxDepth: 8}
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
			_, err := decoder.Value(test.data)
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
		if _, err := decoder.Value(data); err != nil {
			t.Fatalf("Value(%s): %v", data, err)
		}
	}
}

func TestDecoder_depth(t *testing.T) {
	_, err := (Decoder{MaxDepth: 2}).Value([]byte(`{"a":[[]]}`))
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
