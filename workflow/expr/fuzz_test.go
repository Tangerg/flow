package expr_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/Tangerg/flow/workflow"
	"github.com/Tangerg/flow/workflow/expr"
)

// FuzzParse checks that no input makes Parse panic, and that anything Parse
// accepts can be evaluated without panicking either. Evaluation may fail — a type
// error or an undefined reference is a normal outcome — but it must always come
// back as an error.
func FuzzParse(f *testing.F) {
	seeds := []string{
		"",
		"1",
		"1.5",
		`"s"`,
		"true",
		"nil",
		"a.output",
		`node["load-user"].output`,
		"a.output.b[0]",
		`a["k"]`,
		"a.output + 1",
		"a.output / 0",
		"a.output % 0",
		"-a.output",
		"!a.output",
		"a.output > 1 && b.output < 2",
		"a.output > 1 || b.output < 2",
		"len(a.output)",
		"has(a.output)",
		"len(a.output) == len(b.output)",
		"(((a.output)))",
		"9223372036854775808",
		"18446744073709551615",
		"1e309",
		"counter",
		"a.output & 1",
		"a.output[1:2]",
		"func(){}()",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	// A Store with a value of every shape the evaluator distinguishes.
	s := workflow.NewStore().
		WithOutput("a", 3).
		WithOutput("b", 1.5).
		WithCell("a", "k", "text").
		WithCell("b", "flag", true).
		WithCell("c", "list", []any{1, 2}).
		WithCell("c", "obj", map[string]any{"x": 1}).
		WithCell("d", "null", nil).
		WithCell("d", "chan", make(chan int))

	f.Fuzz(func(t *testing.T, src string) {
		e, err := expr.Parse(src)
		if err != nil {
			if e != nil {
				t.Fatalf("Parse(%q) returned both an Expr and an error", src)
			}
			return
		}
		if e == nil {
			t.Fatalf("Parse(%q) returned neither an Expr nor an error", src)
		}
		if e.Source() != src {
			t.Fatalf("Source = %q; want %q", e.Source(), src)
		}
		for _, ref := range e.Refs() {
			if ref.NodeID == "" || ref.Path == "" {
				t.Fatalf("Parse(%q) produced a malformed ref %#v", src, ref)
			}
		}

		// Every requested result type must return, not panic, whatever the Store holds.
		_, _ = e.Eval[any](s)
		_, _ = e.Eval[bool](s)
		_, _ = e.Eval[string](s)
		_, _ = e.Eval[any](workflow.NewStore())
	})
}

// FuzzBindingsJSON locks the configuration boundary to one strict, atomic,
// lossless contract. Any accepted document must have a stable encoded form;
// any rejected document must leave its destination unchanged.
func FuzzBindingsJSON(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte(`{}`),
		[]byte(`null`),
		[]byte(`[]`),
		[]byte(`{"conditions":{"done":"work.output == true"}}`),
		[]byte(`{"resolvers":{"route":"classify.output"}}`),
		[]byte(`{"switches":{"size":{"cases":[{"when":"n.output > 1","then":"large"}],"fallback":"small"}}}`),
		[]byte(`{"conditions":{},"conditions":{}}`),
		[]byte(`{"unknown":true}`),
		{'{', '"', 0xff, '"', ':', '1', '}'},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		target := expr.Bindings{Conditions: map[string]string{"sentinel": "true"}}
		err := json.Unmarshal(data, &target)
		if err != nil {
			if len(target.Conditions) != 1 || target.Conditions["sentinel"] != "true" ||
				target.Resolvers != nil || target.Switches != nil {
				t.Fatalf("failed Unmarshal changed receiver: %#v", target)
			}
			return
		}

		encoded, err := json.Marshal(target)
		if err != nil {
			t.Fatalf("accepted Bindings cannot be marshaled: %v", err)
		}
		var restored expr.Bindings
		if decodeErr := json.Unmarshal(encoded, &restored); decodeErr != nil {
			t.Fatalf("encoded Bindings cannot be decoded: %v", decodeErr)
		}
		reencoded, err := json.Marshal(restored)
		if err != nil {
			t.Fatalf("restored Bindings cannot be marshaled: %v", err)
		}
		if !bytes.Equal(reencoded, encoded) {
			t.Fatalf("encoding is not idempotent: got %s; want %s", reencoded, encoded)
		}
	})
}
