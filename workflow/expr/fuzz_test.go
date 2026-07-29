package expr_test

import (
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
		With("a", "k", "text").
		With("b", "flag", true).
		With("c", "list", []any{1, 2}).
		With("c", "obj", map[string]any{"x": 1}).
		With("d", "null", nil).
		With("d", "chan", make(chan int))

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

		// Every accessor must return, not panic, whatever the Store holds.
		_, _ = e.Eval(s)
		_, _ = e.Bool(s)
		_, _ = e.String(s)
		_, _ = e.Eval(workflow.NewStore())
	})
}
