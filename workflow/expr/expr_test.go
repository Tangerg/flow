package expr_test

import (
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/Tangerg/flow/workflow"
	"github.com/Tangerg/flow/workflow/expr"
)

// store builds a Store from nodeID.key pairs.
func store(pairs ...any) workflow.Store {
	s := workflow.NewStore()
	for i := 0; i+1 < len(pairs); i += 2 {
		ref := pairs[i].(string)
		nodeID, key, _ := strings.Cut(ref, ".")
		s = s.With(nodeID, key, pairs[i+1])
	}
	return s
}

func TestEval(t *testing.T) {
	s := store(
		"n.output", 7,
		"f.output", 2.5,
		"s.output", "beta",
		"b.output", true,
		"list.output", []any{10, 20, 30},
		"obj.output", map[string]any{"k": "v", "deep": map[string]any{"x": 1}},
		"null.output", nil,
	)

	tests := map[string]any{
		// Literals and references.
		"42":                int64(42),
		"1.5":               1.5,
		`"hi"`:              "hi",
		"true":              true,
		"nil":               nil,
		"n.output":          int64(7),
		"f.output":          2.5,
		"s.output":          "beta",
		"list.output[1]":    int64(20),
		"obj.output.k":      "v",
		"obj.output.deep.x": int64(1),
		`obj.output["k"]`:   "v",
		"null.output":       nil,

		// Arithmetic. Integer division truncates as Go's does.
		"n.output + 3":        int64(10),
		"n.output - 10":       int64(-3),
		"n.output * 2":        int64(14),
		"n.output / 2":        int64(3),
		"n.output % 4":        int64(3),
		"n.output + f.output": 9.5,
		"-n.output":           int64(-7),
		"-f.output":           -2.5,
		"(1 + 2) * 3":         int64(9),
		`s.output + "!"`:      "beta!",

		// Comparison, including across int64 and float64.
		"n.output > 5":       true,
		"n.output >= 7":      true,
		"n.output < 5":       false,
		"n.output <= 7":      true,
		"n.output == 7":      true,
		"n.output == 7.0":    true,
		"f.output == 2.5":    true,
		"n.output != 7":      false,
		`s.output == "beta"`: true,
		`s.output < "gamma"`: true,
		"b.output == true":   true,
		"null.output == nil": true,
		"n.output == nil":    false,
		`n.output == "7"`:    false,

		// Logical operators.
		"b.output && n.output > 5": true,
		"b.output && n.output > 9": false,
		"false || n.output > 5":    true,
		"!b.output":                false,

		// Functions.
		"len(list.output)":     int64(3),
		"len(s.output)":        int64(4),
		"len(obj.output)":      int64(2),
		"has(n.output)":        true,
		"has(missing.output)":  false,
		"len(list.output) > 2": true,
	}

	for src, want := range tests {
		t.Run(src, func(t *testing.T) {
			e, err := expr.Parse(src)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got, err := e.Eval(s)
			if err != nil {
				t.Fatalf("Eval: %v", err)
			}
			if got != want {
				t.Fatalf("Eval = %#v (%T); want %#v (%T)", got, got, want, want)
			}
		})
	}
}

// TestEval_operatorMatrix walks every operator against each operand kind it
// accepts, so no combination is left to chance.
func TestEval_operatorMatrix(t *testing.T) {
	s := store("i.output", 7, "j.output", 2, "x.output", 7.5, "y.output", 2.5,
		"s.output", "beta", "t.output", "gamma")

	tests := map[string]any{
		// Integers.
		"i.output + j.output":  int64(9),
		"i.output - j.output":  int64(5),
		"i.output * j.output":  int64(14),
		"i.output / j.output":  int64(3),
		"i.output % j.output":  int64(1),
		"i.output < j.output":  false,
		"i.output <= j.output": false,
		"i.output > j.output":  true,
		"i.output >= j.output": true,
		"i.output == j.output": false,
		"i.output != j.output": true,

		// Floats.
		"x.output + y.output":  10.0,
		"x.output - y.output":  5.0,
		"x.output * y.output":  18.75,
		"x.output / y.output":  3.0,
		"x.output < y.output":  false,
		"x.output <= y.output": false,
		"x.output > y.output":  true,
		"x.output >= y.output": true,
		"x.output == y.output": false,
		"x.output != y.output": true,

		// Mixed integer and float promote to float.
		"i.output + y.output":  9.5,
		"i.output - y.output":  4.5,
		"i.output * y.output":  17.5,
		"i.output / y.output":  2.8,
		"i.output < y.output":  false,
		"i.output <= y.output": false,
		"i.output > y.output":  true,
		"i.output >= y.output": true,
		"i.output != y.output": true,

		// Strings.
		"s.output + t.output":  "betagamma",
		"s.output < t.output":  true,
		"s.output <= t.output": true,
		"s.output > t.output":  false,
		"s.output >= t.output": false,
		"s.output == t.output": false,
		"s.output != t.output": true,

		// Booleans and nil support only equality.
		"true == false": false,
		"true != false": true,
		"nil == nil":    true,
		"nil != nil":    false,
		`nil == "x"`:    false,
		"true == 1":     false,
		"1.5 == true":   false,
		"nil == 1":      false,
	}

	for src, want := range tests {
		t.Run(src, func(t *testing.T) {
			got, err := expr.MustParse(src).Eval(s)
			if err != nil {
				t.Fatalf("Eval: %v", err)
			}
			if got != want {
				t.Fatalf("Eval = %#v (%T); want %#v (%T)", got, got, want, want)
			}
		})
	}
}

func TestEval_stringAndFloatRejectUnsupportedOperators(t *testing.T) {
	s := store("s.output", "beta", "t.output", "gamma", "x.output", 7.5, "y.output", 2.5)
	for _, src := range []string{
		"s.output - t.output",
		"s.output * t.output",
		"s.output / t.output",
		"s.output % t.output",
		"x.output % y.output",
	} {
		t.Run(src, func(t *testing.T) {
			if _, err := expr.MustParse(src).Eval(s); !errors.Is(err, expr.ErrType) {
				t.Fatalf("err = %v; want ErrType", err)
			}
		})
	}
}

func TestParse_indexBounds(t *testing.T) {
	// A path index is text, so it must be a literal the path can carry.
	if _, err := expr.Parse(`a.output[99999999999999999999]`); !errors.Is(err, expr.ErrSyntax) {
		t.Fatalf("oversized index err = %v; want ErrSyntax", err)
	}
	// A negative index parses as unary minus applied to an index, which is not a
	// reference chain at all.
	if _, err := expr.Parse(`a.output[-1]`); err == nil {
		t.Fatal("negative index unexpectedly parsed")
	}
	// An oversized integer literal is a syntax error too, not a silent wrap.
	if _, err := expr.Parse(`99999999999999999999`); !errors.Is(err, expr.ErrSyntax) {
		t.Fatalf("oversized literal err = %v; want ErrSyntax", err)
	}
	if _, err := expr.Parse(`1e999`); !errors.Is(err, expr.ErrSyntax) {
		t.Fatalf("oversized float err = %v; want ErrSyntax", err)
	}
}

func TestEval_normalizesEveryNumericKind(t *testing.T) {
	// A value written as any Go numeric type must compare like the same number
	// decoded from JSON as a float64.
	values := map[string]any{
		"int":     int(5),
		"int8":    int8(5),
		"int16":   int16(5),
		"int32":   int32(5),
		"int64":   int64(5),
		"uint":    uint(5),
		"uint8":   uint8(5),
		"uint16":  uint16(5),
		"uint32":  uint32(5),
		"uint64":  uint64(5),
		"uintptr": uintptr(5),
		"float32": float32(5),
		"float64": float64(5),
	}

	e := expr.MustParse("v.output == 5 && v.output == 5.0")
	for name, value := range values {
		t.Run(name, func(t *testing.T) {
			got, err := e.Bool(store("v.output", value))
			if err != nil || !got {
				t.Fatalf("Bool = %v, %v; want true", got, err)
			}
		})
	}
}

// An expression must behave the same on a fresh Store and on one restored from
// JSON, where every number is a json.Number.
func TestEval_afterJSONRoundTrip(t *testing.T) {
	original := store(
		"n.output", 7,
		"big.output", int64(math.MaxInt64),
		"f.output", 2.5,
		"s.output", "beta",
		"b.output", true,
		"list.output", []any{1, 2, 3},
	)
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var restored workflow.Store
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	tests := map[string]any{
		"n.output":                          int64(7),
		"n.output + 1":                      int64(8),
		"n.output == 7":                     true,
		"n.output > 6.5":                    true,
		"n.output % 4":                      int64(3),
		"f.output * 2":                      5.0,
		"big.output == 9223372036854775807": true, // exact, not rounded
		"len(list.output)":                  int64(3),
		`s.output == "beta"`:                true,
		"b.output":                          true,
	}
	for src, want := range tests {
		t.Run(src, func(t *testing.T) {
			got, err := expr.MustParse(src).Eval(restored)
			if err != nil {
				t.Fatalf("Eval: %v", err)
			}
			if got != want {
				t.Fatalf("Eval = %#v (%T); want %#v (%T)", got, got, want, want)
			}
		})
	}
}

func TestEval_hugeUnsignedBecomesFloat(t *testing.T) {
	// Too large for int64: becoming a float loses precision but must not wrap to
	// a negative number.
	got, err := expr.MustParse("v.output > 0").Bool(store("v.output", uint64(math.MaxUint64)))
	if err != nil || !got {
		t.Fatalf("Bool = %v, %v; want true", got, err)
	}
}

func TestEval_shortCircuits(t *testing.T) {
	s := store("n.output", 7)
	tests := map[string]bool{
		// Without short-circuiting the right operand would fail as undefined.
		"has(missing.output) && missing.output > 1": false,
		"has(n.output) || missing.output > 1":       true,
		"n.output > 100 && missing.output > 1":      false,
	}
	for src, want := range tests {
		t.Run(src, func(t *testing.T) {
			got, err := expr.MustParse(src).Bool(s)
			if err != nil {
				t.Fatalf("Bool: %v", err)
			}
			if got != want {
				t.Fatalf("Bool = %v; want %v", got, want)
			}
		})
	}
}

func TestParse_rejectsEverythingOutsideTheGrammar(t *testing.T) {
	// The whitelist is the compiler, so each of these must fail at Parse rather
	// than reaching evaluation. The host program must be unreachable: no calls
	// but the two builtins, no conversions, no assignment, no pointers.
	sources := map[string]string{
		"empty":                ``,
		"assignment":           `a.output = 1`,
		"function literal":     `func() int { return 1 }()`,
		"composite literal":    `[]int{1}`,
		"user call":            `exec("rm -rf /")`,
		"method call":          `a.output.Close()`,
		"conversion":           `int64(a.output)`,
		"channel receive":      `<-a.output`,
		"pointer deref":        `*a.output`,
		"address of":           `&a.output`,
		"bitwise and":          `a.output & 1`,
		"shift":                `a.output << 1`,
		"bare identifier":      `counter`,
		"select into literal":  `true.field`,
		"computed index":       `a.output[b.output]`,
		"dotted string index":  `a["x.y"]`,
		"empty string index":   `a[""]`,
		"unknown function":     `abs(a.output)`,
		"len without argument": `len()`,
		"len with two":         `len(a.output, b.output)`,
		"variadic":             `len(a.output...)`,
		"imaginary literal":    `1i`,
		"char literal":         `'x'`,
		"slice expression":     `a.output[1:2]`,
		"type assertion":       `a.output.(int)`,
		"trailing garbage":     `1 + `,
		"has of a non-ref":     `has(1 + 2)`,
		"has of a bare name":   `has(counter)`,
	}

	for name, src := range sources {
		t.Run(name, func(t *testing.T) {
			e, err := expr.Parse(src)
			if err == nil {
				t.Fatalf("Parse(%q) unexpectedly succeeded: %#v", src, e)
			}
			if !errors.Is(err, expr.ErrUnsupported) && !errors.Is(err, expr.ErrSyntax) {
				t.Fatalf("Parse(%q) err = %v; want ErrUnsupported or ErrSyntax", src, err)
			}
			var exprErr *expr.Error
			if !errors.As(err, &exprErr) {
				t.Fatalf("Parse(%q) err = %v; want an *expr.Error", src, err)
			}
			if exprErr.Source != src {
				t.Fatalf("Error.Source = %q; want %q", exprErr.Source, src)
			}
		})
	}
}

func TestEval_typeErrors(t *testing.T) {
	s := store(
		"n.output", 7,
		"s.output", "beta",
		"b.output", true,
		"list.output", []any{1},
	)

	for _, src := range []string{
		`n.output + s.output`,        // number and string
		`n.output && b.output`,       // number as bool
		`!n.output`,                  // number as bool
		`-s.output`,                  // string negation
		`n.output < b.output`,        // bool ordering
		`s.output - "a"`,             // string subtraction
		`f.output % 2`,               // remainder on a float, via a float literal below
		`1.5 % 2`,                    // remainder on a float
		`len(n.output)`,              // len of a number
		`list.output == list.output`, // slice equality
	} {
		t.Run(src, func(t *testing.T) {
			_, err := expr.MustParse(src).Eval(s.With("f", "output", 1.5))
			if !errors.Is(err, expr.ErrType) && !errors.Is(err, expr.ErrUndefined) {
				t.Fatalf("Eval err = %v; want ErrType", err)
			}
		})
	}
}

func TestEval_undefinedReference(t *testing.T) {
	_, err := expr.MustParse("missing.output > 1").Eval(workflow.NewStore())
	if !errors.Is(err, expr.ErrUndefined) {
		t.Fatalf("err = %v; want ErrUndefined", err)
	}
	if !strings.Contains(err.Error(), "missing.output") {
		t.Fatalf("err = %v; want it to name the reference", err)
	}
}

func TestEval_divideByZero(t *testing.T) {
	for _, src := range []string{"1 / 0", "1 % 0", "1.0 / 0.0", "1 / z.output"} {
		t.Run(src, func(t *testing.T) {
			s := store("z.output", 0)
			if _, err := expr.MustParse(src).Eval(s); !errors.Is(err, expr.ErrDivideByZero) {
				t.Fatalf("err = %v; want ErrDivideByZero", err)
			}
		})
	}
}

func TestBoolAndString_requireTheirType(t *testing.T) {
	s := store("n.output", 7, "s.output", "beta")

	if _, err := expr.MustParse("n.output").Bool(s); !errors.Is(err, expr.ErrType) {
		t.Fatalf("Bool of a number err = %v; want ErrType", err)
	}
	if _, err := expr.MustParse("n.output").String(s); !errors.Is(err, expr.ErrType) {
		t.Fatalf("String of a number err = %v; want ErrType", err)
	}
	got, err := expr.MustParse("s.output").String(s)
	if err != nil || got != "beta" {
		t.Fatalf("String = %q, %v; want beta", got, err)
	}
}

func TestRefs(t *testing.T) {
	e := expr.MustParse(`has(b.output) && a.output.items[0] > 1 && a.output.items[0] < 9 || z.flag`)
	want := []workflow.Ref{
		workflow.At("a", "output.items.0"),
		workflow.Output("b"),
		workflow.At("z", "flag"),
	}
	if got := e.Refs(); !slices.Equal(got, want) {
		t.Fatalf("Refs = %v; want %v", got, want)
	}
}

func TestSourceAndMustParsePanics(t *testing.T) {
	if got := expr.MustParse("a.output > 1").Source(); got != "a.output > 1" {
		t.Fatalf("Source = %q", got)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("MustParse did not panic on an invalid expression")
		}
	}()
	expr.MustParse("counter")
}

func TestError_reportsPosition(t *testing.T) {
	_, err := expr.Parse("a.output & 1")
	var exprErr *expr.Error
	if !errors.As(err, &exprErr) {
		t.Fatalf("err = %v; want *expr.Error", err)
	}
	if exprErr.Pos <= 0 {
		t.Fatalf("Pos = %d; want a 1-based offset", exprErr.Pos)
	}
	if !strings.Contains(exprErr.Error(), "a.output & 1") {
		t.Fatalf("Error = %q; want it to quote the source", exprErr.Error())
	}
}
