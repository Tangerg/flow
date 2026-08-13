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

type ambiguousJSON struct{}

func (ambiguousJSON) MarshalJSON() ([]byte, error) {
	return []byte(`{"same":1,"same":2}`), nil
}

// store builds a Store from nodeID.key pairs.
func store(pairs ...any) workflow.Store {
	s := workflow.NewStore()
	for i := 0; i+1 < len(pairs); i += 2 {
		ref := pairs[i].(string)
		nodeID, key, _ := strings.Cut(ref, ".")
		s = s.WithCell(nodeID, key, pairs[i+1])
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

func TestParse_rejectsReferencesThatWorkflowPersistenceCannotRepresent(t *testing.T) {
	for _, src := range []string{
		`node["\xff"].output`,
		`value.output["\xff"]`,
	} {
		t.Run(src, func(t *testing.T) {
			_, err := expr.Parse(src)
			if !errors.Is(err, expr.ErrUnsupported) ||
				!strings.Contains(err.Error(), "not valid UTF-8") {
				t.Fatalf("Parse error = %v; want an unsupported persistent reference", err)
			}
		})
	}
}

func TestParse_reportsNestedCompilationErrors(t *testing.T) {
	for _, src := range []string{
		`(a + b)[0]`,
		`a.output[1.5]`,
		`-counter`,
		`counter + 1`,
		`1 + counter`,
		`len(counter)`,
	} {
		if _, err := expr.Parse(src); err == nil {
			t.Fatalf("Parse(%q) unexpectedly succeeded", src)
		}
	}
}

func TestParse_enforcesTheWorkflowNestingLimit(t *testing.T) {
	tests := map[string]struct {
		atLimit string
		tooDeep string
	}{
		"expression tree": {
			atLimit: strings.Repeat("!", workflow.MaxNestingDepth-1) + "true",
			tooDeep: strings.Repeat("!", workflow.MaxNestingDepth) + "true",
		},
		"reference chain": {
			atLimit: "root" + strings.Repeat(".field", workflow.MaxNestingDepth-1),
			tooDeep: "root" + strings.Repeat(".field", workflow.MaxNestingDepth),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := expr.Parse(test.atLimit); err != nil {
				t.Fatalf("Parse at limit: %v", err)
			}
			_, err := expr.Parse(test.tooDeep)
			if !errors.Is(err, expr.ErrUnsupported) ||
				!errors.Is(err, workflow.ErrMaxDepth) {
				t.Fatalf("Parse above limit error = %v; want ErrUnsupported and ErrMaxDepth", err)
			}
		})
	}
}

func TestEval_reportsRightOperandFailure(t *testing.T) {
	if _, err := expr.MustParse(`1 + missing.output`).Eval(workflow.NewStore()); !errors.Is(err, expr.ErrUndefined) {
		t.Fatalf("error = %v; want ErrUndefined", err)
	}
}

func TestEval_preservesNestedStoreResolutionFailure(t *testing.T) {
	s := workflow.NewStore().WithOutput("bad", ambiguousJSON{})
	_, err := expr.MustParse(`bad.output.field`).Eval(s)
	if !errors.Is(err, expr.ErrType) ||
		!errors.Is(err, workflow.ErrTypeMismatch) ||
		errors.Is(err, expr.ErrUndefined) ||
		!strings.Contains(err.Error(), `duplicate object member "same"`) {
		t.Fatalf("Eval error = %v; want expression and Store type errors", err)
	}

	_, err = expr.MustParse(`has(bad.output.field)`).Bool(s)
	if !errors.Is(err, expr.ErrType) ||
		!errors.Is(err, workflow.ErrTypeMismatch) ||
		errors.Is(err, expr.ErrUndefined) ||
		!strings.Contains(err.Error(), `duplicate object member "same"`) {
		t.Fatalf("has error = %v; want expression and Store type errors", err)
	}
}

func TestParse_integerIndexesUseTheStorePathRepresentation(t *testing.T) {
	s := store("list.output", []any{"zero", "one", "two"})
	for src, want := range map[string]string{
		"list.output[0x1]": "one",
		"list.output[0o2]": "two",
	} {
		t.Run(src, func(t *testing.T) {
			e := expr.MustParse(src)
			got, err := e.String(s)
			if err != nil || got != want {
				t.Fatalf("String = %q, %v; want %q", got, err, want)
			}
			if refs := e.Refs(); len(refs) != 1 || refs[0].Path != "/output/"+map[string]string{
				"list.output[0x1]": "1",
				"list.output[0o2]": "2",
			}[src] {
				t.Fatalf("Refs = %v; want a decimal Store path", refs)
			}
		})
	}
}

func TestEval_stringIndexesAddressEveryJSONKey(t *testing.T) {
	s := workflow.NewStore().WithOutput("a", map[string]any{
		"":    "empty",
		"x.y": "dot",
		"x/y": "slash",
		"x~y": "tilde",
	})
	for src, want := range map[string]string{
		`a.output[""]`:    "empty",
		`a.output["x.y"]`: "dot",
		`a.output["x/y"]`: "slash",
		`a.output["x~y"]`: "tilde",
	} {
		t.Run(src, func(t *testing.T) {
			got, err := expr.MustParse(src).String(s)
			if err != nil || got != want {
				t.Fatalf("String = %q, %v; want %q", got, err, want)
			}
		})
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

// Ordinary JSON-compatible values must behave the same on a fresh Store and on
// one restored from JSON, where every number is a json.Number.
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
			for _, current := range []workflow.Store{original, restored} {
				got, evalErr := expr.MustParse(src).Eval(current)
				if evalErr != nil {
					t.Fatalf("Eval: %v", evalErr)
				}
				if got != want {
					t.Fatalf("Eval = %#v (%T); want %#v (%T)", got, got, want, want)
				}
			}
		})
	}
}

func TestEval_mixedNumberComparisonIsExact(t *testing.T) {
	s := store("v.output", int64(9_007_199_254_740_993))
	tests := map[string]bool{
		"v.output == 9007199254740992.0": false,
		"9007199254740992.0 == v.output": false,
		"v.output != 9007199254740992.0": true,
		"9007199254740992.0 != v.output": true,
		"v.output > 9007199254740992.0":  true,
		"9007199254740992.0 < v.output":  true,
	}
	for src, want := range tests {
		t.Run(src, func(t *testing.T) {
			got, err := expr.MustParse(src).Bool(s)
			if err != nil || got != want {
				t.Fatalf("Bool = %v, %v; want %v", got, err, want)
			}
		})
	}
}

func TestEval_integralJSONNumbersRemainExact(t *testing.T) {
	var restored workflow.Store
	if err := json.Unmarshal([]byte(`{
		"decimal":{"output":9007199254740993.0},
		"exponent":{"output":9.007199254740993e15},
		"negative":{"output":-9007199254740993.0},
		"minimum":{"output":-9223372036854775808.0},
		"unsigned":{"output":18446744073709551615.0}
	}`), &restored); err != nil {
		t.Fatalf("Unmarshal Store: %v", err)
	}

	tests := map[string]any{
		"decimal.output":                          int64(9_007_199_254_740_993),
		"exponent.output":                         int64(9_007_199_254_740_993),
		"negative.output":                         int64(-9_007_199_254_740_993),
		"minimum.output":                          int64(math.MinInt64),
		"unsigned.output":                         uint64(math.MaxUint64),
		"decimal.output == 9007199254740993":      true,
		"exponent.output == 9007199254740993":     true,
		"unsigned.output == 18446744073709551615": true,
	}
	for src, want := range tests {
		t.Run(src, func(t *testing.T) {
			got, err := expr.MustParse(src).Eval(restored)
			if err != nil || got != want {
				t.Fatalf("Eval = %#v, %v; want %#v", got, err, want)
			}
		})
	}
}

func TestEval_containerEqualityIsSymmetric(t *testing.T) {
	s := store(
		"list.output", []any{},
		"object.output", map[string]any{},
	)
	for _, src := range []string{
		"nil == list.output",
		"nil != list.output",
		"list.output == nil",
		"list.output == list.output",
		"nil == object.output",
		"object.output == nil",
		"object.output == object.output",
	} {
		t.Run(src, func(t *testing.T) {
			if _, err := expr.MustParse(src).Eval(s); !errors.Is(err, expr.ErrType) {
				t.Fatalf("err = %v; want ErrType", err)
			}
		})
	}
}

func TestEval_lenAcceptsConcreteContainersBeforeAndAfterJSON(t *testing.T) {
	type words []string
	type object map[string]int
	original := store(
		"slice.output", words{"a", "b"},
		"array.output", [3]int{1, 2, 3},
		"object.output", object{"a": 1, "b": 2},
	)
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var restored workflow.Store
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for _, s := range []workflow.Store{original, restored} {
		for src, want := range map[string]int64{
			"len(slice.output)":  int64(2),
			"len(array.output)":  int64(3),
			"len(object.output)": int64(2),
		} {
			got, evalErr := expr.MustParse(src).Eval(s)
			if evalErr != nil || got != want {
				t.Fatalf("%s = %v, %v; want %d", src, got, evalErr, want)
			}
		}
	}
}

func TestParse_quotedNodeID(t *testing.T) {
	e := expr.MustParse(`node["load-user"].output == 7 && has(node["space id"].result)`)
	s := workflow.NewStore().
		WithOutput("load-user", 7).
		WithCell("space id", "result", true)
	got, err := e.Bool(s)
	if err != nil || !got {
		t.Fatalf("Bool = %v, %v; want true", got, err)
	}
	want := []workflow.Ref{
		workflow.Output("load-user"),
		workflow.At("space id", "result"),
	}
	slices.SortFunc(want, func(a, b workflow.Ref) int {
		if a.NodeID < b.NodeID {
			return -1
		}
		if a.NodeID > b.NodeID {
			return 1
		}
		return strings.Compare(a.Path, b.Path)
	})
	if refs := e.Refs(); !slices.Equal(refs, want) {
		t.Fatalf("Refs = %v; want %v", refs, want)
	}
	for _, src := range []string{`node[""].output`, `node[1].output`, `node[id].output`} {
		if _, parseErr := expr.Parse(src); !errors.Is(parseErr, expr.ErrUnsupported) {
			t.Fatalf("Parse(%q) err = %v; want ErrUnsupported", src, parseErr)
		}
	}
}

func TestExprRefs_returnsACopy(t *testing.T) {
	e := expr.MustParse("a.output == b.output")
	refs := e.Refs()
	refs[0] = workflow.Output("changed")
	if got := e.Refs(); got[0].NodeID != "a" {
		t.Fatalf("Refs leaked Expr storage: %v", got)
	}
}

func TestEval_hugeUnsignedStaysExactAcrossJSON(t *testing.T) {
	original := store("v.output", uint64(math.MaxUint64))
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var restored workflow.Store
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for _, s := range []workflow.Store{original, restored} {
		for src, want := range map[string]any{
			"v.output":                          uint64(math.MaxUint64),
			"v.output == 18446744073709551615":  true,
			"v.output > 18446744073709551614":   true,
			"v.output < 18446744073709551616.0": true,
			"v.output % 2":                      uint64(1),
		} {
			got, evalErr := expr.MustParse(src).Eval(s)
			if evalErr != nil || got != want {
				t.Fatalf("%s = %#v, %v; want %#v", src, got, evalErr, want)
			}
		}
	}

	if got, evalErr := expr.MustParse("-9223372036854775808").Eval(workflow.NewStore()); evalErr != nil || got != int64(math.MinInt64) {
		t.Fatalf("minimum int64 literal = %#v, %v", got, evalErr)
	}
	if _, evalErr := expr.MustParse("-18446744073709551615").Eval(workflow.NewStore()); !errors.Is(evalErr, expr.ErrType) {
		t.Fatalf("negating MaxUint64 err = %v; want ErrType", evalErr)
	}
}

func TestEval_numericSemanticsSurviveStoreJSON(t *testing.T) {
	original := workflow.NewStore().
		WithOutput("whole", float64(7)).
		WithOutput("single", float32(1.2)).
		WithOutput("boundary", float64(1<<63))
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var restored workflow.Store
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for src, want := range map[string]any{
		"whole.output / 2":                       int64(3),
		"single.output == 1.2":                   true,
		"boundary.output == 9223372036854776000": true,
	} {
		t.Run(src, func(t *testing.T) {
			e := expr.MustParse(src)
			before, beforeErr := e.Eval(original)
			after, afterErr := e.Eval(restored)
			if beforeErr != nil || afterErr != nil || before != want || after != want {
				t.Fatalf("before = %#v, %v; after = %#v, %v; want %#v",
					before, beforeErr, after, afterErr, want)
			}
		})
	}
}

func TestEval_unsignedArithmeticAndSpecialFloats(t *testing.T) {
	maxUint := uint64(math.MaxUint64)
	s := store(
		"u.output", maxUint,
		"v.output", maxUint-1,
		"negative.output", int64(-1),
		"zero.output", 0,
		"nan.output", math.NaN(),
		"inf.output", math.Inf(1),
	)
	tests := map[string]any{
		"u.output + 1":                                 uint64(0),
		"1 + u.output":                                 uint64(0),
		"u.output - 1":                                 maxUint - 1,
		"u.output * 2":                                 maxUint - 1,
		"u.output / 2":                                 maxUint / 2,
		"u.output % 2":                                 uint64(1),
		"u.output > v.output":                          true,
		"u.output >= v.output":                         true,
		"v.output < u.output":                          true,
		"v.output <= u.output":                         true,
		"negative.output < u.output":                   true,
		"u.output > negative.output":                   true,
		"u.output < 18446744073709551616.0":            true,
		"18446744073709551616.0 > u.output":            true,
		"u.output < inf.output":                        true,
		"nan.output == nan.output":                     false,
		"nan.output != nan.output":                     true,
		"nan.output < u.output":                        false,
		"u.output >= nan.output":                       false,
		"9223372036854775808 == 9223372036854775808.0": true,
	}
	for src, want := range tests {
		t.Run(src, func(t *testing.T) {
			got, err := expr.MustParse(src).Eval(s)
			if err != nil || got != want {
				t.Fatalf("Eval = %#v, %v; want %#v", got, err, want)
			}
		})
	}

	for _, src := range []string{
		"u.output + negative.output",
		"negative.output + u.output",
	} {
		if _, err := expr.MustParse(src).Eval(s); !errors.Is(err, expr.ErrType) {
			t.Fatalf("%s err = %v; want ErrType", src, err)
		}
	}
	for _, src := range []string{"u.output / zero.output", "u.output % zero.output"} {
		if _, err := expr.MustParse(src).Eval(s); !errors.Is(err, expr.ErrDivideByZero) {
			t.Fatalf("%s err = %v; want ErrDivideByZero", src, err)
		}
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
			_, err := expr.MustParse(src).Eval(s.WithCell("f", "output", 1.5))
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
		workflow.At("a", "output", "items", "0"),
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

// A call is checked by name before arity. Reporting how many arguments a
// function takes describes a signature, which a function that does not exist does
// not have — so the same unknown name must not read as a real function used
// wrongly just because the argument count happens to differ.
func TestParse_reportsAnUnknownFunctionBeforeItsArity(t *testing.T) {
	for _, src := range []string{"nope()", "nope(1)", "nope(1, 2)"} {
		t.Run(src, func(t *testing.T) {
			_, err := expr.Parse(src)
			if err == nil || !strings.Contains(err.Error(), `unknown function "nope"`) {
				t.Fatalf("Parse(%q) = %v; want an unknown-function error", src, err)
			}
		})
	}
	for _, src := range []string{"len()", "len(1, 2)", "has(a.output, b.output)"} {
		t.Run(src, func(t *testing.T) {
			_, err := expr.Parse(src)
			if err == nil || !strings.Contains(err.Error(), "takes exactly one argument") {
				t.Fatalf("Parse(%q) = %v; want an arity error", src, err)
			}
		})
	}
}
