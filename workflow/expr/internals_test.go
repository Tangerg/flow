package expr

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/token"
	"math"
	"slices"
	"testing"
)

func TestExprWrap_preservesExpressionErrors(t *testing.T) {
	original := &Error{Source: "inner", Err: ErrType}
	expression := &Expr{source: "outer"}
	if got := expression.wrap(original); got != original {
		t.Fatalf("wrap returned %v; want the original error", got)
	}
}

func TestCompiler_reportsMalformedLiteralNodes(t *testing.T) {
	compiler := new(compiler)
	invalidString := &ast.BasicLit{Kind: token.STRING, Value: `"`}
	for name, compile := range map[string]func() error{
		"literal": func() error {
			_, err := compiler.compileLiteral(invalidString)
			return err
		},
		"node ID": func() error {
			_, err := compiler.nodeID(invalidString)
			return err
		},
		"index": func() error {
			_, err := compiler.indexSegment(invalidString)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := compile(); !errors.Is(err, ErrSyntax) {
				t.Fatalf("error = %v; want ErrSyntax", err)
			}
		})
	}
}

func TestNumericNormalization_preservesOutOfRangeValues(t *testing.T) {
	if got := floatNumber(1e20).normalized(); got != float64(1e20) {
		t.Fatalf("normalized float = %#v; want 1e20", got)
	}
	invalid := json.Number("not-a-number")
	if got := jsonNumber(invalid).normalized(); got != invalid {
		t.Fatalf("normalized invalid json.Number = %#v; want original", got)
	}
	// The three float64 values with no JSON form are named as one guard, so each
	// has to be asked separately -- a guard that admitted one of them would reach
	// an encoder that cannot represent it.
	for name, value := range map[string]float64{
		"NaN":               math.NaN(),
		"positive infinity": math.Inf(1),
		"negative infinity": math.Inf(-1),
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := floatNumber(value).normalized().(float64)
			if !ok || (got != value && !math.IsNaN(value)) || (math.IsNaN(value) && !math.IsNaN(got)) {
				t.Fatalf("normalized %s = %#v; want the float64 unchanged", name, got)
			}
		})
	}
}

// TestNormalizationReadsTheDecimalItWrote pins the base each normalization path
// parses in. Every one of them writes a decimal and reads it back, and a
// single-digit value reads the same in any base — which is what every value these
// paths are otherwise asked about amounts to, because the boundary values that
// matter are all rejected by the parse and land in the big-integer path instead. A
// two-digit value is the smallest one that says base ten.
func TestNormalizationReadsTheDecimalItWrote(t *testing.T) {
	tests := []struct {
		name string
		got  any
		want any
	}{
		{name: "float to signed", got: floatNumber(42).normalized(), want: int64(42)},
		{name: "float to negative signed", got: floatNumber(-42).normalized(), want: int64(-42)},
		{name: "JSON integer", got: jsonNumber("42").normalized(), want: int64(42)},
		{name: "JSON negative integer", got: jsonNumber("-42").normalized(), want: int64(-42)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.got != test.want {
				t.Fatalf("normalized = %#v; want %#v", test.got, test.want)
			}
		})
	}
}

func TestExactNumericComparison_boundaries(t *testing.T) {
	signedCases := []struct {
		name     string
		number   signedNumber
		floating floatNumber
		want     numberComparison
	}{
		{name: "NaN", floating: floatNumber(math.NaN()), want: unorderedNumbers()},
		{name: "above int64", floating: floatNumber(float64(1 << 63)), want: orderedNumbers(-1)},
		{name: "below int64", floating: -floatNumber(float64(1<<63)) * 2, want: orderedNumbers(1)},
		// 2^63 and -2^63 are the interval ends the conversion guards name. Each is
		// exactly a float64, so only an operand equal to the truncation it would
		// produce can tell the guard from a comparison that ran past it.
		{
			name:     "at the int64 ceiling",
			number:   math.MaxInt64,
			floating: floatNumber(float64(1 << 63)),
			want:     orderedNumbers(-1),
		},
		{
			name:     "at the int64 floor",
			number:   math.MinInt64,
			floating: -floatNumber(float64(1 << 63)),
			want:     orderedNumbers(0),
		},
		{name: "positive fraction", floating: 0.5, want: orderedNumbers(-1)},
		{name: "negative fraction", floating: -0.5, want: orderedNumbers(1)},
	}
	for _, test := range signedCases {
		t.Run("signed "+test.name, func(t *testing.T) {
			if got := test.number.compareFloat(test.floating); got != test.want {
				t.Fatalf("compareFloat = %+v; want %+v", got, test.want)
			}
		})
	}

	unsignedCases := []struct {
		name     string
		number   unsignedNumber
		floating floatNumber
		want     numberComparison
	}{
		{name: "negative", number: 1, floating: -1, want: orderedNumbers(1)},
		{name: "different integer", number: 2, floating: 1, want: orderedNumbers(1)},
		{name: "fraction", number: 1, floating: 1.5, want: orderedNumbers(-1)},
		// Zero is the end of the negative guard's interval. Normalization never
		// hands comparison an unsigned operand this small, so the equality is the
		// routine's own contract rather than a reachable expression.
		{name: "zero", number: 0, floating: 0, want: orderedNumbers(0)},
		// A fraction between zero and one is on the other side of that same end: it
		// is not negative, so it has to reach the truncation and the remainder that
		// distinguishes it from the integer it truncates to.
		{name: "fraction below one", number: 0, floating: 0.5, want: orderedNumbers(-1)},
	}
	for _, test := range unsignedCases {
		t.Run("unsigned "+test.name, func(t *testing.T) {
			if got := test.number.compareFloat(test.floating); got != test.want {
				t.Fatalf("compareFloat = %+v; want %+v", got, test.want)
			}
		})
	}

	// compareUnsigned guards on the signed operand being negative, so zero is the
	// end of that interval from the signed side: it is not negative, and must
	// therefore compare as the magnitude it is rather than as less than everything.
	unsignedPairs := []struct {
		name     string
		signed   signedNumber
		unsigned unsignedNumber
		order    int
	}{
		{name: "zero and zero"},
		{name: "zero below", unsigned: 1, order: -1},
		{name: "negative", signed: -1, order: -1},
		{name: "greater magnitude", signed: 2, unsigned: 1, order: 1},
	}
	for _, test := range unsignedPairs {
		t.Run("signed against unsigned "+test.name, func(t *testing.T) {
			if order := test.signed.compareUnsigned(test.unsigned); order != test.order {
				t.Fatalf("compareUnsigned = %d; want %d", order, test.order)
			}
		})
	}

	if unsignedNumber(1).compareOperand(true).bothNumbers {
		t.Fatal("unsigned integer unexpectedly compared with bool")
	}
	if value, ok := (operand{raw: uint64(2)}).asFloat(); !ok || value != 2 {
		t.Fatalf("asFloat = %v, %v; want 2, true", value, ok)
	}
}

// Normalization leaves a value unsigned only above MaxInt64. Comparison relies
// on that rather than restating it, so pin it at the boundary every path that
// produces a number decides it on.
func TestNormalizationLeavesUnsignedOnlyAboveInt64(t *testing.T) {
	tests := []struct {
		name string
		got  any
		want any
	}{
		{name: "unsigned at the limit", got: unsignedNumber(math.MaxInt64).normalized(), want: int64(math.MaxInt64)},
		{name: "unsigned past it", got: unsignedNumber(math.MaxInt64 + 1).normalized(), want: uint64(math.MaxInt64 + 1)},
		// 2^63 encodes as 9223372036854776000, the shortest decimal that reads back
		// as the same float64, and that is the integer a Store round trip preserves.
		{name: "float past it", got: floatNumber(float64(1 << 63)).normalized(), want: uint64(9223372036854776000)},
		// A JSON integer reaches this switch only in a form ParseInt cannot read,
		// so each boundary needs the fractional spelling of itself.
		{name: "JSON integer at the limit", got: jsonNumber("9223372036854775807.0").normalized(), want: int64(math.MaxInt64)},
		{name: "JSON integer past it", got: jsonNumber("9223372036854775808.0").normalized(), want: uint64(math.MaxInt64 + 1)},
		{name: "JSON integer at the floor", got: jsonNumber("-9223372036854775808.0").normalized(), want: int64(math.MinInt64)},
		{name: "JSON integer above the floor", got: jsonNumber("-9223372036854775807.0").normalized(), want: int64(-math.MaxInt64)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Comparing as any holds the representation too: the same magnitude as
			// int64 and as uint64 are different answers here.
			if test.got != test.want {
				t.Fatalf("normalized = %#v; want %#v", test.got, test.want)
			}
		})
	}
}

// Ordering belongs to the operator rather than to the operand type: applyOrder
// answers from a numeric comparison and applyString from Go's byte order, and
// both owe the same answers to this one table. The equal column is the only one
// where the four tokens disagree with each other, which is what makes it the
// column worth stating.
func TestOrderingTokensAgreeAcrossNumbersAndStrings(t *testing.T) {
	tests := []struct {
		token               token.Token
		below, equal, above bool
	}{
		{token: token.LSS, below: true},
		{token: token.LEQ, below: true, equal: true},
		{token: token.GTR, above: true},
		{token: token.GEQ, equal: true, above: true},
	}
	for _, test := range tests {
		t.Run(test.token.String(), func(t *testing.T) {
			operator := binaryOperator{Token: test.token}
			for _, expected := range []struct {
				order       int
				left, right string
				want        bool
			}{
				{order: -1, left: "a", right: "b", want: test.below},
				{order: 0, left: "a", right: "a", want: test.equal},
				{order: 1, left: "b", right: "a", want: test.above},
			} {
				if got := operator.applyOrder(orderedNumbers(expected.order)); got != expected.want {
					t.Fatalf("applyOrder(%d) = %t; want %t", expected.order, got, expected.want)
				}
				if operator.applyOrder(unorderedNumbers()) {
					t.Fatalf("applyOrder(%d) on unordered operands = true; want false", expected.order)
				}
				got, err := operator.applyString(expected.left, expected.right)
				if err != nil || got != expected.want {
					t.Fatalf(
						"applyString(%q, %q) = %v, %v; want %t, nil",
						expected.left, expected.right, got, err, expected.want,
					)
				}
			}
		})
	}
}

// A NaN on either side makes a comparison unordered. Requiring both would let
// [cmp.Compare] answer for the pair, which orders NaN below every number.
func TestFloatComparisonIsUnorderedWhenEitherSideIsNaN(t *testing.T) {
	tests := []struct {
		name  string
		left  floatNumber
		right any
	}{
		{name: "left", left: floatNumber(math.NaN()), right: 1.5},
		{name: "right", left: 1.5, right: math.NaN()},
		{name: "both", left: floatNumber(math.NaN()), right: math.NaN()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.left.compareOperand(test.right); got != unorderedNumbers() {
				t.Fatalf("compareOperand = %+v; want %+v", got, unorderedNumbers())
			}
		})
	}
}

// TestRefListSortedUnique_leavesItsCallerSliceAlone pins the copy every helper
// here owes the slice it was handed. Sorting in place would reorder the caller's
// own slice, and the one caller inside this package never looks at it again --
// so the copy is load-bearing for anyone else and observable for nobody today.
func TestRefListSortedUnique_leavesItsCallerSliceAlone(t *testing.T) {
	collected := refList{
		{NodeID: "b", Path: "/output"},
		{NodeID: "a", Path: "/output"},
		{NodeID: "b", Path: "/output"},
	}
	original := slices.Clone(collected)

	if unique := collected.sortedUnique(); len(unique) != 2 {
		t.Fatalf("sortedUnique = %v; want the two distinct references", unique)
	}
	if !slices.Equal(collected, original) {
		t.Fatalf("sortedUnique reordered its caller's slice: %v; want %v", collected, original)
	}
}
