package expr

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/token"
	"math"
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
}

func TestExactNumericComparison_boundaries(t *testing.T) {
	signedCases := []struct {
		name      string
		number    signedNumber
		floating  floatNumber
		order     int
		unordered bool
	}{
		{name: "NaN", floating: floatNumber(math.NaN()), unordered: true},
		{name: "above int64", floating: floatNumber(float64(1 << 63)), order: -1},
		{name: "below int64", floating: -floatNumber(float64(1<<63)) * 2, order: 1},
		// 2^63 and -2^63 are the interval ends the conversion guards name. Each is
		// exactly a float64, so only an operand equal to the truncation it would
		// produce can tell the guard from a comparison that ran past it.
		{name: "at the int64 ceiling", number: math.MaxInt64, floating: floatNumber(float64(1 << 63)), order: -1},
		{name: "at the int64 floor", number: math.MinInt64, floating: -floatNumber(float64(1 << 63))},
		{name: "positive fraction", floating: 0.5, order: -1},
		{name: "negative fraction", floating: -0.5, order: 1},
	}
	for _, test := range signedCases {
		t.Run("signed "+test.name, func(t *testing.T) {
			order, unordered := test.number.compareFloat(test.floating)
			if order != test.order || unordered != test.unordered {
				t.Fatalf("compareFloat = %d, %v; want %d, %v", order, unordered, test.order, test.unordered)
			}
		})
	}

	unsignedCases := []struct {
		name     string
		number   unsignedNumber
		floating floatNumber
		order    int
	}{
		{name: "negative", number: 1, floating: -1, order: 1},
		{name: "different integer", number: 2, floating: 1, order: 1},
		{name: "fraction", number: 1, floating: 1.5, order: -1},
		// Zero is the end of the negative guard's interval. Normalization never
		// hands comparison an unsigned operand this small, so the equality is the
		// routine's own contract rather than a reachable expression.
		{name: "zero", number: 0, floating: 0},
	}
	for _, test := range unsignedCases {
		t.Run("unsigned "+test.name, func(t *testing.T) {
			order, unordered := test.number.compareFloat(test.floating)
			if order != test.order || unordered {
				t.Fatalf("compareFloat = %d, %v; want %d, false", order, unordered, test.order)
			}
		})
	}

	if _, _, ok := unsignedNumber(1).compareOperand(true); ok {
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
				if got := operator.applyOrder(expected.order, false); got != expected.want {
					t.Fatalf("applyOrder(%d) = %t; want %t", expected.order, got, expected.want)
				}
				if operator.applyOrder(expected.order, true) {
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
// cmp.Compare answer for the pair, which orders NaN below every number.
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
			order, unordered, ok := test.left.compareOperand(test.right)
			if !ok || !unordered || order != 0 {
				t.Fatalf("compareOperand = %d, %t, %t; want 0, true, true", order, unordered, ok)
			}
		})
	}
}
