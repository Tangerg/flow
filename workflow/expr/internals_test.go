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
