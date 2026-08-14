package jsonnum_test

import (
	"errors"
	"math"
	"math/big"
	"strconv"
	"testing"

	"github.com/Tangerg/flow/internal/jsonnum"
)

func TestParseInteger(t *testing.T) {
	tests := []struct {
		text string
		want jsonnum.Integer
		err  error
	}{
		{text: "0"},
		{text: "-0.0e-999999999999999999999"},
		{text: "42", want: jsonnum.Integer{Magnitude: 42}},
		{text: "-42", want: jsonnum.Integer{Magnitude: 42, Negative: true}},
		{text: "42.000", want: jsonnum.Integer{Magnitude: 42}},
		{text: "0.004200e4", want: jsonnum.Integer{Magnitude: 42}},
		// An exponent that cancels a long fraction leaves the value behind a run of
		// zeros. Every other case here is short enough to fit whether or not those
		// zeros are dropped first; this one has 22 digits and a value of 1, so it
		// says the width judged is the number's, not the text's.
		{text: "0.000000000000000000001e21", want: jsonnum.Integer{Magnitude: 1}},
		{text: "4200e-2", want: jsonnum.Integer{Magnitude: 42}},
		{text: "4.2e1", want: jsonnum.Integer{Magnitude: 42}},
		{text: "1e2", want: jsonnum.Integer{Magnitude: 100}},
		// JSON spells the exponent with either case, so both reach the same parse.
		{text: "1E2", want: jsonnum.Integer{Magnitude: 100}},
		{text: "12E-1", err: jsonnum.ErrFractional},
		{text: "18446744073709551615.0", want: jsonnum.Integer{Magnitude: math.MaxUint64}},
		{text: "", err: jsonnum.ErrSyntax},
		{text: "-", err: jsonnum.ErrSyntax},
		{text: "+1", err: jsonnum.ErrSyntax},
		{text: "01", err: jsonnum.ErrSyntax},
		{text: ".1", err: jsonnum.ErrSyntax},
		{text: "1.", err: jsonnum.ErrSyntax},
		{text: "1x", err: jsonnum.ErrSyntax},
		{text: "1e", err: jsonnum.ErrSyntax},
		{text: "1e+", err: jsonnum.ErrSyntax},
		{text: "1e+x", err: jsonnum.ErrSyntax},
		{text: "0.1", err: jsonnum.ErrFractional},
		{text: "12e-1", err: jsonnum.ErrFractional},
		{text: "1e-999999999999999999999", err: jsonnum.ErrFractional},
		{text: "18446744073709551616", err: jsonnum.ErrRange},
		{text: "1e1000000000", err: jsonnum.ErrRange},
		{text: "1e999999999999999999999", err: jsonnum.ErrRange},
	}

	for _, test := range tests {
		t.Run(test.text, func(t *testing.T) {
			got, err := jsonnum.ParseInteger(test.text)
			if !errors.Is(err, test.err) {
				t.Fatalf("ParseInteger error = %v; want %v", err, test.err)
			}
			if got != test.want {
				t.Fatalf("ParseInteger = %+v; want %+v", got, test.want)
			}
		})
	}
}

func TestParseInteger_matchesRationalArithmetic(t *testing.T) {
	for coefficient := -100; coefficient <= 100; coefficient++ {
		for exponent := -30; exponent <= 30; exponent++ {
			text := strconv.Itoa(coefficient) + "e" + strconv.Itoa(exponent)
			rational, ok := new(big.Rat).SetString(text)
			if !ok {
				t.Fatalf("big.Rat rejected generated JSON number %q", text)
			}

			got, err := jsonnum.ParseInteger(text)
			switch {
			case !rational.IsInt():
				if !errors.Is(err, jsonnum.ErrFractional) {
					t.Fatalf("ParseInteger(%q) error = %v; want ErrFractional", text, err)
				}
			case rational.Num().BitLen() > 64:
				if !errors.Is(err, jsonnum.ErrRange) {
					t.Fatalf("ParseInteger(%q) error = %v; want ErrRange", text, err)
				}
			default:
				want := jsonnum.Integer{
					Magnitude: new(big.Int).Abs(rational.Num()).Uint64(),
					Negative:  rational.Sign() < 0,
				}
				if err != nil || got != want {
					t.Fatalf("ParseInteger(%q) = %+v, %v; want %+v", text, got, err, want)
				}
			}
		}
	}
}

func FuzzParseInteger(f *testing.F) {
	for _, seed := range []string{"", "-", "0", "-42.0e1", "1e1000000000"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		integer, err := jsonnum.ParseInteger(text)
		if err != nil {
			return
		}
		if integer.Magnitude == 0 && integer.Negative {
			t.Fatal("zero retained a negative sign")
		}
		canonical := integer.String()
		reparsed, reparsedErr := jsonnum.ParseInteger(canonical)
		if reparsedErr != nil || reparsed != integer {
			t.Fatalf(
				"canonical ParseInteger(%q) = %+v, %v; want %+v",
				canonical,
				reparsed,
				reparsedErr,
				integer,
			)
		}
	})
}

// TestInteger_widthsHoldTheirBoundsFromBothSides covers what a caller used to
// derive from the sign and the magnitude. int64's bounds are not symmetric -- the
// negative one is a magnitude no positive value may have -- so each direction is
// asked at the last value that fits and the first that does not. Unsigned has one
// bound, the sign itself.
func TestInteger_widthsHoldTheirBoundsFromBothSides(t *testing.T) {
	const beyondInt64 = uint64(math.MaxInt64) + 1
	tests := map[string]struct {
		integer      jsonnum.Integer
		signed       int64
		signedFits   bool
		unsigned     uint64
		unsignedFits bool
		text         string
	}{
		"zero": {
			integer: jsonnum.Integer{}, signedFits: true, unsignedFits: true, text: "0",
		},
		"positive": {
			integer: jsonnum.Integer{Magnitude: 42},
			signed:  42, signedFits: true,
			unsigned: 42, unsignedFits: true,
			text: "42",
		},
		"at the signed ceiling": {
			integer: jsonnum.Integer{Magnitude: math.MaxInt64},
			signed:  math.MaxInt64, signedFits: true,
			unsigned: math.MaxInt64, unsignedFits: true,
			text: "9223372036854775807",
		},
		"past the signed ceiling": {
			integer:  jsonnum.Integer{Magnitude: beyondInt64},
			unsigned: beyondInt64, unsignedFits: true,
			text: "9223372036854775808",
		},
		"at the unsigned ceiling": {
			integer:  jsonnum.Integer{Magnitude: math.MaxUint64},
			unsigned: math.MaxUint64, unsignedFits: true,
			text: "18446744073709551615",
		},
		"negative": {
			integer: jsonnum.Integer{Magnitude: 42, Negative: true},
			signed:  -42, signedFits: true,
			text: "-42",
		},
		// The magnitude the negative side accepts and the positive side does not is
		// the only value that distinguishes the two bounds from each other.
		"one above the signed floor": {
			integer: jsonnum.Integer{Magnitude: math.MaxInt64, Negative: true},
			signed:  -math.MaxInt64, signedFits: true,
			text: "-9223372036854775807",
		},
		"at the signed floor": {
			integer: jsonnum.Integer{Magnitude: beyondInt64, Negative: true},
			signed:  math.MinInt64, signedFits: true,
			text: "-9223372036854775808",
		},
		"past the signed floor": {
			integer: jsonnum.Integer{Magnitude: beyondInt64 + 1, Negative: true},
			text:    "-9223372036854775809",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			signed, signedFits := test.integer.Signed()
			if signed != test.signed || signedFits != test.signedFits {
				t.Fatalf("Signed() = %d, %t; want %d, %t", signed, signedFits, test.signed, test.signedFits)
			}
			unsigned, unsignedFits := test.integer.Unsigned()
			if unsigned != test.unsigned || unsignedFits != test.unsignedFits {
				t.Fatalf(
					"Unsigned() = %d, %t; want %d, %t",
					unsigned, unsignedFits, test.unsigned, test.unsignedFits,
				)
			}
			if got := test.integer.String(); got != test.text {
				t.Fatalf("String() = %q; want %q", got, test.text)
			}
			// The decimal spelling reads back as the same value, which is what makes
			// it the canonical one.
			if reparsed, err := jsonnum.ParseInteger(test.text); err != nil || reparsed != test.integer {
				t.Fatalf("ParseInteger(%q) = %+v, %v; want %+v", test.text, reparsed, err, test.integer)
			}
		})
	}
}
