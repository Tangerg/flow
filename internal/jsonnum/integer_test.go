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
		canonical := strconv.FormatUint(integer.Magnitude, 10)
		if integer.Negative {
			canonical = "-" + canonical
		}
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
