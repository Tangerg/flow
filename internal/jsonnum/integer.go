// Package jsonnum contains bounded helpers for the JSON number domain shared
// by workflow definition, persistence, and expression boundaries.
package jsonnum

import (
	"errors"
	"math"
	"strconv"
	"strings"
)

// Integer is the sign and magnitude of a mathematical JSON integer. Zero is
// always non-negative.
type Integer struct {
	Magnitude uint64
	Negative  bool
}

// ParseInteger errors. Callers normally translate them into the vocabulary of
// their public boundary.
var (
	ErrSyntax     = errors.New("invalid JSON number")
	ErrFractional = errors.New("JSON number is not an integer")
	ErrRange      = errors.New("JSON integer exceeds uint64 magnitude")
)

const (
	decimalRadix           = 10
	maxUint64DecimalDigits = 20
)

// ParseInteger parses JSON's mathematical integer domain without converting
// through float64 or allocating in proportion to the exponent. Decimal and
// exponent spellings are accepted when their value is integral. Magnitudes
// larger than uint64 report ErrRange.
func ParseInteger(text string) (Integer, error) {
	parsed, err := (numberParser{text: text}).parse()
	if err != nil {
		return Integer{}, err
	}
	return parsed.integer()
}

type numberParser struct {
	text             string
	offset           int
	negative         bool
	integerStart     int
	integerEnd       int
	fractionStart    int
	fractionEnd      int
	exponentNegative bool
	exponent         int
}

func (n numberParser) parse() (numberParser, error) {
	if n.text == "" {
		return numberParser{}, ErrSyntax
	}
	var err error
	n, err = n.parseSign()
	if err != nil {
		return numberParser{}, err
	}
	n, err = n.parseIntegerPart()
	if err != nil {
		return numberParser{}, err
	}
	n, err = n.parseFraction()
	if err != nil {
		return numberParser{}, err
	}
	n, err = n.parseExponentPart()
	if err != nil {
		return numberParser{}, err
	}
	if n.offset != len(n.text) {
		return numberParser{}, ErrSyntax
	}
	return n, nil
}

func (n numberParser) parseSign() (numberParser, error) {
	if n.text[n.offset] == '-' {
		n.negative = true
		n.offset++
		if n.offset == len(n.text) {
			return numberParser{}, ErrSyntax
		}
	}
	return n, nil
}

func (n numberParser) parseIntegerPart() (numberParser, error) {
	n.integerStart = n.offset
	switch {
	case n.text[n.offset] == '0':
		n.offset++
		if n.offset < len(n.text) && isDigit(n.text[n.offset]) {
			return numberParser{}, ErrSyntax
		}
	case n.text[n.offset] >= '1' && n.text[n.offset] <= '9':
		n.offset = scanDigits(n.text, n.offset+1)
	default:
		return numberParser{}, ErrSyntax
	}
	n.integerEnd = n.offset
	return n, nil
}

func (n numberParser) parseFraction() (numberParser, error) {
	if n.offset >= len(n.text) || n.text[n.offset] != '.' {
		return n, nil
	}
	n.offset++
	n.fractionStart = n.offset
	n.offset = scanDigits(n.text, n.offset)
	if n.offset == n.fractionStart {
		return numberParser{}, ErrSyntax
	}
	n.fractionEnd = n.offset
	return n, nil
}

func (n numberParser) parseExponentPart() (numberParser, error) {
	if n.offset >= len(n.text) || (n.text[n.offset] != 'e' && n.text[n.offset] != 'E') {
		return n, nil
	}
	return n.parseExponent()
}

func (n numberParser) parseExponent() (numberParser, error) {
	n.offset++
	if n.offset < len(n.text) && (n.text[n.offset] == '+' || n.text[n.offset] == '-') {
		n.exponentNegative = n.text[n.offset] == '-'
		n.offset++
	}
	start := n.offset
	limit := exponentLimit(len(n.text))
	for n.offset < len(n.text) && isDigit(n.text[n.offset]) {
		digit := int(n.text[n.offset] - '0')
		if n.exponent < limit {
			if n.exponent > (limit-digit)/decimalRadix {
				n.exponent = limit
			} else {
				n.exponent = n.exponent*decimalRadix + digit
			}
		}
		n.offset++
	}
	if n.offset == start {
		return numberParser{}, ErrSyntax
	}
	return n, nil
}

func exponentLimit(length int) int {
	if length > math.MaxInt-maxUint64DecimalDigits-1 {
		return math.MaxInt
	}
	return length + maxUint64DecimalDigits + 1
}

func (n numberParser) integer() (Integer, error) {
	digits := n.text[n.integerStart:n.integerEnd] + n.text[n.fractionStart:n.fractionEnd]
	if !strings.ContainsAny(digits, "123456789") {
		// Zero at any exponent, and -0, are the one non-negative zero.
		return Integer{}, nil
	}

	// The exponent shifts the decimal point across digits. Shifting it left, or
	// not far enough right, leaves a fractional part that must be all zeros;
	// shifting it further right appends that many zeros.
	//
	// Each bound below runs before the sum it guards. parseExponent saturates the
	// exponent just past the length of the text, so on a machine whose int cannot
	// hold that — and only there — an exponent large enough to overflow one of
	// these sums is one this rejects first.
	fractionDigits := n.fractionEnd - n.fractionStart
	var (
		integerDigits string
		trailingZeros int
	)
	switch {
	case n.exponentNegative:
		if n.exponent > len(digits)-fractionDigits {
			return Integer{}, ErrFractional
		}
		var err error
		integerDigits, err = integralPrefix(digits, fractionDigits+n.exponent)
		if err != nil {
			return Integer{}, err
		}
	case n.exponent < fractionDigits:
		var err error
		integerDigits, err = integralPrefix(digits, fractionDigits-n.exponent)
		if err != nil {
			return Integer{}, err
		}
	default:
		integerDigits = digits
		trailingZeros = n.exponent - fractionDigits
	}

	integerDigits = strings.TrimLeft(integerDigits, "0")
	if trailingZeros > maxUint64DecimalDigits ||
		len(integerDigits)+trailingZeros > maxUint64DecimalDigits {
		return Integer{}, ErrRange
	}
	if trailingZeros > 0 {
		integerDigits += strings.Repeat("0", trailingZeros)
	}
	magnitude, err := strconv.ParseUint(integerDigits, 10, 64)
	if err != nil {
		return Integer{}, ErrRange
	}
	return Integer{Magnitude: magnitude, Negative: n.negative}, nil
}

func integralPrefix(digits string, fractionalDigits int) (string, error) {
	if fractionalDigits > len(digits) ||
		strings.Trim(digits[len(digits)-fractionalDigits:], "0") != "" {
		return "", ErrFractional
	}
	return digits[:len(digits)-fractionalDigits], nil
}

func scanDigits(text string, offset int) int {
	for offset < len(text) && isDigit(text[offset]) {
		offset++
	}
	return offset
}

func isDigit(character byte) bool { return character >= '0' && character <= '9' }
