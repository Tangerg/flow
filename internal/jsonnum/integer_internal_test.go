package jsonnum

import (
	"math"
	"testing"
)

func TestExponentLimitSaturates(t *testing.T) {
	if got := exponentLimit(math.MaxInt); got != math.MaxInt {
		t.Fatalf("exponentLimit(MaxInt) = %d; want %d", got, math.MaxInt)
	}
}
