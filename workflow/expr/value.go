package expr

import (
	"encoding/json"
	"fmt"
	"go/token"
	"math"
	"strconv"
)

// The value domain is deliberately narrow: nil, bool, string, int64, float64,
// and whatever else a Store happens to hold. Numbers are normalized on read so
// that a value written as a Go int and the same value decoded from JSON as a
// float64 behave identically.

// normalize maps every numeric type a Store may hold onto int64 or float64 and
// leaves other values alone. An unsigned value too large for int64 becomes a
// float64, which loses precision rather than wrapping to a negative number.
//
// A [json.Number] — what a Store holds after being deserialized — normalizes to
// int64 when it is integral and float64 otherwise, so an expression behaves the
// same on a fresh Store and on a restored one.
func normalize(v any) any {
	switch n := v.(type) {
	case json.Number:
		return normalizeNumber(n)
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case uint:
		return normalizeUint(uint64(n))
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return normalizeUint(n)
	case uintptr:
		return normalizeUint(uint64(n))
	case float32:
		return float64(n)
	default:
		return v
	}
}

func normalizeUint(n uint64) any {
	if n > math.MaxInt64 {
		return float64(n)
	}
	return int64(n)
}

// normalizeNumber prefers an exact int64 and falls back to float64. A literal
// that is neither is left as-is so that comparing it reports a type error rather
// than silently reading as zero.
func normalizeNumber(n json.Number) any {
	if i, err := strconv.ParseInt(n.String(), 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(n.String(), 64); err == nil {
		return f
	}
	return n
}

// typeName names a value for a diagnostic. Numbers report as "number" because
// the distinction between int64 and float64 is an implementation detail of the
// value domain, not something an expression author chose.
func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "nil"
	case bool:
		return "bool"
	case string:
		return "string"
	case int64, float64:
		return "number"
	default:
		return fmt.Sprintf("%T", v)
	}
}

func supportedBinaryOp(op token.Token) bool {
	switch op {
	case token.ADD, token.SUB, token.MUL, token.QUO, token.REM,
		token.EQL, token.NEQ, token.LSS, token.LEQ, token.GTR, token.GEQ:
		return true
	default:
		return false
	}
}

func negate(v any) (any, error) {
	switch n := v.(type) {
	case int64:
		return -n, nil
	case float64:
		return -n, nil
	default:
		return nil, fmt.Errorf("%w: unary - wants number, got %s", ErrType, typeName(v))
	}
}

func length(v any) (any, error) {
	switch c := v.(type) {
	case string:
		return int64(len(c)), nil
	case []any:
		return int64(len(c)), nil
	case map[string]any:
		return int64(len(c)), nil
	default:
		return nil, fmt.Errorf("%w: len wants string, array, or object, got %s", ErrType, typeName(v))
	}
}

// apply evaluates a binary operator over two normalized values.
func apply(op token.Token, left, right any) (any, error) {
	switch op {
	case token.EQL:
		return equal(op, left, right)
	case token.NEQ:
		eq, err := equal(op, left, right)
		if err != nil {
			return nil, err
		}
		return !eq.(bool), nil
	}

	// String concatenation and string ordering are the only non-numeric
	// arithmetic; everything else needs two numbers.
	if ls, ok := left.(string); ok {
		if rs, ok := right.(string); ok {
			return applyString(op, ls, rs)
		}
	}
	if li, ok := left.(int64); ok {
		if ri, ok := right.(int64); ok {
			return applyInt(op, li, ri)
		}
	}
	lf, lok := toFloat(left)
	rf, rok := toFloat(right)
	if !lok || !rok {
		return nil, fmt.Errorf("%w: %s wants two numbers or two strings, got %s and %s",
			ErrType, op.String(), typeName(left), typeName(right))
	}
	return applyFloat(op, lf, rf)
}

// equal compares two values for identity of kind and value. Slices and maps are
// rejected rather than compared, since Go's == panics on them and a deep
// comparison is not what a workflow condition should silently do.
func equal(op token.Token, left, right any) (any, error) {
	switch l := left.(type) {
	case nil:
		return right == nil, nil
	case bool:
		r, ok := right.(bool)
		return ok && l == r, nil
	case string:
		r, ok := right.(string)
		return ok && l == r, nil
	case int64:
		switch r := right.(type) {
		case int64:
			return l == r, nil
		case float64:
			return float64(l) == r, nil
		default:
			return false, nil
		}
	case float64:
		switch r := right.(type) {
		case int64:
			return l == float64(r), nil
		case float64:
			return l == r, nil
		default:
			return false, nil
		}
	default:
		return nil, fmt.Errorf("%w: %s cannot compare %s", ErrType, op.String(), typeName(left))
	}
}

func applyString(op token.Token, left, right string) (any, error) {
	switch op {
	case token.ADD:
		return left + right, nil
	case token.LSS:
		return left < right, nil
	case token.LEQ:
		return left <= right, nil
	case token.GTR:
		return left > right, nil
	case token.GEQ:
		return left >= right, nil
	default:
		return nil, fmt.Errorf("%w: %s does not accept strings", ErrType, op.String())
	}
}

// applyInt keeps integer arithmetic exact. Like Go, it wraps on overflow.
func applyInt(op token.Token, left, right int64) (any, error) {
	switch op {
	case token.ADD:
		return left + right, nil
	case token.SUB:
		return left - right, nil
	case token.MUL:
		return left * right, nil
	case token.QUO:
		if right == 0 {
			return nil, ErrDivideByZero
		}
		return left / right, nil
	case token.REM:
		if right == 0 {
			return nil, ErrDivideByZero
		}
		return left % right, nil
	case token.LSS:
		return left < right, nil
	case token.LEQ:
		return left <= right, nil
	case token.GTR:
		return left > right, nil
	case token.GEQ:
		return left >= right, nil
	default:
		return nil, fmt.Errorf("%w: %s does not accept numbers", ErrType, op.String())
	}
}

func applyFloat(op token.Token, left, right float64) (any, error) {
	switch op {
	case token.ADD:
		return left + right, nil
	case token.SUB:
		return left - right, nil
	case token.MUL:
		return left * right, nil
	case token.QUO:
		if right == 0 {
			return nil, ErrDivideByZero
		}
		return left / right, nil
	case token.REM:
		return nil, fmt.Errorf("%w: %% wants two integers", ErrType)
	case token.LSS:
		return left < right, nil
	case token.LEQ:
		return left <= right, nil
	case token.GTR:
		return left > right, nil
	case token.GEQ:
		return left >= right, nil
	default:
		return nil, fmt.Errorf("%w: %s does not accept numbers", ErrType, op.String())
	}
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}
