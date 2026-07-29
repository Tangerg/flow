package expr

import (
	"cmp"
	"encoding/json"
	"fmt"
	"go/token"
	"math"
	"reflect"
	"strconv"
)

// The value domain is deliberately narrow: nil, bool, string, int64, uint64,
// float64, and whatever else a Store happens to hold. Scalars are normalized on
// read to their JSON numeric semantics so named Go values and values restored
// from JSON behave the same.

// normalize maps every numeric type a Store may hold onto int64, uint64, or
// float64 and leaves other values alone. Integral floats become integers because
// JSON encodes them as integer tokens.
//
// A [json.Number] — what a Store holds after being deserialized — normalizes to
// an exact integer when possible and float64 otherwise, so an expression behaves
// the same on a fresh Store and on a restored one.
func normalize(v any) any {
	if number, ok := v.(json.Number); ok {
		return normalizeNumber(number)
	}
	if v == nil {
		return nil
	}

	value := reflect.ValueOf(v)
	switch value.Kind() {
	case reflect.Bool:
		return value.Bool()
	case reflect.String:
		return value.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return normalizeUint(value.Uint())
	case reflect.Float32:
		// encoding/json emits the shortest decimal that round-trips to the
		// original float32. Normalize through that same representation so a
		// fresh value and its decoded json.Number compare identically.
		decimal := strconv.FormatFloat(value.Float(), 'g', -1, 32)
		floating, _ := strconv.ParseFloat(decimal, 64)
		return normalizeFloat(floating)
	case reflect.Float64:
		return normalizeFloat(value.Float())
	}
	return v
}

func normalizeUint(n uint64) any {
	if n > math.MaxInt64 {
		return n
	}
	return int64(n)
}

// normalizeFloat erases a Go numeric distinction that JSON cannot preserve:
// an integral float is encoded as an integer token. It derives that integer from
// encoding/json's actual decimal representation rather than converting the
// binary float directly. Near the integer limits those values can differ (for
// example, 2^63 encodes as 9223372036854776000), and only the former survives a
// Store round trip.
func normalizeFloat(n float64) any {
	if math.IsNaN(n) || math.IsInf(n, 0) || n != math.Trunc(n) {
		return n
	}
	encoded, err := json.Marshal(n)
	if err != nil {
		return n
	}
	if integer, err := strconv.ParseInt(string(encoded), 10, 64); err == nil {
		return integer
	}
	if integer, err := strconv.ParseUint(string(encoded), 10, 64); err == nil {
		return integer
	}
	return n
}

// normalizeNumber prefers an exact signed or unsigned integer and falls back to
// float64. A literal that is none of those is left as-is so comparing it reports
// a type error rather than silently reading as zero.
func normalizeNumber(n json.Number) any {
	if i, err := strconv.ParseInt(n.String(), 10, 64); err == nil {
		return i
	}
	if i, err := strconv.ParseUint(n.String(), 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(n.String(), 64); err == nil {
		return normalizeFloat(f)
	}
	return n
}

// typeName names a value for a diagnostic. Numbers report as "number" because
// the distinction between int64, uint64, and float64 is an implementation detail
// of the value domain, not something an expression author chose.
func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "nil"
	case bool:
		return "bool"
	case string:
		return "string"
	case int64, uint64, float64:
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
	case uint64:
		if n == uint64(math.MaxInt64)+1 {
			return int64(math.MinInt64), nil
		}
		if n <= math.MaxInt64 {
			return -int64(n), nil
		}
		return nil, fmt.Errorf("%w: unary - overflows int64", ErrType)
	case float64:
		return -n, nil
	default:
		return nil, fmt.Errorf("%w: unary - wants number, got %s", ErrType, typeName(v))
	}
}

func length(v any) (any, error) {
	if v != nil {
		value := reflect.ValueOf(v)
		switch value.Kind() {
		case reflect.String, reflect.Array, reflect.Slice, reflect.Map:
			return int64(value.Len()), nil
		}
	}
	return nil, fmt.Errorf("%w: len wants string, array, or object, got %s", ErrType, typeName(v))
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

	if order, unordered, ok := compareNumbers(left, right); ok && isOrdering(op) {
		return applyOrder(op, order, unordered), nil
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
	if value, err, ok := applyUnsigned(op, left, right); ok {
		return value, err
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
	if !scalar(left) || !scalar(right) {
		return nil, fmt.Errorf("%w: %s wants scalar operands, got %s and %s",
			ErrType, op.String(), typeName(left), typeName(right))
	}
	if order, unordered, ok := compareNumbers(left, right); ok {
		return !unordered && order == 0, nil
	}

	switch l := left.(type) {
	case nil:
		return right == nil, nil
	case bool:
		r, ok := right.(bool)
		return ok && l == r, nil
	case string:
		r, ok := right.(string)
		return ok && l == r, nil
	default:
		return false, nil
	}
}

func scalar(v any) bool {
	switch v.(type) {
	case nil, bool, string, int64, uint64, float64:
		return true
	default:
		return false
	}
}

func isOrdering(op token.Token) bool {
	switch op {
	case token.LSS, token.LEQ, token.GTR, token.GEQ:
		return true
	default:
		return false
	}
}

func applyOrder(op token.Token, order int, unordered bool) bool {
	if unordered {
		return false
	}
	switch op {
	case token.LSS:
		return order < 0
	case token.LEQ:
		return order <= 0
	case token.GTR:
		return order > 0
	case token.GEQ:
		return order >= 0
	default:
		panic("expr: applyOrder called with non-ordering operator")
	}
}

// compareNumbers compares normalized numeric values without converting an
// integer to float64. The second result reports an unordered NaN comparison;
// the third reports whether both operands are numbers.
func compareNumbers(left, right any) (order int, unordered, ok bool) {
	switch l := left.(type) {
	case int64:
		switch r := right.(type) {
		case int64:
			return cmp.Compare(l, r), false, true
		case uint64:
			return compareIntUint(l, r), false, true
		case float64:
			order, unordered = compareIntFloat(l, r)
			return order, unordered, true
		}
	case uint64:
		switch r := right.(type) {
		case int64:
			return -compareIntUint(r, l), false, true
		case uint64:
			return cmp.Compare(l, r), false, true
		case float64:
			order, unordered = compareUintFloat(l, r)
			return order, unordered, true
		}
	case float64:
		switch r := right.(type) {
		case int64:
			order, unordered = compareIntFloat(r, l)
			return -order, unordered, true
		case uint64:
			order, unordered = compareUintFloat(r, l)
			return -order, unordered, true
		case float64:
			if math.IsNaN(l) || math.IsNaN(r) {
				return 0, true, true
			}
			return cmp.Compare(l, r), false, true
		}
	}
	return 0, false, false
}

func compareIntUint(signed int64, unsigned uint64) int {
	if signed < 0 {
		return -1
	}
	return cmp.Compare(uint64(signed), unsigned)
}

func compareIntFloat(integer int64, floating float64) (order int, unordered bool) {
	if math.IsNaN(floating) {
		return 0, true
	}
	// 2^63 is exactly representable as a float64. Keeping conversion inside this
	// interval avoids implementation-dependent out-of-range float-to-int results.
	if floating >= float64(1<<63) {
		return -1, false
	}
	if floating < -float64(1<<63) {
		return 1, false
	}

	truncated := int64(floating)
	if order := cmp.Compare(integer, truncated); order != 0 {
		return order, false
	}
	switch converted := float64(truncated); {
	case converted < floating:
		return -1, false
	case converted > floating:
		return 1, false
	default:
		return 0, false
	}
}

func compareUintFloat(integer uint64, floating float64) (order int, unordered bool) {
	if math.IsNaN(floating) {
		return 0, true
	}
	if floating < 0 {
		return 1, false
	}
	if floating >= float64(1<<64) {
		return -1, false
	}

	truncated := uint64(floating)
	if order := cmp.Compare(integer, truncated); order != 0 {
		return order, false
	}
	switch converted := float64(truncated); {
	case converted < floating:
		return -1, false
	case converted > floating:
		return 1, false
	default:
		return 0, false
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
	default:
		return nil, fmt.Errorf("%w: %s does not accept numbers", ErrType, op.String())
	}
}

// applyUnsigned handles arithmetic when at least one integer needs uint64's
// range. Small unsigned values normalize to int64, so a mixed pair can be
// converted without loss only when its signed operand is non-negative.
func applyUnsigned(op token.Token, left, right any) (any, error, bool) {
	var l, r uint64
	switch value := left.(type) {
	case uint64:
		l = value
	case int64:
		if value < 0 {
			if _, ok := right.(uint64); ok {
				return nil, fmt.Errorf("%w: %s cannot mix a negative integer with uint64", ErrType, op), true
			}
			return nil, nil, false
		}
		l = uint64(value)
	default:
		return nil, nil, false
	}
	switch value := right.(type) {
	case uint64:
		r = value
	case int64:
		if value < 0 {
			if _, ok := left.(uint64); ok {
				return nil, fmt.Errorf("%w: %s cannot mix uint64 with a negative integer", ErrType, op), true
			}
			return nil, nil, false
		}
		r = uint64(value)
	default:
		return nil, nil, false
	}
	if _, leftUnsigned := left.(uint64); !leftUnsigned {
		if _, rightUnsigned := right.(uint64); !rightUnsigned {
			return nil, nil, false
		}
	}

	switch op {
	case token.ADD:
		return l + r, nil, true
	case token.SUB:
		return l - r, nil, true
	case token.MUL:
		return l * r, nil, true
	case token.QUO:
		if r == 0 {
			return nil, ErrDivideByZero, true
		}
		return l / r, nil, true
	case token.REM:
		if r == 0 {
			return nil, ErrDivideByZero, true
		}
		return l % r, nil, true
	default:
		return nil, fmt.Errorf("%w: %s does not accept numbers", ErrType, op), true
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
	default:
		return nil, fmt.Errorf("%w: %s does not accept numbers", ErrType, op.String())
	}
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}
