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

type operand struct {
	raw any
}

type (
	binaryOperator struct{ token.Token }
	signedNumber   int64
	unsignedNumber uint64
	floatNumber    float64
	jsonNumber     json.Number
	integerOperand struct {
		value    uint64
		unsigned bool
		negative bool
	}
)

// normalized maps every numeric type a Store may hold onto int64, uint64, or
// float64 and leaves other values alone. Integral floats become integers because
// JSON encodes them as integer tokens.
//
// A [json.Number] — what a Store holds after being deserialized — normalizes to
// an exact integer when possible and float64 otherwise, so an expression behaves
// the same on a fresh Store and on a restored one.
func (o operand) normalized() any {
	if number, ok := o.raw.(json.Number); ok {
		return jsonNumber(number).normalized()
	}
	if o.raw == nil {
		return nil
	}

	value := reflect.ValueOf(o.raw)
	switch value.Kind() {
	case reflect.Bool:
		return value.Bool()
	case reflect.String:
		return value.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return unsignedNumber(value.Uint()).normalized()
	case reflect.Float32:
		// encoding/json emits the shortest decimal that round-trips to the
		// original float32. Normalize through that same representation so a
		// fresh value and its decoded json.Number compare identically.
		decimal := strconv.FormatFloat(value.Float(), 'g', -1, 32)
		floating, _ := strconv.ParseFloat(decimal, 64)
		return floatNumber(floating).normalized()
	case reflect.Float64:
		return floatNumber(value.Float()).normalized()
	default:
		// Composites and unsupported kinds are carried through untouched; only a
		// scalar has a canonical Store representation to normalize towards.
		return o.raw
	}
}

func (u unsignedNumber) normalized() any {
	if u > math.MaxInt64 {
		return uint64(u)
	}
	return int64(u) // #nosec G115 -- guarded by the MaxInt64 check above.
}

// normalized erases a Go numeric distinction that JSON cannot preserve:
// an integral float is encoded as an integer token. It derives that integer from
// encoding/json's actual decimal representation rather than converting the
// binary float directly. Near the integer limits those values can differ (for
// example, 2^63 encodes as 9223372036854776000), and only the former survives a
// Store round trip.
func (f floatNumber) normalized() any {
	value := float64(f)
	if math.IsNaN(value) || math.IsInf(value, 0) || value != math.Trunc(value) {
		return float64(f)
	}
	// Finite float64 values are always representable by encoding/json.
	//
	//nolint:errchkjson // NaN and Inf, the only failing float64 values, return above.
	encoded, _ := json.Marshal(float64(f))
	if integer, err := strconv.ParseInt(string(encoded), 10, 64); err == nil {
		return integer
	}
	if integer, err := strconv.ParseUint(string(encoded), 10, 64); err == nil {
		return integer
	}
	return float64(f)
}

// normalized prefers an exact signed or unsigned integer and falls back to
// float64. A literal that is none of those is left as-is so comparing it reports
// a type error rather than silently reading as zero.
func (j jsonNumber) normalized() any {
	text := json.Number(j).String()
	if i, err := strconv.ParseInt(text, 10, 64); err == nil {
		return i
	}
	if i, err := strconv.ParseUint(text, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(text, 64); err == nil {
		return floatNumber(f).normalized()
	}
	return json.Number(j)
}

// typeName names a value for a diagnostic. Numbers report as "number" because
// the distinction between int64, uint64, and float64 is an implementation detail
// of the value domain, not something an expression author chose.
func (o operand) typeName() string {
	switch o.raw.(type) {
	case nil:
		return "nil"
	case bool:
		return "bool"
	case string:
		return "string"
	case int64, uint64, float64:
		return "number"
	default:
		return fmt.Sprintf("%T", o.raw)
	}
}

func (b binaryOperator) supported() bool {
	switch b.Token {
	case token.ADD, token.SUB, token.MUL, token.QUO, token.REM,
		token.EQL, token.NEQ, token.LSS, token.LEQ, token.GTR, token.GEQ:
		return true
	default:
		return false
	}
}

func (o operand) negate() (any, error) {
	switch n := o.raw.(type) {
	case int64:
		return -n, nil
	case uint64:
		if n == uint64(math.MaxInt64)+1 {
			return int64(math.MinInt64), nil
		}
		// normalized converts smaller unsigned values to int64 before evaluation.
		return nil, fmt.Errorf("%w: unary - overflows int64", ErrType)
	case float64:
		return -n, nil
	default:
		return nil, fmt.Errorf("%w: unary - wants number, got %s", ErrType, o.typeName())
	}
}

func (o operand) length() (any, error) {
	if o.raw != nil {
		value := reflect.ValueOf(o.raw)
		//nolint:exhaustive // Filters the four countable kinds; the rest share one error below.
		switch value.Kind() {
		case reflect.String, reflect.Array, reflect.Slice, reflect.Map:
			return int64(value.Len()), nil
		}
	}
	return nil, fmt.Errorf("%w: len wants string, array, or object, got %s", ErrType, o.typeName())
}

// apply evaluates a binary operator over two normalized values.
func (b binaryOperator) apply(left, right operand) (any, error) {
	if result, handled, err := b.applyEquality(left, right); handled {
		return result, err
	}
	if order, unordered, ok := left.compareNumber(right); ok && b.ordering() {
		return b.applyOrder(order, unordered), nil
	}
	return b.applyArithmetic(left, right)
}

func (b binaryOperator) applyEquality(
	left, right operand,
) (result any, handled bool, err error) {
	switch b.Token {
	case token.EQL:
		result, err = b.equal(left, right)
		return result, true, err
	case token.NEQ:
		equal, err := b.equal(left, right)
		if err != nil {
			return nil, true, err
		}
		return !equal, true, nil
	default:
		return nil, false, nil
	}
}

func (b binaryOperator) applyArithmetic(left, right operand) (any, error) {
	// String concatenation and string ordering are the only non-numeric
	// arithmetic; everything else needs two numbers.
	if ls, ok := left.raw.(string); ok {
		if rs, ok := right.raw.(string); ok {
			return b.applyString(ls, rs)
		}
	}
	if li, ok := left.raw.(int64); ok {
		if ri, ok := right.raw.(int64); ok {
			return b.applyInt(li, ri)
		}
	}
	if value, ok, err := b.applyUnsigned(left, right); ok {
		return value, err
	}
	leftFloat, leftOK := left.asFloat()
	rightFloat, rightOK := right.asFloat()
	if !leftOK || !rightOK {
		return nil, fmt.Errorf("%w: %s wants two numbers or two strings, got %s and %s",
			ErrType, b.String(), left.typeName(), right.typeName())
	}
	return b.applyFloat(leftFloat, rightFloat)
}

// equal compares two values for identity of kind and value. Slices and maps are
// rejected rather than compared, since Go's == panics on them and a deep
// comparison is not what a workflow condition should silently do.
func (b binaryOperator) equal(left, right operand) (bool, error) {
	if !left.scalar() || !right.scalar() {
		return false, fmt.Errorf("%w: %s wants scalar operands, got %s and %s",
			ErrType, b.String(), left.typeName(), right.typeName())
	}
	if order, unordered, ok := left.compareNumber(right); ok {
		return !unordered && order == 0, nil
	}
	return left.raw == right.raw, nil
}

func (o operand) scalar() bool {
	switch o.raw.(type) {
	case nil, bool, string, int64, uint64, float64:
		return true
	default:
		return false
	}
}

func (b binaryOperator) ordering() bool {
	switch b.Token {
	case token.LSS, token.LEQ, token.GTR, token.GEQ:
		return true
	default:
		return false
	}
}

func (b binaryOperator) applyOrder(order int, unordered bool) bool {
	if unordered {
		return false
	}
	switch b.Token {
	case token.LSS:
		return order < 0
	case token.LEQ:
		return order <= 0
	case token.GTR:
		return order > 0
	default: // token.GEQ; callers establish ordering before applying it.
		return order >= 0
	}
}

// compareNumber compares normalized numeric values without converting an
// integer to float64. The second result reports an unordered NaN comparison;
// the third reports whether both operands are numbers.
func (o operand) compareNumber(other operand) (order int, unordered, ok bool) {
	switch left := o.raw.(type) {
	case int64:
		return signedNumber(left).compareOperand(other.raw)
	case uint64:
		return unsignedNumber(left).compareOperand(other.raw)
	case float64:
		return floatNumber(left).compareOperand(other.raw)
	}
	return 0, false, false
}

func (s signedNumber) compareOperand(other any) (order int, unordered, ok bool) {
	switch other := other.(type) {
	case int64:
		return cmp.Compare(int64(s), other), false, true
	case uint64:
		return s.compareUnsigned(unsignedNumber(other)), false, true
	case float64:
		order, unordered = s.compareFloat(floatNumber(other))
		return order, unordered, true
	default:
		return 0, false, false
	}
}

func (u unsignedNumber) compareOperand(other any) (order int, unordered, ok bool) {
	switch other := other.(type) {
	case int64:
		return -signedNumber(other).compareUnsigned(u), false, true
	case uint64:
		return cmp.Compare(uint64(u), other), false, true
	case float64:
		order, unordered = u.compareFloat(floatNumber(other))
		return order, unordered, true
	default:
		return 0, false, false
	}
}

func (f floatNumber) compareOperand(other any) (order int, unordered, ok bool) {
	switch other := other.(type) {
	case int64:
		order, unordered = signedNumber(other).compareFloat(f)
		return -order, unordered, true
	case uint64:
		order, unordered = unsignedNumber(other).compareFloat(f)
		return -order, unordered, true
	case float64:
		if math.IsNaN(float64(f)) || math.IsNaN(other) {
			return 0, true, true
		}
		return cmp.Compare(float64(f), other), false, true
	default:
		return 0, false, false
	}
}

func (s signedNumber) compareUnsigned(other unsignedNumber) int {
	if s < 0 {
		return -1
	}
	// #nosec G115 -- negative values return above.
	return cmp.Compare(uint64(s), uint64(other))
}

func (s signedNumber) compareFloat(other floatNumber) (order int, unordered bool) {
	floating := float64(other)
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
	if order := cmp.Compare(int64(s), truncated); order != 0 {
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

func (u unsignedNumber) compareFloat(other floatNumber) (order int, unordered bool) {
	floating := float64(other)
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
	if order := cmp.Compare(uint64(u), truncated); order != 0 {
		return order, false
	}
	// For a non-negative in-range float, truncation and conversion back to
	// float64 can only preserve the value or move below it.
	if float64(truncated) < floating {
		return -1, false
	}
	return 0, false
}

func (b binaryOperator) applyString(left, right string) (any, error) {
	switch b.Token {
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
		return nil, fmt.Errorf("%w: %s does not accept strings", ErrType, b.String())
	}
}

// applyInt keeps integer arithmetic exact. Like Go, it wraps on overflow.
func (b binaryOperator) applyInt(left, right int64) (any, error) {
	switch b.Token {
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
	default: // token.REM; compilation rejects every other operator.
		if right == 0 {
			return nil, ErrDivideByZero
		}
		return left % right, nil
	}
}

// applyUnsigned handles arithmetic when at least one integer needs uint64's
// range. Small unsigned values normalize to int64, so a mixed pair can be
// converted without loss only when its signed operand is non-negative.
func (b binaryOperator) applyUnsigned(left, right operand) (any, bool, error) {
	leftInteger, leftOK := left.integer()
	rightInteger, rightOK := right.integer()
	if !leftOK || !rightOK {
		return nil, false, nil
	}
	if leftInteger.negative {
		return nil, true, fmt.Errorf(
			"%w: %s cannot mix a negative integer with uint64",
			ErrType,
			b,
		)
	}
	if rightInteger.negative {
		return nil, true, fmt.Errorf(
			"%w: %s cannot mix uint64 with a negative integer",
			ErrType,
			b,
		)
	}

	value, err := b.applyUint(leftInteger.value, rightInteger.value)
	return value, true, err
}

func (o operand) integer() (integerOperand, bool) {
	switch value := o.raw.(type) {
	case int64:
		if value < 0 {
			return integerOperand{negative: true}, true
		}
		return integerOperand{value: uint64(value)}, true
	case uint64:
		return integerOperand{value: value, unsigned: true}, true
	default:
		return integerOperand{}, false
	}
}

func (b binaryOperator) applyUint(left, right uint64) (any, error) {
	switch b.Token {
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
	default: // token.REM; compilation rejects every other operator.
		if right == 0 {
			return nil, ErrDivideByZero
		}
		return left % right, nil
	}
}

func (b binaryOperator) applyFloat(left, right float64) (any, error) {
	switch b.Token {
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
	default: // token.REM; compilation rejects every other operator.
		return nil, fmt.Errorf("%w: %% wants two integers", ErrType)
	}
}

func (o operand) asFloat() (float64, bool) {
	switch n := o.raw.(type) {
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
