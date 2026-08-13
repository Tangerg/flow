package expr

import (
	"errors"
	"fmt"
)

// Stable sentinel errors. Test for them with [errors.Is] rather than matching
// text.
//
// Each states only its condition. Every expression failure reaches a caller
// inside an [Error], which names this package and the expression that failed, so
// a sentinel that also named the package would say it twice.
var (
	// ErrSyntax reports an expression that is not a well-formed expression.
	ErrSyntax = errors.New("syntax error")
	// ErrUnsupported reports a construct or nesting depth outside the supported
	// grammar. It is always reported by [Parse], never at evaluation time.
	ErrUnsupported = errors.New("unsupported expression")
	// ErrType reports an operator applied to values it does not accept.
	ErrType = errors.New("type error")
	// ErrUndefined reports a reference missing from the [workflow.Store]. Guard
	// a reference with has() when it may legitimately be absent. has() still
	// reports malformed data and conversion failures as [ErrType].
	ErrUndefined = errors.New("undefined reference")
	// ErrDivideByZero reports a division or remainder by zero. Unlike Go, expr
	// never yields an infinity.
	ErrDivideByZero = errors.New("divide by zero")
)

// Error identifies the expression, and where in it, a failure occurred. Pos is a
// 1-based byte offset into Source, or zero when the failure has no single
// position.
//
// Every expression failure reaches a caller as an Error, whether it comes from
// [Parse] or from evaluation, so this is where the package names itself. Nothing
// it wraps repeats that name.
type Error struct {
	Source string
	Pos    int
	Err    error
}

func (e *Error) Error() string {
	if e.Pos > 0 {
		return fmt.Sprintf("expr %q at %d: %v", e.Source, e.Pos, e.Err)
	}
	return fmt.Sprintf("expr %q: %v", e.Source, e.Err)
}

// Unwrap returns the underlying cause.
func (e *Error) Unwrap() error { return e.Err }
