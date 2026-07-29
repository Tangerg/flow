package expr

import (
	"errors"
	"fmt"
)

// Stable sentinel errors. Test for them with [errors.Is] rather than matching
// text.
var (
	// ErrSyntax reports an expression that is not a well-formed expression.
	ErrSyntax = errors.New("expr: syntax error")
	// ErrUnsupported reports a construct outside the supported grammar. It is
	// always reported by [Parse], never at evaluation time.
	ErrUnsupported = errors.New("expr: unsupported expression")
	// ErrType reports an operator applied to values it does not accept.
	ErrType = errors.New("expr: type error")
	// ErrUndefined reports a reference the [workflow.Store] does not resolve.
	// Guard a reference with has() when it may legitimately be absent.
	ErrUndefined = errors.New("expr: undefined reference")
	// ErrDivideByZero reports a division or remainder by zero. Unlike Go, expr
	// never yields an infinity.
	ErrDivideByZero = errors.New("expr: divide by zero")
)

// Error identifies the expression, and where in it, a failure occurred. Pos is a
// 1-based byte offset into Source, or zero when the failure has no single
// position.
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
