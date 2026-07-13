package flow

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the root combinators. Test for them with
// [errors.Is].
var (
	// ErrNilNode is returned when a nil Node or nil NodeFunc is Run.
	ErrNilNode = errors.New("flow: nil node")
	// ErrNilFunc is returned when a required function argument is nil.
	ErrNilFunc = errors.New("flow: nil function")
	// ErrNoCase is returned by Switch when the resolved key matches no case.
	ErrNoCase = errors.New("flow: no matching case")
	// ErrMaxIterations is returned by Loop when it reaches its iteration cap
	// without the body reporting done.
	ErrMaxIterations = errors.New("flow: max iterations exceeded")
	// ErrNoNodes is returned by Race when given no nodes.
	ErrNoNodes = errors.New("flow: no nodes")
)

// IndexError reports an error produced while processing one element of an
// ordered collection. Map and higher-level concurrent combinators use it so
// callers can recover the failing input position with [errors.As] while still
// matching the underlying error with [errors.Is].
type IndexError struct {
	Index int
	Err   error
}

func (e *IndexError) Error() string {
	return fmt.Sprintf("flow: index %d: %v", e.Index, e.Err)
}

// Unwrap returns the underlying element error.
func (e *IndexError) Unwrap() error { return e.Err }
