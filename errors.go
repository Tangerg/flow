package flow

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the root combinators. Test for them with
// [errors.Is].
var (
	// ErrNilNode is returned when a nil Node or nil NodeFunc is Run through a
	// built-in adapter or composite. Other adapters may use the same sentinel
	// for their own invalid nil value.
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
	// ErrInvalidConfig is returned when a combinator's configuration is invalid.
	ErrInvalidConfig = errors.New("flow: invalid config")
)

// IndexError reports an error produced while processing one element of an
// ordered collection. Map and higher-level concurrent combinators use it so
// callers can recover the failing input position with [errors.As] while still
// matching the underlying error with [errors.Is].
type IndexError struct {
	Index int
	Err   error
}

// Error states the position and defers to the cause. This package's sentinels
// carry its name themselves, because most of them reach a caller with nothing
// wrapping them; a location that repeated the name would say it twice, and
// nested locations would say it once per level.
func (i *IndexError) Error() string {
	return fmt.Sprintf("index %d: %v", i.Index, i.Err)
}

// Unwrap returns the underlying element error.
func (i *IndexError) Unwrap() error { return i.Err }
