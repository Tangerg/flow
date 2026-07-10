package core

import "errors"

// Sentinel errors returned by the core combinators. Test for them with
// [errors.Is].
var (
	// ErrNilNode is returned when a nil Node or nil Func is Run.
	ErrNilNode = errors.New("flow: nil node")
	// ErrNilFunc is returned when a required function argument (a resolver,
	// merge, or loop body) is nil.
	ErrNilFunc = errors.New("flow: nil function")
	// ErrNoCase is returned by Switch when the resolved key matches no case.
	ErrNoCase = errors.New("flow: no matching case")
	// ErrMaxIterations is returned by Loop when it reaches its iteration cap
	// without the body reporting done.
	ErrMaxIterations = errors.New("flow: max iterations exceeded")
)
