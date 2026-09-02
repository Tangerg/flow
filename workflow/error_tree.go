package workflow

import (
	"errors"
	"reflect"
	"slices"
)

// An error tree is the standard Go shape: Unwrap() error for a wrapper,
// Unwrap() []error for a join. What every walk over one needs is here -- which
// multi-errors this package may look inside, the identity match, and the
// engine-owned view that copying and rendering both start from. Each walk keeps
// its branches on the heap: an application error tree crosses no workflow
// nesting boundary, so nothing bounds its depth.

// errorTree is the standard Go error-tree shape with an iterative matcher.
// Application validators and factories may return arbitrarily deep joined
// errors without crossing a workflow nesting boundary, while [errors.Is] walks
// Unwrap() []error branches recursively. Keeping this representation private
// avoids changing the public error protocol merely to make engine boundaries
// independent of application-tree depth.
type errorTree struct {
	root error
}

// standardJoinErrorTypes is what [errors.Join] returns, at both arities it could
// specialize. Deriving the types from the constructor avoids depending on an
// unexported standard-library name.
var standardJoinErrorTypes = [...]reflect.Type{
	reflect.TypeOf(errors.Join(ErrInvalidSpec)),
	reflect.TypeOf(errors.Join(ErrInvalidSpec, ErrInvalidGraph)),
}

// standardJoinChildren recognizes only the concrete multi-error produced by
// [errors.Join]. A caller-defined Unwrap() []error remains an opaque application
// error: interpreting its branches could change presentation or ownership rules
// chosen by that type.
//
// The capability comes before the identity because the assertion that yields the
// children is also what proves they can be read. Asking the table first would
// leave the assertion licensed by a type comparison, which is a promise about a
// standard-library implementation rather than about this value.
func standardJoinChildren(err error) ([]error, bool) {
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return nil, false
	}
	typeOf := reflect.TypeOf(err)
	if typeOf != standardJoinErrorTypes[0] && typeOf != standardJoinErrorTypes[1] {
		return nil, false
	}
	return joined.Unwrap(), true
}

// matches has the ordering and matching rules of [errors.Is]: visit each node
// before its children, follow depth first from left to right, compare a
// comparable target directly, then honor a node's shallow Is method. The only
// difference is the explicit branch worklist.
func (e errorTree) matches(target error) bool {
	if e.root == nil || target == nil {
		//nolint:errorlint // Mirrors errors.Is's nil identity before tree traversal.
		return e.root == target
	}
	targetComparable := reflect.TypeOf(target).Comparable()
	pending := []error{e.root}
	for len(pending) > 0 {
		last := len(pending) - 1
		current := pending[last]
		pending = pending[:last]
		for current != nil {
			//nolint:errorlint // This is one node of the explicit errors.Is traversal.
			if targetComparable && current == target {
				return true
			}
			matcher, ok := current.(interface{ Is(target error) bool })
			if ok && matcher.Is(target) {
				return true
			}
			//nolint:errorlint // The explicit worklist makes multi-error traversal stack-safe.
			switch wrapped := current.(type) {
			case interface{ Unwrap() error }:
				current = wrapped.Unwrap()
			case interface{ Unwrap() []error }:
				for _, child := range slices.Backward(wrapped.Unwrap()) {
					if child != nil {
						pending = append(pending, child)
					}
				}
				current = nil
			default:
				current = nil
			}
		}
	}
	return false
}

// ownedError is the engine-owned view of one error tree. It knows how to copy
// and render only the mutable workflow locations, root flow locations, and the
// exact branching shape produced by [errors.Join]. Every other wrapper is an
// opaque application value governed by Go's immutable-error convention.
type ownedError struct {
	root error
}
