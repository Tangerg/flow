// Package flow provides the minimal, type-safe building blocks for composing
// in-process control flow.
//
// It is deliberately reduced to the irreducible set of primitives — those that
// cannot be expressed in terms of the others:
//
//   - [Node] and [NodeFunc]: the abstraction and its function adapter.
//   - [Then]: sequential composition.
//   - [Switch]: data-dependent selection.
//   - [Loop]: bounded iteration (with an optional [LoopConfig] limit).
//   - [Map]: concurrent execution over a collection — AND, wait for all.
//   - [Race]: concurrent execution over one input — OR, first success wins.
//
// Together these are control-complete — sequence, selection, and iteration — plus
// the two concurrency atoms [Map] (AND) and [Race] (OR), neither expressible in
// terms of the other. Every other convenience (fan-out, heterogeneous fan-in,
// variadic sequencing, fallback) is derivable from these and therefore belongs in
// higher-level packages, not here. For example fan-out over nodes is [Map] applied
// to the nodes as data, and a try/else fallback is a node that runs an alternate
// when the primary fails.
//
// Errors preserve their causes. Concurrent collection operations report item
// positions with [IndexError], allowing callers to use errors.Is and errors.As
// instead of matching strings.
//
// The package has no third-party dependencies. Durability, distribution, and
// deterministic replay are out of scope; for those use a workflow engine such as
// Temporal.
package flow
