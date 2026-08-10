// Package expr turns a workflow's branch and loop logic into data.
//
// A serialized [workflow.Spec] names its resolvers and conditions and the
// Registry supplies the code, so changing a threshold means changing Go and
// redeploying. This package closes that gap: it compiles a small expression
// language over a [workflow.Store] into ordinary [workflow.Condition] and
// [workflow.Resolver] values, which a caller then registers. Everything here is
// opt-in — flow and workflow do not import it, so the core stays free of an
// interpreter.
//
//	condition, err := expr.Condition("review.output.score >= 0.9")
//	if err != nil { ... }
//	reg.MustRegisterCondition("goodEnough", condition)
//
// [Bindings] is the config-shaped form: a strict JSON object of named
// expressions that registers as a group. Non-object documents, unknown and
// duplicate members, invalid Unicode, and excessive nesting are rejected
// before the receiver is changed.
//
// # Grammar
//
// Expressions are parsed with go/parser, so the syntax is a subset of Go's, and
// compiled to closures. The supported grammar is the compiler: [Parse] rejects
// everything else up front, and there is no code path that evaluates a construct
// it rejected. In particular there are no assignments, no function literals, no
// user-defined calls, no type conversions, and no way to reach the host program.
// Expression trees and reference chains are bounded by
// [workflow.MaxNestingDepth], matching the runtime's other recursive input
// boundaries.
//
//   - References. A reference is a node ID followed by a path:
//     "load.output", "load.output.items[0]", or "params[\"rate\"]". IDs that are
//     not Go identifiers use the quoted root form, as in
//     "node[\"load-user\"].output". A bare name is not a reference, since a
//     Store address needs both parts. Indexes must be literals because a Store
//     path is text.
//   - Literals. Integers, floats, quoted strings, true, false, and nil.
//   - Operators. Comparison (== != < <= > >=), logical (&& || !) with
//     short-circuit evaluation, and arithmetic (+ - * / %). String operands
//     support + as concatenation and the ordering comparisons.
//   - Functions. Exactly two: len(x) for a string, array, or object, and
//     has(ref) reporting whether a reference resolves.
//
// # Values
//
// A Store holds values as any, so ordinary scalar values are normalized on read
// to the semantics that survive encoding/json: integer kinds become int64 or
// uint64, fractional floats become float64, integral floats become integers,
// and named bool and string types become their underlying values. Integer
// comparison remains exact even against a float outside float64's exact-integer
// range.
//
// As with [workflow.Store.Lookup], a whole-cell value with a custom JSON
// representation is still its Go value before persistence and its encoded
// domain afterward. If a rule reads such a value, store the representation the
// rule should evaluate (for example, a timestamp string) rather than relying on
// a custom marshaler to change its kind across the persistence boundary.
// Integer arithmetic stays exact and wraps on overflow as Go's does; arithmetic
// involving a fractional float uses float64. Division or remainder by zero is
// [ErrDivideByZero] rather than an infinity.
//
// len accepts strings, arrays, slices, and maps of any concrete Go type. It
// measures the value currently held by the Store; its result survives a Store
// round trip only when that value's JSON representation preserves the same
// top-level kind and length. For example, []byte is a slice in memory but a
// base64 string in JSON. Equality is deliberately scalar: arrays, slices, maps,
// structs, and other host values report [ErrType] rather than silently choosing
// deep equality.
//
// There is no implicit truthiness and no implicit conversion. A condition must
// evaluate to a bool and a resolver to a string, or the result is [ErrType].
// References and Switch branch names must be valid UTF-8, matching workflow's
// persistent definition boundaries.
// Reading a missing reference is [ErrUndefined]; guard it with has() when a
// value is legitimately optional. An existing typed value whose JSON
// representation cannot resolve a nested path is [ErrType] and also preserves
// [workflow.ErrTypeMismatch]. has() reports false only for absence; it preserves
// those errors when the Store cannot determine whether a malformed path exists.
// Because evaluation failures are errors rather than false, a broken condition
// is never mistaken for "keep looping". A [Switch] with no matching expression
// and no fallback returns [flow.ErrNoCase], keeping rule selection distinct from
// missing Store data.
//
// # Analysis
//
// [Expr.Refs] reports the references an expression reads, so a rule set can be
// checked against the values a graph actually produces before it runs.
package expr
