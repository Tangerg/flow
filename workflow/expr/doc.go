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
// [Bindings] is the config-shaped form: a JSON document of named expressions
// that registers as a group.
//
// # Grammar
//
// Expressions are parsed with go/parser, so the syntax is a subset of Go's, and
// compiled to closures. The supported grammar is the compiler: [Parse] rejects
// everything else up front, and there is no code path that evaluates a construct
// it rejected. In particular there are no assignments, no function literals, no
// user-defined calls, no type conversions, and no way to reach the host program.
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
// A Store holds values as any, so scalar values are normalized on read to the
// semantics that survive encoding/json: integer kinds become int64 or uint64,
// fractional floats become float64, integral floats become integers, and named
// bool and string types become their underlying values. Integer comparison
// remains exact even against a float outside float64's exact-integer range.
// Integer arithmetic stays exact and wraps on overflow as Go's does; arithmetic
// involving a fractional float uses float64. Division or remainder by zero is
// [ErrDivideByZero] rather than an infinity.
//
// len accepts strings, arrays, slices, and maps of any concrete Go type, so a
// JSON-compatible value behaves the same before and after Store serialization.
// Equality is deliberately scalar: arrays, slices, maps, structs, and other host
// values report [ErrType] rather than silently choosing deep equality.
//
// There is no implicit truthiness and no implicit conversion. A condition must
// evaluate to a bool and a resolver to a string, or the result is [ErrType].
// Reading a reference the Store does not resolve is [ErrUndefined]; guard it with
// has() when a value is legitimately optional. Because evaluation failures are
// errors rather than false, a broken condition is never mistaken for "keep
// looping".
//
// # Analysis
//
// [Expr.Refs] reports the references an expression reads, so a rule set can be
// checked against the values a graph actually produces before it runs.
package expr
