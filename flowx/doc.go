// Package flowx provides derived combinators and decorators built on package
// flow's minimal primitives.
//
// Package flow stays intentionally small; conveniences derivable from it
// live here instead:
//
//   - Combinators: [FanOut], [FanOutAll], [MapAll], [Combine2] (heterogeneous
//     fan-in), [Race] (first success wins), [Identity], and [Chain].
//   - Decorators — type-preserving flow.Node[I, O] -> flow.Node[I, O]: [Retry],
//     [Timeout], [Trace], and [Fallback].
//
// All combinators except Race are thin derivations of flow.Map; Race needs its
// own goroutines because "first to finish" cannot be expressed by a wait-for-all
// map. Collecting combinators return [Result] values whose Err field preserves
// per-item failures and whose Value may contain a partial result.
package flowx
