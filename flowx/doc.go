// Package flowx provides the derived combinators and decorators built on the
// minimal [core] primitives.
//
// core stays intentionally small; the conveniences that are derivable from it
// live here instead:
//
//   - Combinators: [FanOut], [FanOutAll], [MapAll], [Combine2] (heterogeneous
//     fan-in), [Race] (first success wins), [Identity], and [Chain].
//   - Decorators — type-preserving core.Node[I, O] -> core.Node[I, O]: [Retry],
//     [Timeout], [Trace], and [Fallback], composed fluently with [Wrap].
//
// All combinators except Race are thin derivations of core.Map; Race needs its
// own goroutines because "first to finish" cannot be expressed by a wait-for-all
// map.
package flowx
