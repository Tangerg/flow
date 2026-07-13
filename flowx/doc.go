// Package flowx provides derived control-flow combinators built on package
// flow's minimal primitives.
//
// Package flow stays intentionally small; the control-flow shapes derivable from
// it live here instead: [FanOut], [FanOutAll], [MapAll], [Combine2]
// (heterogeneous fan-in), [Race] (first success wins), [Fallback] (try then an
// alternate), [Identity], and [Chain].
//
// These are pure composition — no tunable policies and no external integrations.
// Resilience (retry, timeout, circuit breaking) and observability are the
// caller's concern: a decorator is just a flow.Node[I, O] -> flow.Node[I, O], so
// wrap a node yourself or use a dedicated library.
//
// All combinators except Race are thin derivations of flow.Map; Race needs its
// own goroutines because "first to finish" cannot be expressed by a
// wait-for-all map. Collecting combinators return [Result] values whose Err
// field preserves per-item failures.
package flowx
