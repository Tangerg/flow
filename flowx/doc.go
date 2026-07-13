// Package flowx provides derived control-flow combinators built on package
// flow's minimal primitives — the syntactic sugar the core deliberately omits.
//
// Package flow stays intentionally small: it holds only the primitives that
// cannot be expressed in terms of one another. Everything derivable lives here,
// with exactly one implementation per control-flow shape:
//
//   - [Chain]: variadic same-type sequence (sugar over flow.Then).
//   - [FanOut]: run several nodes on the same input concurrently.
//   - [Combine]: heterogeneous fan-in — merge two differently typed nodes.
//   - [Fallback]: run a primary node, then an alternate if it fails.
//
// These are pure control-flow composition — no tunable policies (retry, timeout,
// circuit breaking) and no external integrations (tracing, metrics). A decorator
// is just a flow.Node[I, O] -> flow.Node[I, O]: wrap a node yourself or reach for
// a dedicated library. The parallel-any primitive ("first success wins") lives in
// the core as flow.Race, the disjunction twin of flow.Map.
package flowx
