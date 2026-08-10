// Package flowx provides control-flow conveniences built from package flow's
// core operations.
//
// Each function adds a derived composition shape rather than a new Node
// protocol:
//
//   - [Chain]: variadic same-type sequence (sugar over flow.Then).
//   - [FanOut]: run several nodes on the same input concurrently.
//   - [Combine]: heterogeneous fan-in from two differently typed nodes.
//   - [Fallback]: run a primary node, then an alternate if it fails.
//
// These composites expose their complete visible child definitions to
// [flow.Validate], just like the core composites.
//
// Definition collections are snapshots: [FanOut] copies its node slice, and
// [Chain] consumes its variadic argument during construction. The Node values
// themselves are retained as-is and must be safe for every concurrent use the
// selected composite permits.
//
// Retry, timeout, circuit breaking, tracing, and metrics are policies rather
// than composition shapes. Implement them as flow.Node[I, O] decorators or use
// a dedicated package. The first-success operation remains [flow.Race] in the
// core.
package flowx
