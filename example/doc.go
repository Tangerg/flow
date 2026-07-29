// Package example contains an executable learning path for flow.
//
// The examples progress from a single typed node through composition, dynamic
// workflows, DAGs, the JSON DSL, data-driven routing, and persisted resumption.
// They intentionally use only public APIs and are run by "go test", so the
// documentation cannot silently drift away from the library.
//
// This package is instructional. It exports no helpers; application code should
// import flow, flowx, workflow, or workflow/expr directly.
package example
