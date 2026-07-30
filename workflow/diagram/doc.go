// Package diagram renders workflow Graph definitions for diagnostics and
// documentation. Output is deterministic: nodes retain declaration order,
// named ports are sorted, and generated Mermaid node IDs contain no application
// data.
//
// Rendering does not validate a Graph; use workflow.Registry.ValidateGraph
// before presenting a definition as executable.
package diagram
