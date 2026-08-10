package workflow

import (
	"encoding/json"
	"errors"
)

// Kind identifies the structural shape of a workflow step. [Spec] accepts the
// configurable kinds below, while [Description] may also report code-built
// boundary kinds. As a string type, Kind still permits application-defined
// [Describer] implementations to name their own shapes.
type Kind string

// Kinds shared by Spec and Description.
const (
	KindLeaf      Kind = "leaf"
	KindSequence  Kind = "sequence"
	KindParallel  Kind = "parallel"
	KindBranch    Kind = "branch"
	KindLoop      Kind = "loop"
	KindIteration Kind = "iteration"
	KindSubgraph  Kind = "subgraph"
)

// Additional kinds produced by Describe for code-built boundaries that are not
// standalone Spec shapes.
const (
	KindAwait     Kind = "await"
	KindGraph     Kind = "graph"
	KindInterrupt Kind = "interrupt"
	KindOpaque    Kind = "opaque"
)

// Spec is a serializable structured workflow definition. Its Kind selects which
// fields apply; [Registry.CompileSpec] compiles it into a [Step]. Behavior is
// referenced by name and resolved through the Registry. The zero Spec is
// invalid because every Spec requires an explicit Kind.
//
// Treat nested Specs and their maps, slices, pointers, and raw config as
// immutable while validating or compiling. A compiled Step does not retain the
// Spec or any of those mutable values.
//
//nolint:recvcheck // UnmarshalJSON must be a pointer method to satisfy json.Unmarshaler.
type Spec struct {
	Kind Kind `json:"kind"`

	// Node ID, required by every kind that records something under its own name:
	// leaf, branch, loop, iteration, and subgraph. Sequence and parallel are
	// purely structural and take none.
	ID string `json:"id,omitempty"`

	// Leaf: registered type and its raw config. Config is absent only when it has
	// zero length; non-empty bytes must contain one complete JSON value.
	Type   string          `json:"type,omitempty"`
	Config json.RawMessage `json:"config,omitempty"`

	// Iteration array input. Leaf inputs are always named in Inputs. A zero Ref
	// means the field is absent.
	Input Ref `json:"input,omitzero"`

	// Leaf inputs wired by port name. A single-input leaf uses [DefaultPort]. For
	// a subgraph, keys name inner seed IDs and values reference the outer Store.
	Inputs Inputs `json:"inputs,omitempty"`

	// Sequence and parallel children.
	Steps []Spec `json:"steps,omitempty"`

	// Branch: registered resolver name and named cases.
	Resolver string          `json:"resolver,omitempty"`
	Cases    map[string]Spec `json:"cases,omitempty"`

	// Loop, iteration, and subgraph body.
	Body *Spec `json:"body,omitempty"`

	// Loop: registered condition name and iteration cap.
	Condition     string `json:"condition,omitempty"`
	MaxIterations int    `json:"maxIterations,omitempty"`

	// Iteration and subgraph: where to read the body's result in its post-run
	// Store. A zero Ref means the field is absent.
	BodyOutput Ref `json:"bodyOutput,omitzero"`

	// Parallel and iteration concurrency limit (0 = unbounded).
	Concurrency int `json:"concurrency,omitempty"`
}

// MarshalJSON encodes s only when the complete document can cross the strict
// JSON boundary unchanged and within [MaxNestingDepth]. Registry-dependent
// definition checks remain the responsibility of [Registry.ValidateSpec] or
// compilation.
func (s Spec) MarshalJSON() ([]byte, error) {
	encoder := specJSONEncoder{
		root:   s,
		active: make(map[*Spec]struct{}),
	}
	return encoder.marshal()
}

// UnmarshalJSON atomically replaces s with one strictly decoded Spec. It uses
// the same JSON Schema, duplicate-member, Unicode, integer, unknown-field, and
// nesting rules as [ValidateSpecJSON] and [Registry.CompileSpecJSON].
func (s *Spec) UnmarshalJSON(data []byte) error {
	if s == nil {
		return &SpecError{Field: fieldJSON, Err: errors.New("nil spec receiver")}
	}
	next, err := decodeSpecDocument(jsonDocument(data))
	if err != nil {
		return &SpecError{Field: fieldJSON, Err: err}
	}
	*s = next
	return nil
}
