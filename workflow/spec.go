package workflow

import "encoding/json"

// SpecKind identifies the shape of a [Spec].
type SpecKind string

// Spec kinds.
const (
	KindLeaf      SpecKind = "leaf"
	KindSequence  SpecKind = "sequence"
	KindParallel  SpecKind = "parallel"
	KindBranch    SpecKind = "branch"
	KindLoop      SpecKind = "loop"
	KindIteration SpecKind = "iteration"
	KindSubgraph  SpecKind = "subgraph"
)

// Spec is a serializable description of a workflow graph. Its Kind selects
// which fields apply; [Registry.CompileSpec] compiles it into a [Step].
// Behavior is referenced by name and resolved through the Registry.
type Spec struct {
	Kind SpecKind `json:"kind"`

	// Node ID, required by every kind that records something under its own name:
	// leaf, branch, loop, iteration, and subgraph. Sequence and parallel are
	// purely structural and take none.
	ID string `json:"id,omitempty"`

	// Leaf: registered type and its raw config.
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
