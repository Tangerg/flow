package workflow

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
)

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
)

// Spec is a serializable description of a workflow graph. Its Kind selects which
// fields apply; [Registry.CompileSpec] compiles it into a [Step]. Behavior (leaf types,
// resolvers, conditions) is referenced by name and resolved through the Registry.
type Spec struct {
	Kind SpecKind `json:"kind"`

	// Leaf and iteration node ID.
	ID string `json:"id,omitempty"`

	// Leaf: registered type and its raw config.
	Type   string          `json:"type,omitempty"`
	Config json.RawMessage `json:"config,omitempty"`

	// Leaf input, and iteration array input.
	Input *Ref `json:"input,omitempty"`

	// Sequence and parallel children.
	Steps []Spec `json:"steps,omitempty"`

	// Branch: registered resolver name and named cases.
	Resolver string          `json:"resolver,omitempty"`
	Cases    map[string]Spec `json:"cases,omitempty"`

	// Loop and iteration body.
	Body *Spec `json:"body,omitempty"`

	// Loop: registered condition name and iteration cap.
	Condition     string `json:"condition,omitempty"`
	MaxIterations int    `json:"maxIterations,omitempty"`

	// Iteration: where to read each element's result in the post-run Store.
	BodyOutput *Ref `json:"bodyOutput,omitempty"`

	// Parallel and iteration concurrency limit (0 = unbounded).
	Concurrency int `json:"concurrency,omitempty"`
}

// CompileSpec compiles a Spec into a Step using the registered building blocks.
func (r *Registry) CompileSpec(spec Spec) (Step, error) {
	if err := r.validateSpec(spec); err != nil {
		return nil, err
	}
	return r.build(spec)
}

func (r *Registry) build(spec Spec) (Step, error) {
	switch spec.Kind {
	case KindLeaf:
		return r.buildLeaf(spec)
	case KindSequence:
		steps, err := r.buildAll(spec.Steps)
		if err != nil {
			return nil, err
		}
		return Sequence(steps...), nil
	case KindParallel:
		steps, err := r.buildAll(spec.Steps)
		if err != nil {
			return nil, err
		}
		if spec.Concurrency > 0 {
			return ParallelN(spec.Concurrency, steps...), nil
		}
		return Parallel(steps...), nil
	case KindBranch:
		return r.buildBranch(spec)
	case KindLoop:
		return r.buildLoop(spec)
	case KindIteration:
		return r.buildIteration(spec)
	default:
		return nil, specError(spec, "kind", fmt.Errorf("%w: unknown kind %q", ErrInvalidSpec, spec.Kind))
	}
}

// CompileSpecJSON strictly unmarshals data into a Spec and compiles it.
func (r *Registry) CompileSpecJSON(data []byte) (Step, error) {
	var spec Spec
	if err := decodeStrict(data, &spec); err != nil {
		return nil, &SpecError{Field: "json", Err: fmt.Errorf("%w: %v", ErrInvalidSpec, err)}
	}
	return r.CompileSpec(spec)
}

func (r *Registry) buildAll(specs []Spec) ([]Step, error) {
	steps := make([]Step, len(specs))
	for i, sp := range specs {
		step, err := r.build(sp)
		if err != nil {
			return nil, err
		}
		steps[i] = step
	}
	return steps, nil
}

func (r *Registry) buildLeaf(spec Spec) (Step, error) {
	f, ok := r.leafFactory(spec.Type)
	if !ok {
		return nil, specError(spec, "type", fmt.Errorf("%w %q", ErrUnknownNodeType, spec.Type))
	}
	var input Ref
	if spec.Input != nil {
		input = *spec.Input
	}
	step, err := f(spec.ID, input, spec.Config)
	if err != nil {
		return nil, specError(spec, "config", err)
	}
	return step, nil
}

func (r *Registry) buildBranch(spec Spec) (Step, error) {
	resolve, ok := r.resolver(spec.Resolver)
	if !ok {
		return nil, specError(spec, "resolver", fmt.Errorf("%w: unknown resolver %q", ErrInvalidSpec, spec.Resolver))
	}
	cases := make(map[string]Step, len(spec.Cases))
	for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
		step, err := r.build(spec.Cases[name])
		if err != nil {
			return nil, err
		}
		cases[name] = step
	}
	return Branch(resolve, cases), nil
}

func (r *Registry) buildLoop(spec Spec) (Step, error) {
	if spec.Body == nil {
		return nil, specError(spec, "body", fmt.Errorf("%w: required", ErrInvalidSpec))
	}
	cond, ok := r.condition(spec.Condition)
	if !ok {
		return nil, specError(spec, "condition", fmt.Errorf("%w: unknown condition %q", ErrInvalidSpec, spec.Condition))
	}
	body, err := r.build(*spec.Body)
	if err != nil {
		return nil, err
	}
	if spec.MaxIterations > 0 {
		return LoopLimit(spec.MaxIterations, body, cond), nil
	}
	return Loop(body, cond), nil
}

func (r *Registry) buildIteration(spec Spec) (Step, error) {
	if spec.Body == nil {
		return nil, specError(spec, "body", fmt.Errorf("%w: required", ErrInvalidSpec))
	}
	if spec.Input == nil || spec.BodyOutput == nil {
		return nil, specError(spec, "iteration", fmt.Errorf("%w: input and bodyOutput are required", ErrInvalidSpec))
	}
	body, err := r.build(*spec.Body)
	if err != nil {
		return nil, err
	}
	if spec.Concurrency > 0 {
		return IterationN(spec.Concurrency, spec.ID, *spec.Input, body, *spec.BodyOutput), nil
	}
	return Iteration(spec.ID, *spec.Input, body, *spec.BodyOutput), nil
}
