package workflow

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// CompileSpec compiles a Spec into a Step using the registered building blocks.
func (r *Registry) CompileSpec(spec Spec) (Step, error) {
	if err := r.validateSpec(spec); err != nil {
		return nil, err
	}
	return (specCompiler{
		leafCompiler: leafCompiler{registry: r},
	}).compile(spec)
}

// specCompiler owns the recursive construction of one validated Spec.
type specCompiler struct {
	leafCompiler
}

func (s specCompiler) compile(spec Spec) (Step, error) {
	switch spec.Kind {
	case KindLeaf:
		return s.compileLeaf(spec)
	case KindSequence:
		steps, err := s.compileAll(spec.Steps)
		if err != nil {
			return nil, err
		}
		return Sequence(steps...), nil
	case KindParallel:
		steps, err := s.compileAll(spec.Steps)
		if err != nil {
			return nil, err
		}
		return Parallel(steps, ParallelConfig{Concurrency: spec.Concurrency}), nil
	case KindBranch:
		return s.compileBranch(spec)
	case KindLoop:
		return s.compileLoop(spec)
	case KindIteration:
		return s.compileIteration(spec)
	case KindSubgraph:
		return s.compileSubgraph(spec)
	default:
		return nil, spec.fieldError(
			fieldKind,
			fmt.Errorf("%w: unknown kind %q", ErrInvalidSpec, spec.Kind),
		)
	}
}

// CompileSpecJSON validates data against [SpecJSONSchema], strictly unmarshals
// it into a Spec, and compiles it.
func (r *Registry) CompileSpecJSON(data []byte) (Step, error) {
	var spec Spec
	if err := schemaLoader(loadSpecSchema).decode(jsonDocument(data), &spec); err != nil {
		return nil, &SpecError{Field: fieldJSON, Err: fmt.Errorf("%w: %w", ErrInvalidSpec, err)}
	}
	return r.CompileSpec(spec)
}

func (s specCompiler) compileAll(specs []Spec) ([]Step, error) {
	steps := make([]Step, len(specs))
	for index, spec := range specs {
		step, err := s.compile(spec)
		if err != nil {
			return nil, err
		}
		steps[index] = step
	}
	return steps, nil
}

func (s specCompiler) compileLeaf(spec Spec) (Step, error) {
	step, field, err := s.leafCompiler.compile(spec)
	if err != nil {
		return nil, spec.fieldError(field, err)
	}
	return step, nil
}

// leafCompiler builds one leaf without attaching a nested-Spec or flat-Graph
// boundary. The two compilers share the construction logic but report failures
// in their own vocabulary.
type leafCompiler struct {
	registry *Registry
}

func (l leafCompiler) compile(spec Spec) (Step, string, error) {
	factory, ok := l.registry.lookupNode(spec.Type)
	if !ok {
		return nil, fieldType, fmt.Errorf("%w %q", ErrUnknownNodeType, spec.Type)
	}
	step, err := factory(NodeSpec{
		ID:     spec.ID,
		Inputs: maps.Clone(spec.Inputs),
		Config: bytes.Clone(spec.Config),
	})
	if err != nil {
		field := fieldConfig
		if errors.Is(err, ErrMissingPort) ||
			errors.Is(err, ErrUnknownPort) {
			field = fieldInputs
		}
		return nil, field, err
	}
	if isNilNode(step) {
		return nil, fieldType, ErrNilStep
	}
	return step, "", nil
}

func (s specCompiler) compileBranch(spec Spec) (Step, error) {
	resolver, ok := s.registry.lookupResolver(spec.Resolver)
	if !ok {
		return nil, spec.fieldError(
			fieldResolver,
			fmt.Errorf("%w: unknown resolver %q", ErrInvalidSpec, spec.Resolver),
		)
	}
	cases := make(map[string]Step, len(spec.Cases))
	for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
		step, err := s.compile(spec.Cases[name])
		if err != nil {
			return nil, err
		}
		cases[name] = step
	}
	return Branch(spec.ID, resolver, cases), nil
}

func (s specCompiler) compileLoop(spec Spec) (Step, error) {
	if spec.Body == nil {
		return nil, spec.fieldError(
			fieldBody,
			fmt.Errorf("%w: loop body is required", ErrInvalidSpec),
		)
	}
	condition, ok := s.registry.lookupCondition(spec.Condition)
	if !ok {
		return nil, spec.fieldError(
			fieldCondition,
			fmt.Errorf("%w: unknown condition %q", ErrInvalidSpec, spec.Condition),
		)
	}
	body, err := s.compile(*spec.Body)
	if err != nil {
		return nil, err
	}
	return Loop(
		spec.ID,
		body,
		condition,
		LoopConfig{MaxIterations: spec.MaxIterations},
	), nil
}

func (s specCompiler) compileIteration(spec Spec) (Step, error) {
	switch {
	case spec.Input == (Ref{}):
		return nil, spec.fieldError(
			fieldInput,
			fmt.Errorf("%w: iteration input is required", ErrInvalidSpec),
		)
	case spec.Body == nil:
		return nil, spec.fieldError(
			fieldBody,
			fmt.Errorf("%w: iteration body is required", ErrInvalidSpec),
		)
	case spec.BodyOutput == (Ref{}):
		return nil, spec.fieldError(
			fieldBodyOutput,
			fmt.Errorf("%w: iteration body output is required", ErrInvalidSpec),
		)
	}
	body, err := s.compile(*spec.Body)
	if err != nil {
		return nil, err
	}
	return Iteration(IterationConfig{
		ID:          spec.ID,
		Input:       spec.Input,
		Body:        body,
		BodyOutput:  spec.BodyOutput,
		Concurrency: spec.Concurrency,
	}), nil
}

func (s specCompiler) compileSubgraph(spec Spec) (Step, error) {
	if spec.Body == nil {
		return nil, spec.fieldError(
			fieldBody,
			fmt.Errorf("%w: subgraph body is required", ErrInvalidSpec),
		)
	}
	if spec.BodyOutput == (Ref{}) {
		return nil, spec.fieldError(
			fieldBodyOutput,
			fmt.Errorf("%w: subgraph body output is required", ErrInvalidSpec),
		)
	}
	body, err := s.compile(*spec.Body)
	if err != nil {
		return nil, err
	}
	return Subgraph(SubgraphConfig{
		ID:         spec.ID,
		Inputs:     spec.Inputs,
		Body:       body,
		BodyOutput: spec.BodyOutput,
	}), nil
}
