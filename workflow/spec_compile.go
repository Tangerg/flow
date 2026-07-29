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

func (compiler specCompiler) compile(spec Spec) (Step, error) {
	switch spec.Kind {
	case KindLeaf:
		return compiler.compileLeaf(spec)
	case KindSequence:
		steps, err := compiler.compileAll(spec.Steps)
		if err != nil {
			return nil, err
		}
		return Sequence(steps...), nil
	case KindParallel:
		steps, err := compiler.compileAll(spec.Steps)
		if err != nil {
			return nil, err
		}
		return Parallel(steps, ParallelConfig{Concurrency: spec.Concurrency}), nil
	case KindBranch:
		return compiler.compileBranch(spec)
	case KindLoop:
		return compiler.compileLoop(spec)
	case KindIteration:
		return compiler.compileIteration(spec)
	default:
		return nil, spec.fieldError(
			"kind",
			fmt.Errorf("%w: unknown kind %q", ErrInvalidSpec, spec.Kind),
		)
	}
}

// CompileSpecJSON validates data against [SpecJSONSchema], strictly unmarshals
// it into a Spec, and compiles it.
func (r *Registry) CompileSpecJSON(data []byte) (Step, error) {
	var spec Spec
	if err := schemaLoader(loadSpecSchema).decode(jsonDocument(data), &spec); err != nil {
		return nil, &SpecError{Field: "json", Err: fmt.Errorf("%w: %w", ErrInvalidSpec, err)}
	}
	return r.CompileSpec(spec)
}

func (compiler specCompiler) compileAll(specs []Spec) ([]Step, error) {
	steps := make([]Step, len(specs))
	for index, spec := range specs {
		step, err := compiler.compile(spec)
		if err != nil {
			return nil, err
		}
		steps[index] = step
	}
	return steps, nil
}

func (compiler specCompiler) compileLeaf(spec Spec) (Step, error) {
	step, field, err := compiler.leafCompiler.compile(spec)
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

func (compiler leafCompiler) compile(spec Spec) (Step, string, error) {
	factory, ok := compiler.registry.lookupLeaf(spec.Type)
	if !ok {
		return nil, "type", fmt.Errorf("%w %q", ErrUnknownNodeType, spec.Type)
	}
	inputs, err := spec.Inputs.withDefault(spec.Input)
	if err != nil {
		return nil, "inputs", err
	}
	step, err := factory(LeafSpec{
		ID:     spec.ID,
		Inputs: inputs,
		Config: bytes.Clone(spec.Config),
	})
	if err != nil {
		field := "config"
		if errors.Is(err, ErrMissingPort) ||
			errors.Is(err, ErrUnknownPort) ||
			errors.Is(err, ErrDuplicatePort) {
			field = "inputs"
		}
		return nil, field, err
	}
	if step == nil {
		return nil, "type", ErrNilStep
	}
	return step, "", nil
}

func (compiler specCompiler) compileBranch(spec Spec) (Step, error) {
	resolver, ok := compiler.registry.lookupResolver(spec.Resolver)
	if !ok {
		return nil, spec.fieldError(
			"resolver",
			fmt.Errorf("%w: unknown resolver %q", ErrInvalidSpec, spec.Resolver),
		)
	}
	cases := make(map[string]Step, len(spec.Cases))
	for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
		step, err := compiler.compile(spec.Cases[name])
		if err != nil {
			return nil, err
		}
		cases[name] = step
	}
	return Branch(spec.ID, resolver, cases), nil
}

func (compiler specCompiler) compileLoop(spec Spec) (Step, error) {
	if spec.Body == nil {
		return nil, spec.fieldError(
			"body",
			fmt.Errorf("%w: loop body is required", ErrInvalidSpec),
		)
	}
	condition, ok := compiler.registry.lookupCondition(spec.Condition)
	if !ok {
		return nil, spec.fieldError(
			"condition",
			fmt.Errorf("%w: unknown condition %q", ErrInvalidSpec, spec.Condition),
		)
	}
	body, err := compiler.compile(*spec.Body)
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

func (compiler specCompiler) compileIteration(spec Spec) (Step, error) {
	switch {
	case spec.Input == (Ref{}):
		return nil, spec.fieldError(
			"input",
			fmt.Errorf("%w: iteration input is required", ErrInvalidSpec),
		)
	case spec.Body == nil:
		return nil, spec.fieldError(
			"body",
			fmt.Errorf("%w: iteration body is required", ErrInvalidSpec),
		)
	case spec.BodyOutput == (Ref{}):
		return nil, spec.fieldError(
			"bodyOutput",
			fmt.Errorf("%w: iteration body output is required", ErrInvalidSpec),
		)
	}
	body, err := compiler.compile(*spec.Body)
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
