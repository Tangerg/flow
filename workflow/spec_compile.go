package workflow

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/Tangerg/flow"
)

// CompileSpec validates a Spec and the visible static shape of the built-in
// definition constructed from it, then returns the compiled Step.
func (r *Registry) CompileSpec(spec Spec) (Step, error) {
	return r.snapshot().compileSpec(spec)
}

func (r registrySnapshot) compileSpec(spec Spec) (Step, error) {
	if err := r.validateSpec(spec); err != nil {
		return nil, err
	}
	compiled, err := (specCompiler{
		leafCompiler: leafCompiler{registry: r},
	}).compile(spec)
	if err != nil {
		return nil, err
	}
	if err := validateNode(compiled); err != nil {
		return nil, spec.fieldError("", err)
	}
	return compiled, nil
}

// specCompiler owns the recursive construction of one validated Spec.
// specValidator rejects structural constraints it can prove from registration
// metadata; construction closes the remaining extension-boundary uncertainty
// against the concrete Steps returned by factories.
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
		return Parallel(ParallelConfig{Steps: steps, Concurrency: spec.Concurrency}), nil
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
			fmt.Errorf("unknown kind %q", spec.Kind),
		)
	}
}

// CompileSpecJSON validates data against [SpecJSONSchema], strictly unmarshals
// it into a Spec, and compiles it.
func (r *Registry) CompileSpecJSON(data []byte) (Step, error) {
	spec, err := decodeSpecDocument(jsonDocument(data))
	if err != nil {
		return nil, &SpecError{Field: fieldJSON, Err: err}
	}
	return r.CompileSpec(spec)
}

func (s specCompiler) compileAll(specs []Spec) ([]Step, error) {
	steps := make([]Step, len(specs))
	for index, spec := range specs {
		step, err := s.compile(spec)
		if err != nil {
			return nil, locateSpecError(err, fieldSteps, strconv.Itoa(index))
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
	registry registrySnapshot
}

func (l leafCompiler) compile(spec Spec) (definedStep, string, error) {
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
		if errors.Is(err, ErrSuspended) {
			// A factory constructs an immutable definition. Treat a suspension
			// here as a broken node-type contract, never as a resumable run or
			// a JSON config error the caller could repair.
			return nil, fieldType, normalizeDefinitionError("node factory", err)
		}
		return nil, l.errorField(err), err
	}
	if isNilNode(step) {
		return nil, fieldType, ErrNilStep
	}
	boundary, err := (nodeBoundary{id: spec.ID}).validate(step)
	if err != nil {
		return nil, fieldType, err
	}
	if registered, known := l.registry.lookupNodeSchema(spec.Type); known {
		if err := registered.validateOutput(boundary.definition().output); err != nil {
			return nil, fieldType, &RegistrationError{
				Kind: registrationSchema,
				Name: spec.Type,
				Err:  err,
			}
		}
	}
	return boundary, "", nil
}

// errorField maps the stable error vocabulary of a NodeFactory to the part of
// a definition a caller can fix. Missing wiring is an input error; registration
// failures and nil construction functions or nodes are type errors. Other
// factory failures are attributed to config, the only application-defined
// construction input left after ID and wiring have passed validation.
func (leafCompiler) errorField(err error) string {
	switch {
	case errors.Is(err, ErrInvalidRegistration):
		return fieldType
	case errors.Is(err, ErrMissingPort), errors.Is(err, ErrUnknownPort):
		return fieldInputs
	case errors.Is(err, flow.ErrNilFunc),
		errors.Is(err, flow.ErrNilNode),
		errors.Is(err, ErrNilStep):
		return fieldType
	default:
		return fieldConfig
	}
}

// nodeBoundary owns the contract between an extensible NodeFactory and the
// compiled runtime. Factories may choose the concrete Step, but a successful
// compilation must still be able to prove one execution identity and one
// sealed Store boundary for the enclosing GraphNode or leaf Spec.
type nodeBoundary struct {
	id string
}

func (n nodeBoundary) validate(step Step) (definedStep, error) {
	if err := validateNode(step); err != nil {
		return nil, err
	}
	defined, ok := step.(definedStep)
	if !ok {
		return nil, errors.New(
			"node factory returned an opaque Step; wrap typed work with Leaf or a composite with Subgraph",
		)
	}
	definition := defined.definition()
	if !definition.nodeBoundary() {
		return nil, fmt.Errorf(
			"node factory returned an unsealed %q Step; wrap composites with Subgraph",
			Describe(step).Kind,
		)
	}
	if definition.id != n.id {
		return nil, fmt.Errorf(
			"node factory returned step ID %q; want %q",
			definition.id,
			n.id,
		)
	}
	return defined, nil
}

func (s specCompiler) compileBranch(spec Spec) (Step, error) {
	resolver, ok := s.registry.lookupResolver(spec.Resolver)
	if !ok {
		return nil, spec.fieldError(
			fieldResolver,
			fmt.Errorf("unknown resolver %q", spec.Resolver),
		)
	}
	cases := make(map[string]Step, len(spec.Cases))
	for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
		step, err := s.compile(spec.Cases[name])
		if err != nil {
			return nil, locateSpecError(err, fieldCases, name)
		}
		cases[name] = step
	}
	return Branch(BranchConfig{ID: spec.ID, Resolve: resolver, Cases: cases}), nil
}

func (s specCompiler) compileLoop(spec Spec) (Step, error) {
	if spec.Body == nil {
		return nil, spec.fieldError(
			fieldBody,
			errors.New("loop body is required"),
		)
	}
	condition, ok := s.registry.lookupCondition(spec.Condition)
	if !ok {
		return nil, spec.fieldError(
			fieldCondition,
			fmt.Errorf("unknown condition %q", spec.Condition),
		)
	}
	body, err := s.compile(*spec.Body)
	if err != nil {
		return nil, locateSpecError(err, fieldBody)
	}
	return Loop(LoopConfig{
		ID:            spec.ID,
		Body:          body,
		Done:          condition,
		MaxIterations: spec.MaxIterations,
	}), nil
}

func (s specCompiler) compileIteration(spec Spec) (Step, error) {
	switch {
	case spec.Input == (Ref{}):
		return nil, spec.fieldError(
			fieldInput,
			errors.New("iteration input is required"),
		)
	case spec.Body == nil:
		return nil, spec.fieldError(
			fieldBody,
			errors.New("iteration body is required"),
		)
	case spec.BodyOutput == (Ref{}):
		return nil, spec.fieldError(
			fieldBodyOutput,
			errors.New("iteration body output is required"),
		)
	}
	body, err := s.compile(*spec.Body)
	if err != nil {
		return nil, locateSpecError(err, fieldBody)
	}
	step := (IterationConfig{
		ID:          spec.ID,
		Input:       spec.Input,
		Body:        body,
		BodyOutput:  spec.BodyOutput,
		Concurrency: spec.Concurrency,
	}).step()
	if err := step.definition().validateIterationOutput(); err != nil {
		return nil, spec.fieldError(fieldBodyOutput, err)
	}
	return step, nil
}

func (s specCompiler) compileSubgraph(spec Spec) (Step, error) {
	if spec.Body == nil {
		return nil, spec.fieldError(
			fieldBody,
			errors.New("subgraph body is required"),
		)
	}
	if spec.BodyOutput == (Ref{}) {
		return nil, spec.fieldError(
			fieldBodyOutput,
			errors.New("subgraph body output is required"),
		)
	}
	body, err := s.compile(*spec.Body)
	if err != nil {
		return nil, locateSpecError(err, fieldBody)
	}
	step := (SubgraphConfig{
		ID:         spec.ID,
		Inputs:     spec.Inputs,
		Body:       body,
		BodyOutput: spec.BodyOutput,
	}).step()
	if err := step.definition().validateSubgraphOutput(); err != nil {
		return nil, spec.fieldError(fieldBodyOutput, err)
	}
	return step, nil
}
