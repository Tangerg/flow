package workflow

import (
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
		registry: r,
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
	// Its caller has validated this already; compilation asks again because it
	// dereferences Body, and asking the same rule keeps the defense from becoming
	// a second copy of it.
	if err := spec.requireKindFields(); err != nil {
		return nil, err
	}
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
		return nil, spec.unknownKindError()
	}
}

// CompileSpecJSON validates data against [SpecJSONSchema], strictly unmarshals
// it into a Spec, and compiles it.
func (r *Registry) CompileSpecJSON(data []byte) (Step, error) {
	spec, err := decodeSpecDocument(data)
	if err != nil {
		return nil, specJSONError(err)
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
	step, field, err := s.leafCompiler.compile(spec.leafNode())
	if err != nil {
		return nil, spec.fieldError(field, err)
	}
	return step, nil
}

// leafNode is one leaf reduced to what building it takes: the registered type
// that builds it and the construction inputs its [NodeFactory] receives. A
// nested Spec and a flat GraphNode each convert to it, so the leaf compiler
// never receives a composite definition whose remaining fields it would have to
// ignore.
type leafNode struct {
	nodeType string
	spec     NodeSpec
}

func (s Spec) leafNode() leafNode {
	return leafNode{
		nodeType: s.Type,
		spec:     NodeSpec{ID: s.ID, Inputs: s.Inputs, Config: s.Config},
	}
}

// leafCompiler builds one leaf without attaching a nested-Spec or flat-Graph
// boundary. The two compilers share the construction logic but report failures
// in their own vocabulary.
type leafCompiler struct {
	registry registrySnapshot
}

// compile builds one leaf and names the field a failure belongs to. Both callers
// check the node type against this same registry snapshot before arriving, so the
// missing-factory branch is not one a public route can reach: it keeps a table that
// lost an entry from becoming a nil call, and the only way in is compiling without
// validating first, which one internal test does. The category a caller does see is
// pinned where it is actually reported --
// TestUnknownNodeTypeIsMatchableOnEveryRouteThatReportsIt.
func (l leafCompiler) compile(node leafNode) (definedStep, string, error) {
	factory, ok := l.registry.lookupNode(node.nodeType)
	if !ok {
		return nil, fieldType, fmt.Errorf("%w %q", ErrUnknownNodeType, node.nodeType)
	}
	step, err := factory(node.spec.clone())
	if err != nil {
		suspended, normalized := normalizeDefinitionError("node factory", err)
		if suspended {
			// A factory constructs an immutable definition. Treat a suspension
			// here as a broken node-type contract, never as a resumable run or
			// a JSON config error the caller could repair.
			return nil, fieldType, normalized
		}
		return nil, factoryErrorField(err), err
	}
	if isNilNode(step) {
		return nil, fieldType, ErrNilStep
	}
	boundary, err := (nodeBoundary{id: node.spec.ID}).validate(step)
	if err != nil {
		return nil, fieldType, err
	}
	if registered, known := l.registry.lookupNodeSchema(node.nodeType); known {
		if err := registered.validateOutput(boundary.definition().output); err != nil {
			return nil, fieldType, &RegistrationError{
				Kind: registrationSchema,
				Name: node.nodeType,
				Err:  err,
			}
		}
	}
	return boundary, "", nil
}

// factoryErrorField maps the stable error vocabulary of a NodeFactory to the part of
// a definition a caller can fix. Missing wiring is an input error; registration
// failures and nil construction functions or nodes are type errors. Other
// factory failures are attributed to config, the only application-defined
// construction input left after ID and wiring have passed validation.
func factoryErrorField(err error) string {
	tree := errorTree{root: err}
	switch {
	case tree.matches(ErrInvalidRegistration):
		return fieldType
	case tree.matches(ErrMissingPort), tree.matches(ErrUnknownPort):
		return fieldInputs
	case tree.matches(flow.ErrNilFunc),
		tree.matches(flow.ErrNilNode),
		tree.matches(ErrNilStep):
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
	resolver, err := s.registry.requireResolver(spec)
	if err != nil {
		return nil, err
	}
	cases := make(map[string]Step, len(spec.Cases))
	for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
		step, err := s.compile(spec.Cases[name])
		if err != nil {
			return nil, locateSpecError(err, fieldCases, name)
		}
		cases[name] = step
	}
	return Branch(BranchConfig{ID: spec.ID, Resolver: resolver, Cases: cases}), nil
}

func (s specCompiler) compileLoop(spec Spec) (Step, error) {
	condition, err := s.registry.requireCondition(spec)
	if err != nil {
		return nil, err
	}
	body, err := s.compile(*spec.Body)
	if err != nil {
		return nil, locateSpecError(err, fieldBody)
	}
	return Loop(LoopConfig{
		ID:            spec.ID,
		Body:          body,
		Condition:     condition,
		MaxIterations: spec.MaxIterations,
	}), nil
}

func (s specCompiler) compileIteration(spec Spec) (Step, error) {
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
	if err := step.definition().iterationOutputCondition(); err != nil {
		return nil, spec.fieldError(fieldBodyOutput, err)
	}
	return step, nil
}

func (s specCompiler) compileSubgraph(spec Spec) (Step, error) {
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
	if err := step.definition().subgraphOutputCondition(); err != nil {
		return nil, spec.fieldError(fieldBodyOutput, err)
	}
	return step, nil
}
