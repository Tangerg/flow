package workflow

import (
	"fmt"
	"slices"
)

// ValidateGraph checks a Graph without compiling it: unique IDs, known node
// types, cycles, and well-formed dependencies. For node types with a registered
// [NodeSchema], it also checks config, routing outlets, complete input wiring,
// output presence, and edge types. It is intended to power a visual editor's
// live feedback without executing user factories.
func (r *Registry) ValidateGraph(graph Graph) error {
	_, err := r.snapshot().validateGraph(graph)
	return err
}

func (r registrySnapshot) validateGraph(graph Graph) (graphPlan, error) {
	plan, err := graph.plan()
	if err != nil {
		return graphPlan{}, err
	}
	validator := graphValidator{registry: r, plan: plan}
	if err := validator.validateNodes(graph.Nodes); err != nil {
		return graphPlan{}, err
	}
	return plan, nil
}

// graphValidator owns the immutable registration and dependency views used by
// one semantic validation pass. Keeping that state together prevents the
// Registry abstraction from accumulating graph-specific methods and makes
// every node check observe the same plan.
type graphValidator struct {
	registry registrySnapshot
	plan     graphPlan
}

func (g graphValidator) validateNodes(nodes []GraphNode) error {
	for index, node := range nodes {
		if err := g.validateNode(index, node); err != nil {
			return err
		}
	}
	return nil
}

func (g graphValidator) validateNode(index int, node GraphNode) error {
	path := graphNodePath(index)
	if _, ok := g.registry.lookupNode(node.Type); !ok {
		return &GraphError{
			Path:   path,
			NodeID: node.ID,
			Field:  fieldType,
			Err:    fmt.Errorf("%w %q", ErrUnknownNodeType, node.Type),
		}
	}

	registered, schemaKnown := g.registry.lookupNodeSchema(node.Type)
	if err := registered.validateConfig(node.Config); err != nil {
		return &GraphError{
			Path:   path,
			NodeID: node.ID,
			Field:  fieldConfig,
			Err:    err,
		}
	}
	if err := g.validateOutputRefs(path, node); err != nil {
		return err
	}
	if schemaKnown {
		if err := registered.schema.validateInputs(node.Inputs, g.producerOutput); err != nil {
			return &GraphError{
				Path:   path,
				NodeID: node.ID,
				Field:  fieldInputs,
				Err:    err,
			}
		}
	}
	return g.validateGates(path, node)
}

func (g graphValidator) producerOutput(ref Ref) (ValueType, bool) {
	producer, internal := g.plan.nodesByID[ref.NodeID]
	if !internal {
		// External input from the seed Store has no registered type.
		return "", false
	}
	// NodeSchema describes the conventional output as a whole. A nested member
	// has no independently declared type.
	if ref.Path != outputPath {
		return TypeAny, true
	}
	producerSchema, known := g.registry.lookupNodeSchema(producer.Type)
	if !known {
		return TypeAny, true
	}
	return producerSchema.schema.Output, true
}

// validateOutputRefs enforces the Store-sealed NodeFactory boundary at the
// graph edge: an internal node exposes only its conventional output and values
// nested below it. DependsOn expresses ordering without data. This check is
// independent of the consuming node's schema, because unchecked input types do
// not make an impossible Store cell appear at run time. When schema metadata is
// absent, CompileGraph performs the corresponding output-presence check against
// the concrete boundary returned by the NodeFactory.
func (g graphValidator) validateOutputRefs(path string, node GraphNode) error {
	for _, port := range node.Inputs.PortNames() {
		ref := node.Inputs[port]
		producer, internal := g.plan.nodesByID[ref.NodeID]
		if !internal {
			continue
		}
		if !ref.withinOutput() {
			return &GraphError{
				Path:   path,
				NodeID: node.ID,
				Field:  fieldInputs,
				Err: fmt.Errorf(
					"%w: input port %q reads %s outside the producer's output",
					ErrIncompatibleType,
					port,
					ref,
				),
			}
		}
		registered, known := g.registry.lookupNodeSchema(producer.Type)
		if known && registered.schema.Output == "" {
			return &GraphError{
				Path:   path,
				NodeID: node.ID,
				Field:  fieldInputs,
				Err: fmt.Errorf(
					"%w: input port %q reads %s from node type %q, which declares no output",
					ErrIncompatibleType,
					port,
					ref,
					producer.Type,
				),
			}
		}
		if known && !registered.schema.Output.acceptsCellPath(
			ref,
			producer.ID,
			outputKey,
		) {
			return &GraphError{
				Path:   path,
				NodeID: node.ID,
				Field:  fieldInputs,
				Err: fmt.Errorf(
					"%w: input port %q reads %s, which cannot resolve within output type %q",
					ErrIncompatibleType,
					port,
					ref,
					registered.schema.Output,
				),
			}
		}
	}
	return nil
}

func (g graphValidator) validateGates(path string, node GraphNode) error {
	for _, gate := range node.When {
		source := g.plan.nodesByID[gate.NodeID]
		registered, ok := g.registry.lookupNodeSchema(source.Type)
		if !ok || len(registered.schema.Outlets) == 0 {
			return &GraphError{
				Path:   path,
				NodeID: node.ID,
				Field:  fieldWhen,
				Err: fmt.Errorf(
					"routing node %q type %q declares no outlets",
					gate.NodeID,
					source.Type,
				),
			}
		}
		if !slices.Contains(registered.schema.Outlets, gate.Outlet) {
			return &GraphError{
				Path:   path,
				NodeID: node.ID,
				Field:  fieldWhen,
				Err: fmt.Errorf(
					"%w %q on routing node %q",
					ErrUnknownOutlet,
					gate.Outlet,
					gate.NodeID,
				),
			}
		}
	}
	return nil
}
