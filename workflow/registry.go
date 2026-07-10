package workflow

import (
	"context"
	"encoding/json"
)

// LeafFactory builds a leaf [Step] from its ID, input reference, and raw config.
// The factory knows the leaf's concrete input/output types and typically ends in
// a call to [Adapt].
type LeafFactory func(id string, input Ref, config json.RawMessage) (Step, error)

// Resolver picks a branch name from the Store (see [Branch]).
type Resolver func(ctx context.Context, s Store) (string, error)

// Condition decides whether a [Loop] should stop after an iteration.
type Condition func(ctx context.Context, iter int, s Store) bool

// Registry holds the named building blocks that a [Spec] refers to: leaf node
// types, branch resolvers, and loop conditions.
//
// A serialized graph cannot carry closures, so it names its behavior and the
// Registry supplies the code. This is the same constraint every durable/dynamic
// engine has: nodes are registered types, not inline functions.
type Registry struct {
	leaves     map[string]LeafFactory
	resolvers  map[string]Resolver
	conditions map[string]Condition
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		leaves:     map[string]LeafFactory{},
		resolvers:  map[string]Resolver{},
		conditions: map[string]Condition{},
	}
}

// RegisterLeaf registers a leaf factory under a node type name. It returns the
// Registry for chaining.
func (r *Registry) RegisterLeaf(nodeType string, f LeafFactory) *Registry {
	r.leaves[nodeType] = f
	return r
}

// RegisterResolver registers a branch resolver under a name.
func (r *Registry) RegisterResolver(name string, f Resolver) *Registry {
	r.resolvers[name] = f
	return r
}

// RegisterCondition registers a loop condition under a name.
func (r *Registry) RegisterCondition(name string, f Condition) *Registry {
	r.conditions[name] = f
	return r
}
