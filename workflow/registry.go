package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// LeafFactory builds a leaf [Step] from its ID, input reference, and raw config.
// The factory knows the leaf's concrete input/output types and typically ends in
// a call to [Adapt].
type LeafFactory func(id string, input Ref, config json.RawMessage) (Step, error)

// Resolver picks a branch name from the Store (see [Branch]).
type Resolver func(ctx context.Context, s Store) (string, error)

// Condition decides whether a [Loop] should stop after an iteration. It may
// return an error when the condition cannot be evaluated from the current
// Store.
type Condition func(ctx context.Context, iter int, s Store) (bool, error)

// Registry holds the named building blocks that a [Spec] refers to: leaf node
// types, branch resolvers, and loop conditions.
//
// A serialized graph cannot carry closures, so it names its behavior and the
// Registry supplies the code. This is the same constraint every durable/dynamic
// engine has: nodes are registered types, not inline functions.
//
// Registry is safe for concurrent access, although applications should normally
// finish registration before calling Build, Compile, or Validate. Invalid and
// duplicate registrations are accumulated and reported by [Registry.Err]. A
// Registry must not be copied after first use.
type Registry struct {
	mu         sync.RWMutex
	leaves     map[string]LeafFactory
	resolvers  map[string]Resolver
	conditions map[string]Condition
	schemas    map[string]Schema
	problems   []error
}

// NewRegistry returns an empty Registry. The zero Registry is also ready to use.
func NewRegistry() *Registry {
	return &Registry{}
}

// RegisterLeaf registers a leaf factory under a node type name. It returns the
// Registry for chaining.
func (r *Registry) RegisterLeaf(nodeType string, f LeafFactory) *Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	switch {
	case nodeType == "":
		r.addProblemLocked("leaf type is empty")
	case f == nil:
		r.addProblemLocked("leaf %q has a nil factory", nodeType)
	case r.leaves[nodeType] != nil:
		r.addProblemLocked("leaf %q is already registered", nodeType)
	default:
		r.leaves[nodeType] = f
	}
	return r
}

// RegisterResolver registers a branch resolver under a name.
func (r *Registry) RegisterResolver(name string, f Resolver) *Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	switch {
	case name == "":
		r.addProblemLocked("resolver name is empty")
	case f == nil:
		r.addProblemLocked("resolver %q is nil", name)
	case r.resolvers[name] != nil:
		r.addProblemLocked("resolver %q is already registered", name)
	default:
		r.resolvers[name] = f
	}
	return r
}

// RegisterCondition registers a loop condition under a name.
func (r *Registry) RegisterCondition(name string, f Condition) *Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	switch {
	case name == "":
		r.addProblemLocked("condition name is empty")
	case f == nil:
		r.addProblemLocked("condition %q is nil", name)
	case r.conditions[name] != nil:
		r.addProblemLocked("condition %q is already registered", name)
	default:
		r.conditions[name] = f
	}
	return r
}

// Err reports invalid or duplicate registrations accumulated by the fluent
// Register methods. Build, Compile, and Validate return the same error before
// doing any work.
func (r *Registry) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return errors.Join(r.problems...)
}

func (r *Registry) addProblemLocked(format string, args ...any) {
	r.problems = append(r.problems, fmt.Errorf("workflow: registry: "+format, args...))
}

func (r *Registry) initLocked() {
	if r.leaves == nil {
		r.leaves = make(map[string]LeafFactory)
	}
	if r.resolvers == nil {
		r.resolvers = make(map[string]Resolver)
	}
	if r.conditions == nil {
		r.conditions = make(map[string]Condition)
	}
	if r.schemas == nil {
		r.schemas = make(map[string]Schema)
	}
}

func (r *Registry) leafFactory(nodeType string) (LeafFactory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.leaves[nodeType]
	return f, ok
}

func (r *Registry) resolver(name string) (Resolver, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.resolvers[name]
	return f, ok
}

func (r *Registry) condition(name string) (Condition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.conditions[name]
	return f, ok
}

func (r *Registry) schema(nodeType string) Schema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.schemas[nodeType]
}
