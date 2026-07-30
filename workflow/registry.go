package workflow

import (
	"context"
	"fmt"
	"sync"
)

// LeafFactory builds a leaf [Step] from a [LeafSpec]. The factory knows the
// leaf's concrete input/output types and typically ends in a call to [Leaf].
// [Factory] covers the common single-input case; write a LeafFactory directly
// when a node reads several ports.
type LeafFactory func(LeafSpec) (Step, error)

// Resolver picks a branch or outlet name from the Store (see [Branch] and
// [Route]).
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
// finish registration before compiling workflows. A Registry must not be copied
// after first use.
type Registry struct {
	mu         sync.RWMutex
	leaves     map[string]LeafFactory
	resolvers  map[string]Resolver
	conditions map[string]Condition
	schemas    map[string]registeredNodeSchema
}

// NewRegistry returns an empty Registry. The zero Registry is also ready to use.
func NewRegistry() *Registry {
	return &Registry{}
}

// RegisterLeaf registers a leaf factory under a node type name. It reports an
// empty name, nil factory, or duplicate registration immediately.
func (r *Registry) RegisterLeaf(nodeType string, factory LeafFactory) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	switch {
	case nodeType == "":
		return &RegistrationError{
			Kind: "leaf",
			Err:  fmt.Errorf("%w: node type is empty", ErrInvalidRegistration),
		}
	case factory == nil:
		return &RegistrationError{
			Kind: "leaf",
			Name: nodeType,
			Err:  fmt.Errorf("%w: factory is nil", ErrInvalidRegistration),
		}
	case r.leaves[nodeType] != nil:
		return &RegistrationError{Kind: "leaf", Name: nodeType, Err: ErrDuplicateRegistration}
	default:
		r.leaves[nodeType] = factory
	}
	return nil
}

// MustRegisterLeaf is like [Registry.RegisterLeaf] but panics on error. It
// returns r so startup-time registrations can be chained.
func (r *Registry) MustRegisterLeaf(nodeType string, factory LeafFactory) *Registry {
	if err := r.RegisterLeaf(nodeType, factory); err != nil {
		panic(err)
	}
	return r
}

// RegisterResolver registers a branch resolver under a name.
func (r *Registry) RegisterResolver(name string, resolver Resolver) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	switch {
	case name == "":
		return &RegistrationError{
			Kind: "resolver",
			Err:  fmt.Errorf("%w: name is empty", ErrInvalidRegistration),
		}
	case resolver == nil:
		return &RegistrationError{
			Kind: "resolver",
			Name: name,
			Err:  fmt.Errorf("%w: resolver is nil", ErrInvalidRegistration),
		}
	case r.resolvers[name] != nil:
		return &RegistrationError{Kind: "resolver", Name: name, Err: ErrDuplicateRegistration}
	default:
		r.resolvers[name] = resolver
	}
	return nil
}

// MustRegisterResolver is like [Registry.RegisterResolver] but panics on error.
func (r *Registry) MustRegisterResolver(name string, resolver Resolver) *Registry {
	if err := r.RegisterResolver(name, resolver); err != nil {
		panic(err)
	}
	return r
}

// RegisterCondition registers a loop condition under a name.
func (r *Registry) RegisterCondition(name string, condition Condition) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	switch {
	case name == "":
		return &RegistrationError{
			Kind: "condition",
			Err:  fmt.Errorf("%w: name is empty", ErrInvalidRegistration),
		}
	case condition == nil:
		return &RegistrationError{
			Kind: "condition",
			Name: name,
			Err:  fmt.Errorf("%w: condition is nil", ErrInvalidRegistration),
		}
	case r.conditions[name] != nil:
		return &RegistrationError{Kind: "condition", Name: name, Err: ErrDuplicateRegistration}
	default:
		r.conditions[name] = condition
	}
	return nil
}

// MustRegisterCondition is like [Registry.RegisterCondition] but panics on
// error.
func (r *Registry) MustRegisterCondition(name string, condition Condition) *Registry {
	if err := r.RegisterCondition(name, condition); err != nil {
		panic(err)
	}
	return r
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
		r.schemas = make(map[string]registeredNodeSchema)
	}
}

func (r *Registry) lookupLeaf(nodeType string) (LeafFactory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.leaves[nodeType]
	return factory, ok
}

func (r *Registry) lookupResolver(name string) (Resolver, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	resolver, ok := r.resolvers[name]
	return resolver, ok
}

func (r *Registry) lookupCondition(name string) (Condition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	condition, ok := r.conditions[name]
	return condition, ok
}

func (r *Registry) lookupNodeSchema(nodeType string) (registeredNodeSchema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	schema, ok := r.schemas[nodeType]
	return schema, ok
}
