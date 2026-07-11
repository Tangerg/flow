package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// LeafFactory builds a leaf [Step] from its ID, input reference, and raw config.
// The factory knows the leaf's concrete input/output types and typically ends in
// a call to [Leaf].
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
func (r *Registry) RegisterLeaf(nodeType string, f LeafFactory) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	switch {
	case nodeType == "":
		return &RegistrationError{Kind: "leaf", Err: fmt.Errorf("%w: empty type", ErrInvalidRegistration)}
	case f == nil:
		return &RegistrationError{Kind: "leaf", Name: nodeType, Err: fmt.Errorf("%w: nil factory", ErrInvalidRegistration)}
	case r.leaves[nodeType] != nil:
		return &RegistrationError{Kind: "leaf", Name: nodeType, Err: ErrDuplicateRegistration}
	default:
		r.leaves[nodeType] = f
	}
	return nil
}

// MustRegisterLeaf is like [Registry.RegisterLeaf] but panics on error. It
// returns r so startup-time registrations can be chained.
func (r *Registry) MustRegisterLeaf(nodeType string, f LeafFactory) *Registry {
	if err := r.RegisterLeaf(nodeType, f); err != nil {
		panic(err)
	}
	return r
}

// RegisterResolver registers a branch resolver under a name.
func (r *Registry) RegisterResolver(name string, f Resolver) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	switch {
	case name == "":
		return &RegistrationError{Kind: "resolver", Err: fmt.Errorf("%w: empty name", ErrInvalidRegistration)}
	case f == nil:
		return &RegistrationError{Kind: "resolver", Name: name, Err: fmt.Errorf("%w: nil resolver", ErrInvalidRegistration)}
	case r.resolvers[name] != nil:
		return &RegistrationError{Kind: "resolver", Name: name, Err: ErrDuplicateRegistration}
	default:
		r.resolvers[name] = f
	}
	return nil
}

// MustRegisterResolver is like [Registry.RegisterResolver] but panics on error.
func (r *Registry) MustRegisterResolver(name string, f Resolver) *Registry {
	if err := r.RegisterResolver(name, f); err != nil {
		panic(err)
	}
	return r
}

// RegisterCondition registers a loop condition under a name.
func (r *Registry) RegisterCondition(name string, f Condition) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	switch {
	case name == "":
		return &RegistrationError{Kind: "condition", Err: fmt.Errorf("%w: empty name", ErrInvalidRegistration)}
	case f == nil:
		return &RegistrationError{Kind: "condition", Name: name, Err: fmt.Errorf("%w: nil condition", ErrInvalidRegistration)}
	case r.conditions[name] != nil:
		return &RegistrationError{Kind: "condition", Name: name, Err: ErrDuplicateRegistration}
	default:
		r.conditions[name] = f
	}
	return nil
}

// MustRegisterCondition is like [Registry.RegisterCondition] but panics on
// error.
func (r *Registry) MustRegisterCondition(name string, f Condition) *Registry {
	if err := r.RegisterCondition(name, f); err != nil {
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

func (r *Registry) nodeSchema(nodeType string) NodeSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.schemas[nodeType].schema
}

func (r *Registry) registeredNodeSchema(nodeType string) registeredNodeSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.schemas[nodeType]
}
