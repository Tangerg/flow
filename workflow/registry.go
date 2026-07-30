package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// NodeSpec carries everything a [NodeFactory] needs to build one node: the ID it
// must report in events and Store writes, its wired input ports, and its raw JSON
// config. It is the part of a [GraphNode] or [Spec] that survives compilation;
// wiring and ordering are the enclosing definition's concern, not the factory's.
type NodeSpec struct {
	ID     string
	Inputs Inputs
	Config json.RawMessage
}

// NodeFactory builds the [Step] behind one registered node type. The factory
// knows the node's concrete input and output types and usually ends in a call to
// [Leaf]; [Subgraph] and the other composites are equally valid results, which is
// why this is not called a leaf factory.
//
// [Factory] covers the common single-input case; write a NodeFactory directly
// when a node reads several ports.
type NodeFactory func(NodeSpec) (Step, error)

// Resolver picks a branch or outlet name from the Store (see [Branch] and
// [Route]).
type Resolver func(ctx context.Context, s Store) (string, error)

// Condition decides whether a [Loop] should stop after an iteration. It may
// return an error when the condition cannot be evaluated from the current
// Store.
type Condition func(ctx context.Context, iter int, s Store) (bool, error)

// Registry holds the named building blocks that a [Spec] refers to: node
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
	nodes      map[string]NodeFactory
	resolvers  map[string]Resolver
	conditions map[string]Condition
	schemas    map[string]registeredNodeSchema
}

// NewRegistry returns an empty Registry. The zero Registry is also ready to use.
func NewRegistry() *Registry {
	return &Registry{}
}

// RegisterNode registers the factory for a node type. It reports an
// empty name, nil factory, or duplicate registration immediately.
func (r *Registry) RegisterNode(nodeType string, factory NodeFactory) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initLocked()
	switch {
	case nodeType == "":
		return &RegistrationError{
			Kind: "node",
			Err:  fmt.Errorf("%w: node type is empty", ErrInvalidRegistration),
		}
	case factory == nil:
		return &RegistrationError{
			Kind: "node",
			Name: nodeType,
			Err:  fmt.Errorf("%w: factory is nil", ErrInvalidRegistration),
		}
	case r.nodes[nodeType] != nil:
		return &RegistrationError{Kind: "node", Name: nodeType, Err: ErrDuplicateRegistration}
	default:
		r.nodes[nodeType] = factory
	}
	return nil
}

// MustRegisterNode is like [Registry.RegisterNode] but panics on error. It
// returns r so startup-time registrations can be chained.
func (r *Registry) MustRegisterNode(nodeType string, factory NodeFactory) *Registry {
	if err := r.RegisterNode(nodeType, factory); err != nil {
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
	if r.nodes == nil {
		r.nodes = make(map[string]NodeFactory)
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

func (r *Registry) lookupNode(nodeType string) (NodeFactory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.nodes[nodeType]
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
