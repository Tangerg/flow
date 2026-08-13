package workflow

import (
	"encoding/json"
	"maps"
	"slices"
	"sync"

	"github.com/Tangerg/flow"
)

const (
	registrationCondition = "condition"
	registrationNode      = "node"
	registrationResolver  = "resolver"
	registrationSchema    = "schema"
)

// registrationTable owns the common zero-value, uniqueness, snapshot, and
// lookup semantics of one Registry namespace. Registry supplies synchronization
// around mutation; a registrySnapshot receives cloned tables and is immutable.
type registrationTable[T any] map[string]T

func (r *registrationTable[T]) add(kind, name string, value T) error {
	if *r == nil {
		*r = make(registrationTable[T])
	}
	if _, exists := (*r)[name]; exists {
		return &RegistrationError{
			Kind: kind,
			Name: name,
			Err:  ErrDuplicateRegistration,
		}
	}
	(*r)[name] = value
	return nil
}

func (r registrationTable[T]) clone() registrationTable[T] {
	return maps.Clone(r)
}

func (r registrationTable[T]) lookup(name string) (T, bool) {
	value, ok := r[name]
	return value, ok
}

func (r registrationTable[T]) names() []string {
	return slices.Sorted(maps.Keys(r))
}

// NodeSpec carries everything a [NodeFactory] needs to build one node: its
// execution ID, wired input ports, and raw JSON config. The returned boundary
// must own ID as its execution identity and conventional output name when it
// produces one; built-in boundaries also derive descriptions, scopes, events,
// and Journal keys from that identity where those concepts apply. Wiring and
// ordering remain the enclosing definition's concern. Inputs and Config are
// per-call copies owned by the factory; retaining or modifying them cannot
// mutate the Graph or Spec being compiled.
type NodeSpec struct {
	ID     string
	Inputs Inputs
	// Config is absent only when it has zero length. Non-empty bytes must contain
	// one complete JSON value; whitespace alone is not an omitted value.
	Config json.RawMessage
}

// NodeFactory builds the [Step] behind one registered node type. The result must
// be a Store-sealed boundary with spec.ID: a [Leaf], [Await], [Interrupt],
// [Iteration], or [Subgraph]. Compilation rejects opaque Steps, mismatched IDs,
// and unsealed composites. Wrap a sequence, parallel, branch, loop, or nested
// graph with Subgraph before returning it; this keeps child cells and execution
// identities from leaking through the GraphNode that contains it.
//
// [Factory] covers the common single-input case; write a NodeFactory directly
// when a node reads several ports.
//
// A Registry may call a factory concurrently when callers compile definitions
// concurrently. A factory must therefore keep per-build state local. Registry
// compilation validates the complete visible built-in boundary before any
// execution begins and verifies its output presence against a registered
// NodeSchema. A factory is definition construction rather than execution: it
// should return promptly, must not return a suspension, and leaves blocking or
// cancellable I/O to the Node it builds. Compilation reports a suspension from
// a factory as [flow.ErrInvalidConfig], not as a resumable run-time outcome.
type NodeFactory func(NodeSpec) (Step, error)

// Resolver is the typed node shape that picks a branch or outlet name from a
// Store (see [Branch] and [Route]). It is an alias rather than a second
// execution protocol, so an ordinary or composed flow.Node[Store, string] can
// be registered and reused directly. Adapt a function with [flow.NodeFunc].
type Resolver = flow.Node[Store, string]

// Condition is the typed node shape that decides whether a [Loop] should stop
// after an iteration. Like [Resolver] it is an alias rather than a second
// execution protocol, so the two decision shapes differ only in what they
// return. Adapt a function with [flow.NodeFunc].
//
// A condition runs inside the iteration's indexed scope, so [Scope] reports the
// zero-based iteration index in its last frame when a decision depends on it.
// It may return an error when the condition cannot be evaluated from the current
// Store. It must be safe for concurrent use by concurrent runs of the same
// compiled definition.
type Condition = flow.Node[Store, bool]

// Registry holds the named building blocks that a [Spec] refers to: node
// types, branch resolvers, and loop conditions.
//
// A serialized graph cannot carry closures, so it names its behavior and the
// Registry supplies the code. This is the same constraint every durable/dynamic
// engine has: nodes are registered types, not inline functions.
//
// Registry is safe for concurrent access. Each validation or compilation sees
// one registration snapshot even if another goroutine is registering entries;
// later operations see registrations that have completed. Applications should
// normally finish registration before compiling workflows. A Registry must not
// be copied after first use.
type Registry struct {
	mu         sync.RWMutex
	nodes      registrationTable[NodeFactory]
	resolvers  registrationTable[Resolver]
	conditions registrationTable[Condition]
	schemas    registrationTable[registeredNodeSchema]
}

// registrySnapshot is the immutable registration view consumed by one
// validation or compilation. Keeping it distinct from Registry makes the
// ownership boundary structural: compilation cannot accidentally mutate its
// source or carry synchronization intended only for registration.
type registrySnapshot struct {
	nodes      registrationTable[NodeFactory]
	resolvers  registrationTable[Resolver]
	conditions registrationTable[Condition]
	schemas    registrationTable[registeredNodeSchema]
}

// NewRegistry returns an empty Registry. The zero Registry is also ready to use.
func NewRegistry() *Registry {
	return &Registry{}
}

// RegisterNode registers the factory for a node type. It reports an
// empty or non-UTF-8 name, nil factory, or duplicate registration immediately.
func (r *Registry) RegisterNode(nodeType string, factory NodeFactory) error {
	if err := validateRegistrationName(registrationNode, nodeType); err != nil {
		return err
	}
	if factory == nil {
		return &RegistrationError{
			Kind: registrationNode,
			Name: nodeType,
			Err:  flow.ErrNilFunc,
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.nodes.add(registrationNode, nodeType, factory)
}

// MustRegisterNode is like [Registry.RegisterNode] but panics on error. It
// returns r so startup-time registrations can be chained.
func (r *Registry) MustRegisterNode(nodeType string, factory NodeFactory) *Registry {
	if err := r.RegisterNode(nodeType, factory); err != nil {
		panic(err)
	}
	return r
}

// RegisterResolver registers a branch resolver under a name. The Resolver may
// be an ordinary or composed flow.Node. Its complete visible definition is
// validated at registration, so Registry validation cannot accept a name whose
// implementation is already invalid.
func (r *Registry) RegisterResolver(name string, resolver Resolver) error {
	return registerDecision(r, &r.resolvers, registrationResolver, name, resolver)
}

// MustRegisterResolver is like [Registry.RegisterResolver] but panics on error.
func (r *Registry) MustRegisterResolver(name string, resolver Resolver) *Registry {
	if err := r.RegisterResolver(name, resolver); err != nil {
		panic(err)
	}
	return r
}

// RegisterCondition registers a loop condition under a name. The Condition may
// be an ordinary or composed flow.Node. Its complete visible definition is
// validated at registration, so Registry validation cannot accept a name whose
// implementation is already invalid.
func (r *Registry) RegisterCondition(name string, condition Condition) error {
	return registerDecision(r, &r.conditions, registrationCondition, name, condition)
}

// MustRegisterCondition is like [Registry.RegisterCondition] but panics on
// error.
func (r *Registry) MustRegisterCondition(name string, condition Condition) *Registry {
	if err := r.RegisterCondition(name, condition); err != nil {
		panic(err)
	}
	return r
}

// registerDecision registers one of the two decision shapes. [Resolver] and
// [Condition] differ only in what they return, so they are admitted the same
// way: check the name, then the node's complete visible definition, then claim
// the name under the Registry lock.
func registerDecision[O any](
	r *Registry,
	table *registrationTable[flow.Node[Store, O]],
	kind, name string,
	node flow.Node[Store, O],
) error {
	if err := validateRegistrationName(kind, name); err != nil {
		return err
	}
	if err := validateNode(node); err != nil {
		return &RegistrationError{
			Kind: kind,
			Name: name,
			Err:  err,
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return table.add(kind, name, node)
}

func validateRegistrationName(kind, name string) error {
	label := "name"
	if kind == registrationNode || kind == registrationSchema {
		label = nameNodeType
	}
	if err := validateName(label, name); err != nil {
		return &RegistrationError{
			Kind: kind,
			Name: name,
			Err:  err,
		}
	}
	return nil
}

// snapshot returns one immutable logical view for a multi-stage validation or
// compilation. Holding Registry.mu while calling a user factory would risk a
// deadlock if that code registered another capability; taking separate locks
// would let one compilation observe a registration only halfway through.
func (r *Registry) snapshot() registrySnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return registrySnapshot{
		nodes:      r.nodes.clone(),
		resolvers:  r.resolvers.clone(),
		conditions: r.conditions.clone(),
		schemas:    r.schemas.clone(),
	}
}

func (r registrySnapshot) lookupNode(nodeType string) (NodeFactory, bool) {
	return r.nodes.lookup(nodeType)
}

func (r registrySnapshot) lookupResolver(name string) (Resolver, bool) {
	return r.resolvers.lookup(name)
}

func (r registrySnapshot) lookupCondition(name string) (Condition, bool) {
	return r.conditions.lookup(name)
}

func (r registrySnapshot) lookupNodeSchema(nodeType string) (registeredNodeSchema, bool) {
	return r.schemas.lookup(nodeType)
}
