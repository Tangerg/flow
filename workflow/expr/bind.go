package expr

import (
	"fmt"
	"maps"
	"slices"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// Bindings is a set of named expressions — the shape a config file carries so
// that branch and loop logic can change without rebuilding the program. Register
// it on a [workflow.Registry] and the names become usable from a [workflow.Spec].
// JSON decoding requires an object, rejects unknown or duplicate members,
// invalid Unicode, and excessive nesting, and replaces the destination only
// after complete success.
//
//	var b expr.Bindings
//	if err := json.Unmarshal(data, &b); err != nil { ... }
//	if err := b.Register(reg); err != nil { ... }
type Bindings struct { //nolint:recvcheck // UnmarshalJSON must use a pointer receiver.
	// Conditions are loop stop conditions, each a boolean expression.
	Conditions map[string]string `json:"conditions,omitempty"`
	// Resolvers are branch resolvers, each a string-valued expression.
	Resolvers map[string]string `json:"resolvers,omitempty"`
	// Switches are branch resolvers built from ordered boolean cases.
	Switches map[string]SwitchSpec `json:"switches,omitempty"`
}

// A binding kind names itself in every diagnostic about it: compiling its
// expression, checking that its text crosses JSON unchanged, and collecting the
// references it reads. Naming each once keeps those from disagreeing.
const (
	kindCondition = "condition"
	kindResolver  = "resolver"
	kindSwitch    = "switch"
)

// Register compiles every expression and registers it under its name. It
// compiles all of them before registering any, so a Bindings with a bad
// expression leaves the Registry untouched.
//
// Names are registered in sorted order. Registry validation still occurs at
// that mutation boundary, so registration is not a transaction: if a name is
// invalid, already present, or otherwise rejected, names before it in that
// order remain registered.
func (b Bindings) Register(registry *workflow.Registry) error {
	if registry == nil {
		return fmt.Errorf("%w: registry is nil", workflow.ErrInvalidRegistration)
	}
	registrar := bindingRegistrar{
		bindings:   b,
		registry:   registry,
		conditions: make(map[string]workflow.Condition, len(b.Conditions)),
		resolvers:  make(map[string]workflow.Resolver, len(b.Resolvers)+len(b.Switches)),
	}
	return registrar.register()
}

// bindingRegistrar owns the compile-before-mutate phase for one set of
// bindings. It keeps partially compiled functions private until all expressions
// have succeeded; individual Registry registrations retain their documented
// non-transactional behavior.
type bindingRegistrar struct {
	bindings   Bindings
	registry   *workflow.Registry
	conditions map[string]workflow.Condition
	resolvers  map[string]workflow.Resolver
}

func (b *bindingRegistrar) register() error {
	if err := b.compileConditions(); err != nil {
		return err
	}
	if err := b.compileResolvers(); err != nil {
		return err
	}
	if err := b.compileSwitches(); err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(b.conditions)) {
		if err := b.registry.RegisterCondition(name, b.conditions[name]); err != nil {
			return err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(b.resolvers)) {
		if err := b.registry.RegisterResolver(name, b.resolvers[name]); err != nil {
			return err
		}
	}
	return nil
}

func (b *bindingRegistrar) compileConditions() error {
	return compileNamed(b.bindings.Conditions, Condition, kindCondition, b.conditions)
}

func (b *bindingRegistrar) compileResolvers() error {
	return compileNamed(b.bindings.Resolvers, Resolver, kindResolver, b.resolvers)
}

// compileNamed compiles one table of named expressions in name order, so a
// Bindings with several bad expressions always reports the same one. Both
// tables differ only in what they compile to and what a diagnostic calls them.
func compileNamed[T any](
	source map[string]string,
	compile func(string) (T, error),
	kind string,
	into map[string]T,
) error {
	for _, name := range slices.Sorted(maps.Keys(source)) {
		compiled, err := compile(source[name])
		if err != nil {
			return fmt.Errorf("%s %q: %w", kind, name, err)
		}
		into[name] = compiled
	}
	return nil
}

func (b *bindingRegistrar) compileSwitches() error {
	for _, name := range slices.Sorted(maps.Keys(b.bindings.Switches)) {
		if _, duplicate := b.resolvers[name]; duplicate {
			return fmt.Errorf(
				"%w: name %q is used by both a resolver and a switch",
				flow.ErrInvalidConfig,
				name,
			)
		}
		resolver, err := Switch(b.bindings.Switches[name])
		if err != nil {
			return fmt.Errorf("%s %q: %w", kindSwitch, name, err)
		}
		b.resolvers[name] = resolver
	}
	return nil
}

// Refs returns every reference the bindings read, deduplicated and sorted. The
// returned slice is a copy. An editor uses it to check that a rule set only
// reads values the graph produces.
func (b Bindings) Refs() ([]workflow.Ref, error) {
	var refs []workflow.Ref
	collect := func(kind, name, src string) error {
		e, err := Parse(src)
		if err != nil {
			return fmt.Errorf("%s %q: %w", kind, name, err)
		}
		refs = append(refs, e.Refs()...)
		return nil
	}

	for _, name := range slices.Sorted(maps.Keys(b.Conditions)) {
		if err := collect(kindCondition, name, b.Conditions[name]); err != nil {
			return nil, err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(b.Resolvers)) {
		if err := collect(kindResolver, name, b.Resolvers[name]); err != nil {
			return nil, err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(b.Switches)) {
		caseRefs, err := b.Switches[name].Refs()
		if err != nil {
			return nil, fmt.Errorf("%s %q: %w", kindSwitch, name, err)
		}
		refs = append(refs, caseRefs...)
	}

	return refList(refs).sortedUnique(), nil
}
