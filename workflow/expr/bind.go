package expr

import (
	"fmt"
	"maps"
	"slices"

	"github.com/Tangerg/flow/workflow"
)

// Bindings is a set of named expressions — the shape a config file carries so
// that branch and loop logic can change without rebuilding the program. Register
// it on a [workflow.Registry] and the names become usable from a [workflow.Spec].
//
//	var b expr.Bindings
//	if err := json.Unmarshal(data, &b); err != nil { ... }
//	if err := b.Register(reg); err != nil { ... }
type Bindings struct {
	// Conditions are loop stop conditions, each a boolean expression.
	Conditions map[string]string `json:"conditions,omitempty"`
	// Resolvers are branch resolvers, each a string-valued expression.
	Resolvers map[string]string `json:"resolvers,omitempty"`
	// Switches are branch resolvers built from ordered boolean cases.
	Switches map[string]SwitchSpec `json:"switches,omitempty"`
}

// Register compiles every expression and registers it under its name. It
// compiles all of them before registering any, so a Bindings with a bad
// expression leaves the Registry untouched.
//
// Names are registered in sorted order, and a name already present in reg is
// reported as a duplicate registration.
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

// bindingRegistrar owns the compile-before-mutate transaction for one set of
// bindings. It keeps partially compiled functions private until all expressions
// have succeeded.
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
	for _, name := range slices.Sorted(maps.Keys(b.bindings.Conditions)) {
		condition, err := Condition(b.bindings.Conditions[name])
		if err != nil {
			return fmt.Errorf("condition %q: %w", name, err)
		}
		b.conditions[name] = condition
	}
	return nil
}

func (b *bindingRegistrar) compileResolvers() error {
	for _, name := range slices.Sorted(maps.Keys(b.bindings.Resolvers)) {
		resolver, err := Resolver(b.bindings.Resolvers[name])
		if err != nil {
			return fmt.Errorf("resolver %q: %w", name, err)
		}
		b.resolvers[name] = resolver
	}
	return nil
}

func (b *bindingRegistrar) compileSwitches() error {
	for _, name := range slices.Sorted(maps.Keys(b.bindings.Switches)) {
		if _, duplicate := b.resolvers[name]; duplicate {
			return fmt.Errorf(
				"%w: name %q is used by both a resolver and a switch",
				workflow.ErrInvalidSpec,
				name,
			)
		}
		resolver, err := Switch(b.bindings.Switches[name])
		if err != nil {
			return fmt.Errorf("switch %q: %w", name, err)
		}
		b.resolvers[name] = resolver
	}
	return nil
}

// Refs returns every reference the bindings read, deduplicated and sorted. An
// editor uses it to check that a rule set only reads values the graph produces.
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
		if err := collect("condition", name, b.Conditions[name]); err != nil {
			return nil, err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(b.Resolvers)) {
		if err := collect("resolver", name, b.Resolvers[name]); err != nil {
			return nil, err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(b.Switches)) {
		caseRefs, err := b.Switches[name].Refs()
		if err != nil {
			return nil, fmt.Errorf("switch %q: %w", name, err)
		}
		refs = append(refs, caseRefs...)
	}

	return refList(refs).sortedUnique(), nil
}
