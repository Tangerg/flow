package expr

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/Tangerg/flow/workflow"
)

// Condition compiles src into a [workflow.Loop] stop condition: the loop ends on
// the first iteration whose resulting Store makes src true.
//
// The expression sees only the Store, not the iteration index; cap the number of
// iterations with workflow.LoopConfig.MaxIterations instead. An expression that
// cannot be evaluated returns an error rather than false, so a broken condition
// is never mistaken for "keep looping".
func Condition(src string) (workflow.Condition, error) {
	e, err := Parse(src)
	if err != nil {
		return nil, err
	}
	return e.LoopCondition(), nil
}

// LoopCondition adapts the Expr into a [workflow.Condition].
func (e *Expr) LoopCondition() workflow.Condition {
	return func(_ context.Context, _ int, s workflow.Store) (bool, error) {
		return e.Bool(s)
	}
}

// Resolver compiles src into a [workflow.Branch] resolver that returns the
// expression's string value as the case name. Use it to route on a value a
// previous step produced, such as "classify.output.intent"; use [Switch] to route
// on boolean tests.
func Resolver(src string) (workflow.Resolver, error) {
	e, err := Parse(src)
	if err != nil {
		return nil, err
	}
	return e.BranchResolver(), nil
}

// BranchResolver adapts the Expr into a [workflow.Resolver].
func (e *Expr) BranchResolver() workflow.Resolver {
	return func(_ context.Context, s workflow.Store) (string, error) {
		return e.String(s)
	}
}

// Case pairs a boolean expression with the branch name to select when it holds.
type Case struct {
	When string `json:"when"`
	Then string `json:"then"`
}

// SwitchSpec is the serializable form of a [Switch]: ordered cases plus the
// branch to take when none match.
type SwitchSpec struct {
	Cases []Case `json:"cases"`
	// Fallback is the case name used when no When holds. An empty Fallback makes
	// "no match" an error instead.
	Fallback string `json:"fallback,omitempty"`
}

// Switch compiles an ordered list of boolean cases into a [workflow.Branch]
// resolver. Cases are evaluated in order and the first one that holds selects its
// Then; if none hold, fallback is selected, or the resolver fails when fallback
// is empty.
//
// This is how a rule set carried in config — "route to escalate when the score is
// low" — becomes a branch without a Go redeploy.
func Switch(spec SwitchSpec) (workflow.Resolver, error) {
	if len(spec.Cases) == 0 {
		return nil, fmt.Errorf("%w: switch requires at least one case", ErrUnsupported)
	}

	type compiled struct {
		when *Expr
		then string
	}
	cases := make([]compiled, 0, len(spec.Cases))
	for i, c := range spec.Cases {
		if c.Then == "" {
			return nil, fmt.Errorf("%w: case %d has no branch name", ErrUnsupported, i)
		}
		when, err := Parse(c.When)
		if err != nil {
			return nil, err
		}
		cases = append(cases, compiled{when: when, then: c.Then})
	}

	fallback := spec.Fallback
	return func(_ context.Context, s workflow.Store) (string, error) {
		for _, c := range cases {
			hold, err := c.when.Bool(s)
			if err != nil {
				return "", err
			}
			if hold {
				return c.then, nil
			}
		}
		if fallback == "" {
			return "", fmt.Errorf("%w: no case matched and no fallback is set", ErrUndefined)
		}
		return fallback, nil
	}, nil
}

// Refs returns every reference a SwitchSpec's cases read, deduplicated and
// sorted. It reports a parse error in any case.
func (spec SwitchSpec) Refs() ([]workflow.Ref, error) {
	var refs []workflow.Ref
	for _, c := range spec.Cases {
		e, err := Parse(c.When)
		if err != nil {
			return nil, err
		}
		refs = append(refs, e.Refs()...)
	}
	return refSet(refs).normalized(), nil
}

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
func (b Bindings) Register(reg *workflow.Registry) error {
	if reg == nil {
		return fmt.Errorf("%w: nil registry", ErrUnsupported)
	}

	conditions := make(map[string]workflow.Condition, len(b.Conditions))
	for _, name := range slices.Sorted(maps.Keys(b.Conditions)) {
		condition, err := Condition(b.Conditions[name])
		if err != nil {
			return fmt.Errorf("condition %q: %w", name, err)
		}
		conditions[name] = condition
	}

	resolvers := make(map[string]workflow.Resolver, len(b.Resolvers)+len(b.Switches))
	for _, name := range slices.Sorted(maps.Keys(b.Resolvers)) {
		resolver, err := Resolver(b.Resolvers[name])
		if err != nil {
			return fmt.Errorf("resolver %q: %w", name, err)
		}
		resolvers[name] = resolver
	}
	for _, name := range slices.Sorted(maps.Keys(b.Switches)) {
		if _, duplicate := resolvers[name]; duplicate {
			return fmt.Errorf("%w: %q is both a resolver and a switch", ErrUnsupported, name)
		}
		resolver, err := Switch(b.Switches[name])
		if err != nil {
			return fmt.Errorf("switch %q: %w", name, err)
		}
		resolvers[name] = resolver
	}

	for _, name := range slices.Sorted(maps.Keys(conditions)) {
		if err := reg.RegisterCondition(name, conditions[name]); err != nil {
			return err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(resolvers)) {
		if err := reg.RegisterResolver(name, resolvers[name]); err != nil {
			return err
		}
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

	return refSet(refs).normalized(), nil
}
