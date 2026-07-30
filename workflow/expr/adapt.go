package expr

import (
	"context"
	"fmt"

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
		return nil, fmt.Errorf(
			"%w: switch requires at least one case",
			workflow.ErrInvalidSpec,
		)
	}

	type compiledCase struct {
		when *Expr
		then string
	}
	cases := make([]compiledCase, 0, len(spec.Cases))
	for index, specCase := range spec.Cases {
		if specCase.Then == "" {
			return nil, fmt.Errorf(
				"%w: switch case %d has an empty branch name",
				workflow.ErrInvalidSpec,
				index,
			)
		}
		when, err := Parse(specCase.When)
		if err != nil {
			return nil, err
		}
		cases = append(cases, compiledCase{when: when, then: specCase.Then})
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
func (s SwitchSpec) Refs() ([]workflow.Ref, error) {
	var refs []workflow.Ref
	for _, c := range s.Cases {
		e, err := Parse(c.When)
		if err != nil {
			return nil, err
		}
		refs = append(refs, e.Refs()...)
	}
	return refList(refs).sortedUnique(), nil
}
