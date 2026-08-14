package expr

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// evaluate runs eval between two cancellation checks, so parent cancellation
// observed before or during evaluation takes precedence over whatever the
// expression itself produced. Every adapter in this file shares that rule, so
// it is stated here rather than reassembled per adapter.
//
// It is the same precedence flow applies around a node it runs sequentially.
// This package restates it because a compiled adapter is also a [workflow.Step]
// a caller can run directly, and the helper that applies it in flow is that
// package's own.
func evaluate[T any](
	ctx context.Context,
	store workflow.Store,
	eval func(workflow.Store) (T, error),
) (T, error) {
	var zero T
	if err := context.Cause(ctx); err != nil {
		return zero, err
	}
	value, err := eval(store)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return zero, contextErr
	}
	return value, err
}

// Condition compiles src into a [workflow.Loop] stop condition: the loop ends on
// the first iteration whose resulting Store makes src true.
//
// An expression reads the Store, which is the whole input a [workflow.Condition]
// receives; cap the number of iterations with workflow.LoopConfig.MaxIterations
// rather than writing a rule over the iteration index. An expression that cannot
// be evaluated returns an error rather than false, so a broken condition is
// never mistaken for "keep looping".
func Condition(src string) (workflow.Condition, error) {
	e, err := Parse(src)
	if err != nil {
		return nil, err
	}
	return e.Condition(), nil
}

// Condition adapts the Expr into a [workflow.Condition], the node shape a
// [workflow.Loop] checks after each iteration. Parent cancellation observed
// before or during evaluation takes precedence.
func (e *Expr) Condition() workflow.Condition {
	return flow.NodeFunc[workflow.Store, bool](
		func(ctx context.Context, s workflow.Store) (bool, error) {
			return evaluate(ctx, s, e.Bool)
		},
	)
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
	return e.Resolver(), nil
}

// Resolver adapts the Expr into a [workflow.Resolver], the node shape that
// selects a name for [workflow.Branch] and publishes an outlet for
// [workflow.Route]. Parent cancellation observed before or during evaluation
// takes precedence.
func (e *Expr) Resolver() workflow.Resolver {
	return flow.NodeFunc[workflow.Store, string](func(ctx context.Context, s workflow.Store) (string, error) {
		return evaluate(ctx, s, e.String)
	})
}

// Case pairs a boolean expression with the branch name to select when it holds.
type Case struct {
	When string `json:"when"`
	Then string `json:"then"`
}

// SwitchSpec is the serializable form of a [Switch]: ordered cases plus the
// branch to take when none match. Its JSON boundary requires an object and is
// strict, lossless, and failure-atomic, matching [Bindings].
type SwitchSpec struct { //nolint:recvcheck // UnmarshalJSON must use a pointer receiver.
	Cases []Case `json:"cases"`
	// Fallback is the case name used when no When holds. An empty Fallback makes
	// "no match" an error instead.
	Fallback string `json:"fallback,omitempty"`
}

// validateText checks the text a SwitchSpec carries for the one property both
// its JSON boundary and its compiler require: text that survives encoding
// unchanged, since encoding/json replaces invalid UTF-8 by design. Stating the
// rule once keeps [SwitchSpec.MarshalJSON] and [Switch] from disagreeing about
// which specs are representable.
func (s SwitchSpec) validateText() error {
	for index, entry := range s.Cases {
		if !utf8.ValidString(entry.When) {
			return fmt.Errorf("case %d expression is not valid UTF-8", index)
		}
		if !utf8.ValidString(entry.Then) {
			return fmt.Errorf("case %d branch name is not valid UTF-8", index)
		}
	}
	if !utf8.ValidString(s.Fallback) {
		return errors.New("fallback branch name is not valid UTF-8")
	}
	return nil
}

// Switch compiles an ordered list of boolean cases into a [workflow.Branch]
// resolver. Cases are evaluated in order and the first one that holds selects its
// Then; if none hold, fallback is selected, or the resolver fails when fallback
// is empty. Parent cancellation takes precedence and stops later cases from
// being evaluated.
//
// This is how a rule set carried in config — "route to escalate when the score is
// low" — becomes a branch without a Go redeploy.
//
// The resolver holds only compiled, immutable state: it is safe for concurrent
// reuse and retains none of the spec's slices.
func Switch(spec SwitchSpec) (workflow.Resolver, error) {
	if len(spec.Cases) == 0 {
		return nil, fmt.Errorf(
			"%w: switch requires at least one case",
			flow.ErrInvalidConfig,
		)
	}
	if err := spec.validateText(); err != nil {
		return nil, fmt.Errorf("%w: %w", flow.ErrInvalidConfig, err)
	}

	compiled := switchResolver{
		cases:    make([]compiledCase, 0, len(spec.Cases)),
		fallback: spec.Fallback,
	}
	for index, specCase := range spec.Cases {
		entry, err := compileCase(index, specCase)
		if err != nil {
			return nil, err
		}
		compiled.cases = append(compiled.cases, entry)
	}
	return compiled, nil
}

type switchResolver struct {
	cases    []compiledCase
	fallback string
}

type compiledCase struct {
	when *Expr
	then string
}

// compileCase reports a case by its index alone. Whoever holds the switch names
// it — [Bindings] by its member name, a direct Switch caller by having called
// it — so repeating "switch" here would say it twice.
func compileCase(index int, spec Case) (compiledCase, error) {
	if spec.Then == "" {
		return compiledCase{}, fmt.Errorf(
			"%w: case %d has an empty branch name",
			flow.ErrInvalidConfig,
			index,
		)
	}
	when, err := Parse(spec.When)
	if err != nil {
		return compiledCase{}, fmt.Errorf("case %d: %w", index, err)
	}
	return compiledCase{when: when, then: spec.Then}, nil
}

func (s switchResolver) Run(ctx context.Context, store workflow.Store) (string, error) {
	for _, entry := range s.cases {
		hold, err := evaluate(ctx, store, entry.when.Bool)
		if err != nil {
			return "", err
		}
		if hold {
			return entry.then, nil
		}
	}
	if s.fallback == "" {
		return "", fmt.Errorf("%w: no expression matched and no fallback is set", flow.ErrNoCase)
	}
	return s.fallback, nil
}

// Refs returns every reference a SwitchSpec's cases read, deduplicated and
// sorted. The returned slice is a copy. It reports a parse error in any case.
func (s SwitchSpec) Refs() ([]workflow.Ref, error) {
	var refs []workflow.Ref
	for index, c := range s.Cases {
		e, err := Parse(c.When)
		if err != nil {
			return nil, fmt.Errorf("case %d: %w", index, err)
		}
		refs = append(refs, e.Refs()...)
	}
	return refList(refs).sortedUnique(), nil
}
