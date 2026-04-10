package flow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// BranchConfig holds the configuration for a Branch node.
type BranchConfig[I, O any] struct {
	// Branches maps each branch name to its handler function.
	// All handlers must share the same input and output types.
	Branches map[string]func(context.Context, I) (O, error)

	// BranchResolver selects which branch to execute based on the input.
	// If nil and exactly one branch is configured, that branch is used automatically.
	BranchResolver func(context.Context, I) string
}

// validate checks the configuration and fills in safe defaults where possible.
func (cfg *BranchConfig[I, O]) validate() error {
	if cfg == nil {
		return errors.New("branch config cannot be nil")
	}

	if len(cfg.Branches) == 0 {
		return errors.New("at least one branch is required")
	}

	// When only one branch exists and no resolver is provided, use that branch by default.
	if len(cfg.Branches) == 1 && cfg.BranchResolver == nil {
		var defaultBranch string
		for defaultBranch = range cfg.Branches {
			break
		}
		cfg.BranchResolver = func(context.Context, I) string {
			return defaultBranch
		}
	}

	if cfg.BranchResolver == nil {
		return errors.New("branch resolver cannot be nil for multiple branches")
	}

	return nil
}

var _ Node[any, any] = (*Branch[any, any])(nil)

// Branch routes each execution to one of several named handlers based on
// the result of a resolver function evaluated at runtime.
type Branch[I, O any] struct {
	handlers       map[string]func(context.Context, I) (O, error)
	branchResolver func(context.Context, I) string
}

// NewBranch creates a Branch node from the given configuration.
// Returns an error if the configuration is invalid.
func NewBranch[I, O any](cfg BranchConfig[I, O]) (*Branch[I, O], error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid branch config: %w", err)
	}

	return &Branch[I, O]{
		handlers:       maps.Clone(cfg.Branches),
		branchResolver: cfg.BranchResolver,
	}, nil
}

// resolveBranch asks the resolver which branch to use, then looks up and
// returns the corresponding handler. Returns an error if the resolved name
// does not match any registered branch.
func (b *Branch[I, O]) resolveBranch(ctx context.Context, input I) (func(context.Context, I) (O, error), error) {
	name := b.branchResolver(ctx, input)

	handler, ok := b.handlers[name]
	if !ok {
		available := slices.Sorted(maps.Keys(b.handlers))
		return nil, fmt.Errorf("branch '%s' not found: available branches are %v", name, available)
	}

	return handler, nil
}

// Run resolves the target branch and executes its handler with the given input.
func (b *Branch[I, O]) Run(ctx context.Context, input I) (O, error) {
	handler, err := b.resolveBranch(ctx, input)
	if err != nil {
		var zero O
		return zero, err
	}

	result, err := handler(ctx, input)
	if err != nil {
		var zero O
		return zero, fmt.Errorf("branch execution failed: %w", err)
	}

	return result, nil
}
