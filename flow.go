package flow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// BranchBuilder constructs a Branch node through a fluent interface.
type BranchBuilder[I, O any] struct {
	config BranchConfig[I, O]
}

// NewBranchBuilder creates an empty BranchBuilder.
func NewBranchBuilder[I, O any]() *BranchBuilder[I, O] {
	return &BranchBuilder[I, O]{
		config: BranchConfig[I, O]{},
	}
}

// WithBranches sets the named handlers available for routing.
// The map is cloned so later mutations do not affect the builder.
func (b *BranchBuilder[I, O]) WithBranches(branches map[string]func(context.Context, I) (O, error)) *BranchBuilder[I, O] {
	b.config.Branches = maps.Clone(branches)
	return b
}

// WithBranchResolver sets the function that picks a branch name at runtime.
func (b *BranchBuilder[I, O]) WithBranchResolver(resolver func(context.Context, I) string) *BranchBuilder[I, O] {
	b.config.BranchResolver = resolver
	return b
}

// Build validates the configuration and returns the Branch node.
func (b *BranchBuilder[I, O]) Build() (*Branch[I, O], error) {
	branch, err := NewBranch[I, O](b.config)
	if err != nil {
		return nil, fmt.Errorf("failed to build branch: %w", err)
	}

	return branch, nil
}

// IterationBuilder constructs an Iteration node through a fluent interface.
type IterationBuilder[I, O any] struct {
	config IterationConfig[I, O]
}

// NewIterationBuilder creates an empty IterationBuilder.
func NewIterationBuilder[I, O any]() *IterationBuilder[I, O] {
	return &IterationBuilder[I, O]{
		config: IterationConfig[I, O]{},
	}
}

// WithProcessor sets the function applied to each element.
// It receives the element's zero-based index and its value.
func (b *IterationBuilder[I, O]) WithProcessor(processor func(context.Context, int, I) (output O, err error)) *IterationBuilder[I, O] {
	b.config.Processor = processor
	return b
}

// WithContinueOnError controls whether processing continues after an element fails.
// If false (default), the first error stops the iteration.
func (b *IterationBuilder[I, O]) WithContinueOnError(continueOnError bool) *IterationBuilder[I, O] {
	b.config.ContinueOnError = continueOnError
	return b
}

// WithConcurrencyLimit sets the maximum number of elements processed concurrently.
// Use 0 or a negative value for sequential processing (the default).
func (b *IterationBuilder[I, O]) WithConcurrencyLimit(limit int) *IterationBuilder[I, O] {
	b.config.ConcurrencyLimit = limit
	return b
}

// Build validates the configuration and returns the Iteration node.
func (b *IterationBuilder[I, O]) Build() (*Iteration[I, O], error) {
	iteration, err := NewIteration(b.config)
	if err != nil {
		return nil, fmt.Errorf("failed to build iteration: %w", err)
	}

	return iteration, nil
}

// LoopBuilder constructs a Loop node through a fluent interface.
type LoopBuilder[T any] struct {
	config LoopConfig[T]
}

// NewLoopBuilder creates an empty LoopBuilder.
func NewLoopBuilder[T any]() *LoopBuilder[T] {
	return &LoopBuilder[T]{
		config: LoopConfig[T]{},
	}
}

// WithMaxIterations sets the maximum number of iterations.
// Defaults to DefaultMaxIterations when left at zero.
func (b *LoopBuilder[T]) WithMaxIterations(maxIterations int) *LoopBuilder[T] {
	b.config.MaxIterations = maxIterations
	return b
}

// WithProcessor sets the function executed on each iteration.
// It receives the current iteration index and the previous output (or the
// initial input on the first call), and returns the next value, a done flag,
// and any error.
func (b *LoopBuilder[T]) WithProcessor(
	processor func(ctx context.Context, iteration int, input T) (output T, done bool, err error),
) *LoopBuilder[T] {
	b.config.Processor = processor
	return b
}

// Build validates the configuration and returns the Loop node.
func (b *LoopBuilder[T]) Build() (*Loop[T], error) {
	loop, err := NewLoop(b.config)
	if err != nil {
		return nil, fmt.Errorf("failed to build loop: %w", err)
	}

	return loop, nil
}

// ParallelBuilder constructs a Parallel node through a fluent interface.
type ParallelBuilder[I, O any] struct {
	config ParallelConfig[I, O]
}

// NewParallelBuilder creates an empty ParallelBuilder.
func NewParallelBuilder[I, O any]() *ParallelBuilder[I, O] {
	return &ParallelBuilder[I, O]{
		config: ParallelConfig[I, O]{},
	}
}

// WithProcessors sets the processors to run concurrently.
// The slice is cloned so later mutations do not affect the builder.
func (b *ParallelBuilder[I, O]) WithProcessors(processors []func(context.Context, I) (O, error)) *ParallelBuilder[I, O] {
	b.config.Processors = slices.Clone(processors)
	return b
}

// WithConcurrencyLimit sets the maximum number of processors running at once.
// Use 0 or a negative value for unlimited concurrency (all start simultaneously).
func (b *ParallelBuilder[I, O]) WithConcurrencyLimit(limit int) *ParallelBuilder[I, O] {
	b.config.ConcurrencyLimit = limit
	return b
}

// WithContinueOnError controls whether remaining processors continue after a failure.
// If false (default), the first error cancels all remaining processors.
func (b *ParallelBuilder[I, O]) WithContinueOnError(continueOnError bool) *ParallelBuilder[I, O] {
	b.config.ContinueOnError = continueOnError
	return b
}

// Build validates the configuration and returns the Parallel node.
func (b *ParallelBuilder[I, O]) Build() (*Parallel[I, O], error) {
	parallel, err := NewParallel(b.config)
	if err != nil {
		return nil, fmt.Errorf("failed to build parallel: %w", err)
	}

	return parallel, nil
}

// Flow is a dynamic-typed workflow builder that accumulates nodes and
// configuration errors, then assembles everything into a pipeline on Build.
//
// For compile-time type safety use Pipe2–Pipe10 directly. Flow trades that
// safety for the ability to compose heterogeneous node types at runtime.
//
// Example:
//
//	node, err := NewFlow().
//	    Loop(func(b *LoopBuilder[any]) {
//	        b.WithMaxIterations(10).WithProcessor(...)
//	    }).
//	    Branch(func(b *BranchBuilder[any, any]) {
//	        b.WithBranches(...).WithBranchResolver(...)
//	    }).
//	    Build()
type Flow struct {
	errors []error
	nodes  []Node[any, any]
}

// NewFlow creates an empty Flow builder.
func NewFlow() *Flow {
	return &Flow{}
}

// append adds a node to the flow, or records the error if node creation failed.
func (f *Flow) append(node Node[any, any], err error) *Flow {
	if err != nil {
		f.errors = append(f.errors, err)
		return f
	}

	if node == nil {
		f.errors = append(f.errors, errors.New("node is nil"))
		return f
	}

	f.nodes = append(f.nodes, node)
	return f
}

// Then appends an already-constructed node to the flow.
// Nil nodes are silently ignored.
func (f *Flow) Then(node Node[any, any]) *Flow {
	if node != nil {
		f.nodes = append(f.nodes, node)
	}
	return f
}

// Loop adds a Loop node configured by the given function.
// Any configuration error is deferred until Build is called.
//
// Example:
//
//	flow.Loop(func(b *LoopBuilder[any]) {
//	    b.WithMaxIterations(5).WithProcessor(func(ctx context.Context, i int, v any) (any, bool, error) {
//	        // return result, shouldStop, error
//	    })
//	})
func (f *Flow) Loop(configure func(*LoopBuilder[any])) *Flow {
	builder := NewLoopBuilder[any]()
	configure(builder)

	loop, err := builder.Build()
	return f.append(loop, err)
}

// Branch adds a Branch node configured by the given function.
// Any configuration error is deferred until Build is called.
//
// Example:
//
//	flow.Branch(func(b *BranchBuilder[any, any]) {
//	    b.WithBranches(map[string]func(context.Context, any) (any, error){
//	        "pathA": processorA,
//	        "pathB": processorB,
//	    }).WithBranchResolver(func(ctx context.Context, input any) string {
//	        // return branch name
//	    })
//	})
func (f *Flow) Branch(configure func(*BranchBuilder[any, any])) *Flow {
	builder := NewBranchBuilder[any, any]()
	configure(builder)

	branch, err := builder.Build()
	return f.append(branch, err)
}

// Iteration adds an Iteration node configured by the given function.
// The node expects its input to be a []any slice at runtime.
// Any configuration error is deferred until Build is called.
//
// Example:
//
//	flow.Iteration(func(b *IterationBuilder[any, any]) {
//	    b.WithProcessor(func(ctx context.Context, idx int, item any) (any, error) {
//	        // process each element
//	    }).WithConcurrencyLimit(10)
//	})
func (f *Flow) Iteration(configure func(*IterationBuilder[any, any])) *Flow {
	builder := NewIterationBuilder[any, any]()
	configure(builder)

	iteration, err := builder.Build()

	var node Node[any, any]
	if iteration != nil {
		node = Func[any, any](func(ctx context.Context, input any) (any, error) {
			inputs, ok := input.([]any)
			if !ok {
				return nil, fmt.Errorf("iteration expects []any input, got %T", input)
			}

			return iteration.Run(ctx, inputs)
		})
	}

	return f.append(node, err)
}

// Parallel adds a Parallel node configured by the given function.
// Any configuration error is deferred until Build is called.
//
// Example:
//
//	flow.Parallel(func(b *ParallelBuilder[any, any]) {
//	    b.WithProcessors([]func(context.Context, any) (any, error){
//	        processorA,
//	        processorB,
//	    }).WithConcurrencyLimit(2)
//	})
func (f *Flow) Parallel(configure func(*ParallelBuilder[any, any])) *Flow {
	builder := NewParallelBuilder[any, any]()
	configure(builder)

	parallel, err := builder.Build()

	var node Node[any, any]
	if parallel != nil {
		node = Func[any, any](func(ctx context.Context, input any) (any, error) {
			return parallel.Run(ctx, input)
		})
	}

	return f.append(node, err)
}

// validate ensures the flow is ready to be built.
func (f *Flow) validate() error {
	if len(f.errors) > 0 {
		return fmt.Errorf("flow configuration failed: %w", errors.Join(f.errors...))
	}

	if len(f.nodes) == 0 {
		return errors.New("flow must contain at least one node")
	}

	return nil
}

// Build validates the accumulated configuration and assembles the final pipeline.
// Returns an error if any node configuration failed, or if no nodes were added.
func (f *Flow) Build() (Node[any, any], error) {
	if err := f.validate(); err != nil {
		return nil, err
	}

	pipeline, err := Pipe(f.nodes...)
	if err != nil {
		return nil, fmt.Errorf("failed to build flow pipeline: %w", err)
	}

	return pipeline, nil
}
