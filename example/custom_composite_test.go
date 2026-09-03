package example_test

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

type repeatStep struct {
	id    string
	body  workflow.Step
	count int
}

func (r repeatStep) Validate() error {
	if err := (workflow.ScopeFrame{ID: r.id, Indexed: true}).Validate(); err != nil {
		return fmt.Errorf("repeat scope: %w", err)
	}
	if r.count < 0 {
		return fmt.Errorf("%w: repeat count must be non-negative, got %d", flow.ErrInvalidConfig, r.count)
	}
	return flow.Validate(r.body)
}

func (r repeatStep) Run(
	ctx context.Context,
	store workflow.Store,
) (workflow.Store, error) {
	if err := r.Validate(); err != nil {
		return store, err
	}
	current := store
	for index := range r.count {
		// RunChild applies the cancellation contract a composite owes its
		// children, so this loop states the rollback instead of the rule: a
		// workflow composite keeps the Store it handed the child, because that
		// Store is what already completed.
		next, err := flow.RunChild(
			workflow.WithScopeIndex(ctx, r.id, uint64(index)),
			r.body,
			current,
		)
		if err != nil {
			return current, err
		}
		current = next
	}
	return current, context.Cause(ctx)
}

// A caller-defined repeated composite uses WithScopeIndex to give each body
// invocation a stable identity, and flow.RunChild to invoke that body under the
// same cancellation contract a built-in composite applies. Prefer Loop or
// Iteration when either built-in expresses the required control flow.
func Example_customRepeatedComposite() {
	runs := 0
	body := workflow.LeafFunc(
		"work",
		workflow.Output("input"),
		func(_ context.Context, input int) (int, error) {
			runs++
			return input * 2, nil
		},
	)
	repeated := repeatStep{id: "batch", body: body, count: 2}
	journal := workflow.NewJournal()

	for range 2 {
		_, err := workflow.Run(
			context.Background(),
			repeated,
			workflow.NewStore().WithOutput("input", 3),
			workflow.RunConfig{Journal: journal},
		)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
	}

	fmt.Println("runs:", runs)
	for _, key := range journal.Keys() {
		fmt.Println(key.Scope[0], key.ID)
	}

	// Output:
	// runs: 2
	// batch[0] work
	// batch[1] work
}
