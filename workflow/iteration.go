package workflow

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
)

const (
	itemKey  = "item"
	indexKey = "index"
)

// Item returns the reference under which [Iteration] stores the current item.
func Item(id string) Ref { return At(id, itemKey) }

// ItemIndex returns the reference under which [Iteration] stores the current
// item's zero-based index.
func ItemIndex(id string) Ref { return At(id, indexKey) }

// IterationConfig configures [Iteration].
type IterationConfig struct {
	// ID names the node; each element's result is collected under Output(ID).
	ID string
	// Input references the JSON-compatible array to iterate over.
	Input Ref
	// Body runs once per element on a Store derived from the outer input, under
	// an indexed execution scope (see [Item] and [ItemIndex]).
	Body Step
	// BodyOutput references the value in each post-run Store to collect. For a
	// visible built-in Body it must select a body output, [Item], a value nested
	// under Item, or [ItemIndex] itself. ItemIndex is a scalar and therefore has
	// no child path. An opaque caller-defined Body owns its output contract at
	// run time.
	BodyOutput Ref
	// Concurrency caps concurrent element runs. Zero is unbounded; negative
	// values are invalid.
	Concurrency int
}

// Iteration runs cfg.Body once per element of the array at cfg.Input,
// concurrently, and collects each run's cfg.BodyOutput into a []any written at
// Output(cfg.ID). Typed slices are accepted through [Get]'s JSON conversion.
//
// For element i, Body inherits the outer Store and adds the element under
// [Item](cfg.ID) and its index via [ItemIndex](cfg.ID). Scope isolates execution
// identity, not Store names; wrap Body in [Subgraph] when it needs a sealed
// state namespace. The value at cfg.Input must be JSON-convertible to an array.
// The first observed element failure cancels the rest; when elements fail
// concurrently, completion timing decides which failure is observed first.
// Iteration waits for every admitted element, so bodies already running must
// cooperate with context cancellation.
//
// Because Body runs once per element, each element adds an indexed
// [ScopeFrame] naming the iteration node and element index, so an observer can
// tell the elements' steps apart. That frame is also what lets a [Journal]
// resume an iteration element by element.
//
// A suspended element does not cancel the others: they run to completion, their
// journaled inner boundaries are recorded, and the suspensions are returned
// together. The collected output is written only once every element has
// produced one, since a slice with holes would read as a finished result. Every
// incomplete outcome — suspension, ordinary failure, or parent cancellation —
// returns the input Store without a collected output. Completed journaled
// boundaries inside elements replay on the next run; an opaque Body that owns
// no Journal boundary runs again. Iteration validates its ID, references, and
// body before reading the input, so an empty collection cannot hide an invalid
// definition. Input failures are reported at OpBind; element execution and
// collection failures are reported at OpRun, while their [flow.IndexError]
// still identifies the element.
func Iteration(cfg IterationConfig) Step {
	return cfg.step()
}

func (c IterationConfig) step() iterationStep {
	return iterationStep{
		id:         c.ID,
		input:      c.Input,
		body:       c.Body,
		bodyOutput: c.BodyOutput,
		limit:      c.Concurrency,
	}
}

// elementOutcome is one element's result. A suspension travels as a value
// because it is not a failure; anything else travels as the mapper's error.
type elementOutcome struct {
	value       any
	suspensions suspensionList
}

// iterationStep is the [Step] produced by [Iteration].
type iterationStep struct {
	id         string
	input      Ref
	body       Step
	bodyOutput Ref
	limit      int
}

func (i iterationStep) Run(ctx context.Context, s Store) (Store, error) {
	ctx = ensureRun(ctx)
	if err := i.Validate(); err != nil {
		return s, err
	}
	if err := validateChildScope(scope(ctx)); err != nil {
		return s, newStepError(ctx, i.id, OpValidate, err)
	}
	if err := runFrom(ctx).claim(scope(ctx), i.id); err != nil {
		return s, newStepError(ctx, i.id, OpValidate, err)
	}
	if contextErr := context.Cause(ctx); contextErr != nil {
		return s, contextErr
	}
	items, err := Get[[]any](s, i.input)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return s, contextErr
	}
	if err != nil {
		return s, newStepError(ctx, i.id, OpBind, err)
	}
	execution := iterationExecution{
		iteration: i,
		// Every element derives its own scoped Store from this one concurrently.
		input: s.sharedBase(),
		items: items,
	}
	return execution.execute(ctx)
}

// iterationExecution owns the immutable element set and outer Store for one
// invocation. It doubles as the Node scheduled by flow.Map: concurrent calls
// read shared definition state, while every element creates its own scoped
// Store and result.
type iterationExecution struct {
	iteration iterationStep
	input     Store
	items     []any
}

func (i *iterationExecution) execute(ctx context.Context) (Store, error) {
	outcomes, err := i.runElements(ctx)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return i.input, contextErr
	}
	if err != nil {
		return i.input, newStepError(ctx, i.iteration.id, OpRun, err)
	}
	return i.collect(ctx, outcomes)
}

func (i iterationStep) validate() error {
	if err := validateBody(i.id, i.body); err != nil {
		return err
	}
	if err := (flow.MapConfig{Concurrency: i.limit}).Validate(); err != nil {
		return newValidationError(
			i.id,
			err)
	}
	if err := i.input.Validate(); err != nil {
		return newValidationError(
			i.id,
			fmt.Errorf("%w: iteration input: %w", flow.ErrInvalidConfig, err))
	}
	if err := i.bodyOutput.Validate(); err != nil {
		return newValidationError(
			i.id,
			fmt.Errorf("%w: iteration body output: %w", flow.ErrInvalidConfig, err))
	}
	return nil
}

func (i iterationStep) Validate() error { return validateDefinition(i) }

func (i *iterationExecution) runElements(ctx context.Context) ([]elementOutcome, error) {
	elementIndexes := make([]int, len(i.items))
	for index := range i.items {
		elementIndexes[index] = index
	}
	return flow.Map(i, flow.MapConfig{Concurrency: i.iteration.limit}).Run(ctx, elementIndexes)
}

// Run executes one indexed element. A suspension travels as a value so Map
// does not cancel the remaining elements; ordinary failures retain Map's
// fail-fast behavior.
func (i *iterationExecution) Run(ctx context.Context, index int) (elementOutcome, error) {
	scoped := i.input.
		WithCell(i.iteration.id, itemKey, i.items[index]).
		WithCell(i.iteration.id, indexKey, index)
	body := (scopedStep{step: i.iteration.body}).indexed(i.iteration.id, index)
	result, err := body.run(ctx, scoped)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return elementOutcome{}, contextErr
	}
	if err != nil {
		if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
			return elementOutcome{suspensions: suspensions}, nil
		}
		return elementOutcome{}, err
	}
	value, err := Get[any](result, i.iteration.bodyOutput)
	if err != nil {
		return elementOutcome{}, bodyOutputError(i.iteration.bodyOutput, err)
	}
	return elementOutcome{value: value}, nil
}

func (i *iterationExecution) collect(ctx context.Context, outcomes []elementOutcome) (Store, error) {
	outputs := make([]any, len(outcomes))
	var suspensions suspensionList
	for index, outcome := range outcomes {
		if contextErr := context.Cause(ctx); contextErr != nil {
			return i.input, contextErr
		}
		if len(outcome.suspensions) > 0 {
			suspensions = append(suspensions, outcome.suspensions...)
			continue
		}
		outputs[index] = outcome.value
	}
	if contextErr := context.Cause(ctx); contextErr != nil {
		return i.input, contextErr
	}
	if len(suspensions) > 0 {
		// The collection is incomplete, so it is not written: a partial slice
		// with holes would read as a finished result. Journaled inner boundaries
		// that did finish can replay; opaque work without one runs again.
		return i.input, suspensions.err()
	}
	result := i.input.WithOutput(i.iteration.id, outputs)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return i.input, contextErr
	}
	return result, nil
}

func (i iterationStep) Describe() Description {
	return Description{ID: i.id, Kind: KindIteration, Children: []Description{describe(i.body)}}
}

func (i iterationStep) definition() stepDefinition {
	return stepDefinition{
		kind:       definitionIteration,
		id:         i.id,
		output:     true,
		body:       i.body,
		bodyOutput: i.bodyOutput,
	}
}
