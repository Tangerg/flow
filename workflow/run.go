package workflow

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// RunConfig configures one run of a workflow: what observes it, what receives
// its streaming output, and what lets it resume. Its zero value enables none of
// those facilities.
//
// Configuration belongs to one call to [Run], not to the workflow definition.
// A compiled workflow can therefore be reused concurrently with independent
// observers and journals.
//
// Construct it with keyed fields. New run-scoped settings can then be added
// without breaking callers.
type RunConfig struct {
	// Observer receives observable workflow-boundary events. A nil Observer or nil
	// ObserverFunc disables events.
	Observer Observer

	// Emitter receives values produced by [StreamFunc] nodes inside a [Leaf]. A
	// nil Emitter or nil EmitterFunc discards them without constructing chunks
	// or consuming sequence numbers.
	Emitter Emitter

	// Journal holds replayable leaf outputs, control-flow decisions, and
	// externally recorded interrupt responses. A nil Journal disables resumption.
	Journal *Journal
}

type runKey struct{}

// runState is the carrier stored in the context: the caller's configuration plus
// the run's own bookkeeping. Keeping the counter here rather than in RunConfig
// leaves the exported type plain data.
type runState struct {
	config          RunConfig
	journalRevision uint64
	seq             atomic.Uint64
	claims          scopedSet
	bypassed        scopedSet
}

// scopedSet is a concurrent set of execution identities, keyed the way a
// [Journal] keys its records so that a scope's structure is its identity rather
// than its rendered form. Owning the lock is the point: the discipline that a
// trie is never touched without it becomes part of the type instead of a
// convention each of runState's two sets would otherwise restate.
type scopedSet struct {
	mu   sync.RWMutex
	root journalNode
}

// add records one identity and reports whether it was new.
func (s *scopedSet) add(scope []ScopeFrame, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.root.record(scope, id, journalValue{})
}

func (s *scopedSet) has(scope []ScopeFrame, id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.root.lookup(scope, id)
	return ok
}

// Run executes step once under cfg. Each call establishes a fresh execution
// boundary: signal sequence numbers and identity claims start over, while
// cfg.Journal may deliberately carry completed work across calls. A Run nested
// inside another Run starts at a new root workflow scope; a scope explicitly
// attached before the first Run remains its initial scope. Ordinary context
// values, cancellation, and deadlines still propagate. The input Store is
// passed to step as supplied; Run does not treat it as a namespace or erase
// values from an earlier call. Journal replay is revision-bounded at the start
// of the call: records added during the call belong to a later run.
// Destructive Journal operations such as Forget, Reset, and UnmarshalJSON belong
// between runs. For a caller-defined top-level composite, Run applies its
// optional Validate method before installing the execution boundary, so an
// invalid visible definition performs no work. Steps built by this package
// validate in their own Run method, where they can emit a correctly identified
// failure event. Run cancels the execution context supplied to step before returning,
// and also while unwinding a panic, so correctly context-aware background work
// cannot outlive the run-scoped identity, observers, or Journal configuration.
// A Step remains responsible for joining work whose result belongs to its Run
// call. A nil step returns [ErrNilStep].
//
//	journal := workflow.NewJournal()
//	out, err := workflow.Run(ctx, pipeline, in, workflow.RunConfig{
//		Observer: workflow.ObserverFunc(log),
//		Journal:  journal,
//	})
//
// A Step built by this package may be called directly when no run configuration
// is needed; its composite establishes the zero-config bookkeeping it needs.
// Use Run for a caller-defined top-level composite even with a zero RunConfig,
// so every child invocation shares one identity boundary.
func Run(ctx context.Context, step Step, in Store, cfg RunConfig) (Store, error) {
	if isNilNode(step) {
		// The sentinel names only the condition, and there is no step identity
		// to report here, so Run supplies the context itself.
		return in, fmt.Errorf("workflow: run: %w", ErrNilStep)
	}
	if _, builtIn := step.(definedStep); !builtIn {
		if err := validateDefinition(step); err != nil {
			return in, err
		}
	}
	executionCtx, cancel := context.WithCancel(withConfig(ctx, cfg))
	defer cancel()
	return step.Run(executionCtx, in)
}

// withConfig installs a new execution boundary even for the zero configuration.
// Masking an enclosing run is what makes a nested Run call independent.
func withConfig(ctx context.Context, cfg RunConfig) context.Context {
	if runFrom(ctx) != nil {
		// Scope belongs to one execution tree. Inheriting it here would make a
		// nested Run's otherwise independent Journal keys and signals depend on
		// the caller's current loop, iteration, or subgraph invocation. A scope
		// installed before the first Run is intentional and remains intact.
		ctx = context.WithValue(ctx, scopeKey{}, []ScopeFrame(nil))
	}
	return withRunState(ctx, cfg)
}

func withRunState(ctx context.Context, cfg RunConfig) context.Context {
	return context.WithValue(ctx, runKey{}, &runState{
		config:          cfg,
		journalRevision: cfg.Journal.snapshotRevision(),
	})
}

// ensureRun gives direct composite.Run calls the same per-run identity
// bookkeeping as the package-level Run function, without masking an existing
// configured run.
func ensureRun(ctx context.Context) context.Context {
	if runFrom(ctx) != nil {
		return ctx
	}
	// A direct child invocation is still part of its caller's execution tree,
	// so unlike package-level Run it preserves a scope installed by WithScope.
	return withRunState(ctx, RunConfig{})
}

func runFrom(ctx context.Context) *runState {
	run, _ := ctx.Value(runKey{}).(*runState)
	return run
}

// observing reports whether anything is watching, so a step can skip work that
// only an observer would use. The nil function adapter is disabled just like a
// nil interface; otherwise it would consume sequence numbers without delivering
// an event.
//
// Check it before building an event whose fields cost something — an elapsed
// time reads the clock — and not otherwise: [runState.emit] checks it again, so
// guarding an event that is free to build only adds a branch. That is why the
// events carrying Elapsed are guarded and the others are not.
func (r *runState) observing() bool {
	if r == nil || r.config.Observer == nil {
		return false
	}
	if function, ok := r.config.Observer.(ObserverFunc); ok && function == nil {
		return false
	}
	return true
}

// journal returns the run's Journal, or nil when resumption is disabled.
func (r *runState) journal() *Journal {
	if r == nil {
		return nil
	}
	return r.config.Journal
}

// emitter returns the run's Emitter, or nil when streaming is disabled.
func (r *runState) emitter() Emitter {
	if r == nil || r.config.Emitter == nil {
		return nil
	}
	if function, ok := r.config.Emitter.(EmitterFunc); ok && function == nil {
		return nil
	}
	return r.config.Emitter
}

// nextSeq assigns every externally visible signal a total order within this
// run. Delivery remains concurrent; consumers can sort events and chunks by the
// assigned number when they need one timeline.
func (r *runState) nextSeq() uint64 {
	return r.seq.Add(1)
}

// replay returns a record that existed when this run began. Records written by
// the current run are deliberately excluded: seeing one again means two steps
// claimed the same identity, not that the later step is being resumed. Journal
// lookup may wait on concurrent access, so cancellation is sampled again after
// the lookup before a caller restores state or invokes application code.
func (r *runState) replay(
	ctx context.Context,
	scope []ScopeFrame,
	id string,
) (any, bool, error) {
	if r == nil || r.config.Journal == nil {
		return nil, false, context.Cause(ctx)
	}
	value, ok := r.config.Journal.lookupAt(scope, id, r.journalRevision)
	return value, ok, context.Cause(ctx)
}

// replayDecision returns the control-flow decision id recorded before this run
// began. A record of some other type is a corrupted Journal rather than a
// resumable state, because the composite cannot tell what it decided last time.
// [Branch] and [Loop] are the two composites that journal a decision instead of
// an output, and the type each wants names itself here rather than in prose each
// of them would spell separately -- see
// TestAReplayedDecisionMustCarryTheTypeItsCompositeRecorded.
func replayDecision[T any](
	ctx context.Context,
	run *runState,
	kind Kind,
	id string,
) (T, bool, error) {
	var decision T
	recorded, replayed, err := run.replay(ctx, scope(ctx), id)
	if err != nil || !replayed {
		return decision, false, err
	}
	decision, ok := recorded.(T)
	if !ok {
		return decision, false, newStepError(ctx, id, OpRun, fmt.Errorf(
			"%w: journaled %s decision has type %T; want %T",
			ErrTypeMismatch,
			kind,
			recorded,
			decision,
		))
	}
	return decision, true, nil
}

// claim enforces the execution identity invariant independently of the
// Journal. This catches duplicate IDs even when both invocations would replay
// the same historical record, and it also covers opaque caller-defined wrappers
// that static definition validation cannot see through.
func (r *runState) claim(scope []ScopeFrame, id string) error {
	key := JournalKey{ID: id, Scope: scope}
	if err := key.validate(); err != nil {
		return err
	}
	if r == nil {
		return nil
	}

	if !r.claims.add(scope, id) {
		return fmt.Errorf(
			"%w: step %q in scope %q was invoked more than once in one run",
			ErrDuplicateStep,
			id,
			formatScope(scope),
		)
	}
	return nil
}

// markBypassed records that a gate declined to run id, so a node downstream of
// it can tell "did not run" from "has not run yet".
func (r *runState) markBypassed(scope []ScopeFrame, id string) {
	r.bypassed.add(scope, id)
}

func (r *runState) wasBypassed(scope []ScopeFrame, id string) bool {
	return r.bypassed.has(scope, id)
}

// emit completes event with the run's sequence number and scope, then delivers
// it synchronously. A nil receiver discards the event.
func (r *runState) emit(ctx context.Context, event Event) {
	if !r.observing() {
		return
	}
	event.Seq = r.nextSeq()
	event.Scope = Scope(ctx)
	r.config.Observer.Observe(callbackContext(ctx), event)
}

// emitAndCheck is the cancellation checkpoint used after a non-failure event.
// The synchronous Observer may have blocked while ctx was cancelled. Failure
// paths call emit directly because an already classified failure keeps priority.
func (r *runState) emitAndCheck(ctx context.Context, event Event) error {
	r.emit(ctx, event)
	return context.Cause(ctx)
}

// callbackContext preserves application values, cancellation, and deadlines
// while removing the engine-owned identity of the execution being observed.
// Observer and Emitter callbacks consume signals; directly invoking a Step from
// one must not claim identities, publish signals, or write checkpoints into the
// producing run. A callback that needs another workflow execution can call Run,
// which then establishes an explicit, independent boundary.
func callbackContext(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, runKey{}, (*runState)(nil))
	ctx = context.WithValue(ctx, scopeKey{}, []ScopeFrame(nil))
	return context.WithValue(ctx, emissionKey{}, (*emissionSession)(nil))
}
