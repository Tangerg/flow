package workflow

import (
	"context"
	"fmt"
	"slices"
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
	// Observer receives step lifecycle events. A nil Observer or nil
	// ObserverFunc disables events.
	Observer Observer

	// Emitter receives values produced by a [StreamLeaf]. A nil Emitter or nil
	// EmitterFunc discards them without constructing chunks or consuming
	// sequence numbers.
	Emitter Emitter

	// Journal records completed steps so a later run can resume instead of
	// starting over. A nil Journal disables resumption.
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
	definitionOnce  sync.Once
	definitionErr   error
	claimsMu        sync.Mutex
	claims          journalNode
}

// Run executes step once under cfg. Each call establishes a fresh run boundary:
// signal sequence numbers start at one, while cfg.Journal may deliberately
// carry completed work across calls. Journal replay observes a stable snapshot
// from the start of the call; records added during the call belong to a later
// run. A nil step returns [ErrNilStep].
//
//	journal := workflow.NewJournal()
//	out, err := workflow.Run(ctx, pipeline, in, workflow.RunConfig{
//		Observer: workflow.ObserverFunc(log),
//		Journal:  journal,
//	})
//
// Call step.Run directly when no run configuration is needed.
func Run(ctx context.Context, step Step, in Store, cfg RunConfig) (Store, error) {
	if step == nil {
		return in, ErrNilStep
	}
	return step.Run(withConfig(ctx, cfg), in)
}

// withConfig installs a new run even for the zero configuration. Masking an
// enclosing run is what makes a nested Run call an independent boundary.
func withConfig(ctx context.Context, cfg RunConfig) context.Context {
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
	return withConfig(ctx, RunConfig{})
}

func runFrom(ctx context.Context) *runState {
	run, _ := ctx.Value(runKey{}).(*runState)
	return run
}

// observing reports whether anything is watching, so a step can skip work that
// only an observer would use. The nil function adapter is disabled just like a
// nil interface; otherwise it would consume sequence numbers without delivering
// an event.
func (r *runState) observing() bool {
	if r == nil || r.config.Observer == nil {
		return false
	}
	function, ok := r.config.Observer.(ObserverFunc)
	return !ok || function != nil
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
// claimed the same identity, not that the later step is being resumed.
func (r *runState) replay(path []string, id string) (any, bool) {
	if r == nil || r.config.Journal == nil {
		return nil, false
	}
	return r.config.Journal.lookupAt(path, id, r.journalRevision)
}

// claim enforces the execution identity invariant independently of the
// Journal. This catches duplicate IDs even when both invocations would replay
// the same historical record, and it also covers opaque caller-defined wrappers
// that static definition validation cannot see through.
func (r *runState) claim(path []string, id string) error {
	if r == nil {
		return nil
	}
	key := JournalKey{ID: id, Path: path}
	if err := key.validate(); err != nil {
		return err
	}

	r.claimsMu.Lock()
	defer r.claimsMu.Unlock()
	if !r.claims.record(path, id, journalValue{}) {
		return fmt.Errorf(
			"%w: step %q at path %q was invoked more than once in one run",
			ErrDuplicateStep,
			id,
			path,
		)
	}
	return nil
}

func (r *runState) validateDefinition(step Step) error {
	r.definitionOnce.Do(func() {
		r.definitionErr = (definitionValidator{}).validate(step)
	})
	return r.definitionErr
}

// emit completes event with the run's sequence number and scope, then delivers
// it. A nil receiver discards the event, so callers need not check first.
func (r *runState) emit(ctx context.Context, event Event) {
	if !r.observing() {
		return
	}
	event.Seq = r.nextSeq()
	event.Path = Scope(ctx)
	r.config.Observer.Observe(ctx, event)
}

type scopeKey struct{}

// WithScope returns a context with an additional scope segment. Composites use it
// so that a step running once per loop iteration or per collection element can be
// told apart; write one when building a composite that runs a child more than
// once.
//
// A scope is execution state rather than configuration, which is why it is not a
// [RunConfig] field: composites push segments as they run. It is maintained
// whether or not anything is watching, because it identifies a step rather than
// merely labelling it — [Event.Path] reports it, and a [Journal] keys its records
// by it, which is what keeps resumption correct where one step runs many times.
// Each call copies the path, so concurrent branches never share a slice.
func WithScope(ctx context.Context, segment string) context.Context {
	current := scope(ctx)
	path := make([]string, len(current)+1)
	copy(path, current)
	path[len(current)] = segment
	return context.WithValue(ctx, scopeKey{}, path)
}

// Scope returns the scope path of a step running under ctx: its enclosing
// repeated scopes, outermost first. The returned slice is a copy.
func Scope(ctx context.Context) []string {
	return slices.Clone(scope(ctx))
}

// scope returns the context-owned path for internal reads. Callers that retain
// or expose it must clone it.
func scope(ctx context.Context) []string {
	path, _ := ctx.Value(scopeKey{}).([]string)
	return path
}
