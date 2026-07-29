package workflow

import (
	"context"
	"slices"
	"sync/atomic"
)

// RunConfig configures one run of a workflow: what watches it and what lets it
// resume. Its zero value runs the workflow with neither.
//
// Configuration belongs to one call to [Run], not to the workflow definition.
// A compiled workflow can therefore be reused concurrently with independent
// observers and journals.
//
// Construct it with keyed fields. New run-scoped settings can then be added
// without breaking callers.
type RunConfig struct {
	// Observer receives step lifecycle events. A nil Observer disables events.
	Observer Observer

	// Journal records completed steps so a later run can resume instead of
	// starting over. A nil Journal disables resumption.
	Journal *Journal
}

type runKey struct{}

// runState is the carrier stored in the context: the caller's configuration plus
// the run's own bookkeeping. Keeping the counter here rather than in RunConfig
// leaves the exported type plain data.
type runState struct {
	config RunConfig
	seq    atomic.Uint64
}

// Run executes step once under cfg. Each call establishes a fresh run boundary:
// event sequence numbers start at one, while cfg.Journal may deliberately carry
// completed work across calls. A nil step returns [ErrNilStep].
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
	return context.WithValue(ctx, runKey{}, &runState{config: cfg})
}

func runFrom(ctx context.Context) *runState {
	run, _ := ctx.Value(runKey{}).(*runState)
	return run
}

// observing reports whether anything is watching, so a step can skip work that
// only an observer would use. A nil receiver is not observing.
func (r *runState) observing() bool { return r != nil && r.config.Observer != nil }

// journal returns the run's Journal, or nil when resumption is disabled.
func (r *runState) journal() *Journal {
	if r == nil {
		return nil
	}
	return r.config.Journal
}

// emit completes event with the run's sequence number and scope, then delivers
// it. A nil receiver discards the event, so callers need not check first.
func (r *runState) emit(ctx context.Context, event Event) {
	if !r.observing() {
		return
	}
	event.Seq = r.seq.Add(1)
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
