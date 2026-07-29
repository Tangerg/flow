package workflow

import (
	"context"
	"sync/atomic"
)

// RunConfig configures one run of a workflow: what watches it and what lets it
// resume. Its zero value runs the workflow with neither.
//
// A run's configuration travels in the [context.Context] rather than in a
// [Step]'s construction because it belongs to the run, not to the definition. A
// compiled workflow is built once and run many times, concurrently, and each of
// those runs wants its own [Journal] and may want its own [Observer]; baking
// either into the steps would mean rebuilding the graph per run. It cannot travel
// in the [Store] either — an Observer is a live object, while a Store is
// serializable data that [Parallel] merges.
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
	seq    *atomic.Uint64
}

// WithConfig returns a context that runs a workflow under cfg. Steps running
// under it report events to cfg.Observer and record into cfg.Journal.
//
//	journal := workflow.NewJournal()
//	ctx = workflow.WithConfig(ctx, workflow.RunConfig{
//		Observer: workflow.ObserverFunc(log),
//		Journal:  journal,
//	})
//	out, err := pipeline.Run(ctx, in)
//
// Event sequence numbering starts fresh at each call, so one call marks one run.
// A zero cfg returns ctx unchanged.
func WithConfig(ctx context.Context, cfg RunConfig) context.Context {
	if cfg.Observer == nil && cfg.Journal == nil {
		return ctx
	}
	return context.WithValue(ctx, runKey{}, &runState{config: cfg, seq: new(atomic.Uint64)})
}

// Config returns the configuration a step running under ctx was given, or the
// zero RunConfig outside a configured run. Use it to pass a run's observer and
// journal on to a nested run.
func Config(ctx context.Context) RunConfig {
	if run := runFrom(ctx); run != nil {
		return run.config
	}
	return RunConfig{}
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
	current := Scope(ctx)
	path := make([]string, len(current)+1)
	copy(path, current)
	path[len(current)] = segment
	return context.WithValue(ctx, scopeKey{}, path)
}

// Scope returns the scope path of a step running under ctx: its enclosing
// repeated scopes, outermost first. The result must not be modified.
func Scope(ctx context.Context) []string {
	path, _ := ctx.Value(scopeKey{}).([]string)
	return path
}
