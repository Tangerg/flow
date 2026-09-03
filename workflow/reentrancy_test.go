package workflow_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// TestApplicationCodeMayReenterTheBoundaryThatCalledIt pins the lock discipline
// this package's concurrency rests on, from the outside. Of its five locks
// exactly one spans application code — the emission session's, held across the
// Emitter so delivery stays serialized — and no path holds two at once, which is
// what lets a callback reach back into the objects that invoked it. Three
// comments state their own halves of that arrangement; nothing had tried it.
//
// A broken discipline deadlocks rather than misbehaves, so a failure here is a
// timeout rather than a message. That is the only way the absence of a cycle can
// be observed, and it is why each case performs the reentry its own boundary's
// comment names as the risk.
func TestApplicationCodeMayReenterTheBoundaryThatCalledIt(t *testing.T) {
	t.Run("factory registers into the registry compiling it", testFactoryReentersItsRegistry)
	t.Run("emitter records into the run's journal", testEmitterReentersTheJournal)
	t.Run("observer reads the journal it is completing", testObserverReadsTheJournal)
}

func increment(_ context.Context, value int) (int, error) { return value + 1, nil }

func seededStore() workflow.Store { return workflow.NewStore().WithOutput("seed", 1) }

func testFactoryReentersItsRegistry(t *testing.T) {
	registry := workflow.NewRegistry()
	registry.MustRegisterNode("outer", func(spec workflow.NodeSpec) (workflow.Step, error) {
		// Registry.snapshot exists because holding its lock across a factory
		// would deadlock exactly here.
		if err := registry.RegisterNode("added", workflow.Factory(
			func(struct{}) (flow.Node[int, int], error) {
				return flow.NodeFunc[int, int](increment), nil
			})); err != nil {
			return nil, err
		}
		return workflow.LeafFunc(spec.ID, workflow.Output("seed"), increment), nil
	})

	step, err := registry.CompileSpec(workflow.Spec{
		Kind: workflow.KindLeaf, ID: "a", Type: "outer",
		Inputs: workflow.OneInput(workflow.Output("seed")),
	})
	if err != nil {
		t.Fatalf("CompileSpec: %v", err)
	}
	out, err := workflow.Run(t.Context(), step, seededStore(), workflow.RunConfig{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, getErr := out.Get[int](workflow.Output("a")); getErr != nil || got != 2 {
		t.Fatalf("output = %d, %v; want 2, nil", got, getErr)
	}
	// The registration took effect, which is what makes the reentry real rather
	// than quietly refused.
	if types := registry.NodeTypes(); !slices.Contains(types, "added") {
		t.Fatalf("node types = %v; want the type the factory registered", types)
	}
}

func testEmitterReentersTheJournal(t *testing.T) {
	journal := workflow.NewJournal()
	step := workflow.Leaf(
		"stream",
		workflow.Output("seed").Bind[int](),
		workflow.StreamFunc[int, int, int](
			func(_ context.Context, input int, yield func(int) bool) (int, error) {
				if !yield(input) {
					return 0, errors.New("yield unexpectedly stopped")
				}
				return input, nil
			},
		),
	)

	_, err := workflow.Run(t.Context(), step, seededStore(), workflow.RunConfig{
		Journal: journal,
		Emitter: workflow.EmitterFunc(func(_ context.Context, chunk workflow.Chunk) error {
			// The session's lock is held across this call. The Journal's is not,
			// and nothing holds both.
			return journal.Record(workflow.JournalKey{ID: "from-emitter"}, chunk.Index)
		}),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	recorded := make([]string, 0, 2)
	for _, key := range journal.Keys() {
		recorded = append(recorded, key.ID)
	}
	if !slices.Equal(recorded, []string{"from-emitter", "stream"}) {
		t.Fatalf("journal = %v; want the Emitter's record beside the leaf's", recorded)
	}
}

func testObserverReadsTheJournal(t *testing.T) {
	journal := workflow.NewJournal()
	var visible []string
	_, err := workflow.Run(
		t.Context(),
		workflow.LeafFunc("leaf", workflow.Output("seed"), increment),
		seededStore(),
		workflow.RunConfig{
			Journal: journal,
			Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
				if event.Kind != workflow.EventCompleted {
					return
				}
				for _, key := range journal.Keys() {
					visible = append(visible, key.ID)
				}
			}),
		},
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Reading the Journal from an Observer neither deadlocks nor comes too early:
	// a completion is emitted after its checkpoint is recorded, which is what a
	// host persisting the pair at this event depends on.
	if !slices.Equal(visible, []string{"leaf"}) {
		t.Fatalf("journal at completion = %v; want the completing step's own checkpoint", visible)
	}
}
