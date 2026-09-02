package workflow_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func BenchmarkStoreWithGet(b *testing.B) {
	base := workflow.NewStore().WithOutput("seed", 1)

	b.ReportAllocs()
	for b.Loop() {
		s := base.WithOutput("node", 2)
		_, _ = s.Lookup(workflow.Output("node"))
	}
}

func BenchmarkStoreWithGetScaling(b *testing.B) {
	for _, size := range []int{1, 16, 128, 1024} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			base := workflow.NewStore()
			for i := range size {
				base = base.WithOutput("base-"+strconv.Itoa(i), i)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s := base.WithOutput("node", 2)
				_, _ = s.Lookup(workflow.Output("node"))
			}
		})
	}
}

func BenchmarkStoreLookupScaling(b *testing.B) {
	for _, size := range []int{1, 32, 128, 1024} {
		b.Run(strconv.Itoa(size)+"/oldest", func(b *testing.B) {
			store := benchmarkStore(size)
			ref := workflow.Output("base-0")

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = store.Lookup(ref)
			}
		})
		b.Run(strconv.Itoa(size)+"/newest", func(b *testing.B) {
			store := benchmarkStore(size)
			ref := workflow.Output("base-" + strconv.Itoa(size-1))

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = store.Lookup(ref)
			}
		})
	}
}

// BenchmarkStoreChangesScaling covers both change-detection paths of the public
// Store.Changes. A base the receiver descends from takes the overlay fast path
// and reports a handful of writes; an unrelated base — a separately decoded
// snapshot, say — falls back to comparing revisions and reports every cell.
func BenchmarkStoreChangesScaling(b *testing.B) {
	for _, cells := range []int{64, 1024} {
		related := benchmarkStore(cells)
		descendant := related
		for index := range 4 {
			descendant = descendant.WithOutput("later-"+strconv.Itoa(index), index)
		}
		unrelated := benchmarkStore(cells)

		b.Run(strconv.Itoa(cells)+"/descendant", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = descendant.Changes(related)
			}
		})
		b.Run(strconv.Itoa(cells)+"/unrelated", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = unrelated.Changes(related)
			}
		})
	}
}

func BenchmarkStoreJSONScaling(b *testing.B) {
	for _, size := range []int{16, 128, 1024} {
		store := benchmarkStore(size)
		encoded, err := json.Marshal(store)
		if err != nil {
			b.Fatalf("Marshal setup: %v", err)
		}

		b.Run(strconv.Itoa(size)+"/marshal", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := json.Marshal(store); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(strconv.Itoa(size)+"/unmarshal", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var decoded workflow.Store
				if err := json.Unmarshal(encoded, &decoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSequenceRun(b *testing.B) {
	ctx := b.Context()
	inc := func(id, input string) workflow.Step {
		node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
			return in + 1, nil
		})
		return workflow.Leaf(id, workflow.Output(input).Bind[int](), node)
	}
	step := workflow.Sequence(
		inc("a", "seed"),
		inc("b", "a"),
		inc("c", "b"),
	)
	input := workflow.NewStore().WithOutput("seed", 1)

	b.ReportAllocs()
	for b.Loop() {
		_, _ = step.Run(ctx, input)
	}
}

func BenchmarkStreamFunc(b *testing.B) {
	ctx := b.Context()
	node := workflow.StreamFunc[int, int, int](
		func(_ context.Context, input int, yield func(int) bool) (int, error) {
			for value := range 4 {
				if !yield(input + value) {
					return 0, context.Canceled
				}
			}
			return input + 4, nil
		},
	)
	step := workflow.Leaf(
		"stream",
		workflow.Output("seed").Bind[int](),
		node,
	)
	input := workflow.NewStore().WithOutput("seed", 1)

	b.Run("no-emitter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = step.Run(ctx, input)
		}
	})
	b.Run("emitter", func(b *testing.B) {
		emitter := workflow.EmitterFunc(func(context.Context, workflow.Chunk) error {
			return nil
		})
		cfg := workflow.RunConfig{Emitter: emitter}
		b.ReportAllocs()
		for b.Loop() {
			_, _ = workflow.Run(ctx, step, input, cfg)
		}
	})
}

func BenchmarkSequenceRunScaling(b *testing.B) {
	ctx := b.Context()
	for _, size := range []int{1, 16, 128, 512} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			steps := make([]workflow.Step, size)
			input := "seed"
			for i := range size {
				id := "step-" + strconv.Itoa(i)
				steps[i] = benchmarkIncrement(id, input)
				input = id
			}
			step := workflow.Sequence(steps...)
			store := workflow.NewStore().WithOutput("seed", 1)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = step.Run(ctx, store)
			}
		})
	}
}

// BenchmarkSequenceNestingScaling is the depth counterpart of the width scaling
// above, and it is the one shape where validating before invoking any child
// costs more than the work it protects. Every composite validates its whole
// subtree when Run, and a definition cannot ask whether the parent that just did
// so could see it -- a caller-defined step in the middle is opaque, and the
// built-in steps below it have to claim their own identities. So depth d pays
// about d/2 subtree validations, which is quadratic: 512 nested steps cost some
// seventy-five times the same 512 side by side, where a loop running one body
// that many times stays linear because each iteration revalidates only the body.
// Nothing here is worth a second execution path, but a change that makes one
// validation slower is quadratic in this shape alone, which is why it is
// measured rather than assumed.
func BenchmarkSequenceNestingScaling(b *testing.B) {
	ctx := b.Context()
	for _, depth := range []int{1, 16, 128, 512} {
		b.Run(strconv.Itoa(depth), func(b *testing.B) {
			step := benchmarkIncrement("step-0", "seed")
			for index := 1; index < depth; index++ {
				id := "step-" + strconv.Itoa(index)
				step = workflow.Sequence(step, benchmarkIncrement(id, "step-"+strconv.Itoa(index-1)))
			}
			store := workflow.NewStore().WithOutput("seed", 1)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = step.Run(ctx, store)
			}
		})
	}
}

func BenchmarkParallelMerge(b *testing.B) {
	ctx := b.Context()
	base := workflow.NewStore()
	for i := range 128 {
		base = base.WithOutput("base-"+strconv.Itoa(i), i)
	}
	branches := make([]workflow.Step, 8)
	for i := range branches {
		id := "branch-" + strconv.Itoa(i)
		branches[i] = flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
			return s.WithOutput(id, i), nil
		})
	}
	node := workflow.Parallel(workflow.ParallelConfig{Steps: branches})

	b.ReportAllocs()
	for b.Loop() {
		_, _ = node.Run(ctx, base)
	}
}

func BenchmarkParallelArity(b *testing.B) {
	ctx := b.Context()
	for _, size := range []int{0, 1, 2, 8} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			branches := make([]workflow.Step, size)
			for i := range branches {
				id := "branch-" + strconv.Itoa(i)
				branches[i] = flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
					return store.WithOutput(id, i), nil
				})
			}
			step := workflow.Parallel(workflow.ParallelConfig{Steps: branches})
			input := workflow.NewStore().WithOutput("seed", 1)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = step.Run(ctx, input)
			}
		})
	}
}

func BenchmarkParallelBaseScaling(b *testing.B) {
	ctx := b.Context()
	for _, size := range []int{0, 63, 64, 128} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			branches := make([]workflow.Step, 8)
			for i := range branches {
				id := "branch-" + strconv.Itoa(i)
				branches[i] = flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
					return store.WithOutput(id, i), nil
				})
			}
			step := workflow.Parallel(workflow.ParallelConfig{Steps: branches})
			input := benchmarkStore(size)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = step.Run(ctx, input)
			}
		})
	}
}

// BenchmarkIterationBaseScaling varies the input Store's overlay length, named
// by the subtest, while holding the element count fixed. Every element derives
// its own Store from the shared input, so an overlay at the limit is the case
// where each element would flatten the snapshot separately instead of extending
// a shared one.
func BenchmarkIterationBaseScaling(b *testing.B) {
	ctx := b.Context()
	items := make([]string, 16)
	for i := range items {
		items[i] = "item-" + strconv.Itoa(i)
	}
	body := workflow.LeafFunc("body", workflow.Item("rows"),
		func(_ context.Context, value string) (string, error) { return value, nil })
	step := workflow.Iteration(workflow.IterationConfig{
		ID:         "rows",
		Input:      workflow.Output("source"),
		Body:       body,
		BodyOutput: workflow.Output("body"),
	})

	for _, overlay := range []int{1, 32, 64} {
		b.Run(strconv.Itoa(overlay), func(b *testing.B) {
			// The source binding is the overlay's last write, so the padding
			// supplies the rest of the requested length.
			input := benchmarkStore(overlay-1).WithOutput("source", items)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = step.Run(ctx, input)
			}
		})
	}
}

func BenchmarkCompileGraphScaling(b *testing.B) {
	registry := workflow.NewRegistry().MustRegisterNode(
		"noop",
		func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Interrupt(spec.ID, nil), nil
		},
	)

	for _, shape := range []string{"chain", "wide"} {
		for _, size := range []int{16, 128, 512, 1024} {
			b.Run(shape+"/"+strconv.Itoa(size), func(b *testing.B) {
				graph := benchmarkGraph(shape, size)

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_, _ = registry.CompileGraph(graph)
				}
			})
		}
	}
}

func BenchmarkGraphRunScaling(b *testing.B) {
	registry := workflow.NewRegistry().MustRegisterNode("noop", benchmarkNoopNode())

	for _, shape := range []string{"chain", "wide"} {
		for _, size := range []int{16, 128} {
			b.Run(shape+"/"+strconv.Itoa(size), func(b *testing.B) {
				step, err := registry.CompileGraph(benchmarkGraph(shape, size))
				if err != nil {
					b.Fatalf("CompileGraph: %v", err)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if _, err := step.Run(b.Context(), workflow.NewStore()); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkGraphRunInputScaling varies the number of cells in the input Store,
// named by the subtest, over a graph whose nodes are all independent and
// therefore all ready at once. The cost on this axis is the per-run whole-Store
// work of clearing the graph's own namespace, which a large store makes the
// dominant term. The overlay length is not on this axis: the owned cells this
// benchmark adds to force the re-run path are themselves writes, so they carry
// the overlay past the limit and back down to a short one. Reaching the fan-out
// cliff needs an untouched overlay -- see BenchmarkGraphRunBaseScaling.
func BenchmarkGraphRunInputScaling(b *testing.B) {
	registry := workflow.NewRegistry().MustRegisterNode("noop", benchmarkNoopNode())
	step, err := registry.CompileGraph(benchmarkGraph("wide", 16))
	if err != nil {
		b.Fatalf("CompileGraph: %v", err)
	}

	for _, cells := range []int{1, 64, 1024} {
		b.Run(strconv.Itoa(cells), func(b *testing.B) {
			input := benchmarkStore(cells)
			// A re-run: the graph owns cells to clear rather than none, which is
			// the path a resumed or repeated graph actually takes.
			for index := range 16 {
				input = input.WithOutput("node-"+strconv.Itoa(index), index)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := step.Run(b.Context(), input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGraphRunBaseScaling varies the input Store's overlay length, named by
// the subtest, while holding the node count fixed. Every node derives its own
// Store from the shared input, so an overlay at the limit is the case where each
// node would flatten the snapshot separately instead of extending a shared one.
// The input owns no cell the graph clears, which is what keeps the overlay at the
// requested length until the fan-out reads it.
func BenchmarkGraphRunBaseScaling(b *testing.B) {
	registry := workflow.NewRegistry().MustRegisterNode("noop", benchmarkNoopNode())
	step, err := registry.CompileGraph(benchmarkGraph("wide", 16))
	if err != nil {
		b.Fatalf("CompileGraph: %v", err)
	}

	for _, overlay := range []int{1, 32, 64} {
		b.Run(strconv.Itoa(overlay), func(b *testing.B) {
			input := benchmarkStore(overlay)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := step.Run(b.Context(), input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkValidateGraphJSONScaling(b *testing.B) {
	for _, size := range []int{16, 128, 1024} {
		data, err := json.Marshal(benchmarkGraph("chain", size))
		if err != nil {
			b.Fatalf("Marshal setup: %v", err)
		}
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := workflow.ValidateGraphJSON(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkJournalDeepTraversal(b *testing.B) {
	for _, depth := range []int{16, 256, workflow.MaxNestingDepth} {
		scope := make([]workflow.ScopeFrame, depth)
		for index := range scope {
			scope[index] = workflow.ScopeFrame{ID: strconv.Itoa(index)}
		}
		journal := workflow.NewJournal()
		if err := journal.Record(
			workflow.JournalKey{ID: "leaf", Scope: scope},
			true,
		); err != nil {
			b.Fatalf("Record setup: %v", err)
		}

		b.Run(strconv.Itoa(depth)+"/keys", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = journal.Keys()
			}
		})
		b.Run(strconv.Itoa(depth)+"/marshal", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := json.Marshal(journal); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkJournalRecordScaling varies the number of records at a realistic
// shallow scope. BenchmarkJournalDeepTraversal varies scope depth with a single
// record, so the per-record cost of crossing the wire boundary — which a checkpoint
// pays for the whole journal every time it is persisted — is only visible here.
func BenchmarkJournalRecordScaling(b *testing.B) {
	for _, records := range []int{64, 1024} {
		journal := workflow.NewJournal()
		for index := range records {
			key := workflow.JournalKey{
				ID:    "step-" + strconv.Itoa(index),
				Scope: []workflow.ScopeFrame{{ID: "loop", Indexed: true, Index: uint64(index % 8)}},
			}
			if err := journal.Record(key, index); err != nil {
				b.Fatalf("Record setup: %v", err)
			}
		}

		b.Run(strconv.Itoa(records)+"/marshal", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := json.Marshal(journal); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(strconv.Itoa(records)+"/keys", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = journal.Keys()
			}
		})
	}
}

func benchmarkIncrement(id, input string) workflow.Step {
	return workflow.Leaf(
		id,
		workflow.Output(input).Bind[int](),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value + 1, nil
		}),
	)
}

// benchmarkNoopNode is the node type the graph benchmarks register. It binds
// nothing and returns immediately so a measurement reports the engine around the
// node rather than the node.
func benchmarkNoopNode() workflow.NodeFactory {
	return func(spec workflow.NodeSpec) (workflow.Step, error) {
		return workflow.Leaf(
			spec.ID,
			workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
				return struct{}{}, nil
			}),
			flow.NodeFunc[struct{}, struct{}](
				func(context.Context, struct{}) (struct{}, error) {
					return struct{}{}, nil
				},
			),
		), nil
	}
}

func benchmarkStore(size int) workflow.Store {
	store := workflow.NewStore()
	for i := range size {
		store = store.WithOutput("base-"+strconv.Itoa(i), i)
	}
	return store
}

func benchmarkGraph(shape string, size int) workflow.Graph {
	nodes := make([]workflow.GraphNode, size)
	for i := range size {
		id := "node-" + strconv.Itoa(i)
		nodes[i] = workflow.GraphNode{ID: id, Type: "noop"}
		if shape == "chain" && i > 0 {
			nodes[i].DependsOn = []string{"node-" + strconv.Itoa(i-1)}
		}
	}
	return workflow.Graph{Nodes: nodes}
}
