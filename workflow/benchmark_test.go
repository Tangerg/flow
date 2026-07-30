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
		return workflow.Leaf(id, workflow.From[int](workflow.Output(input)), node)
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

func BenchmarkStreamLeaf(b *testing.B) {
	ctx := b.Context()
	node := workflow.StreamNodeFunc[int, int, int](
		func(_ context.Context, input int, yield func(int) bool) (int, error) {
			for value := range 4 {
				if !yield(input + value) {
					return 0, context.Canceled
				}
			}
			return input + 4, nil
		},
	)
	step := workflow.StreamLeaf(
		"stream",
		workflow.From[int](workflow.Output("seed")),
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
	node := workflow.Parallel(branches, workflow.ParallelConfig{})

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
			step := workflow.Parallel(branches, workflow.ParallelConfig{})
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
			step := workflow.Parallel(branches, workflow.ParallelConfig{})
			input := benchmarkStore(size)

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
		func(workflow.NodeSpec) (workflow.Step, error) {
			return flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				return store, nil
			}), nil
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
	registry := workflow.NewRegistry().MustRegisterNode(
		"noop",
		func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, struct{}](
					func(context.Context, struct{}) (struct{}, error) {
						return struct{}{}, nil
					},
				),
			), nil
		},
	)

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
		scope := make([]string, depth)
		for index := range scope {
			scope[index] = strconv.Itoa(index)
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

func benchmarkIncrement(id, input string) workflow.Step {
	return workflow.Leaf(
		id,
		workflow.From[int](workflow.Output(input)),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value + 1, nil
		}),
	)
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
