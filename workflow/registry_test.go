package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// addN is a reusable leaf factory that reads its int input and adds config "n".
func addN() workflow.NodeFactory {
	type config struct {
		N int `json:"n"`
	}
	return workflow.Factory(func(cfg config) (flow.Node[int, int], error) {
		return flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + cfg.N, nil }), nil
	})
}

// TestCompileGraph_acceptsAFactoryThatReturnsAnIteration covers the boundary an
// iteration is allowed to be. A node factory may return one -- the definition
// boundary check admits it beside a leaf and a subgraph -- and the graph then has
// to know it produces the output another node reads. Nothing else exercises an
// iteration as a node's own step: everywhere else it is a composite inside a
// definition, where its collected output is found a different way.
func TestCompileGraph_acceptsAFactoryThatReturnsAnIteration(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("double-each", func(spec workflow.NodeSpec) (workflow.Step, error) {
			input, ok := spec.Inputs.Default()
			if !ok {
				return nil, fmt.Errorf("%w %q", workflow.ErrMissingPort, workflow.DefaultPort)
			}
			return workflow.Iteration(workflow.IterationConfig{
				ID:    spec.ID,
				Input: input,
				Body: workflow.LeafFunc(
					"double",
					workflow.Item(spec.ID),
					func(_ context.Context, value int) (int, error) { return value * 2, nil },
				),
				BodyOutput: workflow.Output("double"),
			}), nil
		}).
		MustRegisterNode("count", workflow.Factory(
			func(struct{}) (flow.Node[[]any, int], error) {
				return flow.NodeFunc[[]any, int](
					func(_ context.Context, items []any) (int, error) { return len(items), nil },
				), nil
			},
		))

	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "each", Type: "double-each", Inputs: workflow.OneInput(workflow.Output("seed"))},
		{ID: "size", Type: "count", Inputs: workflow.OneInput(workflow.Output("each"))},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	out, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("seed", []any{1, 2, 3}),
		workflow.RunConfig{},
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := out.Get[[]int](workflow.Output("each")); err != nil ||
		!slices.Equal(got, []int{2, 4, 6}) {
		t.Fatalf("collected = %v, %v; want [2 4 6]", got, err)
	}
	if got, err := out.Get[int](workflow.Output("size")); err != nil || got != 3 {
		t.Fatalf("size = %d, %v; want 3", got, err)
	}
}

// lateBoundNodeSpecFactory intentionally reads NodeSpec's mutable fields at
// execution time. It makes ownership tests prove that compilation hands the
// factory an independent snapshot instead of relying on Factory eagerly
// decoding the same fields.
func lateBoundNodeSpecFactory() workflow.NodeFactory {
	type config struct {
		N int `json:"n"`
	}
	return func(spec workflow.NodeSpec) (workflow.Step, error) {
		bind := workflow.BinderFunc[int](func(store workflow.Store) (int, error) {
			ref, ok := spec.Inputs.Default()
			if !ok {
				return 0, fmt.Errorf("%w %q", workflow.ErrMissingPort, workflow.DefaultPort)
			}
			input, err := store.Get[int](ref)
			if err != nil {
				return 0, err
			}
			var cfg config
			if err := json.Unmarshal(spec.Config, &cfg); err != nil {
				return 0, err
			}
			return input + cfg.N, nil
		})
		return workflow.Leaf(
			spec.ID,
			bind,
			flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
				return value, nil
			}),
		), nil
	}
}

func TestCompileSpec_ownsTheFactoryDefinitionSnapshot(t *testing.T) {
	inputs := workflow.OneInput(workflow.Output("start"))
	config := json.RawMessage(`{"n":1}`)
	spec := workflow.Spec{
		Kind:   workflow.KindLeaf,
		ID:     "node",
		Type:   "late-bound",
		Inputs: inputs,
		Config: config,
	}
	step, err := workflow.NewRegistry().
		MustRegisterNode("late-bound", lateBoundNodeSpecFactory()).
		CompileSpec(spec)
	if err != nil {
		t.Fatalf("CompileSpec: %v", err)
	}

	inputs[workflow.DefaultPort] = workflow.Output("changed-input")
	config[len(config)-2] = '9'
	spec.ID = "changed-node"
	spec.Type = "changed-type"

	out, err := step.Run(
		t.Context(),
		workflow.NewStore().WithOutput("start", 10).WithOutput("changed-input", 100),
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, getErr := out.Get[int](workflow.Output("node")); getErr != nil || value != 11 {
		t.Fatalf("node output = %d, %v; want 11, nil", value, getErr)
	}
	if _, ok := out.Lookup(workflow.Output("changed-node")); ok {
		t.Fatal("compiled Spec retained its caller's definition storage")
	}
}

func TestRegistry_compileSequenceJSON(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())

	spec := `{
	  "kind": "sequence",
	  "steps": [
	    {"kind":"leaf","id":"a","type":"addN","inputs":{"in":{"nodeID":"start","path":"/output"}},"config":{"n":10}},
	    {"kind":"leaf","id":"b","type":"addN","inputs":{"in":{"nodeID":"a","path":"/output"}},"config":{"n":5}}
	  ]
	}`

	step, err := reg.CompileSpecJSON([]byte(spec))
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	out, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("b")); !ok || v.(int) != 16 {
		t.Fatalf("result = %v, %v; want 16", v, ok) // 1 +10 +5
	}
}

func TestCompileGraphCallsFactoriesOutsideTheRegistryLock(t *testing.T) {
	registry := workflow.NewRegistry()
	registry.MustRegisterNode("register-condition", func(spec workflow.NodeSpec) (workflow.Step, error) {
		// Registration from a factory is unusual, but it is a decisive reentrancy
		// probe: calling application code under Registry's lock would deadlock.
		if err := registry.RegisterCondition(
			"registered-during-build",
			flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) { return true, nil }),
		); err != nil {
			return nil, err
		}
		return workflow.Interrupt(spec.ID, nil), nil
	})

	if _, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
		ID:   "node",
		Type: "register-condition",
	}}}); err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	body := workflow.Spec{Kind: workflow.KindSequence}
	if err := registry.ValidateSpec(workflow.Spec{
		Kind:      workflow.KindLoop,
		ID:        "loop",
		Condition: "registered-during-build",
		Body:      &body,
	}); err != nil {
		t.Fatalf("later validation did not observe factory registration: %v", err)
	}
}

func TestCompileSpec_rejectsInvalidBuiltDefinition(t *testing.T) {
	for name, test := range map[string]struct {
		build func(onRun func()) workflow.Step
		want  error
	}{
		"duplicate identity": {
			build: func(onRun func()) workflow.Step {
				leaf := func() workflow.Step {
					return workflow.LeafFunc(
						"duplicate",
						workflow.Output("start"),
						func(_ context.Context, input int) (int, error) {
							onRun()
							return input, nil
						},
					)
				}
				return workflow.Sequence(leaf(), leaf())
			},
			want: workflow.ErrDuplicateStep,
		},
		"nil nested step": {
			build: func(func()) workflow.Step { return workflow.Sequence(nil) },
			want:  workflow.ErrNilStep,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var calls int
			registry := workflow.NewRegistry().MustRegisterNode(
				"invalid",
				func(workflow.NodeSpec) (workflow.Step, error) {
					return test.build(func() { calls++ }), nil
				},
			)

			_, err := registry.CompileSpec(workflow.Spec{
				Kind: workflow.KindLeaf,
				ID:   "outer",
				Type: "invalid",
			})
			if !errors.Is(err, workflow.ErrInvalidSpec) || !errors.Is(err, test.want) {
				t.Fatalf("CompileSpec error = %v; want ErrInvalidSpec and %v", err, test.want)
			}
			if calls != 0 {
				t.Fatalf("node calls = %d; want validation before execution", calls)
			}
		})
	}
}

func TestRegistry_rejectsUnsealedFactoryComposite(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode(
		"composite",
		func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Sequence(workflow.Interrupt(spec.ID, nil)), nil
		},
	)

	t.Run("Spec", func(t *testing.T) {
		_, err := registry.CompileSpec(workflow.Spec{
			Kind: workflow.KindLeaf,
			ID:   "node",
			Type: "composite",
		})
		var specErr *workflow.SpecError
		if !errors.Is(err, workflow.ErrInvalidSpec) ||
			!errors.As(err, &specErr) || specErr.Field != "type" ||
			!strings.Contains(err.Error(), `unsealed "sequence" Step`) {
			t.Fatalf("CompileSpec error = %v; want unsealed composite error", err)
		}
	})

	t.Run("Graph", func(t *testing.T) {
		_, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
			ID:   "node",
			Type: "composite",
		}}})
		var graphErr *workflow.GraphError
		if !errors.Is(err, workflow.ErrInvalidGraph) ||
			!errors.As(err, &graphErr) || graphErr.Path != "/nodes/0" ||
			graphErr.NodeID != "node" || graphErr.Field != "type" ||
			!strings.Contains(err.Error(), `unsealed "sequence" Step`) {
			t.Fatalf("CompileGraph error = %v; want node unsealed composite error", err)
		}
	})
}

func TestRegistry_reportsFactoryContractErrorsAtNodeType(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode(
		"broken",
		workflow.Factory[struct{}, int, int](nil),
	)

	t.Run("Graph", func(t *testing.T) {
		_, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
			ID: "node", Type: "broken",
		}}})
		var graphErr *workflow.GraphError
		if !errors.Is(err, workflow.ErrInvalidGraph) ||
			!errors.Is(err, flow.ErrNilFunc) ||
			!errors.As(err, &graphErr) || graphErr.Path != "/nodes/0" ||
			graphErr.Field != "type" {
			t.Fatalf("CompileGraph error = %v; want nil factory function at type", err)
		}
	})

	t.Run("Spec", func(t *testing.T) {
		_, err := registry.CompileSpec(workflow.Spec{
			Kind: workflow.KindLeaf, ID: "node", Type: "broken",
		})
		var specErr *workflow.SpecError
		if !errors.Is(err, workflow.ErrInvalidSpec) ||
			!errors.Is(err, flow.ErrNilFunc) ||
			!errors.As(err, &specErr) || specErr.Field != "type" {
			t.Fatalf("CompileSpec error = %v; want nil factory function at type", err)
		}
	})
}

func TestRegistry_factorySuspensionIsAnInvalidDefinition(t *testing.T) {
	factories := map[string]workflow.NodeFactory{
		"direct": func(workflow.NodeSpec) (workflow.Step, error) {
			return nil, workflow.Suspend("factory cannot wait")
		},
		"typed adapter": workflow.Factory(
			func(struct{}) (flow.Node[int, int], error) {
				return nil, workflow.Suspend("builder cannot wait")
			},
		),
	}

	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			registry := workflow.NewRegistry().MustRegisterNode("broken", factory)
			inputs := workflow.OneInput(workflow.Output("external"))

			t.Run("Graph", func(t *testing.T) {
				_, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
					ID: "node", Type: "broken", Inputs: inputs,
				}}})
				var graphErr *workflow.GraphError
				if !errors.Is(err, workflow.ErrInvalidGraph) ||
					!errors.Is(err, flow.ErrInvalidConfig) ||
					errors.Is(err, workflow.ErrSuspended) ||
					workflow.SuspendedOnly(err) ||
					len(workflow.Suspensions(err)) != 0 ||
					!errors.As(err, &graphErr) || graphErr.Field != "type" {
					t.Fatalf("CompileGraph error = %v; want non-suspending type error", err)
				}
			})

			t.Run("Spec", func(t *testing.T) {
				_, err := registry.CompileSpec(workflow.Spec{
					Kind: workflow.KindLeaf, ID: "node", Type: "broken", Inputs: inputs,
				})
				var specErr *workflow.SpecError
				if !errors.Is(err, workflow.ErrInvalidSpec) ||
					!errors.Is(err, flow.ErrInvalidConfig) ||
					errors.Is(err, workflow.ErrSuspended) ||
					workflow.SuspendedOnly(err) ||
					len(workflow.Suspensions(err)) != 0 ||
					!errors.As(err, &specErr) || specErr.Field != "type" {
					t.Fatalf("CompileSpec error = %v; want non-suspending type error", err)
				}
			})
		})
	}
}

// A NodeFactory is application code, so its error tree has not crossed a
// workflow depth boundary. Compilation must use the iterative definition-error
// classifier rather than the recursive standard multi-error walk.
func TestRegistry_factoryClassifiesDeepBranchedSuspensionWithoutStackPerWrapper(t *testing.T) {
	withBoundedStack(t, func() {
		factoryErr := workflow.Suspend("factory cannot wait")
		for range 20_000 {
			factoryErr = errorChildren{factoryErr}
		}
		registry := workflow.NewRegistry().MustRegisterNode(
			"broken",
			func(workflow.NodeSpec) (workflow.Step, error) { return nil, factoryErr },
		)

		_, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
			ID: "node", Type: "broken",
		}}})
		var graphErr *workflow.GraphError
		if !errors.Is(err, workflow.ErrInvalidGraph) ||
			!errors.Is(err, flow.ErrInvalidConfig) ||
			errors.Is(err, workflow.ErrSuspended) ||
			!errors.As(err, &graphErr) || graphErr.Field != "type" {
			t.Fatalf("CompileGraph error = %v; want non-suspending type error", err)
		}
	})
}

// Field attribution also inspects an arbitrary application error tree. A
// factory category below deeply nested joins must retain its field without
// making standard recursive matching part of compilation's stack usage.
func TestRegistry_factoryClassifiesDeepBranchedCategoryWithoutStackPerWrapper(t *testing.T) {
	withBoundedStack(t, func() {
		factoryErr := flow.ErrNilFunc
		for range 20_000 {
			factoryErr = errorChildren{factoryErr}
		}
		registry := workflow.NewRegistry().MustRegisterNode(
			"broken",
			func(workflow.NodeSpec) (workflow.Step, error) { return nil, factoryErr },
		)

		_, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
			ID: "node", Type: "broken",
		}}})
		var graphErr *workflow.GraphError
		if !errors.As(err, &graphErr) || graphErr.Field != "type" {
			t.Fatalf("CompileGraph error field = %q; want type", graphErr.Field)
		}
	})
}

func TestRegistry_factoryErrorFieldPriorityIsStableAcrossJoinedCauses(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		field string
	}{
		{
			name:  "input outranks nil construction",
			err:   errors.Join(flow.ErrNilNode, workflow.ErrMissingPort),
			field: "inputs",
		},
		{
			name:  "registration outranks input",
			err:   errors.Join(workflow.ErrMissingPort, workflow.ErrInvalidRegistration),
			field: "type",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := workflow.NewRegistry().MustRegisterNode(
				"broken",
				func(workflow.NodeSpec) (workflow.Step, error) { return nil, test.err },
			)
			_, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
				ID: "node", Type: "broken",
			}}})
			var graphErr *workflow.GraphError
			if !errors.As(err, &graphErr) || graphErr.Field != test.field {
				t.Fatalf("CompileGraph error field = %q; want %q", graphErr.Field, test.field)
			}
		})
	}
}

func TestRegistry_preservesLeafValidationOperationAcrossCompileBoundaries(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode(
		"broken",
		func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
				flow.NodeFunc[int, int](nil),
			), nil
		},
	)
	assertCause := func(t *testing.T, err, outer error) {
		t.Helper()
		var stepErr *workflow.StepError
		if !errors.Is(err, outer) ||
			!errors.Is(err, flow.ErrNilNode) ||
			!errors.As(err, &stepErr) ||
			stepErr.Op != workflow.OpValidate {
			t.Fatalf(
				"Compile error = %v; want outer boundary containing OpValidate ErrNilNode",
				err,
			)
		}
	}

	t.Run("graph", func(t *testing.T) {
		_, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
			ID: "node", Type: "broken",
		}}})
		assertCause(t, err, workflow.ErrInvalidGraph)
		var graphErr *workflow.GraphError
		if !errors.As(err, &graphErr) || graphErr.Field != "type" {
			t.Fatalf("CompileGraph error = %v; want type GraphError", err)
		}
	})

	t.Run("spec", func(t *testing.T) {
		_, err := registry.CompileSpec(workflow.Spec{
			Kind: workflow.KindLeaf, ID: "node", Type: "broken",
		})
		assertCause(t, err, workflow.ErrInvalidSpec)
		var specErr *workflow.SpecError
		if !errors.As(err, &specErr) || specErr.Field != "type" {
			t.Fatalf("CompileSpec error = %v; want type SpecError", err)
		}
	})
}

func TestRegistry_rejectsSchemaOutputMismatchAtCompilation(t *testing.T) {
	tests := map[string]struct {
		factory workflow.NodeFactory
		schema  workflow.NodeSchema
		inputs  workflow.Inputs
	}{
		"schema promises absent output": {
			factory: workflow.AwaitFactory(),
			schema: workflow.NodeSchema{
				Inputs: workflow.OnePort(workflow.TypeAny),
				Output: workflow.TypeAny,
			},
			inputs: workflow.OneInput(workflow.Output("external")),
		},
		"schema hides produced output": {
			factory: workflow.InterruptFactory(),
			schema:  workflow.NodeSchema{},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			registry := workflow.NewRegistry().
				MustRegisterNode("node", test.factory).
				MustRegisterSchema("node", test.schema)

			t.Run("Graph", func(t *testing.T) {
				_, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
					ID: "node", Type: "node", Inputs: test.inputs,
				}}})
				var registrationErr *workflow.RegistrationError
				if !errors.Is(err, workflow.ErrInvalidGraph) ||
					!errors.Is(err, workflow.ErrInvalidRegistration) ||
					!errors.As(err, &registrationErr) ||
					registrationErr.Kind != "schema" || registrationErr.Name != "node" {
					t.Fatalf("CompileGraph error = %v; want graph and registration errors", err)
				}
				// The envelope names which registration disagreed; the cause says how.
				// Both sentinels above are the envelope's own, so a lost cause reads as
				// the same failure with no reason in it.
				if registrationErr.Err == nil ||
					!strings.Contains(err.Error(), registrationErr.Err.Error()) {
					t.Fatalf("RegistrationError = %+v; want it to carry and render the mismatch", registrationErr)
				}
			})

			t.Run("Spec", func(t *testing.T) {
				_, err := registry.CompileSpec(workflow.Spec{
					Kind: workflow.KindLeaf, ID: "node", Type: "node", Inputs: test.inputs,
				})
				if !errors.Is(err, workflow.ErrInvalidSpec) ||
					!errors.Is(err, workflow.ErrInvalidRegistration) {
					t.Fatalf("CompileSpec error = %v; want spec and registration errors", err)
				}
			})
		})
	}
}

func TestCompileBoundaries_countFactoryNestingInsideTheEnclosingDefinition(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode(
		"deep",
		func(spec workflow.NodeSpec) (workflow.Step, error) {
			body := workflow.Interrupt("value", nil)
			bodyOutput := workflow.Output("value")
			for depth := 1; depth < workflow.MaxNestingDepth; depth++ {
				id := fmt.Sprintf("inner-%d", depth)
				body = workflow.Subgraph(workflow.SubgraphConfig{
					ID:         id,
					Body:       body,
					BodyOutput: bodyOutput,
				})
				bodyOutput = workflow.Output(id)
			}
			return workflow.Subgraph(workflow.SubgraphConfig{
				ID:         spec.ID,
				Body:       body,
				BodyOutput: bodyOutput,
			}), nil
		},
	)

	t.Run("Spec", func(t *testing.T) {
		_, err := registry.CompileSpec(workflow.Spec{
			Kind: workflow.KindSequence,
			Steps: []workflow.Spec{{
				Kind: workflow.KindLeaf,
				ID:   "node",
				Type: "deep",
			}},
		})
		if !errors.Is(err, workflow.ErrInvalidSpec) || !errors.Is(err, workflow.ErrMaxDepth) {
			t.Fatalf("CompileSpec error = %v; want enclosing ErrMaxDepth", err)
		}
	})

	t.Run("Graph", func(t *testing.T) {
		_, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
			ID:   "node",
			Type: "deep",
		}}})
		if !errors.Is(err, workflow.ErrInvalidGraph) || !errors.Is(err, workflow.ErrMaxDepth) {
			t.Fatalf("CompileGraph error = %v; want enclosing ErrMaxDepth", err)
		}
	})
}

func TestRegistry_compileBranch(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterResolver("sign", resolverNode(func(_ context.Context, s workflow.Store) (string, error) {
			v, _ := s.Lookup(workflow.At("start", "output"))
			if v.(int) >= 0 {
				return "pos", nil
			}
			return "neg", nil
		}))

	spec := workflow.Spec{
		Kind:     workflow.KindBranch,
		ID:       "route",
		Resolver: "sign",
		Cases: map[string]workflow.Spec{
			"pos": {
				Kind: workflow.KindLeaf, ID: "p", Type: "addN",
				Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
				Config: json.RawMessage(`{"n":100}`),
			},
			"neg": {
				Kind: workflow.KindLeaf, ID: "n", Type: "addN",
				Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
				Config: json.RawMessage(`{"n":-100}`),
			},
		},
	}

	step, err := reg.CompileSpec(spec)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	out, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", 5))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("p")); !ok || v.(int) != 105 {
		t.Fatalf("pos branch = %v, %v; want 105", v, ok)
	}
}

func TestRegistry_compileIteration(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())

	spec := workflow.Spec{
		Kind:       workflow.KindIteration,
		ID:         "iter",
		Input:      workflow.Output("start"),
		BodyOutput: workflow.Output("el"),
		Body: &workflow.Spec{
			Kind: workflow.KindLeaf, ID: "el", Type: "addN",
			Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Item("iter")},
			Config: json.RawMessage(`{"n":1}`),
		},
	}

	step, err := reg.CompileSpec(spec)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	out, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", []any{1, 2, 3}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out
	raw, ok := got.Lookup(workflow.Output("iter"))
	if !ok {
		t.Fatal("iteration output missing")
	}
	res := raw.([]any)
	want := []int{2, 3, 4}
	for i := range want {
		if res[i].(int) != want[i] {
			t.Fatalf("res[%d] = %v, want %d", i, res[i], want[i])
		}
	}
}

func TestRegistry_unknownType(t *testing.T) {
	reg := workflow.NewRegistry()
	_, err := reg.CompileSpec(workflow.Spec{Kind: workflow.KindLeaf, Type: "nope"})
	if err == nil {
		t.Fatal("expected error for unknown leaf type")
	}
}

func TestRegistry_unknownKind(t *testing.T) {
	reg := workflow.NewRegistry()
	_, err := reg.CompileSpec(workflow.Spec{
		Kind:          "bogus",
		Concurrency:   -1,
		MaxIterations: -1,
	})
	var specErr *workflow.SpecError
	if !errors.As(err, &specErr) ||
		specErr.Field != "kind" ||
		!errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("CompileSpec error = %v; want invalid kind diagnostic", err)
	}
}

func TestValidateSpec_reportsEmptyRequiredRegistrationNames(t *testing.T) {
	registry := workflow.NewRegistry()
	tests := []struct {
		name  string
		spec  workflow.Spec
		field string
		want  string
	}{
		{
			name: "branch resolver",
			spec: workflow.Spec{
				Kind:  workflow.KindBranch,
				ID:    "branch",
				Cases: map[string]workflow.Spec{"case": {Kind: workflow.KindSequence}},
			},
			field: "resolver",
			want:  "resolver name is empty",
		},
		{
			name: "loop condition",
			spec: workflow.Spec{
				Kind: workflow.KindLoop,
				ID:   "loop",
				Body: &workflow.Spec{Kind: workflow.KindSequence},
			},
			field: "condition",
			want:  "condition name is empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := registry.ValidateSpec(test.spec)
			var specErr *workflow.SpecError
			if !errors.As(err, &specErr) ||
				specErr.Field != test.field ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateSpec error = %v; want field %q containing %q", err, test.field, test.want)
			}
		})
	}
}

func TestRegistry_reportsInvalidAndDuplicateRegistrations(t *testing.T) {
	factory := addN()
	reg := workflow.NewRegistry()
	invalid := string([]byte{0xff})
	for name, f := range map[string]workflow.NodeFactory{"": factory, invalid: factory, "nil": nil} {
		if err := reg.RegisterNode(name, f); err == nil {
			t.Fatalf("RegisterNode(%q) unexpectedly succeeded", name)
		}
	}
	// A rejected registration says which concept the name belongs to, and a node
	// registers under a node type rather than under a bare name. Only the empty
	// case can say so: the message for an invalid name is about its bytes.
	if err := reg.RegisterNode("", factory); !strings.Contains(err.Error(), "node type is empty") {
		t.Fatalf("RegisterNode(\"\") error = %v; want it to name the node type", err)
	}
	if err := reg.RegisterNode("addN", factory); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := reg.RegisterNode("addN", factory); err == nil {
		t.Fatal("duplicate registration unexpectedly succeeded")
	}
}

func TestRegistry_reportsInvalidResolverAndConditionRegistrations(t *testing.T) {
	resolver := resolverNode(func(context.Context, workflow.Store) (string, error) {
		return "", nil
	})
	var nilResolver flow.NodeFunc[workflow.Store, string]
	invalidResolver := flow.Then(
		resolver,
		flow.NodeFunc[string, string](nil),
	)
	condition := workflow.Condition(flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) {
		return false, nil
	}))

	invalid := string([]byte{0xff})
	for name, register := range map[string]func(*workflow.Registry) error{
		"resolver empty name": func(reg *workflow.Registry) error {
			return reg.RegisterResolver("", resolver)
		},
		"resolver nil": func(reg *workflow.Registry) error {
			return reg.RegisterResolver("resolver", nil)
		},
		"resolver typed nil": func(reg *workflow.Registry) error {
			return reg.RegisterResolver("resolver", nilResolver)
		},
		"resolver invalid composite": func(reg *workflow.Registry) error {
			return reg.RegisterResolver("resolver", invalidResolver)
		},
		"resolver non-UTF-8 name": func(reg *workflow.Registry) error {
			return reg.RegisterResolver(invalid, resolver)
		},
		"resolver duplicate": func(reg *workflow.Registry) error {
			if err := reg.RegisterResolver("resolver", resolver); err != nil {
				return err
			}
			return reg.RegisterResolver("resolver", resolver)
		},
		"condition empty name": func(reg *workflow.Registry) error {
			return reg.RegisterCondition("", condition)
		},
		"condition nil": func(reg *workflow.Registry) error {
			return reg.RegisterCondition("condition", nil)
		},
		"condition non-UTF-8 name": func(reg *workflow.Registry) error {
			return reg.RegisterCondition(invalid, condition)
		},
		"condition duplicate": func(reg *workflow.Registry) error {
			if err := reg.RegisterCondition("condition", condition); err != nil {
				return err
			}
			return reg.RegisterCondition("condition", condition)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := register(workflow.NewRegistry()); err == nil {
				t.Fatal("registration unexpectedly succeeded")
			}
		})
	}
	err := workflow.NewRegistry().RegisterResolver("resolver", nilResolver)
	if !errors.Is(err, workflow.ErrInvalidRegistration) ||
		!errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("typed nil resolver error = %v; want registration and nil-node categories", err)
	}
	err = workflow.NewRegistry().RegisterResolver("resolver", invalidResolver)
	if !errors.Is(err, workflow.ErrInvalidRegistration) ||
		!errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("composed resolver error = %v; want registration and nil-node categories", err)
	}
	// A Condition is the same node shape as a Resolver, so it reports the same
	// category for an absent implementation.
	err = workflow.NewRegistry().RegisterCondition("condition", nil)
	if !errors.Is(err, workflow.ErrInvalidRegistration) ||
		!errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil condition error = %v; want registration and nil-node categories", err)
	}
	err = workflow.NewRegistry().RegisterCondition(
		"condition",
		flow.NodeFunc[workflow.Store, bool](nil),
	)
	if !errors.Is(err, workflow.ErrInvalidRegistration) ||
		!errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("typed nil condition error = %v; want registration and nil-node categories", err)
	}
}

// NodeFactory is the only registered kind that is a bare function rather than a
// node, so it is the only one whose absence reports flow.ErrNilFunc.
func TestRegistry_preservesNilFunctionRegistrationCauses(t *testing.T) {
	err := workflow.NewRegistry().RegisterNode("node", nil)
	if !errors.Is(err, workflow.ErrInvalidRegistration) ||
		!errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("registration error = %v; want registration and nil-function categories", err)
	}
}

func TestValidateSpec_rejectsNonUTF8DefinitionIdentity(t *testing.T) {
	invalid := string([]byte{0xff})
	registry := workflow.NewRegistry().
		MustRegisterNode("node", addN()).
		MustRegisterResolver("pick", resolverNode(func(context.Context, workflow.Store) (string, error) {
			return "case", nil
		})).
		MustRegisterCondition("done", flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) {
			return true, nil
		}))
	body := workflow.Spec{Kind: workflow.KindSequence}
	tests := map[string]struct {
		spec  workflow.Spec
		field string
		want  error
	}{
		"step ID": {
			spec:  workflow.Spec{Kind: workflow.KindLeaf, ID: invalid, Type: "node"},
			field: "id",
			want:  workflow.ErrInvalidStepID,
		},
		"node type": {
			spec:  workflow.Spec{Kind: workflow.KindLeaf, ID: "node", Type: invalid},
			field: "type",
		},
		"input port": {
			spec: workflow.Spec{
				Kind: workflow.KindLeaf, ID: "node", Type: "node",
				Inputs: workflow.Inputs{invalid: workflow.Output("seed")},
			},
			field: "inputs",
		},
		"resolver": {
			spec: workflow.Spec{
				Kind: workflow.KindBranch, ID: "branch", Resolver: invalid,
				Cases: map[string]workflow.Spec{"case": body},
			},
			field: "resolver",
		},
		"case": {
			spec: workflow.Spec{
				Kind: workflow.KindBranch, ID: "branch", Resolver: "pick",
				Cases: map[string]workflow.Spec{invalid: body},
			},
			field: "cases",
		},
		"condition": {
			spec: workflow.Spec{
				Kind: workflow.KindLoop, ID: "loop", Condition: invalid, Body: &body,
			},
			field: "condition",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := registry.ValidateSpec(test.spec)
			var specErr *workflow.SpecError
			if !errors.Is(err, workflow.ErrInvalidSpec) ||
				(test.want != nil && !errors.Is(err, test.want)) ||
				!errors.As(err, &specErr) || specErr.Field != test.field ||
				!strings.Contains(err.Error(), "not valid UTF-8") {
				t.Fatalf("ValidateSpec error = %v; want field %q UTF-8 error", err, test.field)
			}
		})
	}
}

func TestRegistry_mustRegisterPanics(t *testing.T) {
	tests := map[string]func(){
		"leaf": func() {
			workflow.NewRegistry().MustRegisterNode("", addN())
		},
		"resolver": func() {
			workflow.NewRegistry().MustRegisterResolver("", resolverNode(func(context.Context, workflow.Store) (string, error) {
				return "", nil
			}))
		},
		"condition": func() {
			workflow.NewRegistry().MustRegisterCondition("", flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) {
				return false, nil
			}))
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("MustRegister%s did not panic", name)
				}
			}()
			run()
		})
	}
}

func TestRegistry_rejectsDuplicateIDsInNestedSpec(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	spec := workflow.Spec{Kind: workflow.KindParallel, Steps: []workflow.Spec{
		{Kind: workflow.KindLeaf, ID: "same", Type: "addN"},
		{Kind: workflow.KindLeaf, ID: "same", Type: "addN"},
	}}
	if _, err := reg.CompileSpec(spec); err == nil {
		t.Fatal("expected duplicate step ID error")
	}
}

func TestRegistry_rejectsNegativeConcurrency(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	spec := workflow.Spec{Kind: workflow.KindParallel, Concurrency: -1}
	if _, err := reg.CompileSpec(spec); !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("CompileSpec error = %v; want ErrInvalidConfig", err)
	}
	// Validation rejects it too, and says which field: the step this spec would
	// build refuses the same value on its own, so compiling is not what asks. A
	// caller validating a document before storing it needs the answer without one.
	var specErr *workflow.SpecError
	if err := reg.ValidateSpec(spec); !errors.Is(err, flow.ErrInvalidConfig) ||
		!errors.As(err, &specErr) || specErr.Field != "concurrency" {
		t.Fatalf("ValidateSpec error = %v; want ErrInvalidConfig at concurrency", err)
	}
}

func TestValidateSpec_rejectsFieldsItsKindWouldIgnore(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	tests := map[string]struct {
		spec  workflow.Spec
		field string
	}{
		"sequence type": {
			spec:  workflow.Spec{Kind: workflow.KindSequence, Type: "addN"},
			field: "type",
		},
		"leaf concurrency": {
			spec:  workflow.Spec{Kind: workflow.KindLeaf, ID: "a", Type: "addN", Concurrency: 1},
			field: "concurrency",
		},
		"parallel condition": {
			spec:  workflow.Spec{Kind: workflow.KindParallel, Condition: "ignored"},
			field: "condition",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := reg.ValidateSpec(tt.spec)
			var specErr *workflow.SpecError
			if !errors.As(err, &specErr) || !errors.Is(err, workflow.ErrInvalidSpec) || specErr.Field != tt.field {
				t.Fatalf("err = %v; want invalid field %q", err, tt.field)
			}
		})
	}
}

func TestValidateSpec_rejectsMalformedConfigWithoutANodeSchema(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	for name, config := range map[string]json.RawMessage{
		"truncated":  json.RawMessage(`{"n":`),
		"whitespace": json.RawMessage(" \n\t"),
	} {
		t.Run(name, func(t *testing.T) {
			spec := workflow.Spec{
				Kind:   workflow.KindLeaf,
				ID:     "a",
				Type:   "addN",
				Config: config,
			}
			if err := reg.ValidateSpec(spec); !errors.Is(err, workflow.ErrInvalidSpec) {
				t.Fatalf("err = %v; want ErrInvalidSpec", err)
			}
		})
	}
}

func TestValidateSpec_reportsTheNestedSpecPath(t *testing.T) {
	const caseName = "a/b~c"
	registry := workflow.NewRegistry().MustRegisterResolver(
		"pick",
		resolverNode(func(context.Context, workflow.Store) (string, error) { return caseName, nil }),
	)
	spec := workflow.Spec{Kind: workflow.KindSequence, Steps: []workflow.Spec{{
		Kind:     workflow.KindBranch,
		ID:       "route",
		Resolver: "pick",
		Cases: map[string]workflow.Spec{
			caseName: {
				Kind: workflow.KindSequence,
				Steps: []workflow.Spec{{
					Kind: workflow.KindLeaf,
					ID:   "missing",
					Type: "unknown",
				}},
			},
		},
	}}}

	err := registry.ValidateSpec(spec)
	var specErr *workflow.SpecError
	if !errors.Is(err, workflow.ErrInvalidSpec) ||
		!errors.Is(err, workflow.ErrUnknownNodeType) ||
		!errors.As(err, &specErr) ||
		specErr.Path != "/steps/0/cases/a~1b~0c/steps/0" || specErr.Field != "type" {
		t.Fatalf("ValidateSpec error = %v; want escaped nested path and type field", err)
	}
}

func TestCompileSpec_reportsThePathOfAFactoryContractError(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode(
		"opaque",
		func(workflow.NodeSpec) (workflow.Step, error) {
			return flow.NodeFunc[workflow.Store, workflow.Store](
				func(_ context.Context, store workflow.Store) (workflow.Store, error) {
					return store, nil
				},
			), nil
		},
	)
	spec := workflow.Spec{Kind: workflow.KindSequence, Steps: []workflow.Spec{{
		Kind: workflow.KindLeaf,
		ID:   "node",
		Type: "opaque",
	}}}

	_, err := registry.CompileSpec(spec)
	var specErr *workflow.SpecError
	if !errors.Is(err, workflow.ErrInvalidSpec) ||
		!errors.As(err, &specErr) ||
		specErr.Path != "/steps/0" || specErr.Field != "type" {
		t.Fatalf("CompileSpec error = %v; want /steps/0 type factory error", err)
	}
}

func TestSpecValidation_reportsImpossibleBodyOutputAtItsField(t *testing.T) {
	registry := workflow.NewRegistry()
	tests := map[string]workflow.Spec{
		"iteration": {
			Kind:       workflow.KindIteration,
			ID:         "each",
			Input:      workflow.Output("items"),
			Body:       &workflow.Spec{Kind: workflow.KindSequence},
			BodyOutput: workflow.Output("missing"),
		},
		"iteration index child": {
			Kind:       workflow.KindIteration,
			ID:         "each",
			Input:      workflow.Output("items"),
			Body:       &workflow.Spec{Kind: workflow.KindSequence},
			BodyOutput: workflow.ItemIndex("each").Child("value"),
		},
		"subgraph": {
			Kind:       workflow.KindSubgraph,
			ID:         "sub",
			Body:       &workflow.Spec{Kind: workflow.KindSequence},
			BodyOutput: workflow.Output("missing"),
		},
	}

	for name, child := range tests {
		t.Run(name, func(t *testing.T) {
			definition := workflow.Spec{
				Kind:  workflow.KindSequence,
				Steps: []workflow.Spec{child},
			}
			checks := map[string]func() error{
				"validate": func() error { return registry.ValidateSpec(definition) },
				"compile": func() error {
					_, err := registry.CompileSpec(definition)
					return err
				},
			}
			for operation, check := range checks {
				t.Run(operation, func(t *testing.T) {
					err := check()
					var specErr *workflow.SpecError
					if !errors.Is(err, workflow.ErrInvalidSpec) ||
						!errors.Is(err, flow.ErrInvalidConfig) ||
						!errors.As(err, &specErr) ||
						specErr.Path != "/steps/0" ||
						specErr.Field != "bodyOutput" {
						t.Fatalf("%s error = %v; want /steps/0 bodyOutput error", operation, err)
					}
				})
			}
		})
	}
}

func TestValidateSpec_doesNotGuessAnUnschematizedLeafOutput(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode(
		"wait",
		func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Await(spec.ID, workflow.Output("seed")), nil
		},
	)
	body := workflow.Spec{
		Kind: workflow.KindLeaf,
		ID:   "inner",
		Type: "wait",
	}
	definitions := map[string]workflow.Spec{
		"iteration": {
			Kind:       workflow.KindIteration,
			ID:         "each",
			Input:      workflow.Output("items"),
			Body:       &body,
			BodyOutput: workflow.Output("inner"),
		},
		"subgraph": {
			Kind:       workflow.KindSubgraph,
			ID:         "sub",
			Body:       &body,
			BodyOutput: workflow.Output("inner"),
		},
	}

	for name, definition := range definitions {
		t.Run(name, func(t *testing.T) {
			if err := registry.ValidateSpec(definition); err != nil {
				t.Fatalf("ValidateSpec guessed an output contract without a schema: %v", err)
			}
			_, err := registry.CompileSpec(definition)
			var specErr *workflow.SpecError
			if !errors.Is(err, workflow.ErrInvalidSpec) ||
				!errors.Is(err, flow.ErrInvalidConfig) ||
				!errors.As(err, &specErr) ||
				specErr.Field != "bodyOutput" {
				t.Fatalf("CompileSpec error = %v; want concrete bodyOutput error", err)
			}
		})
	}
}

func TestSpec_enforcesProgrammaticNestingLimit(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode(
		"interrupt",
		workflow.InterruptFactory(),
	)
	nested := workflow.Spec{Kind: workflow.KindLeaf, ID: "leaf", Type: "interrupt"}
	for range workflow.MaxNestingDepth {
		nested = workflow.Spec{Kind: workflow.KindSequence, Steps: []workflow.Spec{nested}}
	}

	if err := registry.ValidateSpec(nested); err != nil {
		t.Fatalf("ValidateSpec at nesting limit: %v", err)
	}
	if _, err := registry.CompileSpec(nested); err != nil {
		t.Fatalf("CompileSpec at nesting limit: %v", err)
	}

	// The bound belongs to the Spec, not to whichever walk reached it: validating,
	// compiling, and encoding descend the same tree, so all three must stop at the
	// same depth and say so the same way.
	tooDeep := workflow.Spec{Kind: workflow.KindSequence, Steps: []workflow.Spec{nested}}
	reached := fmt.Sprintf(
		"nesting depth %d exceeds limit %d",
		workflow.MaxNestingDepth+1,
		workflow.MaxNestingDepth,
	)
	for name, check := range map[string]func() error{
		"validate": func() error { return registry.ValidateSpec(tooDeep) },
		"compile": func() error {
			_, err := registry.CompileSpec(tooDeep)
			return err
		},
		"marshal": func() error {
			_, err := json.Marshal(tooDeep)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := check()
			var specErr *workflow.SpecError
			if !errors.Is(err, workflow.ErrInvalidSpec) ||
				!errors.Is(err, workflow.ErrMaxDepth) ||
				!errors.As(err, &specErr) || specErr.Field != "" {
				t.Fatalf("error = %v; want whole-spec ErrInvalidSpec and ErrMaxDepth", err)
			}
			if !strings.Contains(err.Error(), reached) {
				t.Fatalf("error = %v; want the depth it stopped at, %q", err, reached)
			}
		})
	}
}

// TestSpec_boundsNestingNotBreadth holds that same bound to nesting alone. The
// test above nests a single chain, where every step encountered is also an
// enclosing one, so it cannot tell a depth apart from a count. A sequence of more
// steps than the limit allows is one level deep and says which was measured: a
// walk that raised a depth per child and put it back on the way out would reject
// this flat workflow the moment one return path forgot to.
func TestSpec_boundsNestingNotBreadth(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode(
		"interrupt",
		workflow.InterruptFactory(),
	)
	wide := workflow.Spec{Kind: workflow.KindSequence}
	for index := range workflow.MaxNestingDepth + 2 {
		wide.Steps = append(wide.Steps, workflow.Spec{
			Kind: workflow.KindLeaf,
			ID:   fmt.Sprintf("leaf%d", index),
			Type: "interrupt",
		})
	}

	if err := registry.ValidateSpec(wide); err != nil {
		t.Fatalf("ValidateSpec of a wide spec: %v", err)
	}
	encoded, err := json.Marshal(wide)
	if err != nil {
		t.Fatalf("Marshal of a wide spec: %v", err)
	}
	if steps := strings.Count(string(encoded), `"kind":"leaf"`); steps != len(wide.Steps) {
		t.Fatalf("encoded %d leaf steps; want all %d", steps, len(wide.Steps))
	}
}

func TestValidateSpec_rejectsEveryStructuralBoundary(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterResolver("pick", resolverNode(func(context.Context, workflow.Store) (string, error) {
			return "case", nil
		})).
		MustRegisterCondition("done", flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) {
			return true, nil
		}))
	leaf := func(id string) workflow.Spec {
		return workflow.Spec{Kind: workflow.KindLeaf, ID: id, Type: "addN"}
	}
	body := func() *workflow.Spec {
		value := workflow.Spec{Kind: workflow.KindSequence}
		return &value
	}
	tests := map[string]workflow.Spec{
		"negative max iterations": {
			Kind: workflow.KindLoop, ID: "loop", Body: body(),
			Condition: "done", MaxIterations: -1,
		},
		"duplicate loop ID": {
			Kind: workflow.KindSequence,
			Steps: []workflow.Spec{
				leaf("same"),
				{Kind: workflow.KindLoop, ID: "same", Body: body(), Condition: "done"},
			},
		},
		"missing loop body": {
			Kind: workflow.KindLoop, ID: "loop", Condition: "done",
		},
		"unknown loop condition": {
			Kind: workflow.KindLoop, ID: "loop", Body: body(), Condition: "missing",
		},
		"empty leaf type": {
			Kind: workflow.KindLeaf, ID: "leaf",
		},
		"unknown leaf type": {
			Kind: workflow.KindLeaf, ID: "leaf", Type: "missing",
		},
		"leaf input field": {
			Kind: workflow.KindLeaf, ID: "leaf", Type: "addN",
			Input: workflow.Output("a"),
		},
		"invalid leaf input": {
			Kind: workflow.KindLeaf, ID: "leaf", Type: "addN",
			Inputs: workflow.Inputs{
				workflow.DefaultPort: {NodeID: "source", Path: "/~2"},
			},
		},
		"duplicate branch ID": {
			Kind: workflow.KindSequence,
			Steps: []workflow.Spec{
				leaf("same"),
				{
					Kind: workflow.KindBranch, ID: "same", Resolver: "pick",
					Cases: map[string]workflow.Spec{"case": {Kind: workflow.KindSequence}},
				},
			},
		},
		"missing branch cases": {
			Kind: workflow.KindBranch, ID: "branch", Resolver: "pick",
		},
		"unknown branch resolver": {
			Kind: workflow.KindBranch, ID: "branch", Resolver: "missing",
			Cases: map[string]workflow.Spec{"case": {Kind: workflow.KindSequence}},
		},
		"empty branch case name": {
			Kind: workflow.KindBranch, ID: "branch", Resolver: "pick",
			Cases: map[string]workflow.Spec{"": {Kind: workflow.KindSequence}},
		},
		"duplicate iteration ID": {
			Kind: workflow.KindSequence,
			Steps: []workflow.Spec{
				leaf("same"),
				{
					Kind: workflow.KindIteration, ID: "same",
					Input: workflow.Output("items"), Body: body(),
					BodyOutput: workflow.Output("value"),
				},
			},
		},
		"missing iteration input": {
			Kind: workflow.KindIteration, ID: "each", Body: body(),
			BodyOutput: workflow.Output("value"),
		},
		"missing iteration body": {
			Kind: workflow.KindIteration, ID: "each",
			Input:      workflow.Output("items"),
			BodyOutput: workflow.Output("value"),
		},
		"missing iteration body output": {
			Kind: workflow.KindIteration, ID: "each",
			Input: workflow.Output("items"), Body: body(),
		},
		"invalid iteration body": {
			Kind: workflow.KindIteration, ID: "each",
			Input: workflow.Output("items"), Body: &workflow.Spec{},
			BodyOutput: workflow.Output("value"),
		},
		"invalid iteration input": {
			Kind: workflow.KindIteration, ID: "each",
			Input: workflow.Ref{NodeID: "items", Path: "/~2"}, Body: body(),
			BodyOutput: workflow.Output("value"),
		},
		"invalid iteration body output": {
			Kind: workflow.KindIteration, ID: "each",
			Input: workflow.Output("items"), Body: body(),
			BodyOutput: workflow.Ref{NodeID: "value", Path: "/~2"},
		},
		"duplicate subgraph ID": {
			Kind: workflow.KindSequence,
			Steps: []workflow.Spec{
				leaf("same"),
				{
					Kind: workflow.KindSubgraph, ID: "same",
					Body: body(), BodyOutput: workflow.Output("value"),
				},
			},
		},
		"missing subgraph body": {
			Kind: workflow.KindSubgraph, ID: "sub",
			BodyOutput: workflow.Output("value"),
		},
		"missing subgraph body output": {
			Kind: workflow.KindSubgraph, ID: "sub", Body: body(),
		},
		"invalid subgraph body": {
			Kind: workflow.KindSubgraph, ID: "sub", Body: &workflow.Spec{},
			BodyOutput: workflow.Output("value"),
		},
		"invalid subgraph input": {
			Kind: workflow.KindSubgraph, ID: "sub",
			Inputs: workflow.Inputs{"value": {NodeID: "seed", Path: "/~2"}},
			Body:   body(), BodyOutput: workflow.Output("value"),
		},
		"invalid subgraph body output": {
			Kind: workflow.KindSubgraph, ID: "sub",
			Body: body(), BodyOutput: workflow.Ref{NodeID: "value", Path: "/~2"},
		},
	}

	for name, spec := range tests {
		t.Run(name, func(t *testing.T) {
			if err := reg.ValidateSpec(spec); err == nil {
				t.Fatal("ValidateSpec unexpectedly succeeded")
			}
		})
	}
}

func TestValidateSpec_requiresDeclaredLeafPorts(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterSchema("addN", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeNumber),
			Output: workflow.TypeNumber,
		})
	err := reg.ValidateSpec(workflow.Spec{
		Kind: workflow.KindLeaf,
		ID:   "leaf",
		Type: "addN",
	})
	if !errors.Is(err, workflow.ErrMissingPort) {
		t.Fatalf("error = %v; want ErrMissingPort", err)
	}
}

func TestValidateSpec_registeredZeroInputSchemaRejectsWiring(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("source", addN()).
		MustRegisterSchema("source", workflow.NodeSchema{Output: workflow.TypeNumber})
	err := registry.ValidateSpec(workflow.Spec{
		Kind:   workflow.KindLeaf,
		ID:     "source",
		Type:   "source",
		Inputs: workflow.OneInput(workflow.Output("external")),
	})
	if !errors.Is(err, workflow.ErrUnknownPort) {
		t.Fatalf("ValidateSpec error = %v; want ErrUnknownPort", err)
	}
}

func TestValidateSpec_iterationBodyIDsAreLocalToEachElement(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	spec := workflow.Spec{
		Kind: workflow.KindSequence,
		Steps: []workflow.Spec{
			{
				Kind: workflow.KindLeaf, ID: "value", Type: "addN",
				Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("seed")},
			},
			{
				Kind:       workflow.KindIteration,
				ID:         "each",
				Input:      workflow.Output("items"),
				BodyOutput: workflow.Output("value"),
				Body: &workflow.Spec{
					Kind: workflow.KindLeaf, ID: "value", Type: "addN",
					Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Item("each")},
					Config: json.RawMessage(`{"n":1}`),
				},
			},
		},
	}

	step, compileErr := reg.CompileSpec(spec)
	if compileErr != nil {
		t.Fatalf("CompileSpec: %v", compileErr)
	}
	in := workflow.NewStore().
		WithOutput("seed", 10).
		WithOutput("items", []any{1, 2})
	out, compileErr := step.Run(t.Context(), in)
	if compileErr != nil {
		t.Fatalf("Run: %v", compileErr)
	}
	if got, err := out.Get[int](workflow.Output("value")); err != nil || got != 10 {
		t.Fatalf("outer value = %v, %v; want 10", got, err)
	}
	items, compileErr := out.Get[[]int](workflow.Output("each"))
	if compileErr != nil || len(items) != 2 || items[0] != 2 || items[1] != 3 {
		t.Fatalf("iteration output = %v, %v; want [2 3]", items, compileErr)
	}
}

func TestRegistry_concurrentRegistrationIsRaceFree(t *testing.T) {
	reg := workflow.NewRegistry()
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			if err := reg.RegisterNode(fmt.Sprintf("leaf-%d", i), addN()); err != nil {
				t.Errorf("RegisterNode: %v", err)
			}
		})
	}
	wg.Wait()
}

// TestRegistry_registrationDuringCompilationIsRaceFree covers the interleaving the
// two tests around it leave out: one goroutine registers while another compiles.
// A snapshot is what makes that safe -- it copies the tables under the lock, so a
// compile reads entries nobody can still be writing. Reading the live tables
// instead is a concurrent map access, and neither registering nor compiling on its
// own can reach it.
func TestRegistry_registrationDuringCompilationIsRaceFree(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode("add", addN())
	graph := workflow.Graph{Nodes: []workflow.GraphNode{{
		ID:     "add",
		Type:   "add",
		Inputs: workflow.OneInput(workflow.Output("seed")),
		Config: json.RawMessage(`{"n":1}`),
	}}}

	start := make(chan struct{})
	var workers sync.WaitGroup
	for index := range 16 {
		workers.Go(func() {
			<-start
			if err := registry.RegisterNode(fmt.Sprintf("late-%d", index), addN()); err != nil {
				t.Errorf("RegisterNode: %v", err)
			}
		})
		workers.Go(func() {
			<-start
			if _, err := registry.CompileGraph(graph); err != nil {
				t.Errorf("CompileGraph: %v", err)
			}
		})
	}
	close(start)
	workers.Wait()
}

func TestRegistry_concurrentCompilationSharesOnlyImmutableRegistrations(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("add", addN()).
		MustRegisterSchema("add", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeNumber),
			Output: workflow.TypeNumber,
			ConfigSchema: json.RawMessage(`{
				"type":"object",
				"properties":{"n":{"type":"integer"}},
				"required":["n"],
				"additionalProperties":false
			}`),
		})
	graph := workflow.Graph{Nodes: []workflow.GraphNode{{
		ID:     "add",
		Type:   "add",
		Inputs: workflow.OneInput(workflow.Output("seed")),
		Config: json.RawMessage(`{"n":1}`),
	}}}
	ctx := t.Context()
	start := make(chan struct{})
	var callers sync.WaitGroup
	for range 64 {
		callers.Go(func() {
			<-start
			step, err := registry.CompileGraph(graph)
			if err != nil {
				t.Errorf("CompileGraph: %v", err)
				return
			}
			output, err := step.Run(
				ctx,
				workflow.NewStore().WithOutput("seed", 1),
			)
			if err != nil {
				t.Errorf("Run: %v", err)
				return
			}
			if value, err := output.Get[int](workflow.Output("add")); err != nil || value != 2 {
				t.Errorf("output = %d, %v; want 2, nil", value, err)
			}
		})
	}
	close(start)
	callers.Wait()
}

func TestRegistry_compileUsesOneRegistrationSnapshot(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	registry := workflow.NewRegistry().MustRegisterNode(
		"source",
		func(spec workflow.NodeSpec) (workflow.Step, error) {
			startedOnce.Do(func() { close(started) })
			<-release
			return workflow.Interrupt(spec.ID, nil), nil
		},
	)
	graph := workflow.Graph{Nodes: []workflow.GraphNode{{ID: "source", Type: "source"}}}
	type compileResult struct {
		step workflow.Step
		err  error
	}
	compiled := make(chan compileResult, 1)
	go func() {
		step, err := registry.CompileGraph(graph)
		compiled <- compileResult{step: step, err: err}
	}()

	<-started
	if err := registry.RegisterSchema("source", workflow.NodeSchema{}); err != nil {
		t.Fatalf("RegisterSchema: %v", err)
	}
	close(release)
	first := <-compiled
	if first.err != nil || first.step == nil {
		t.Fatalf("in-flight CompileGraph = %v, %v; want pre-registration snapshot", first.step, first.err)
	}

	_, err := registry.CompileGraph(graph)
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.Is(err, workflow.ErrInvalidRegistration) {
		t.Fatalf("later CompileGraph error = %v; want newly registered schema mismatch", err)
	}
}

func TestRegistry_zeroValueIsUsable(t *testing.T) {
	var reg workflow.Registry
	if err := reg.RegisterNode("addN", addN()); err != nil {
		t.Fatalf("zero Registry: %v", err)
	}
	spec := workflow.Spec{
		Kind:   workflow.KindLeaf,
		ID:     "a",
		Type:   "addN",
		Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
	}
	if _, err := reg.CompileSpec(spec); err != nil {
		t.Fatalf("zero Registry CompileSpec: %v", err)
	}
}
