package workflow_test

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestOneInput(t *testing.T) {
	want := workflow.Output("source")
	inputs := workflow.OneInput(want)

	got, ok := inputs.Default()
	if !ok || got != want {
		t.Fatalf("Default() = %v, %v; want %v, true", got, ok, want)
	}
	if len(inputs) != 1 {
		t.Fatalf("len(OneInput) = %d; want 1", len(inputs))
	}
}

// TestInputsWalkInNameOrderSoTheFirstOffenderIsAlwaysTheSame pins the order
// [workflow.Inputs.All] promises, which several boundaries spend on determinism
// without saying so themselves: each reports the first wired binding that breaks
// its rule, and a rendering emits one edge per port. Inputs is a map, so the
// promise is the only thing standing between those answers and Go's randomized
// iteration order -- a definition that is refused for two reasons would be refused
// for a different one on the next pass.
func TestInputsWalkInNameOrderSoTheFirstOffenderIsAlwaysTheSame(t *testing.T) {
	inputs := workflow.Inputs{
		"zulu":  workflow.Output("z"),
		"alpha": workflow.Output("a"),
		"mike":  workflow.Output("m"),
	}
	var walked []string
	for name, ref := range inputs.All() {
		if want := inputs[name]; ref != want {
			t.Fatalf("All() yielded %s under %q; want %s", ref, name, want)
		}
		walked = append(walked, name)
	}
	if want := []string{"alpha", "mike", "zulu"}; !slices.Equal(walked, want) {
		t.Fatalf("All() walked %v; want %v", walked, want)
	}

	registry := workflow.NewRegistry().
		MustRegisterNode("onePort", addN()).
		MustRegisterNode("noPorts", workflow.InterruptFactory()).
		MustRegisterNode("declared", addN()).
		MustRegisterSchema("declared", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeAny),
			Output: workflow.TypeAny,
		})
	// Every boundary below refuses all three of these bindings, so which one it
	// names is decided by the walk rather than by the definition. "in" is wired
	// alongside them where a schema requires it, which also proves the walk passes
	// over a legitimate binding instead of stopping at the first it examines.
	offenders := workflow.Inputs{
		"alpha": workflow.Output("seed"),
		"mike":  workflow.Output("seed"),
		"zulu":  workflow.Output("seed"),
	}
	declared := workflow.Inputs{workflow.DefaultPort: workflow.Output("seed")}
	maps.Copy(declared, offenders)

	badSeeds := workflow.Inputs{"alpha": {}, "mike": {}, "zulu": {}}
	boundaries := map[string]func() error{
		"a factory that reads one port": func() error {
			_, err := registry.CompileGraph(oneWiredNode("onePort", offenders))
			return err
		},
		"a factory that reads none": func() error {
			_, err := registry.CompileGraph(oneWiredNode("noPorts", offenders))
			return err
		},
		"a registered schema": func() error {
			return registry.ValidateGraph(oneWiredNode("declared", declared))
		},
		"subgraph seeds": func() error {
			return flow.Validate(workflow.Subgraph(workflow.SubgraphConfig{
				ID:     "sealed",
				Inputs: badSeeds,
				Body: workflow.LeafFunc("body", workflow.Output("alpha"),
					func(_ context.Context, x int) (int, error) { return x, nil }),
				BodyOutput: workflow.Output("body"),
			}))
		},
	}
	for _, name := range slices.Sorted(maps.Keys(boundaries)) {
		err := boundaries[name]()
		if err == nil {
			t.Fatalf("%s: accepted three offending bindings", name)
		}
		if !strings.Contains(err.Error(), `"alpha"`) {
			t.Errorf("%s: error = %v; want it to name the first binding, alpha", name, err)
		}
		for _, later := range []string{"mike", "zulu"} {
			if strings.Contains(err.Error(), `"`+later+`"`) {
				t.Errorf("%s: error = %v; want it to stop at alpha, not reach %s", name, err, later)
			}
		}
	}
}

// oneWiredNode is a graph holding a single node of nodeType with inputs wired.
func oneWiredNode(nodeType string, inputs workflow.Inputs) workflow.Graph {
	return workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "n", Type: nodeType, Inputs: inputs},
	}}
}
