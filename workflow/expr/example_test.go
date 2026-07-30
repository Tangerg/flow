package expr_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
	"github.com/Tangerg/flow/workflow/expr"
)

func ExampleParse() {
	e, err := expr.Parse(`review.output.score >= 0.8 && !has(review.output.blocked)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("reads:", e.Refs())

	store := workflow.NewStore().WithOutput("review", map[string]any{"score": 0.91})
	fmt.Println(e.Bool(store))

	// Output:
	// reads: [review#/output/blocked review#/output/score]
	// true <nil>
}

// This example moves a loop's stop condition and a branch's routing rules out of
// Go and into a config document. Changing a threshold is now an edit to the
// config, not a rebuild.
func ExampleBindings() {
	config := []byte(`{
	  "conditions": {
	    "converged": "refine.output >= 100"
	  },
	  "switches": {
	    "bySize": {
	      "cases": [
	        {"when": "refine.output > 500", "then": "large"},
	        {"when": "refine.output > 50",  "then": "medium"}
	      ],
	      "fallback": "small"
	    }
	  }
	}`)

	var bindings expr.Bindings
	if err := json.Unmarshal(config, &bindings); err != nil {
		fmt.Println(err)
		return
	}

	double := workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
		return flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil }), nil
	})
	label := workflow.Factory(func(cfg struct {
		Text string `json:"text"`
	},
	) (flow.Node[int, string], error) {
		return flow.NodeFunc[int, string](func(_ context.Context, x int) (string, error) {
			return fmt.Sprintf("%s:%d", cfg.Text, x), nil
		}), nil
	})

	reg := workflow.NewRegistry().
		MustRegisterLeaf("double", double).
		MustRegisterLeaf("label", label)
	if err := bindings.Register(reg); err != nil {
		fmt.Println(err)
		return
	}

	// Double until the value reaches 100, then route on how large it got. Both
	// "converged" and "bySize" came from the config above.
	spec := []byte(`{
	  "kind": "sequence",
	  "steps": [
	    {
	      "kind": "loop",
	      "id": "refineLoop",
	      "condition": "converged",
	      "maxIterations": 20,
	      "body": {"kind":"leaf","id":"refine","type":"double",
	               "input":{"nodeID":"refine","path":"/output"}}
	    },
	    {
	      "kind": "branch",
	      "id": "route",
	      "resolver": "bySize",
	      "cases": {
	        "small":  {"kind":"leaf","id":"out","type":"label","input":{"nodeID":"refine","path":"/output"},"config":{"text":"small"}},
	        "medium": {"kind":"leaf","id":"out","type":"label","input":{"nodeID":"refine","path":"/output"},"config":{"text":"medium"}},
	        "large":  {"kind":"leaf","id":"out","type":"label","input":{"nodeID":"refine","path":"/output"},"config":{"text":"large"}}
	      }
	    }
	  ]
	}`)

	step, err := reg.CompileSpecJSON(spec)
	if err != nil {
		fmt.Println(err)
		return
	}

	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("refine", 7))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(workflow.Get[string](out, workflow.Output("out")))

	// Output:
	// medium:112 <nil>
}

// This example routes on a value a previous step produced, the shape a classifier
// or a rules engine hands back.
func ExampleResolver() {
	resolver, err := expr.Resolver("classify.output.intent")
	if err != nil {
		fmt.Println(err)
		return
	}

	store := workflow.NewStore().WithOutput("classify", map[string]any{"intent": "refund"})
	fmt.Println(resolver(context.Background(), store))

	// Output: refund <nil>
}

// This example shows that an expression cannot reach the host program: anything
// outside the supported grammar is rejected when it is parsed, not when it runs.
func ExampleParse_rejected() {
	for _, src := range []string{
		`exec("rm -rf /")`,
		`load.output.Close()`,
		`func() int { return 1 }()`,
		`counter`,
	} {
		_, err := expr.Parse(src)
		fmt.Printf("%-28s rejected=%v\n", src, err != nil)
	}

	// Output:
	// exec("rm -rf /")             rejected=true
	// load.output.Close()          rejected=true
	// func() int { return 1 }()    rejected=true
	// counter                      rejected=true
}
