package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

type addConfig struct {
	N int `json:"n"`
}

func addFactory() workflow.LeafFactory {
	return workflow.Factory(func(cfg addConfig) (flow.Node[int, int], error) {
		return flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
			return in + cfg.N, nil
		}), nil
	})
}

// wired returns the LeafSpec of a node whose default port reads input.output.
func wired(config json.RawMessage) workflow.LeafSpec {
	return workflow.LeafSpec{
		ID:     "add",
		Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("input")},
		Config: config,
	}
}

func TestFactory(t *testing.T) {
	tests := []struct {
		name   string
		config json.RawMessage
		want   int
	}{
		{name: "typed config", config: json.RawMessage(`{"n": 2}`), want: 3},
		{name: "empty config", want: 1},
		{name: "whitespace config", config: json.RawMessage(" \n\t"), want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step, err := addFactory()(wired(tt.config))
			if err != nil {
				t.Fatalf("Factory: %v", err)
			}
			out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("input", 1))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			got, err := workflow.Get[int](out, workflow.Output("add"))
			if err != nil || got != tt.want {
				t.Fatalf("Get = %d, %v; want %d, nil", got, err, tt.want)
			}
		})
	}
}

func TestFactory_preservesNumbersInAnyConfig(t *testing.T) {
	var captured any
	factory := workflow.Factory(func(config any) (flow.Node[int, int], error) {
		captured = config
		return flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
			return input, nil
		}), nil
	})

	if _, err := factory(wired(json.RawMessage(`{"n":9007199254740993}`))); err != nil {
		t.Fatalf("Factory: %v", err)
	}
	object, ok := captured.(map[string]any)
	if !ok {
		t.Fatalf("config type = %T; want map[string]any", captured)
	}
	number, ok := object["n"].(json.Number)
	if !ok || number.String() != "9007199254740993" {
		t.Fatalf("config number = %#v (%T); want exact json.Number", object["n"], object["n"])
	}
}

func TestFactory_rejectsUnknownConfigField(t *testing.T) {
	_, err := addFactory()(wired(json.RawMessage(`{"unknown": true}`)))
	if !errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("err = %v; want ErrInvalidSpec", err)
	}
}

func TestFactory_rejectsDuplicateConfigMembers(t *testing.T) {
	_, err := addFactory()(wired(json.RawMessage(`{"n":1,"n":2}`)))
	if !errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("err = %v; want ErrInvalidSpec", err)
	}
}

func TestFactory_rejectsUnwiredDefaultPort(t *testing.T) {
	// A Factory node always reads one port, so an unwired default port can never
	// work and must be reported at build time rather than mid-run.
	_, err := addFactory()(workflow.LeafSpec{ID: "add"})
	if !errors.Is(err, workflow.ErrMissingPort) {
		t.Fatalf("err = %v; want ErrMissingPort", err)
	}
}

func TestFactory_rejectsPortsItCannotRead(t *testing.T) {
	spec := wired(nil)
	spec.Inputs["ignored"] = workflow.Output("other")
	if _, err := addFactory()(spec); !errors.Is(err, workflow.ErrUnknownPort) {
		t.Fatalf("err = %v; want ErrUnknownPort", err)
	}
}

func TestFactory_rejectsNilFunctionsAndNodes(t *testing.T) {
	var build func(addConfig) (flow.Node[int, int], error)
	if _, err := workflow.Factory(build)(wired(nil)); !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("nil build err = %v; want ErrNilFunc", err)
	}

	nilNode := workflow.Factory(func(addConfig) (flow.Node[int, int], error) { return nil, nil })
	if _, err := nilNode(wired(nil)); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil node err = %v; want ErrNilNode", err)
	}
}

// sumPorts adds the two numbers wired to its "a" and "b" ports.
func sumPorts() workflow.LeafFactory {
	return workflow.BindFactory(
		func(_ struct{}, inputs workflow.Inputs) (workflow.BindFunc[[2]int], error) {
			a, aOK := inputs.Ref("a")
			b, bOK := inputs.Ref("b")
			if !aOK || !bOK {
				return nil, fmt.Errorf("%w: want a and b, have %v", workflow.ErrMissingPort, inputs.PortNames())
			}
			return func(s workflow.Store) ([2]int, error) {
				av, err := workflow.Get[int](s, a)
				if err != nil {
					return [2]int{}, err
				}
				bv, err := workflow.Get[int](s, b)
				if err != nil {
					return [2]int{}, err
				}
				return [2]int{av, bv}, nil
			}, nil
		},
		func(struct{}) (flow.Node[[2]int, int], error) {
			return flow.NodeFunc[[2]int, int](func(_ context.Context, p [2]int) (int, error) {
				return p[0] + p[1], nil
			}), nil
		},
	)
}

func TestBindFactory_bindsNamedPorts(t *testing.T) {
	step, err := sumPorts()(workflow.LeafSpec{
		ID:     "sum",
		Inputs: workflow.Inputs{"a": workflow.Output("x"), "b": workflow.Output("y")},
	})
	if err != nil {
		t.Fatalf("BindFactory: %v", err)
	}

	in := workflow.NewStore().WithOutput("x", 3).WithOutput("y", 4)
	out, err := step.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := workflow.Get[int](out, workflow.Output("sum")); err != nil || got != 7 {
		t.Fatalf("sum = %d, %v; want 7, nil", got, err)
	}
}

func TestBindFactory_reportsMissingPort(t *testing.T) {
	_, err := sumPorts()(workflow.LeafSpec{
		ID:     "sum",
		Inputs: workflow.Inputs{"a": workflow.Output("x")},
	})
	if !errors.Is(err, workflow.ErrMissingPort) {
		t.Fatalf("err = %v; want ErrMissingPort", err)
	}
}

func TestBindFactory_rejectsUnknownConfigField(t *testing.T) {
	factory := workflow.BindFactory(
		func(_ addConfig, _ workflow.Inputs) (workflow.BindFunc[int], error) {
			return func(workflow.Store) (int, error) { return 0, nil }, nil
		},
		func(addConfig) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }), nil
		},
	)
	_, err := factory(workflow.LeafSpec{ID: "a", Config: json.RawMessage(`{"unknown": true}`)})
	if !errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("err = %v; want ErrInvalidSpec", err)
	}
}

func TestBindFactory_reportsBuildError(t *testing.T) {
	boom := errors.New("boom")
	factory := workflow.BindFactory(
		func(_ struct{}, _ workflow.Inputs) (workflow.BindFunc[int], error) {
			return func(workflow.Store) (int, error) { return 0, nil }, nil
		},
		func(struct{}) (flow.Node[int, int], error) { return nil, boom },
	)
	if _, err := factory(workflow.LeafSpec{ID: "a"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v; want boom", err)
	}
}

func TestBindFactory_rejectsNilFunctions(t *testing.T) {
	build := func(struct{}) (flow.Node[int, int], error) { return flow.NodeFunc[int, int](nil), nil }
	bind := func(struct{}, workflow.Inputs) (workflow.BindFunc[int], error) { return nil, nil }

	for name, factory := range map[string]workflow.LeafFactory{
		"nil bind":   workflow.BindFactory[struct{}, int, int](nil, build),
		"nil build":  workflow.BindFactory[struct{}, int, int](bind, nil),
		"nil binder": workflow.BindFactory(bind, build),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := factory(workflow.LeafSpec{ID: "a"}); !errors.Is(err, flow.ErrNilFunc) {
				t.Fatalf("err = %v; want ErrNilFunc", err)
			}
		})
	}
}
