package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
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

func TestFactory(t *testing.T) {
	tests := []struct {
		name   string
		config json.RawMessage
		want   int
	}{
		{name: "typed config", config: json.RawMessage(`{"n": 2}`), want: 3},
		{name: "empty config", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step, err := addFactory()("add", workflow.Output("input"), tt.config)
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

func TestFactory_rejectsUnknownConfigField(t *testing.T) {
	_, err := addFactory()("add", workflow.Output("input"), json.RawMessage(`{"unknown": true}`))
	if !errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("err = %v; want ErrInvalidSpec", err)
	}
}

func TestFactory_rejectsNilFunctionsAndNodes(t *testing.T) {
	var build func(addConfig) (flow.Node[int, int], error)
	if _, err := workflow.Factory(build)("add", workflow.Output("input"), nil); !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("nil build err = %v; want ErrNilFunc", err)
	}

	nilNode := workflow.Factory(func(addConfig) (flow.Node[int, int], error) { return nil, nil })
	if _, err := nilNode("add", workflow.Output("input"), nil); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil node err = %v; want ErrNilNode", err)
	}
}
