package workflow

import (
	"fmt"

	"github.com/Tangerg/flow"
)

// Factory adapts a typed node constructor into a [LeafFactory]. It strictly
// decodes the raw JSON config into C, binds the node's input from the
// [DefaultPort] reference, calls build, and wraps the node with [Leaf]. An empty
// config uses the zero value of C.
//
// A node built this way always reads exactly one port, so an unwired
// [DefaultPort] is reported as [ErrMissingPort] at build time rather than
// failing mid-run. Use [BindFactory] to bind several ports, or write a
// [LeafFactory] directly for a non-standard binding strategy.
func Factory[C, I, O any](build func(C) (flow.Node[I, O], error)) LeafFactory {
	return BindFactory(func(_ C, inputs Inputs) (BindFunc[I], error) {
		ref, ok := inputs.Default()
		if !ok {
			return nil, fmt.Errorf("%w %q", ErrMissingPort, DefaultPort)
		}
		return From[I](ref), nil
	}, build)
}

// BindFactory adapts a typed node constructor that reads several input ports
// into a [LeafFactory]. It strictly decodes the raw JSON config into C, calls
// bind to build the node's [BindFunc] from the wired ports, calls build, and
// wraps the result with [Leaf].
//
// Declaring ports rather than reading extra references out of the config is what
// keeps the graph layer able to see the node's data flow — dependency ordering
// and edge type checks both derive from the wired ports.
//
//	sum := workflow.BindFactory(
//		func(_ struct{}, in workflow.Inputs) (workflow.BindFunc[[2]float64], error) {
//			a, aOK := in.Ref("a")
//			b, bOK := in.Ref("b")
//			if !aOK || !bOK {
//				return nil, fmt.Errorf("%w: a and b", workflow.ErrMissingPort)
//			}
//			return func(s workflow.Store) ([2]float64, error) { ... }, nil
//		},
//		func(struct{}) (flow.Node[[2]float64, float64], error) { ... },
//	)
func BindFactory[C, I, O any](bind func(cfg C, inputs Inputs) (BindFunc[I], error), build func(C) (flow.Node[I, O], error)) LeafFactory {
	return func(spec LeafSpec) (Step, error) {
		if bind == nil || build == nil {
			return nil, flow.ErrNilFunc
		}

		var cfg C
		if len(spec.Config) > 0 {
			if err := decodeStrict(spec.Config, &cfg); err != nil {
				return nil, fmt.Errorf("%w: decode config: %w", ErrInvalidSpec, err)
			}
		}
		binder, err := bind(cfg, spec.Inputs)
		if err != nil {
			return nil, err
		}
		if binder == nil {
			return nil, flow.ErrNilFunc
		}
		node, err := build(cfg)
		if err != nil {
			return nil, fmt.Errorf("workflow: build node: %w", err)
		}
		if node == nil {
			return nil, flow.ErrNilNode
		}
		return Leaf(spec.ID, binder, node), nil
	}
}
