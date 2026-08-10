package workflow

import (
	"fmt"

	"github.com/Tangerg/flow"
)

// Factory adapts a typed node constructor into a [NodeFactory]. It decodes the
// raw JSON config into C, rejects unknown fields on ordinary structs, binds the
// node's input from the [DefaultPort] reference, calls build, and wraps the node
// with [Leaf]. An omitted, zero-length config uses the zero value of C; every
// non-empty config must contain one complete JSON value. As in encoding/json, a
// config type with its own UnmarshalJSON method defines its field-level decoding
// contract; strict structural policy for such a type belongs in that method or
// its registered [NodeSchema.ConfigSchema].
//
// A node built this way always reads exactly one port, so an unwired
// [DefaultPort] is reported as [ErrMissingPort] at build time rather than
// failing mid-run, and every other port is [ErrUnknownPort]. Use [BindFactory]
// to bind several ports, or write a [NodeFactory] directly for a non-standard
// binding strategy.
func Factory[C, I, O any](build func(C) (flow.Node[I, O], error)) NodeFactory {
	return BindFactory(func(_ C, inputs Inputs) (Binder[I], error) {
		for _, port := range inputs.PortNames() {
			if port != DefaultPort {
				return nil, fmt.Errorf("%w %q", ErrUnknownPort, port)
			}
		}
		ref, ok := inputs.Default()
		if !ok {
			return nil, fmt.Errorf("%w %q", ErrMissingPort, DefaultPort)
		}
		return From[I](ref), nil
	}, build)
}

// BindFactory adapts a typed node constructor that reads several input ports
// into a [NodeFactory]. It uses Factory's config-decoding contract, calls bind
// to build the node's [Binder] from the wired ports, calls build, and wraps the
// result with [Leaf].
//
// Declaring ports rather than reading extra references out of the config is what
// keeps the graph layer able to see the node's data flow: dependency ordering
// and edge type checks both derive from the wired ports.
//
//	sum := workflow.BindFactory(
//		func(_ struct{}, in workflow.Inputs) (workflow.Binder[[2]float64], error) {
//			a, aOK := in.Ref("a")
//			b, bOK := in.Ref("b")
//			if !aOK || !bOK {
//				return nil, fmt.Errorf("%w: a and b", workflow.ErrMissingPort)
//			}
//			return workflow.BinderFunc[[2]float64](func(s workflow.Store) ([2]float64, error) { ... }), nil
//		},
//		func(struct{}) (flow.Node[[2]float64, float64], error) { ... },
//	)
func BindFactory[C, I, O any](bind func(cfg C, inputs Inputs) (Binder[I], error), build func(C) (flow.Node[I, O], error)) NodeFactory {
	return func(spec NodeSpec) (Step, error) {
		if bind == nil || build == nil {
			return nil, flow.ErrNilFunc
		}

		var cfg C
		if len(spec.Config) > 0 {
			if err := jsonDocument(spec.Config).decode(&cfg); err != nil {
				return nil, fmt.Errorf("%w: decode config: %w", flow.ErrInvalidConfig, err)
			}
		}
		binder, err := bind(cfg, spec.Inputs)
		if err != nil {
			return nil, err
		}
		if isNilBinder(binder) {
			return nil, flow.ErrNilFunc
		}
		node, err := build(cfg)
		if err != nil {
			return nil, fmt.Errorf("workflow: build node: %w", err)
		}
		if err := validateNode(node); err != nil {
			return nil, err
		}
		return Leaf(spec.ID, binder, node), nil
	}
}
