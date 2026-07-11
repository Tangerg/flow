package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/Tangerg/flow"
)

// Factory adapts a typed node constructor into a [LeafFactory]. It strictly
// decodes the raw JSON config into C, calls build, binds the node input from the
// supplied reference, and wraps the node with [Leaf]. An empty config uses the
// zero value of C.
//
// Use a custom LeafFactory when a node needs multiple input references or a
// non-standard binding strategy.
func Factory[C, I, O any](build func(C) (flow.Node[I, O], error)) LeafFactory {
	return func(id string, input Ref, config json.RawMessage) (Step, error) {
		if build == nil {
			return nil, flow.ErrNilFunc
		}

		var cfg C
		if len(config) > 0 {
			if err := decodeStrict(config, &cfg); err != nil {
				return nil, fmt.Errorf("%w: decode config: %v", ErrInvalidSpec, err)
			}
		}
		node, err := build(cfg)
		if err != nil {
			return nil, fmt.Errorf("build node: %w", err)
		}
		if node == nil {
			return nil, flow.ErrNilNode
		}
		return Leaf(id, From[I](input), node), nil
	}
}
