package workflow

import "github.com/Tangerg/flow"

// Route evaluates resolve against the current Store and publishes the selected
// outlet name as its output. Graph gates compare that output with [Gate.Outlet].
//
// Route is an ordinary leaf boundary: its decision is observed, journaled, and
// restored on replay. Use it when a routing rule is naturally expressed as a
// [Resolver]; a typed node that already returns a string needs no adapter.
func Route(id string, resolve Resolver) Step {
	return Leaf(
		id,
		func(store Store) (Store, error) {
			return store, nil
		},
		flow.NodeFunc[Store, string](resolve),
	)
}
