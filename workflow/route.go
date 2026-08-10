package workflow

// Route evaluates resolve against the current Store and publishes the selected
// outlet name as its output. Graph gates compare that output with [Gate.Outlet].
//
// Route is an ordinary leaf boundary: its decision is observed, journaled, and
// restored on replay. It gives any [Resolver], including a composed resolver,
// that named workflow identity.
func Route(id string, resolve Resolver) Step {
	return Leaf(
		id,
		BinderFunc[Store](func(store Store) (Store, error) {
			return store, nil
		}),
		resolve,
	)
}
