package workflow

import (
	"context"
	"maps"
	"slices"

	"github.com/Tangerg/flow/core"
)

// Branch routes the Store to one of several steps. It runs resolve to pick a
// branch name from the Store, then runs the step registered under that name. If
// resolve yields a name with no matching case, Run fails (see core.ErrNoCase).
// It composes with core.Switch.
func Branch(resolve func(ctx context.Context, s Store) (string, error), cases map[string]Step) Step {
	return branch{
		cases: cases,
		node:  core.Switch(core.Func[Store, string](resolve), cases),
	}
}

// branch is the [Step] produced by [Branch].
type branch struct {
	cases map[string]Step
	node  Step
}

func (b branch) Run(ctx context.Context, s Store) (Store, error) { return b.node.Run(ctx, s) }

func (b branch) Describe() Description {
	children := make([]Description, 0, len(b.cases))
	for _, name := range slices.Sorted(maps.Keys(b.cases)) {
		d := Describe(b.cases[name])
		d.ID = name
		children = append(children, d)
	}
	return Description{Kind: "branch", Children: children}
}
