package workflow

import (
	"context"

	"github.com/Tangerg/flow/core"
)

// Branch routes the Store to one of several steps. It runs resolve to pick a
// branch name from the Store, then runs the step registered under that name. If
// resolve yields a name with no matching case, Run fails (see core.ErrNoCase).
//
// It is a thin specialization of core.Switch over Step.
func Branch(resolve func(ctx context.Context, s Store) (string, error), cases map[string]Step) Step {
	return core.Switch(core.Func[Store, string](resolve), cases)
}
