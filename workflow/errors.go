package workflow

import "errors"

// ErrNilStep is returned when a nil [Step] is run inside a composite (Loop,
// Parallel, Iteration). Test for it with [errors.Is].
var ErrNilStep = errors.New("workflow: nil step")
