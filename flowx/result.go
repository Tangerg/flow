package flowx

import (
	"errors"

	"github.com/Tangerg/flow/core"
)

// Result pairs a value with an error. The collecting combinators [FanOutAll] and
// [MapAll] return one Result per item so a single failure does not discard the
// rest.
type Result[V any] struct {
	Value V
	Err   error
}

// Collect splits results into their values and the joined error of any failures
// (nil if none failed). Values, including partial values returned alongside an
// error, are preserved. Each failure is wrapped in [core.IndexError].
func Collect[V any](rs []Result[V]) ([]V, error) {
	vals := make([]V, len(rs))
	var errs []error
	for i, r := range rs {
		vals[i] = r.Value
		if r.Err != nil {
			errs = append(errs, &core.IndexError{Index: i, Err: r.Err})
		}
	}
	return vals, errors.Join(errs...)
}
