package flowx

import "errors"

// Result pairs a value with an error. The collecting combinators [FanOutAll] and
// [MapAll] return one Result per item so a single failure does not discard the
// rest.
type Result[V any] struct {
	Value V
	Error error
}

// Collect splits results into their values and the joined error of any failures
// (nil if none failed). Values from failed items are their zero value.
func Collect[V any](rs []Result[V]) ([]V, error) {
	vals := make([]V, len(rs))
	var errs []error
	for i, r := range rs {
		vals[i] = r.Value
		if r.Error != nil {
			errs = append(errs, r.Error)
		}
	}
	return vals, errors.Join(errs...)
}
