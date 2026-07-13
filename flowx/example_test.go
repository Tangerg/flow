package flowx_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/flowx"
)

// This example serves a cached value when the primary node fails.
func ExampleFallback() {
	primary := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
		return 0, errors.New("upstream unavailable")
	})
	cache := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		return x * 2, nil
	})

	out, err := flowx.Fallback(primary, cache).Run(context.Background(), 21)
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
	// Output: 42
}
