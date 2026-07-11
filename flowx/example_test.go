package flowx_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/flowx"
)

// This example decorates a flaky node with retry and a timeout using the fluent
// Wrap builder. The node fails once, then succeeds on the retry.
func ExampleWrap() {
	attempts := 0
	flaky := core.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		attempts++
		if attempts == 1 {
			return 0, errors.New("temporary failure")
		}
		return x * 2, nil
	})

	node := flowx.Wrap(flaky).
		Retry(flowx.WithAttempts(3)).
		Timeout(time.Second)

	out, err := node.Run(context.Background(), 21)
	if err != nil {
		panic(err)
	}
	fmt.Println(out, "in", attempts, "attempts")
	// Output: 42 in 2 attempts
}
