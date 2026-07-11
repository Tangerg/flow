package flowx_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/flowx"
)

// This example decorates a flaky node with retry and a timeout. The node fails
// once, then succeeds on the retry.
func ExampleRetry() {
	attempts := 0
	flaky := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		attempts++
		if attempts == 1 {
			return 0, errors.New("temporary failure")
		}
		return x * 2, nil
	})

	node := flowx.Timeout(
		flowx.Retry(flaky, flowx.WithAttempts(3)),
		time.Second,
	)

	out, err := node.Run(context.Background(), 21)
	if err != nil {
		panic(err)
	}
	fmt.Println(out, "in", attempts, "attempts")
	// Output: 42 in 2 attempts
}
