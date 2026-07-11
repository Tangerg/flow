package flowx_test

import (
	"context"
	"testing"
	"time"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/flowx"
)

func BenchmarkDecoratorStack(b *testing.B) {
	ctx := context.Background()
	base := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in + 1, nil
	})
	node := flowx.Wrap(base).
		Retry(flowx.WithAttempts(3)).
		Timeout(time.Minute).
		Fallback(base)

	b.ReportAllocs()
	for b.Loop() {
		_, _ = node.Run(ctx, 1)
	}
}
