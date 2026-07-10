package flowx_test

import (
	"context"
	"testing"
	"time"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/flowx"
)

func BenchmarkDecoratorStack(b *testing.B) {
	ctx := context.Background()
	base := core.Func[int, int](func(_ context.Context, in int) (int, error) {
		return in + 1, nil
	})
	node := flowx.Wrap(base).
		Retry(flowx.WithAttempts(3)).
		Timeout(time.Minute).
		Fallback(base).
		Node()

	b.ReportAllocs()
	for b.Loop() {
		_, _ = node.Run(ctx, 1)
	}
}
