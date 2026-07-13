package flowx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/flowx"
)

func BenchmarkFallback(b *testing.B) {
	ctx := context.Background()
	boom := errors.New("boom")
	primary := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	alt := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil })
	node := flowx.Fallback(primary, alt)

	b.ReportAllocs()
	for b.Loop() {
		_, _ = node.Run(ctx, 1)
	}
}
