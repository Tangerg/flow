package flow_test

import (
	"context"
	"testing"

	"github.com/Tangerg/flow"
)

func BenchmarkFunc(b *testing.B) {
	ctx := context.Background()
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in + 1, nil
	})

	b.ReportAllocs()
	for b.Loop() {
		_, _ = node.Run(ctx, 1)
	}
}

func BenchmarkThen(b *testing.B) {
	ctx := context.Background()
	inc := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in + 1, nil
	})
	node := flow.Then(flow.Then(inc, inc), inc)

	b.ReportAllocs()
	for b.Loop() {
		_, _ = node.Run(ctx, 1)
	}
}

func BenchmarkMap(b *testing.B) {
	ctx := context.Background()
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in + 1, nil
	})

	for _, size := range []int{1, 16, 256} {
		input := make([]int, size)
		b.Run("unbounded/"+benchmarkSize(size), func(b *testing.B) {
			mapped := flow.Map(node)
			b.ReportAllocs()
			for b.Loop() {
				_, _ = mapped.Run(ctx, input)
			}
		})
		b.Run("limit1/"+benchmarkSize(size), func(b *testing.B) {
			mapped := flow.Map(node, flow.MapConfig{Concurrency: 1})
			b.ReportAllocs()
			for b.Loop() {
				_, _ = mapped.Run(ctx, input)
			}
		})
		b.Run("limit8/"+benchmarkSize(size), func(b *testing.B) {
			mapped := flow.Map(node, flow.MapConfig{Concurrency: 8})
			b.ReportAllocs()
			for b.Loop() {
				_, _ = mapped.Run(ctx, input)
			}
		})
	}
}

func benchmarkSize(n int) string {
	switch n {
	case 1:
		return "1"
	case 16:
		return "16"
	case 256:
		return "256"
	default:
		return "n"
	}
}
