package core_test

import (
	"context"
	"testing"

	"github.com/Tangerg/flow/core"
)

func BenchmarkFunc(b *testing.B) {
	ctx := context.Background()
	node := core.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in + 1, nil
	})

	b.ReportAllocs()
	for b.Loop() {
		_, _ = node.Run(ctx, 1)
	}
}

func BenchmarkThen(b *testing.B) {
	ctx := context.Background()
	inc := core.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in + 1, nil
	})
	node := core.Then(core.Then(inc, inc), inc)

	b.ReportAllocs()
	for b.Loop() {
		_, _ = node.Run(ctx, 1)
	}
}

func BenchmarkMap(b *testing.B) {
	ctx := context.Background()
	node := core.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in + 1, nil
	})

	for _, size := range []int{1, 16, 256} {
		input := make([]int, size)
		b.Run("unbounded/"+benchmarkSize(size), func(b *testing.B) {
			mapped := core.Map(node)
			b.ReportAllocs()
			for b.Loop() {
				_, _ = mapped.Run(ctx, input)
			}
		})
		b.Run("limit1/"+benchmarkSize(size), func(b *testing.B) {
			mapped := core.Map(node, core.WithConcurrency(1))
			b.ReportAllocs()
			for b.Loop() {
				_, _ = mapped.Run(ctx, input)
			}
		})
		b.Run("limit8/"+benchmarkSize(size), func(b *testing.B) {
			mapped := core.Map(node, core.WithConcurrency(8))
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
