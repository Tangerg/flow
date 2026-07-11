package flow_test

import (
	"context"
	"testing"

	"github.com/Tangerg/flow"
)

func FuzzThenAssociative(f *testing.F) {
	f.Add(0)
	f.Add(42)
	f.Add(-7)

	f.Fuzz(func(t *testing.T, input int) {
		addOne := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in + 1, nil })
		double := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in * 2, nil })
		minusThree := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in - 3, nil })

		left := flow.Then(flow.Then(addOne, double), minusThree)
		right := flow.Then(addOne, flow.Then(double, minusThree))
		lv, lerr := left.Run(context.Background(), input)
		rv, rerr := right.Run(context.Background(), input)
		if lerr != nil || rerr != nil || lv != rv {
			t.Fatalf("left=(%d,%v), right=(%d,%v)", lv, lerr, rv, rerr)
		}
	})
}

func FuzzMapPreservesOrder(f *testing.F) {
	f.Add([]byte{1, 2, 3}, uint8(2))
	f.Add([]byte{}, uint8(0))

	f.Fuzz(func(t *testing.T, input []byte, rawLimit uint8) {
		if len(input) > 256 {
			input = input[:256]
		}
		limit := int(rawLimit%8) + 1
		node := flow.NodeFunc[byte, byte](func(_ context.Context, in byte) (byte, error) { return in + 1, nil })
		out, err := flow.Map(node, flow.WithConcurrency(limit)).Run(context.Background(), input)
		if err != nil {
			t.Fatalf("Map: %v", err)
		}
		if len(out) != len(input) {
			t.Fatalf("len(out) = %d, want %d", len(out), len(input))
		}
		for i := range input {
			if out[i] != input[i]+1 {
				t.Fatalf("out[%d] = %d, want %d", i, out[i], input[i]+1)
			}
		}
	})
}
