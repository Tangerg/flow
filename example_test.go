package flow_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/Tangerg/flow"
)

func ExampleThen() {
	double := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil })
	addOne := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })

	pipe := flow.Then(double, addOne)

	out, _ := pipe.Run(context.Background(), 10)
	fmt.Println(out)
	// Output: 21
}

func ExampleMap() {
	square := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * x, nil })

	out, _ := flow.Map(square).Run(context.Background(), []int{1, 2, 3, 4})
	fmt.Println(out)
	// Output: [1 4 9 16]
}

func ExampleIndexError() {
	boom := errors.New("boom")
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		if in == 2 {
			return 0, boom
		}
		return in, nil
	})

	_, err := flow.Map(node, flow.WithConcurrency(1)).Run(context.Background(), []int{1, 2, 3})
	var indexed *flow.IndexError
	fmt.Println(errors.As(err, &indexed), indexed.Index, errors.Is(err, boom))
	// Output: true 1 true
}

func ExampleLoop() {
	// Double until the value reaches at least 100.
	grow := flow.Loop(func(_ context.Context, _ int, x int) (int, bool, error) {
		x *= 2
		return x, x >= 100, nil
	})

	out, _ := grow.Run(context.Background(), 1)
	fmt.Println(out)
	// Output: 128
}
