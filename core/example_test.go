package core_test

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow/core"
)

func ExampleThen() {
	double := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil })
	addOne := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })

	pipe := core.Then(double, addOne)

	out, _ := pipe.Run(context.Background(), 10)
	fmt.Println(out)
	// Output: 21
}

func ExampleMap() {
	square := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x * x, nil })

	out, _ := core.Map(square).Run(context.Background(), []int{1, 2, 3, 4})
	fmt.Println(out)
	// Output: [1 4 9 16]
}

func ExampleLoop() {
	// Double until the value reaches at least 100.
	grow := core.Loop(func(_ context.Context, _ int, x int) (int, bool, error) {
		x *= 2
		return x, x >= 100, nil
	})

	out, _ := grow.Run(context.Background(), 1)
	fmt.Println(out)
	// Output: 128
}
