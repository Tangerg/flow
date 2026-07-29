package example_test

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
)

// Composites are Nodes too. Here Map squares values concurrently, and Then
// feeds the complete slice to a reducer.
func Example_composition() {
	square := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in * in, nil
	})
	sum := flow.NodeFunc[[]int, int](func(_ context.Context, in []int) (int, error) {
		total := 0
		for _, value := range in {
			total += value
		}
		return total, nil
	})

	pipeline := flow.Then(
		flow.Map(square, flow.MapConfig{Concurrency: 2}),
		sum,
	)
	out, err := pipeline.Run(context.Background(), []int{1, 2, 3, 4})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(out)

	// Output: 30
}
