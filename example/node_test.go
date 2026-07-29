package example_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Tangerg/flow"
)

// A Node is the smallest unit: one typed input, one typed output, and an error.
// Then connects unlike types without introducing a workflow runtime.
func Example_node() {
	parse := flow.NodeFunc[string, int](func(_ context.Context, in string) (int, error) {
		return strconv.Atoi(strings.TrimSpace(in))
	})
	double := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in * 2, nil
	})

	pipeline := flow.Then(parse, double)
	out, err := pipeline.Run(context.Background(), " 21 ")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(out)

	// Output: 42
}
