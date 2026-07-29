package example_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// Interrupt and Journal form an explicit request/response boundary. Store and
// Journal can be persisted independently and restored in another process.
func Example_resume() {
	type approvalRequest struct {
		Question string `json:"question"`
	}

	prepare := workflow.Leaf(
		"prepare",
		workflow.From[string](workflow.Output("topic")),
		flow.NodeFunc[string, string](func(_ context.Context, topic string) (string, error) {
			fmt.Println("preparing:", topic)
			return topic, nil
		}),
	)
	approval := workflow.Interrupt("approval", approvalRequest{
		Question: `Publish "guide"?`,
	})
	publish := workflow.Leaf(
		"publish",
		workflow.From[string](workflow.Output("approval")),
		flow.NodeFunc[string, string](func(_ context.Context, title string) (string, error) {
			return "published: " + title, nil
		}),
	)
	pipeline := workflow.Sequence(prepare, approval, publish)

	journal := workflow.NewJournal()
	paused, err := workflow.Run(
		context.Background(),
		pipeline,
		workflow.NewStore().WithOutput("topic", "guide"),
		workflow.RunConfig{Journal: journal},
	)
	if !errors.Is(err, workflow.ErrSuspended) {
		fmt.Println("error:", err)
		return
	}
	wait := workflow.Suspensions(err)[0]
	request := wait.Value.(approvalRequest)
	fmt.Println("waiting:", request.Question)

	storeJSON, err := json.Marshal(paused)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	journalJSON, err := json.Marshal(journal)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	var restoredStore workflow.Store
	if err := json.Unmarshal(storeJSON, &restoredStore); err != nil {
		fmt.Println("error:", err)
		return
	}
	var restoredJournal workflow.Journal
	if err := json.Unmarshal(journalJSON, &restoredJournal); err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := restoredJournal.Record(wait.Key(), "guide"); err != nil {
		fmt.Println("error:", err)
		return
	}

	finished, err := workflow.Run(
		context.Background(),
		pipeline,
		restoredStore,
		workflow.RunConfig{Journal: &restoredJournal},
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	result, err := workflow.Get[string](finished, workflow.Output("publish"))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(result)

	// Output:
	// preparing: guide
	// waiting: Publish "guide"?
	// published: guide
}
