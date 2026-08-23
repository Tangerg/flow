package workflow

import (
	"fmt"
	"os"
	"os/exec"
	"runtime/debug"
	"testing"
)

const wideStoreBatchChild = "FLOW_WIDE_STORE_BATCH_CHILD"

// TestStoreWideBatchDoesNotSpendStackPerWrite keeps a wide merge from becoming
// recursive depth. Graph width is deliberately unbounded by MaxNestingDepth, so
// collecting many independent node changes must consume heap proportional to
// the batch rather than one call frame per node.
func TestStoreWideBatchDoesNotSpendStackPerWrite(t *testing.T) {
	if os.Getenv(wideStoreBatchChild) != "" {
		debug.SetMaxStack(256 << 10)
		const count = 20_000
		changes := make([]storeChange, count)
		for index := range count {
			revision := uint64(index + 1)
			changes[index] = storeChange{
				key:  storeKey{nodeID: fmt.Sprintf("node-%05d", index), key: outputKey},
				cell: cell{value: index, revision: revision, lineage: revision},
			}
		}
		store := (Store{}).withChanges(changes)
		if got := len(store.Changes(Store{})); got != count {
			t.Fatalf("Changes count = %d; want %d", got, count)
		}
		return
	}

	//nolint:gosec // Re-executes this test binary with a constant test filter.
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestStoreWideBatchDoesNotSpendStackPerWrite$")
	command.Env = append(os.Environ(), wideStoreBatchChild+"=1")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("wide Store batch exhausted a bounded stack: %v\n%s", err, output)
	}
}
