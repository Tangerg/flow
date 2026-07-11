package flow_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Tangerg/flow"
)

func TestIndexError(t *testing.T) {
	boom := errors.New("boom")
	err := &flow.IndexError{Index: 2, Err: boom}
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "index 2") {
		t.Fatalf("err = %v; want index and wrapped cause", err)
	}
}
