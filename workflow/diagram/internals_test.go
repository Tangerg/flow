package diagram

import (
	"testing"

	"github.com/Tangerg/flow/workflow"
)

// TestExternalNodeCompare_tellsApartEverythingItOrdersBy states the ordering rule
// where it lives instead of through a rendered diagram. Two externals that differ
// only in the field one link reads must not compare equal: a sort cannot order what
// it cannot tell apart, and it shows that as a permutation drawn from map iteration
// order. A rendered test sees the wrong permutation only sometimes -- a single run
// caught a missing node-ID comparison one time in four -- while the comparator
// answers the same question every time.
//
// The precedence case is here for the same reason: it is the only one where two
// links disagree, so it is the only one that says which of them is asked first.
func TestExternalNodeCompare_tellsApartEverythingItOrdersBy(t *testing.T) {
	tests := map[string]struct {
		first  externalNode
		second externalNode
	}{
		// Two sources: the label is all there is.
		"label": {
			first:  externalNode{source: "a"},
			second: externalNode{source: "b"},
		},
		// The labels disagree with the kinds. Read the other way round, the source
		// would come first because a whole source precedes a path into one.
		"label before kind": {
			first:  externalNode{ref: workflow.At("a", "value"), valueRef: true},
			second: externalNode{source: "b"},
		},
		// A malformed graph can wire a reference with no node ID, whose display form
		// is then "#/output", and a dependency can name a node spelled exactly that.
		// Label and node ID both tie, so the kind is the only thing left.
		"kind": {
			first:  externalNode{source: "#/output"},
			second: externalNode{ref: workflow.Ref{Path: "/output"}, valueRef: true},
		},
		// A node ID may contain the separator a Ref renders with, so the same label
		// can split into different cells. Kinds tie; the node ID decides.
		"node ID": {
			first:  externalNode{ref: workflow.At("a", "b#", "c"), valueRef: true},
			second: externalNode{ref: workflow.At("a#/b", "c"), valueRef: true},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if order := test.first.compare(test.second); order >= 0 {
				t.Fatalf("compare = %d; want a negative order", order)
			}
			if order := test.second.compare(test.first); order <= 0 {
				t.Fatalf("reversed compare = %d; want a positive order", order)
			}
			if order := test.first.compare(test.first); order != 0 {
				t.Fatalf("self compare = %d; want 0", order)
			}
		})
	}
}
