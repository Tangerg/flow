package workflow

import (
	"errors"
	"slices"

	"github.com/Tangerg/flow"
)

// Copying an owned error tree. The locations this package owns have exported
// mutable fields, so an [Observer] receives a tree it can hold without sharing
// it with the run that failed. Each wrapper is copied apart from its cause and
// refilled once the branches below it are rebuilt, which is what keeps both the
// linear and the joined depth of a caller-assembled tree off the call stack.

// errorCloneFrame is one copied wrapper whose cause the walk has not rebuilt
// yet. cause points into the copy, so the case that made the copy is the only
// place that has to know which field carries a cause; reattaching later needs
// no second identification of the wrapper, and a wrapper this walk learns to
// copy cannot be one it has forgotten how to fill in.
type errorCloneFrame struct {
	wrapper error
	cause   *error
}

type errorCloneFrames []errorCloneFrame

type errorCloneNode struct {
	frames   errorCloneFrames
	children []int
	result   error
}

type errorCloner struct {
	nodes  []errorCloneNode
	cursor int
}

type errorCloneResult struct {
	frame    errorCloneFrame
	next     error
	owned    bool
	terminal bool
}

// clone copies the complete owned structure without looking through an
// application wrapper. Its worklists keep both linear and joined depth off the
// call stack; exported location errors need not originate in a bounded Spec.
func (e ownedError) clone() error {
	cloner := errorCloner{nodes: []errorCloneNode{{result: e.root}}}
	return cloner.clone()
}

func (c *errorCloner) clone() error {
	for c.expandNext() {
	}
	for index := range slices.Backward(c.nodes) {
		c.rebuild(index)
	}
	return c.nodes[0].result
}

func (c *errorCloner) expandNext() bool {
	if c.cursor >= len(c.nodes) {
		return false
	}
	c.expand(c.cursor)
	c.cursor++
	return true
}

func (c *errorCloner) expand(index int) {
	frames, terminal := peel(c.nodes[index].result)
	c.nodes[index].frames = frames
	children, joined := standardJoinChildren(terminal)
	if !joined {
		c.nodes[index].result = frames.attach(terminal)
		return
	}

	c.nodes[index].result = nil
	c.nodes[index].children = make([]int, len(children))
	for childIndex, child := range children {
		c.nodes[index].children[childIndex] = len(c.nodes)
		c.nodes = append(c.nodes, errorCloneNode{result: child})
	}
}

func (c *errorCloner) rebuild(index int) {
	node := &c.nodes[index]
	if len(node.children) == 0 {
		return
	}
	children := make([]error, len(node.children))
	for childIndex, nodeIndex := range node.children {
		children[childIndex] = c.nodes[nodeIndex].result
	}
	node.result = node.frames.attach(errors.Join(children...))
}

// peel copies one exact linear chain and returns the first error outside it.
// [errors.As] would cross an application wrapper and is therefore deliberately
// not used here.
func peel(err error) (errorCloneFrames, error) {
	var frames errorCloneFrames
	for {
		result := cloneWorkflowFrame(err)
		if !result.owned {
			result = cloneCompositionFrame(err)
		}
		if !result.owned || result.terminal {
			return frames, result.next
		}
		frames = append(frames, result.frame)
		err = result.next
	}
}

// copiedFrame records one copy of an owned wrapper. cause is the copy's own
// cause field: emptying it here and handing the frame its address is what keeps
// a copy from ever being reachable while it still borrows the original's cause.
// next continues the walk at the original cause, which the copy no longer holds.
func copiedFrame(wrapper error, cause *error, next error) errorCloneResult {
	*cause = nil
	return errorCloneResult{
		frame: errorCloneFrame{wrapper: wrapper, cause: cause},
		next:  next,
		owned: true,
	}
}

// ownedTerminal ends the walk at an owned value the copy cannot improve on: a
// typed nil has no location to copy, and a Suspension copies itself.
func ownedTerminal(next error) errorCloneResult {
	return errorCloneResult{next: next, owned: true, terminal: true}
}

//nolint:errorlint // Exact wrapper identity determines ownership.
func cloneWorkflowFrame(err error) errorCloneResult {
	switch current := err.(type) {
	case *StepError:
		if current == nil {
			return ownedTerminal(err)
		}
		copied := *current
		copied.Scope = slices.Clone(current.Scope)
		return copiedFrame(&copied, &copied.Err, current.Err)
	case *RefError:
		if current == nil {
			return ownedTerminal(err)
		}
		copied := *current
		return copiedFrame(&copied, &copied.Err, current.Err)
	case *RegistrationError:
		if current == nil {
			return ownedTerminal(err)
		}
		copied := *current
		return copiedFrame(&copied, &copied.Err, current.Err)
	case *GraphError:
		if current == nil {
			return ownedTerminal(err)
		}
		copied := *current
		return copiedFrame(&copied, &copied.Err, current.Err)
	case *SpecError:
		if current == nil {
			return ownedTerminal(err)
		}
		copied := *current
		return copiedFrame(&copied, &copied.Err, current.Err)
	default:
		return errorCloneResult{next: err}
	}
}

//nolint:errorlint // Exact wrapper identity determines ownership.
func cloneCompositionFrame(err error) errorCloneResult {
	switch current := err.(type) {
	case *flow.IndexError:
		if current == nil {
			return ownedTerminal(err)
		}
		copied := *current
		return copiedFrame(&copied, &copied.Err, current.Err)
	case *flow.CaseError:
		if current == nil {
			return ownedTerminal(err)
		}
		copied := *current
		return copiedFrame(&copied, &copied.Err, current.Err)
	case *detailError:
		if current == nil {
			return ownedTerminal(err)
		}
		copied := *current
		return copiedFrame(&copied, &copied.err, current.err)
	case *factoryBuildError:
		if current == nil {
			return ownedTerminal(err)
		}
		copied := *current
		return copiedFrame(&copied, &copied.err, current.err)
	case *Suspension:
		if current == nil {
			return ownedTerminal(err)
		}
		return ownedTerminal(current.clone())
	default:
		return errorCloneResult{next: err}
	}
}

func (f errorCloneFrames) attach(cause error) error {
	for _, frame := range slices.Backward(f) {
		cause = frame.wrap(cause)
	}
	return cause
}

func (f errorCloneFrame) wrap(cause error) error {
	*f.cause = cause
	return f.wrapper
}
