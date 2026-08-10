package workflow

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ScopeFrame identifies one enclosing composite invocation. ID names the
// composite. Indexed and Index distinguish one invocation of a repeated body,
// such as an element of Iteration or an iteration of Loop, from an ordinary
// namespace such as Subgraph.
//
// Construct values with keyed fields. An ordinary frame leaves Indexed false
// and Index zero; an indexed frame sets Indexed true and Index to its invocation
// index. Index is uint64 so persisted Journal identity is independent of the
// machine word size. Scope, Event, Chunk, Suspension, and JournalKey expose
// frames as immutable values.
type ScopeFrame struct {
	ID      string `json:"id"`
	Indexed bool   `json:"indexed,omitempty"`
	Index   uint64 `json:"index,omitempty"`
}

// String returns a compact display form: ordinary frames use ID and indexed
// frames use ID[index]. It is for diagnostics only; use the fields themselves
// for identity because an ordinary ID may contain the same text.
func (s ScopeFrame) String() string {
	if s.Indexed {
		return s.ID + "[" + strconv.FormatUint(s.Index, 10) + "]"
	}
	return s.ID
}

// Validate reports whether s is a well-formed execution identity. Caller-defined
// composites should validate the frame they add even when they may invoke no
// child, because no child boundary would otherwise observe it.
func (s ScopeFrame) Validate() error {
	if err := validateName("scope frame ID", s.ID); err != nil {
		return err
	}
	if !s.Indexed && s.Index != 0 {
		return fmt.Errorf("scope frame %q has index %d without Indexed", s.ID, s.Index)
	}
	return nil
}

func (s ScopeFrame) compare(other ScopeFrame) int {
	return cmp.Or(
		strings.Compare(s.ID, other.ID),
		compareBool(s.Indexed, other.Indexed),
		cmp.Compare(s.Index, other.Index),
	)
}

func compareBool(left, right bool) int {
	switch {
	case left == right:
		return 0
	case !left:
		return -1
	default:
		return 1
	}
}

func compareScope(left, right []ScopeFrame) int {
	for index := range min(len(left), len(right)) {
		if order := left[index].compare(right[index]); order != 0 {
			return order
		}
	}
	return cmp.Compare(len(left), len(right))
}

func formatScope(scope []ScopeFrame) string {
	if len(scope) == 0 {
		return "<root>"
	}
	frames := make([]string, len(scope))
	for index, frame := range scope {
		frames[index] = frame.String()
	}
	return strings.Join(frames, "/")
}

func validateScope(scope []ScopeFrame) error {
	if err := validateScopeDepth(len(scope)); err != nil {
		return err
	}
	for index, frame := range scope {
		if err := frame.Validate(); err != nil {
			return fmt.Errorf("scope frame %d: %w", index, err)
		}
	}
	return nil
}

func validateScopeDepth(depth int) error {
	if depth <= MaxNestingDepth {
		return nil
	}
	return fmt.Errorf(
		"%w: scope depth %d exceeds limit %d",
		ErrMaxDepth,
		depth,
		MaxNestingDepth,
	)
}

// validateChildScope checks a built-in composite's child namespace before it
// reads inputs or enters the body. The composite has already validated the
// frame it will add, so this only validates the inherited scope and capacity.
func validateChildScope(scope []ScopeFrame) error {
	if err := validateScopeDepth(len(scope) + 1); err != nil {
		return err
	}
	return validateScope(scope)
}

type scopeKey struct{}

// WithScope returns a context with an additional ordinary execution-scope
// frame. Caller-defined composites use it to isolate an inner namespace.
// Use [WithScopeIndex] when a caller-defined repeated composite needs an indexed
// frame; built-in repeated composites add those frames internally. Invoke the
// child Step directly with the returned context; package-level [Run] deliberately
// starts a nested execution at a new root scope.
// An empty or non-UTF-8 ID, or a resulting scope deeper than
// [MaxNestingDepth], is rejected by the built-in Step that claims an identity
// under it. A caller-defined composite should use [ScopeFrame.Validate] to
// reject its own frame even when it invokes no child.
//
// A scope is execution state rather than configuration, which is why it is not
// a RunConfig field. It is maintained whether or not anything is watching,
// because it identifies a step rather than merely labelling it: Event.Scope
// reports it, and a Journal keys its records by it. Each call copies the scope,
// so concurrent branches never share a slice.
func WithScope(ctx context.Context, id string) context.Context {
	return withScopeFrame(ctx, ScopeFrame{ID: id})
}

// WithScopeIndex returns a context with an indexed execution-scope frame.
// Caller-defined repeated composites use it so separate invocations of the same
// child have distinct Journal and signal identities. Invoke the child Step
// directly with the returned context; package-level [Run] starts an independent
// execution boundary.
//
// The inherited scope is copied. An empty or non-UTF-8 ID, or excessive depth,
// is reported by the built-in Step that first claims an identity under the
// resulting context, before application work begins. A caller-defined
// composite should use [ScopeFrame.Validate] to reject its own frame even when
// it invokes no child.
func WithScopeIndex(ctx context.Context, id string, index uint64) context.Context {
	return withScopeFrame(ctx, ScopeFrame{ID: id, Indexed: true, Index: index})
}

func withScopeFrame(ctx context.Context, frame ScopeFrame) context.Context {
	current := scope(ctx)
	extended := make([]ScopeFrame, len(current)+1)
	copy(extended, current)
	extended[len(current)] = frame
	return context.WithValue(ctx, scopeKey{}, extended)
}

// Scope returns the scope of a step running under ctx, outermost first. The
// returned slice is a copy.
func Scope(ctx context.Context) []ScopeFrame {
	return slices.Clone(scope(ctx))
}

// scope returns the context-owned scope for internal reads. Callers that retain
// or expose it must clone it.
func scope(ctx context.Context) []ScopeFrame {
	current, _ := ctx.Value(scopeKey{}).([]ScopeFrame)
	return current
}
