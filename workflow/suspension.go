package workflow

import (
	"cmp"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// ErrSuspended marks a run that stopped because a step is waiting for something
// the workflow cannot produce on its own — a human decision, an external job, a
// retry window. Match it with [errors.Is] and read the details with [errors.As]
// on [*Suspension].
//
// A suspension is a third outcome alongside success and failure, not a kind of
// failure. Composites treat it accordingly: [Parallel] and [Iteration] let their
// remaining work finish instead of cancelling it, because "not yet" says nothing
// about whether the rest should proceed.
var ErrSuspended = errors.New("workflow: suspended")

// Suspension reports why a run stopped and what would let it continue.
//
// Await names a value that must appear in the Store. It is the zero Ref for an
// [Interrupt] or for a node that called [Suspend]. Value is the information
// exposed to the caller: commonly a string, but it may be any application value
// such as an approval request, form, or external-job descriptor.
// Suspension's JSON representation is strict, failure-atomic, and validates
// its engine-owned identity. An anonymous suspension has neither ID nor Scope;
// once a step identifies it, an empty Scope means the workflow root. Value uses
// encoding/json's application-value semantics when encoding and decodes into
// the lossless JSON domain, including [json.Number] for numbers.
//
// The name deliberately omits an Error suffix. A suspension travels as an error
// so that it propagates without a parallel return path, but it reports a third
// outcome rather than a failure; calling it SuspensionError would contradict the
// semantics every composite in this package is built around.
//
//nolint:errname,recvcheck // A third outcome; UnmarshalJSON requires a pointer receiver.
type Suspension struct {
	// ID is the step that suspended. A non-empty ID marks the identity as
	// complete; a nil Scope then means the workflow root rather than an
	// unidentified scope.
	ID string `json:"id,omitempty"`
	// Scope is the step's enclosing structured execution scopes, as on
	// [Event.Scope].
	Scope []ScopeFrame `json:"scope,omitempty"`
	// Await is the reference whose absence caused the suspension, if any.
	Await Ref `json:"await,omitzero"`
	// Value is application-owned and must be treated as immutable. A caller that
	// persists a Suspension is responsible for using a codec that can represent
	// the concrete value.
	Value any `json:"value,omitempty"`
}

func (s *Suspension) Error() string {
	if s == nil {
		return ErrSuspended.Error()
	}
	var message strings.Builder
	message.WriteString("workflow:")
	if s.ID != "" {
		message.WriteString(" step ")
		message.WriteString(strconv.Quote(s.ID))
	}
	if len(s.Scope) > 0 {
		message.WriteString(" in ")
		message.WriteString(formatScope(s.Scope))
	}
	message.WriteString(" suspended")
	reason := suspensionReason(s.Value)
	switch {
	case reason != "":
		message.WriteString(": ")
		message.WriteString(reason)
	case s.Await != (Ref{}):
		message.WriteString(": awaiting ")
		message.WriteString(s.Await.String())
	}
	return message.String()
}

// suspensionReason returns the value a message can state directly, which is text
// and nothing else: an approval request or a job descriptor is structured data a
// caller reads through [Suspensions], not a sentence. It asks for the kind rather
// than asserting string, because an application's own string type is still text
// and a type assertion would drop it -- see TestSuspension_errorMessage.
func suspensionReason(value any) string {
	if value == nil {
		return ""
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.String {
		return ""
	}
	return reflected.String()
}

// Unwrap returns [ErrSuspended].
func (s *Suspension) Unwrap() error { return ErrSuspended }

// Suspend returns an error that stops the run at the calling step. Use it inside
// a node when the work cannot proceed yet; the caller resumes by supplying what
// is missing and running the same workflow again with the run's [Journal].
//
// The step ID and scope are filled in by the step that returns it, so a node need
// only provide the value to expose. Mutable values must not be modified after
// this call. A caller may either supply the missing external state and let the
// step run again, or record the step's expected result under [Suspension.Key] so
// a journaled boundary replays it as completed. [Interrupt] packages the latter
// pattern as a Step.
func Suspend(value any) error {
	return &Suspension{Value: value}
}

// Key returns the structured identity of the suspended step. The returned Scope
// is a copy. It is the key to pass to [Journal.Record] when supplying the result
// of an [Interrupt] or another journaled boundary that called [Suspend].
func (s *Suspension) Key() JournalKey {
	if s == nil {
		return JournalKey{}
	}
	return JournalKey{ID: s.ID, Scope: slices.Clone(s.Scope)}
}

// suspensionTree owns classification of the standard Go error tree.
type suspensionTree struct {
	err error
}

// suspensions reports the suspension leaves and whether every error leaf is a
// suspension. Composites use the second result to keep a joined failure from
// being mistaken for "not yet".
func (s suspensionTree) suspensions() (suspensionList, bool) {
	suspensions, only := s.collect()
	if len(suspensions) == 0 {
		return nil, false
	}
	return suspensions.normalized(), only
}

// collect walks both forms supported by the standard error tree:
// Unwrap() error and Unwrap() []error. It returns copies so identifying a wait
// at a workflow boundary never mutates an error owned by its caller. The
// explicit worklist makes both linear wrapping and nested joins stack-safe;
// application error trees do not cross a workflow nesting boundary first.
func (s suspensionTree) collect() (suspensionList, bool) {
	collector := suspensionCollector{
		pending:         []error{s.err},
		onlySuspensions: true,
	}
	return collector.collect()
}

// suspensionCollector owns the mutable state of one iterative error-tree walk.
// pending holds unexplored join branches; a linear wrapper chain never enters
// it and is collapsed in place.
type suspensionCollector struct {
	pending         []error
	suspensions     suspensionList
	leaves          int
	onlySuspensions bool
}

func (s *suspensionCollector) collect() (suspensionList, bool) {
	for len(s.pending) > 0 {
		last := len(s.pending) - 1
		err := s.pending[last]
		s.pending = s.pending[:last]
		if err != nil {
			s.collectChain(err)
		}
	}
	return s.suspensions, s.leaves > 0 && s.onlySuspensions
}

func (s *suspensionCollector) collectChain(err error) {
	for {
		if wait := waitAt(err); wait != nil {
			s.accept(wait)
			return
		}
		if many, ok := err.(interface{ Unwrap() []error }); ok {
			if s.push(many.Unwrap()) {
				return
			}
			s.acceptIdentity(err)
			return
		}
		one, ok := err.(interface{ Unwrap() error })
		if !ok {
			s.acceptIdentity(err)
			return
		}
		child := one.Unwrap()
		if child == nil {
			s.acceptIdentity(err)
			return
		}
		// Exact identity distinguishes a wrapper directly around ErrSuspended
		// from a wrapper whose deeper tree merely contains one.
		//
		//nolint:errorlint // [errors.Is] cannot tell those shapes apart.
		if child == ErrSuspended {
			s.accept(suspensionList{{Value: err.Error()}})
			return
		}
		err = child
	}
}

// push schedules non-nil children in the order a depth-first walk will pop
// them. It reports whether the multi-error had any child to traverse; a value
// exposing only nil children is itself an error leaf.
func (s *suspensionCollector) push(children []error) bool {
	count := 0
	for _, child := range slices.Backward(children) {
		if child == nil {
			continue
		}
		s.pending = append(s.pending, child)
		count++
	}
	return count > 0
}

func (s *suspensionCollector) acceptIdentity(err error) {
	s.accept(suspensionIdentity(err))
}

// accept records one error leaf together with the waits it turned out to be. A
// leaf that is a wait always produces at least one, an anonymous one where it
// carries no identity, so the waits themselves report whether every leaf so far
// was a wait. A boolean beside them would be the same fact twice, and the two
// could disagree.
func (s *suspensionCollector) accept(found suspensionList) {
	s.suspensions = append(s.suspensions, found...)
	s.leaves++
	s.onlySuspensions = s.onlySuspensions && len(found) > 0
}

// waitAt reports the wait one error node already is, before anything unwraps it,
// and nil for a node that is not one. It deliberately inspects that node rather
// than using [errors.As], which would skip wrappers and could read a mixed
// joined tree as a pure tree of waits. Separating it from the walk keeps
// [suspensionTree.collect] about the shape of the tree and this about what a
// single node means.
func waitAt(err error) suspensionList {
	//nolint:errorlint // Unwrapping here would defeat the per-node classification.
	if suspension, ok := err.(*Suspension); ok {
		if suspension == nil {
			// A typed nil still satisfies error and matches ErrSuspended through
			// Suspension.Unwrap. Preserve that meaning as an anonymous wait instead
			// of normalizing it away into a nil error.
			return suspensionList{{}}
		}
		return suspensionList{suspension.clone()}
	}
	// Exact identity leaves a direct wrapper's message to the walk, which keeps it.
	//
	//nolint:errorlint // [errors.Is] would match wrappers this must not consume.
	if err == ErrSuspended {
		return suspensionList{{}}
	}
	return nil
}

// suspensionIdentity handles a leaf that participates in [errors.Is] without
// being a Suspension value. An error exposing only nil children is still a
// leaf: [errors.Is] checks its Is method before consulting Unwrap, so
// classification must preserve the same meaning. Like [waitAt] it answers with
// the waits a node is, so a caller cannot pair one node with another node's
// verdict.
func suspensionIdentity(err error) suspensionList {
	if errors.Is(err, ErrSuspended) {
		return suspensionList{{Value: err.Error()}}
	}
	return nil
}

type suspensionList []*Suspension

// errAt reports these waits as one error after naming the boundary that owns any
// of them that arrived anonymous. ID and Scope are one identity: an existing ID
// means the wait was already identified, including when its nil Scope deliberately
// names the root of an independent nested Run. An anonymous wait is owned wholly
// by the current boundary rather than retaining a caller-supplied partial identity.
//
// Naming and reporting are one operation because an ID is part of the sort key:
// ordering the list before the names are in place would order it by identities it
// does not have yet. It writes through the pointers it holds, which is safe
// because every list reaching it came from [suspensionList.normalized] and
// therefore holds clones — see [Suspension.clone].
func (s suspensionList) errAt(key JournalKey) error {
	for _, suspension := range s {
		if suspension.ID == "" {
			suspension.ID = key.ID
			suspension.Scope = slices.Clone(key.Scope)
		}
	}
	return s.err()
}

// err reports the suspensions of a fan-out as one error. Several branches may
// be waiting at once, and a caller needs every reason to know what to supply
// before resuming.
func (s suspensionList) err() error {
	s = s.normalized()
	switch len(s) {
	case 0:
		return nil
	case 1:
		return s[0]
	}
	reasons := make([]string, 0, len(s))
	for _, suspension := range s {
		reasons = append(reasons, suspension.Error())
	}
	// The envelope counts the waits and joins them; it does not name this
	// package, because every suspension it carries already does. Adding a
	// prefix here would say it once per fan-out on top of once per branch.
	return &multiSuspension{
		suspensions: s,
		message: fmt.Sprintf(
			"%d steps suspended: %s",
			len(s),
			strings.Join(reasons, "; "),
		),
	}
}

func (s suspensionList) normalized() suspensionList {
	normalized := make(suspensionList, 0, len(s))
	for _, suspension := range s {
		if suspension != nil {
			normalized = append(normalized, suspension.clone())
		}
	}
	slices.SortStableFunc(normalized, func(left, right *Suspension) int {
		return left.compare(right)
	})
	return normalized
}

func (s *Suspension) compare(other *Suspension) int {
	return cmp.Or(
		strings.Compare(s.ID, other.ID),
		compareScope(s.Scope, other.Scope),
		s.Await.Compare(other.Await),
	)
}

// clone copies a suspension and its scope so identifying a wait at a workflow
// boundary never mutates an error owned by a caller. Callers filter nil entries
// first, so the receiver is never nil.
func (s *Suspension) clone() *Suspension {
	clone := *s
	clone.Scope = slices.Clone(s.Scope)
	return &clone
}

// multiSuspension carries every suspension of one fan-out.
//
//nolint:errname // Named for [Suspension], which is a third outcome, not a failure.
type multiSuspension struct {
	suspensions []*Suspension
	message     string
}

func (m *multiSuspension) Error() string { return m.message }

// Unwrap returns a copy of each suspension so [errors.As] finds the first and
// [errors.Is] matches [ErrSuspended] without exposing multiSuspension's immutable
// error tree to mutation through Suspension's exported fields.
func (m *multiSuspension) Unwrap() []error {
	errs := make([]error, len(m.suspensions))
	for index, suspension := range m.suspensions {
		errs[index] = suspension.clone()
	}
	return errs
}

// Suspensions returns every suspension in err's error tree, ordered by step ID
// and then scope. A run that stopped in one place yields one; nested fan-out may
// yield several. The returned Suspension values and their scopes are copies;
// application-owned mutable Values remain borrowed and must not be modified.
func Suspensions(err error) []*Suspension {
	suspensions, _ := (suspensionTree{err: err}).suspensions()
	return suspensions
}

// SuspendedOnly reports whether err contains one or more suspensions and no
// failure leaves. It understands both standard error wrapping and errors
// joined with [errors.Join]. A nil error is not a suspension.
//
// Caller-defined composites can use SuspendedOnly to preserve the distinction
// between "not yet" and failure without depending on this package's internal
// error representation.
func SuspendedOnly(err error) bool {
	_, only := (suspensionTree{err: err}).suspensions()
	return only
}

// JoinSuspensions returns one error containing every non-nil suspension,
// ordered by step ID and then scope. It returns nil when all arguments are nil.
// The supplied suspensions and their scopes are copied.
//
// Caller-defined fan-out composites use JoinSuspensions after allowing every
// suspended branch to finish. Use [SuspendedOnly] to distinguish suspensions
// from ordinary failures before collecting them with [Suspensions].
func JoinSuspensions(suspensions ...*Suspension) error {
	return suspensionList(suspensions).err()
}
