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
// the lossless JSON domain, including json.Number for numbers.
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
// at a workflow boundary never mutates an error owned by its caller.
func (s suspensionTree) collect() (suspensionList, bool) {
	for s.err != nil {
		err := s.err
		// This intentionally inspects the current node rather than using errors.As:
		// As would skip wrappers and could misclassify a mixed joined tree as a pure
		// suspension tree.
		//
		//nolint:errorlint // Unwrapping here would defeat the per-node classification.
		if suspension, ok := err.(*Suspension); ok {
			if suspension == nil {
				// A typed nil still satisfies error and matches ErrSuspended through
				// Suspension.Unwrap. Preserve that meaning as an anonymous wait
				// instead of normalizing it away into a nil error.
				return suspensionList{{}}, true
			}
			return suspensionList{suspension.clone()}, true
		}
		// Exact identity preserves a direct wrapper's message below.
		//
		//nolint:errorlint,err113 // errors.Is would match wrappers this must not consume.
		if err == ErrSuspended {
			return suspensionList{{}}, true
		}

		if many, ok := err.(interface{ Unwrap() []error }); ok {
			children := many.Unwrap()
			if slices.ContainsFunc(children, func(err error) bool { return err != nil }) {
				return s.collectMany(children)
			}
			return s.collectIdentity()
		}
		if one, ok := err.(interface{ Unwrap() error }); ok {
			child := one.Unwrap()
			if child == nil {
				return s.collectIdentity()
			}
			// Exact identity distinguishes a wrapper directly around ErrSuspended
			// from a wrapper whose deeper tree merely contains one.
			//
			//nolint:errorlint,err113 // errors.Is cannot tell those shapes apart.
			if child == ErrSuspended {
				return suspensionList{{Value: err.Error()}}, true
			}
			// Match the standard errors traversal: a linear chain is a loop, while
			// recursion is reserved for the branches of Unwrap() []error. Besides
			// using less stack, this keeps ordinary wrapping the simpler case.
			s.err = child
			continue
		}
		return s.collectIdentity()
	}
	return nil, false
}

// collectIdentity handles a leaf that participates in errors.Is without being
// a Suspension value. An error exposing only nil children is still a leaf:
// errors.Is checks its Is method before consulting Unwrap, so classification
// must preserve the same meaning.
func (s suspensionTree) collectIdentity() (suspensionList, bool) {
	if errors.Is(s.err, ErrSuspended) {
		return suspensionList{{Value: s.err.Error()}}, true
	}
	return nil, false
}

func (s suspensionTree) collectMany(children []error) (suspensionList, bool) {
	var suspensions suspensionList
	onlySuspensions := true
	childCount := 0
	for _, child := range children {
		if child == nil {
			continue
		}
		childCount++
		found, childOnly := (suspensionTree{err: child}).collect()
		suspensions = append(suspensions, found...)
		onlySuspensions = onlySuspensions && childOnly
	}
	return suspensions, childCount > 0 && onlySuspensions
}

type suspensionList []*Suspension

// identify fills in the workflow boundary that owns an otherwise anonymous
// suspension. ID and Scope are one identity: an existing ID means the wait was
// already identified, including when its nil Scope deliberately names the root
// of an independent nested Run. An anonymous wait is owned wholly by the
// current boundary rather than retaining a caller-supplied partial identity.
func (s suspensionList) identify(id string, scope []ScopeFrame) suspensionList {
	for _, suspension := range s {
		if suspension.ID == "" {
			suspension.ID = id
			suspension.Scope = slices.Clone(scope)
		}
	}
	return s
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
	return &multiSuspension{
		suspensions: s,
		message: fmt.Sprintf(
			"workflow: %d steps suspended: %s",
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
		s.Await.compare(other.Await),
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

// Unwrap returns a copy of each suspension so errors.As finds the first and
// errors.Is matches [ErrSuspended] without exposing multiSuspension's immutable
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
