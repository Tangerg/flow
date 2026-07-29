package workflow

import (
	"errors"
	"fmt"
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
type Suspension struct {
	// ID is the step that suspended.
	ID string `json:"id,omitempty"`
	// Path is the step's enclosing repeated scopes, as on [Event.Path].
	Path []string `json:"path,omitempty"`
	// Await is the reference whose absence caused the suspension, if any.
	Await Ref `json:"await,omitzero"`
	// Value is application-owned and must be treated as immutable. A caller that
	// persists a Suspension is responsible for using a codec that can represent
	// the concrete value.
	Value any `json:"value,omitempty"`
}

func (suspension *Suspension) Error() string {
	if suspension == nil {
		return ErrSuspended.Error()
	}
	var message strings.Builder
	message.WriteString("workflow:")
	if suspension.ID != "" {
		message.WriteString(" step ")
		message.WriteString(strconv.Quote(suspension.ID))
	}
	if len(suspension.Path) > 0 {
		message.WriteString(" in ")
		message.WriteString(strings.Join(suspension.Path, "/"))
	}
	message.WriteString(" suspended")
	reason, _ := suspension.Value.(string)
	switch {
	case reason != "":
		message.WriteString(": ")
		message.WriteString(reason)
	case suspension.Await != (Ref{}):
		message.WriteString(": awaiting ")
		message.WriteString(suspension.Await.String())
	}
	return message.String()
}

// Unwrap returns [ErrSuspended].
func (suspension *Suspension) Unwrap() error { return ErrSuspended }

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

// Key returns the structured identity of the suspended step. The returned Path
// is a copy. It is the key to pass to [Journal.Record] when supplying the result
// of an [Interrupt] or another journaled boundary that called [Suspend].
func (suspension *Suspension) Key() JournalKey {
	if suspension == nil {
		return JournalKey{}
	}
	return JournalKey{ID: suspension.ID, Path: slices.Clone(suspension.Path)}
}

// suspensionTree owns classification of the standard Go error tree.
type suspensionTree struct {
	err error
}

// suspensions reports the suspension leaves and whether every error leaf is a
// suspension. Composites use the second result to keep a joined failure from
// being mistaken for "not yet".
func (tree suspensionTree) suspensions() (suspensionList, bool) {
	suspensions, only := tree.collect()
	if len(suspensions) == 0 {
		return nil, false
	}
	return suspensions.normalized(), only
}

// collect walks both forms supported by the standard error tree:
// Unwrap() error and Unwrap() []error. It returns copies so identifying a wait
// at a workflow boundary never mutates an error owned by its caller.
func (tree suspensionTree) collect() (suspensionList, bool) {
	err := tree.err
	if err == nil {
		return nil, false
	}
	// This intentionally inspects the current node rather than using errors.As:
	// As would skip wrappers and could misclassify a mixed joined tree as a pure
	// suspension tree.
	if suspension, ok := err.(*Suspension); ok {
		if suspension == nil {
			// A typed nil still satisfies error and matches ErrSuspended through
			// Suspension.Unwrap. Preserve that meaning as an anonymous wait
			// instead of normalizing it away into a nil error.
			return suspensionList{{}}, true
		}
		return suspensionList{suspension.clone()}, true
	}
	// Exact identity preserves a wrapper's own message in collectOne below.
	if err == ErrSuspended {
		return suspensionList{{}}, true
	}

	if many, ok := err.(interface{ Unwrap() []error }); ok {
		return tree.collectMany(many.Unwrap())
	}
	if one, ok := err.(interface{ Unwrap() error }); ok {
		return tree.collectOne(one.Unwrap())
	}

	// A custom error may participate in errors.Is without exposing an unwrap.
	if errors.Is(err, ErrSuspended) {
		return suspensionList{{Value: err.Error()}}, true
	}
	return nil, false
}

func (tree suspensionTree) collectMany(children []error) (suspensionList, bool) {
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

func (tree suspensionTree) collectOne(child error) (suspensionList, bool) {
	// Exact identity distinguishes a wrapper directly around ErrSuspended from
	// a wrapper whose deeper tree merely contains one.
	if child == ErrSuspended {
		return suspensionList{{Value: tree.err.Error()}}, true
	}
	return (suspensionTree{err: child}).collect()
}

type suspensionList []*Suspension

// identify fills in the workflow boundary that owns an otherwise anonymous
// suspension. Already-identified nested waits keep their identity.
func (list suspensionList) identify(id string, path []string) suspensionList {
	for _, suspension := range list {
		if suspension.ID == "" {
			suspension.ID = id
		}
		if suspension.Path == nil {
			suspension.Path = slices.Clone(path)
		}
	}
	return list
}

// err reports the suspensions of a fan-out as one error. Several branches may
// be waiting at once, and a caller needs every reason to know what to supply
// before resuming.
func (list suspensionList) err() error {
	list = list.normalized()
	switch len(list) {
	case 0:
		return nil
	case 1:
		return list[0]
	}
	reasons := make([]string, 0, len(list))
	for _, suspension := range list {
		reasons = append(reasons, suspension.Error())
	}
	return &multiSuspension{
		suspensions: list,
		message: fmt.Sprintf(
			"workflow: %d steps suspended: %s",
			len(list),
			strings.Join(reasons, "; "),
		),
	}
}

func (list suspensionList) normalized() suspensionList {
	normalized := make(suspensionList, 0, len(list))
	for _, suspension := range list {
		if suspension != nil {
			normalized = append(normalized, suspension.clone())
		}
	}
	slices.SortStableFunc(normalized, func(left, right *Suspension) int {
		return left.compare(right)
	})
	return normalized
}

func (suspension *Suspension) compare(other *Suspension) int {
	if order := strings.Compare(suspension.ID, other.ID); order != 0 {
		return order
	}
	if order := slices.Compare(suspension.Path, other.Path); order != 0 {
		return order
	}
	return suspension.Await.compare(other.Await)
}

// clone copies a suspension and its path so identifying a wait at a workflow
// boundary never mutates an error owned by a caller. Callers filter nil entries
// first, so the receiver is never nil.
func (suspension *Suspension) clone() *Suspension {
	clone := *suspension
	clone.Path = slices.Clone(suspension.Path)
	return &clone
}

// multiSuspension carries every suspension of one fan-out.
type multiSuspension struct {
	suspensions []*Suspension
	message     string
}

func (multi *multiSuspension) Error() string { return multi.message }

// Unwrap returns each suspension so errors.As finds the first and errors.Is
// matches [ErrSuspended].
func (multi *multiSuspension) Unwrap() []error {
	errs := make([]error, len(multi.suspensions))
	for index, suspension := range multi.suspensions {
		errs[index] = suspension
	}
	return errs
}

// Suspensions returns every suspension in err's error tree, ordered by step ID
// and then scope. A run that stopped in one place yields one; nested fan-out may
// yield several. The returned Suspension values and their paths are copies;
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
// The supplied suspensions and their paths are copied.
//
// Caller-defined fan-out composites use JoinSuspensions after allowing every
// suspended branch to finish. Use [SuspendedOnly] to distinguish suspensions
// from ordinary failures before collecting them with [Suspensions].
func JoinSuspensions(suspensions ...*Suspension) error {
	return suspensionList(suspensions).err()
}
