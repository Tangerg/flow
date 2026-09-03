package workflow

import (
	"fmt"
	"slices"
	"strings"

	"github.com/Tangerg/flow"
)

// Rendering an owned error tree as one message: each location this package owns
// contributes its prefix, a joined branch continues on its own line, and a
// failed reference adds what it wanted after the cause that explains why.

// errorFormatter renders the owned part of an error tree. Applications may
// construct its exported locations directly, so neither linear depth nor
// [errors.Join] depth may consume the call stack. A caller-defined wrapper or
// multi-error remains one opaque cause.
type errorFormatter struct {
	message strings.Builder
	tasks   []errorFormatTask
}

type errorFormatTask struct {
	err        error
	newline    bool
	references []*RefError
	cause      error
	suffix     bool
}

// errorFormatFrame answers two questions with one value: whether this package
// renders the wrapper, and whether anything below it still has to be rendered.
// Both send the walk to finish, which is why a typed nil location -- owned, with
// nothing under it -- reads the same however either flag is spelled: the loop
// would find nil next and stop there instead.
type errorFormatFrame struct {
	next      error
	reference *RefError
	owned     bool
	terminal  bool
}

func (e ownedError) format() string {
	formatter := errorFormatter{tasks: []errorFormatTask{{err: e.root}}}
	return formatter.format()
}

func (f *errorFormatter) format() string {
	for len(f.tasks) > 0 {
		last := len(f.tasks) - 1
		task := f.tasks[last]
		f.tasks = f.tasks[:last]
		f.render(task)
	}
	return f.message.String()
}

func (f *errorFormatter) render(task errorFormatTask) {
	if task.suffix {
		f.appendMismatches(task.references, task.cause)
		return
	}
	if task.newline {
		f.message.WriteByte('\n')
	}

	err := task.err
	var references []*RefError
	for {
		if children, joined := standardJoinChildren(err); joined {
			f.scheduleJoin(children, references, err)
			return
		}
		frame := f.appendFrame(err)
		if !frame.owned || frame.terminal {
			f.finish(references, frame.next)
			return
		}
		if frame.reference != nil {
			references = append(references, frame.reference)
		}
		err = frame.next
	}
}

// scheduleJoin queues the branches of a join, and the mismatch suffix that
// belongs to the references collected on the way to it. The suffix goes on
// first so it renders last, after every branch.
//
// Its guard is not observable: a suffix task with no references appends nothing,
// so scheduling one unconditionally renders the same message. It stays because
// that task is not free -- deciding whether to print a mismatch walks the cause's
// error tree for [ErrNotFound] -- and because a suffix with nothing to say is a
// thing this walk should not schedule.
func (f *errorFormatter) scheduleJoin(children []error, references []*RefError, cause error) {
	if len(references) > 0 {
		f.tasks = append(f.tasks, errorFormatTask{
			references: references,
			cause:      cause,
			suffix:     true,
		})
	}
	for index, child := range slices.Backward(children) {
		f.tasks = append(f.tasks, errorFormatTask{
			err:     child,
			newline: index > 0,
		})
	}
}

// appendFrame consumes one exact engine-owned location. A type assertion via
// [errors.As] would cross an application wrapper and change its presentation.
//
//nolint:errorlint // Exact wrapper identity determines formatting ownership.
func (f *errorFormatter) appendFrame(err error) errorFormatFrame {
	switch frame := err.(type) {
	case errorPrefixAppender:
		valid, next := frame.appendErrorPrefix(&f.message)
		reference, _ := frame.(*RefError)
		return errorFormatFrame{
			next:      next,
			reference: reference,
			owned:     true,
			terminal:  !valid,
		}
	case *flow.IndexError:
		if frame == nil {
			return errorFormatFrame{owned: true, terminal: true}
		}
		fmt.Fprintf(&f.message, "index %d: ", frame.Index)
		return errorFormatFrame{next: frame.Err, owned: true}
	case *flow.CaseError:
		if frame == nil {
			return errorFormatFrame{owned: true, terminal: true}
		}
		fmt.Fprintf(&f.message, "switch case %#v: ", frame.Key)
		return errorFormatFrame{next: frame.Err, owned: true}
	default:
		return errorFormatFrame{next: err}
	}
}

func (f *errorFormatter) finish(references []*RefError, cause error) {
	fmt.Fprint(&f.message, cause)
	f.appendMismatches(references, cause)
}

func (f *errorFormatter) appendMismatches(references []*RefError, cause error) {
	if (errorTree{root: cause}).matches(ErrNotFound) {
		return
	}
	for _, reference := range slices.Backward(references) {
		reference.appendMismatch(&f.message)
	}
}
