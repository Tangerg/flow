package flow

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// Sentinel errors returned by the root combinators. Test for them with
// [errors.Is].
var (
	// ErrNilNode is returned when a nil Node or nil NodeFunc is Run through a
	// built-in adapter or composite. Other adapters may use the same sentinel
	// for their own invalid nil value.
	ErrNilNode = errors.New("flow: nil node")
	// ErrNilFunc is returned when a required function argument is nil.
	ErrNilFunc = errors.New("flow: nil function")
	// ErrNoCase is returned by Switch when the resolved key matches no case.
	ErrNoCase = errors.New("flow: no matching case")
	// ErrMaxIterations is returned by Loop when it reaches its iteration cap
	// without the body reporting done.
	ErrMaxIterations = errors.New("flow: max iterations exceeded")
	// ErrNoNodes is returned by Race when given no nodes.
	ErrNoNodes = errors.New("flow: no nodes")
	// ErrInvalidConfig is returned when a combinator's configuration is invalid.
	ErrInvalidConfig = errors.New("flow: invalid config")
)

// IndexError reports an error produced while processing one element of an
// ordered collection. Map and higher-level concurrent combinators use it so
// callers can recover the failing input position with [errors.As] while still
// matching the underlying error with [errors.Is].
type IndexError struct {
	Index int
	Err   error
}

// Error states the position and defers to the cause. This package's sentinels
// carry its name themselves, because most of them reach a caller with nothing
// wrapping them; a location that repeated the name would say it twice, and
// nested locations would say it once per level.
func (i *IndexError) Error() string {
	return formatLocationError(i)
}

// Unwrap returns the underlying element error.
func (i *IndexError) Unwrap() error {
	if i == nil {
		return nil
	}
	return i.Err
}

// CaseError reports an error associated with one case of [Switch]. Key retains
// the value used to register that case, so callers can locate an invalid branch
// with [errors.As] without parsing diagnostic text. Its dynamic type is the K
// accepted by Switch.
type CaseError struct {
	Key any
	Err error
}

// Error states the case key and defers to the cause. Like [IndexError], the
// location does not repeat this package's name; root sentinels carry it and a
// nested error retains the package name of its own boundary.
func (c *CaseError) Error() string {
	return formatLocationError(c)
}

// standardJoinTypes is what [errors.Join] returns, at both arities it could
// specialize. Deriving the types from the constructor avoids depending on an
// unexported standard-library name.
var standardJoinTypes = [...]reflect.Type{
	reflect.TypeOf(errors.Join(ErrInvalidConfig)),
	reflect.TypeOf(errors.Join(ErrInvalidConfig, ErrNilNode)),
}

// standardJoinChildren recognizes only the concrete multi-error produced by
// [errors.Join]. A caller-defined Unwrap() []error remains an opaque application
// error: interpreting its branches could change presentation or ownership rules
// chosen by that type.
//
// The capability comes before the identity because the assertion that yields the
// children is also what proves they can be read. Asking the table first would
// leave the assertion licensed by a type comparison, which is a promise about a
// standard-library implementation rather than about this value.
func standardJoinChildren(err error) ([]error, bool) {
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return nil, false
	}
	typeOf := reflect.TypeOf(err)
	if typeOf != standardJoinTypes[0] && typeOf != standardJoinTypes[1] {
		return nil, false
	}
	return joined.Unwrap(), true
}

// formatLocationError is the one presentation path for exact location wrappers
// owned by this package. A composite can freely nest collection and selection
// locations across standard [errors.Join] branches without turning their depth
// into call-stack depth. Every other error remains opaque, so this package never
// reinterprets a caller's wrapper.
func formatLocationError(err error) string {
	formatter := locationFormatter{tasks: []locationFormatTask{{err: err}}}
	return formatter.format()
}

type locationFormatTask struct {
	err     error
	newline bool
}

type locationFormatter struct {
	message strings.Builder
	tasks   []locationFormatTask
}

func (f *locationFormatter) format() string {
	for len(f.tasks) > 0 {
		last := len(f.tasks) - 1
		task := f.tasks[last]
		f.tasks = f.tasks[:last]
		f.render(task)
	}
	return f.message.String()
}

func (f *locationFormatter) render(task locationFormatTask) {
	if task.newline {
		f.message.WriteByte('\n')
	}
	err := task.err
	for {
		if children, joined := standardJoinChildren(err); joined {
			for index, child := range slices.Backward(children) {
				f.tasks = append(f.tasks, locationFormatTask{err: child, newline: index > 0})
			}
			return
		}

		//nolint:errorlint // Exact wrapper identity determines formatting ownership.
		switch located := err.(type) {
		case *IndexError:
			if located == nil {
				fmt.Fprint(&f.message, nil)
				return
			}
			fmt.Fprintf(&f.message, "index %d: ", located.Index)
			err = located.Err
		case *CaseError:
			if located == nil {
				fmt.Fprint(&f.message, nil)
				return
			}
			fmt.Fprintf(&f.message, "switch case %#v: ", located.Key)
			err = located.Err
		default:
			fmt.Fprint(&f.message, err)
			return
		}
	}
}

// Unwrap returns the underlying case error.
func (c *CaseError) Unwrap() error {
	if c == nil {
		return nil
	}
	return c.Err
}
