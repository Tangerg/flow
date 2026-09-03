package expr

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// Expr is a compiled expression over a [workflow.Store]. Values are produced by
// [Parse] or [MustParse]; the zero value and a nil *Expr are not compiled
// expressions. A compiled Expr is immutable and safe for concurrent use.
type Expr struct {
	source string
	eval   evalFunc
	refs   []workflow.Ref
}

// evalFunc is one compiled node. Compiling to closures means the supported
// grammar is the compiler: there is no code path that evaluates a construct
// [Parse] rejected.
type evalFunc func(workflow.Store) (any, error)

// Parse compiles src, reporting every unsupported construct up front so a
// malformed expression fails when a workflow is built rather than mid-run.
func Parse(src string) (*Expr, error) {
	node, err := parser.ParseExpr(src)
	if err != nil {
		return nil, &Error{Source: src, Err: fmt.Errorf("%w: %w", ErrSyntax, err)}
	}

	c := compiler{source: src}
	eval, err := c.compile(node, 0)
	if err != nil {
		return nil, err
	}
	refs := refList(c.refs).sortedUnique()
	return &Expr{source: src, eval: eval, refs: refs}, nil
}

type refList []workflow.Ref

// sortedUnique is the reference list this package hands back: the canonical
// reference order, which [workflow.Ref.Compare] owns, with duplicates removed.
// An editor compares one of these against what a graph produces, so the order has
// to be the same one workflow's own results use rather than a second one.
func (r refList) sortedUnique() []workflow.Ref {
	r = slices.Clone(r)
	slices.SortFunc(r, workflow.Ref.Compare)
	return slices.Compact(r)
}

// MustParse is like [Parse] but panics on error. Use it for expressions fixed at
// compile time; parse caller- or config-supplied text with [Parse].
func MustParse(src string) *Expr {
	e, err := Parse(src)
	if err != nil {
		panic(err)
	}
	return e
}

// Source returns the expression text the Expr was parsed from.
func (e *Expr) Source() string { return e.source }

// Refs returns the Store references the expression reads, deduplicated and
// sorted. References inside has() are included, since the expression still
// depends on them. The returned slice is a fresh value the caller owns.
func (e *Expr) Refs() []workflow.Ref { return slices.Clone(e.refs) }

// Eval evaluates the expression against s and requires a T result. There is no
// implicit truthiness or conversion: a result of another type is an [ErrType]
// error. Use T = any when the expression's dynamic result is wanted. A mutable
// result read directly from the Store is borrowed and must not be modified;
// expression evaluation does not turn Store values into owned copies. The zero
// Expr and a nil *Expr return an error wrapping [flow.ErrInvalidConfig].
func (e *Expr) Eval[T any](s workflow.Store) (T, error) {
	var zero T
	if err := e.validate(); err != nil {
		return zero, err
	}
	value, err := e.eval(s)
	if err != nil {
		return zero, e.wrap(err)
	}
	target := reflect.TypeFor[T]()
	if value == nil && nilAssignable(target.Kind()) {
		return zero, nil
	}
	typed, ok := value.(T)
	if !ok {
		return zero, e.wrap(fmt.Errorf(
			"%w: want %s, got %s",
			ErrType,
			target,
			(operand{raw: value}).typeName(),
		))
	}
	return typed, nil
}

// validate checks the one invariant Parse establishes: an Expr has compiled
// evaluation code. The concrete type is exported, so its zero value and a nil
// pointer remain constructible even though neither is a compiled expression;
// returning a structured definition error keeps those states from turning an
// Eval or a workflow registration into a delayed panic.
func (e *Expr) validate() error {
	if e != nil && e.eval != nil {
		return nil
	}
	source := ""
	if e != nil {
		source = e.source
	}
	return &Error{
		Source: source,
		Err: fmt.Errorf(
			"%w: expression is not compiled",
			flow.ErrInvalidConfig,
		),
	}
}

// nilAssignable reports the Go kinds to which an untyped nil result can be
// assigned. Eval owns this generic type boundary so a nil expression or Store
// value is accepted for T = any and other nilable types without weakening exact
// type checks for concrete values.
func nilAssignable(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice,
		reflect.UnsafePointer:
		return true
	default:
		return false
	}
}

func (e *Expr) wrap(err error) error {
	if _, wrapped := errors.AsType[*Error](err); wrapped {
		return err
	}
	return &Error{Source: e.source, Err: err}
}

// compiler turns one AST into a tree of closures, collecting the references it
// reads along the way. Only what the whole expression accumulates lives here;
// how deep a node sits is passed down, the way [compiler.flattenAt] passes it,
// because a field would have to be put back on every return.
type compiler struct {
	source string
	refs   []workflow.Ref
}

func (c *compiler) errorAt(node ast.Node, err error) error {
	return &Error{Source: c.source, Pos: int(node.Pos()), Err: err}
}

func (c *compiler) unsupported(node ast.Node, what string) error {
	return c.errorAt(node, fmt.Errorf("%w: %s", ErrUnsupported, what))
}

// compile turns one node into its closure. depth is how many nodes enclose it,
// so the limit bounds nesting alone: a wide expression stays compilable however
// many operands it has -- see TestParse_boundsNestingNotBreadth.
func (c *compiler) compile(node ast.Expr, depth int) (evalFunc, error) {
	if depth >= workflow.MaxNestingDepth {
		return nil, c.depthError(node)
	}

	switch n := node.(type) {
	case *ast.ParenExpr:
		return c.compile(n.X, depth+1)
	case *ast.BasicLit:
		return c.compileLiteral(n)
	case *ast.Ident:
		return c.compileIdent(n)
	case *ast.SelectorExpr, *ast.IndexExpr:
		return c.compileRef(node)
	case *ast.UnaryExpr:
		return c.compileUnary(n, depth)
	case *ast.BinaryExpr:
		return c.compileBinary(n, depth)
	case *ast.CallExpr:
		return c.compileCall(n, depth)
	default:
		return nil, c.unsupported(node, fmt.Sprintf("%T", node))
	}
}

func (c *compiler) depthError(node ast.Node) error {
	return c.errorAt(node, fmt.Errorf(
		"%w: %w: depth exceeds limit %d",
		ErrUnsupported,
		workflow.ErrMaxDepth,
		workflow.MaxNestingDepth,
	))
}

func (c *compiler) compileLiteral(lit *ast.BasicLit) (evalFunc, error) {
	switch lit.Kind {
	case token.INT:
		n, err := strconv.ParseInt(lit.Value, 0, 64)
		if err == nil {
			return c.constant(n), nil
		}
		u, unsignedErr := strconv.ParseUint(lit.Value, 0, 64)
		if unsignedErr == nil {
			return c.constant(u), nil
		}
		return nil, c.errorAt(lit, fmt.Errorf("%w: integer literal %s: %w", ErrSyntax, lit.Value, err))
	case token.FLOAT:
		f, err := strconv.ParseFloat(lit.Value, 64)
		if err != nil {
			return nil, c.errorAt(lit, fmt.Errorf("%w: float literal %s: %w", ErrSyntax, lit.Value, err))
		}
		return c.constant(f), nil
	case token.STRING:
		s, err := strconv.Unquote(lit.Value)
		if err != nil {
			return nil, c.errorAt(lit, fmt.Errorf("%w: string literal %s: %w", ErrSyntax, lit.Value, err))
		}
		return c.constant(s), nil
	default:
		return nil, c.unsupported(lit, strings.ToLower(lit.Kind.String())+" literal")
	}
}

func (c *compiler) compileIdent(id *ast.Ident) (evalFunc, error) {
	if value, predeclared := predeclaredIdent(id.Name); predeclared {
		return c.constant(value), nil
	}
	return nil, c.errorAt(id, fmt.Errorf(
		"%w: %q is not a reference; a reference needs a node ID and a path, as in %s.output",
		ErrUnsupported, id.Name, id.Name))
}

// predeclaredIdent is the single vocabulary shared by bare-expression and
// reference-root validation. A switch keeps this immutable grammar rule out of
// mutable package state.
func predeclaredIdent(name string) (any, bool) {
	switch name {
	case "true":
		return true, true
	case "false":
		return false, true
	case "nil":
		return nil, true
	default:
		return nil, false
	}
}

func (c *compiler) compileRef(node ast.Expr) (evalFunc, error) {
	ref, err := c.reference(node)
	if err != nil {
		return nil, err
	}
	return func(s workflow.Store) (any, error) {
		return resolveReference(s, ref)
	}, nil
}

// resolveReference is the one expression-side Store read boundary. Missing
// values become ErrUndefined; malformed JSON views and conversion failures
// remain type errors, including when the caller is has().
func resolveReference(store workflow.Store, ref workflow.Ref) (any, error) {
	value, err := store.Get[any](ref)
	if errors.Is(err, workflow.ErrNotFound) {
		return nil, fmt.Errorf("%w %s", ErrUndefined, ref)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: read %s: %w", ErrType, ref, err)
	}
	return (operand{raw: value}).normalized(), nil
}

func (c *compiler) reference(node ast.Expr) (workflow.Ref, error) {
	root, segments, err := c.flatten(node)
	if err != nil {
		return workflow.Ref{}, err
	}
	if len(segments) == 0 {
		return workflow.Ref{}, c.errorAt(node, fmt.Errorf(
			"%w: %q is not a reference; a reference needs a node ID and a path", ErrUnsupported, root))
	}
	ref := workflow.At(root, segments[0], segments[1:]...)
	if err := ref.Validate(); err != nil {
		return workflow.Ref{}, c.errorAt(node, fmt.Errorf(
			"%w: invalid reference: %w",
			ErrUnsupported,
			err,
		))
	}
	c.refs = append(c.refs, ref)
	return ref, nil
}

// flatten walks a chain such as a.output.items[0] into its root node ID and the
// path segments below it. node["any ID"] is the quoted root form for IDs that
// are not Go identifiers.
func (c *compiler) flatten(node ast.Expr) (string, []string, error) {
	return c.flattenAt(node, 0)
}

func (c *compiler) flattenAt(node ast.Expr, depth int) (string, []string, error) {
	if depth >= workflow.MaxNestingDepth {
		return "", nil, c.depthError(node)
	}
	switch n := node.(type) {
	case *ast.Ident:
		return c.flattenIdent(n)
	case *ast.SelectorExpr:
		return c.flattenSelector(n, depth)
	case *ast.IndexExpr:
		return c.flattenIndex(n, depth)
	default:
		return "", nil, c.unsupported(node, "reference may only use names and constant indexes")
	}
}

func (c *compiler) flattenIdent(node *ast.Ident) (string, []string, error) {
	if _, predeclared := predeclaredIdent(node.Name); predeclared {
		return "", nil, c.errorAt(
			node,
			fmt.Errorf("%w: cannot select into %s", ErrUnsupported, node.Name),
		)
	}
	return node.Name, nil, nil
}

func (c *compiler) flattenSelector(
	node *ast.SelectorExpr,
	depth int,
) (string, []string, error) {
	root, segments, err := c.flattenAt(node.X, depth+1)
	if err != nil {
		return "", nil, err
	}
	return root, append(segments, node.Sel.Name), nil
}

func (c *compiler) flattenIndex(
	node *ast.IndexExpr,
	depth int,
) (string, []string, error) {
	if namespace, ok := node.X.(*ast.Ident); ok && namespace.Name == "node" {
		root, err := c.nodeID(node.Index)
		return root, nil, err
	}
	root, segments, err := c.flattenAt(node.X, depth+1)
	if err != nil {
		return "", nil, err
	}
	segment, err := c.indexSegment(node.Index)
	if err != nil {
		return "", nil, err
	}
	return root, append(segments, segment), nil
}

func (c *compiler) nodeID(index ast.Expr) (string, error) {
	lit, ok := index.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", c.unsupported(index, `node ID must be a non-empty string literal`)
	}
	id, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", c.errorAt(lit, fmt.Errorf("%w: node ID %s: %w", ErrSyntax, lit.Value, err))
	}
	if id == "" {
		return "", c.errorAt(lit, fmt.Errorf("%w: node ID must not be empty", ErrUnsupported))
	}
	return id, nil
}

// indexSegment requires a literal: a Store path is text, so an index cannot be
// computed.
func (c *compiler) indexSegment(index ast.Expr) (string, error) {
	lit, ok := index.(*ast.BasicLit)
	if !ok {
		return "", c.unusableIndex(index)
	}
	switch lit.Kind {
	case token.INT:
		value, err := strconv.ParseInt(lit.Value, 0, 64)
		if err != nil {
			return "", c.malformedIndex(lit, err)
		}
		return strconv.FormatInt(value, 10), nil
	case token.STRING:
		segment, err := strconv.Unquote(lit.Value)
		if err != nil {
			return "", c.malformedIndex(lit, err)
		}
		return segment, nil
	default:
		return "", c.unusableIndex(lit)
	}
}

func (c *compiler) unusableIndex(node ast.Node) error {
	return c.unsupported(node, "index must be an integer or string literal")
}

func (c *compiler) malformedIndex(lit *ast.BasicLit, cause error) error {
	return c.errorAt(lit, fmt.Errorf("%w: index %s: %w", ErrSyntax, lit.Value, cause))
}

func (c *compiler) compileUnary(n *ast.UnaryExpr, depth int) (evalFunc, error) {
	eval, err := c.compile(n.X, depth+1)
	if err != nil {
		return nil, err
	}
	switch n.Op {
	case token.NOT:
		return func(s workflow.Store) (any, error) {
			// The operator names itself in the failure, so ! states its own spelling
			// once -- here it is the same rule && and || ask for, and one message.
			value, err := eval.bool(s, n.Op)
			if err != nil {
				return nil, err
			}
			return !value, nil
		}, nil
	case token.SUB:
		return func(s workflow.Store) (any, error) {
			v, err := eval(s)
			if err != nil {
				return nil, err
			}
			return (operand{raw: v}).negate()
		}, nil
	default:
		return nil, c.unsupported(n, "unary "+n.Op.String())
	}
}

func (c *compiler) compileBinary(n *ast.BinaryExpr, depth int) (evalFunc, error) {
	left, err := c.compile(n.X, depth+1)
	if err != nil {
		return nil, err
	}
	right, err := c.compile(n.Y, depth+1)
	if err != nil {
		return nil, err
	}

	if n.Op == token.LAND || n.Op == token.LOR {
		return shortCircuit(left, right, n.Op), nil
	}

	// The operator is built once and captured, so the closure holds the operator
	// rather than the AST node it came from.
	operator := binaryOperator{Token: n.Op}
	if !operator.supported() {
		return nil, c.unsupported(n, "operator "+operator.String())
	}
	return operator.eval(left, right), nil
}

// shortCircuit evaluates the right operand only when the left one has not already
// decided the answer, which is what makes has(x) && x > 1 a usable guard. Both
// operands are read as booleans, so op names the operator each conversion failure
// belongs to.
func shortCircuit(left, right evalFunc, op token.Token) evalFunc {
	stopAt := op == token.LOR
	return func(s workflow.Store) (any, error) {
		decided, err := left.bool(s, op)
		if err != nil {
			return nil, err
		}
		if decided == stopAt {
			return stopAt, nil
		}
		return right.bool(s, op)
	}
}

// eval belongs to the operator because nothing of the expression it came from
// survives compilation.
func (b binaryOperator) eval(left, right evalFunc) evalFunc {
	return func(s workflow.Store) (any, error) {
		leftValue, err := left(s)
		if err != nil {
			return nil, err
		}
		rightValue, err := right(s)
		if err != nil {
			return nil, err
		}
		return b.apply(operand{raw: leftValue}, operand{raw: rightValue})
	}
}

func (c *compiler) compileCall(n *ast.CallExpr, depth int) (evalFunc, error) {
	name, ok := n.Fun.(*ast.Ident)
	if !ok {
		return nil, c.unsupported(n.Fun, "call target must be a builtin name")
	}
	switch name.Name {
	case "has":
		return c.compileHas(n)
	case "len":
		return c.compileLen(n, depth)
	default:
		return nil, c.errorAt(n, fmt.Errorf(
			"%w: unknown function %q; the only functions are has and len",
			ErrUnsupported, name.Name))
	}
}

// compileHas compiles has(ref). It takes the reference itself rather than a
// compiled argument, because absence is the answer here instead of an evaluation
// error. Every other read failure still propagates: malformed data is not the
// same as no data.
func (c *compiler) compileHas(n *ast.CallExpr) (evalFunc, error) {
	if err := c.oneArgument(n, "has"); err != nil {
		return nil, err
	}
	ref, err := c.reference(n.Args[0])
	if err != nil {
		return nil, err
	}
	return func(s workflow.Store) (any, error) {
		_, err := resolveReference(s, ref)
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, ErrUndefined):
			return false, nil
		default:
			return nil, err
		}
	}, nil
}

func (c *compiler) compileLen(n *ast.CallExpr, depth int) (evalFunc, error) {
	if err := c.oneArgument(n, "len"); err != nil {
		return nil, err
	}
	arg, err := c.compile(n.Args[0], depth+1)
	if err != nil {
		return nil, err
	}
	return func(s workflow.Store) (any, error) {
		value, err := arg(s)
		if err != nil {
			return nil, err
		}
		return (operand{raw: value}).length()
	}, nil
}

// oneArgument checks the arity of a call this package recognizes. Its caller
// matches the name first: reporting an arity for a function that does not exist
// would describe a signature the caller cannot use.
func (c *compiler) oneArgument(n *ast.CallExpr, name string) error {
	if len(n.Args) != 1 || n.Ellipsis != token.NoPos {
		return c.errorAt(n, fmt.Errorf("%w: %s takes exactly one argument", ErrUnsupported, name))
	}
	return nil
}

func (e evalFunc) bool(s workflow.Store, op token.Token) (bool, error) {
	v, err := e(s)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("%w: %s wants bool, got %s", ErrType, op.String(), (operand{raw: v}).typeName())
	}
	return b, nil
}

func (*compiler) constant(v any) evalFunc {
	return func(workflow.Store) (any, error) { return v, nil }
}
