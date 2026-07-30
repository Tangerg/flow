package expr

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
	"strings"

	"github.com/Tangerg/flow/workflow"
)

// Expr is a compiled expression over a [workflow.Store]. An Expr is immutable
// and safe for concurrent use.
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

	compiler := compiler{source: src}
	eval, err := compiler.compile(node)
	if err != nil {
		return nil, err
	}
	refs := refList(compiler.refs).sortedUnique()
	return &Expr{source: src, eval: eval, refs: refs}, nil
}

type refList []workflow.Ref

func (r refList) sortedUnique() []workflow.Ref {
	r = slices.Clone(r)
	slices.SortFunc(r, func(left, right workflow.Ref) int {
		if order := strings.Compare(left.NodeID, right.NodeID); order != 0 {
			return order
		}
		return strings.Compare(left.Path, right.Path)
	})
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
// depends on them. The returned slice is a copy.
func (e *Expr) Refs() []workflow.Ref { return slices.Clone(e.refs) }

// Eval evaluates the expression against s.
func (e *Expr) Eval(s workflow.Store) (any, error) {
	v, err := e.eval(s)
	if err != nil {
		return nil, e.wrap(err)
	}
	return v, nil
}

// Bool evaluates the expression and requires a bool result. There is no implicit
// truthiness: a non-bool result is an [ErrType] error rather than a silent
// coercion.
func (e *Expr) Bool(s workflow.Store) (bool, error) {
	v, err := e.Eval(s)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, e.wrap(fmt.Errorf("%w: want bool, got %s", ErrType, (operand{raw: v}).typeName()))
	}
	return b, nil
}

// String evaluates the expression and requires a string result.
func (e *Expr) String(s workflow.Store) (string, error) {
	v, err := e.Eval(s)
	if err != nil {
		return "", err
	}
	str, ok := v.(string)
	if !ok {
		return "", e.wrap(fmt.Errorf("%w: want string, got %s", ErrType, (operand{raw: v}).typeName()))
	}
	return str, nil
}

// wrap attaches the expression source to an evaluation error, leaving an error
// that already carries it untouched.
func (e *Expr) wrap(err error) error {
	var exprErr *Error
	if errors.As(err, &exprErr) {
		return err
	}
	return &Error{Source: e.source, Err: err}
}

// compiler turns one AST into a tree of closures, collecting the references it
// reads along the way.
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

func (c *compiler) compile(node ast.Expr) (evalFunc, error) {
	switch n := node.(type) {
	case *ast.ParenExpr:
		return c.compile(n.X)
	case *ast.BasicLit:
		return c.compileLiteral(n)
	case *ast.Ident:
		return c.compileIdent(n)
	case *ast.SelectorExpr, *ast.IndexExpr:
		return c.compileRef(node)
	case *ast.UnaryExpr:
		return c.compileUnary(n)
	case *ast.BinaryExpr:
		return c.compileBinary(n)
	case *ast.CallExpr:
		return c.compileCall(n)
	default:
		return nil, c.unsupported(node, fmt.Sprintf("%T", node))
	}
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

// predeclaredIdents are the only bare identifiers an expression may use, and the
// value each one evaluates to. Keeping the set in one place means a new
// predeclared name cannot be accepted by one walk and rejected by the other.
var predeclaredIdents = map[string]any{
	"true":  true,
	"false": false,
	"nil":   nil,
}

// compileIdent accepts only the predeclared constants. Every other bare
// identifier is a reference missing its path, which is worth saying plainly.
func (c *compiler) compileIdent(id *ast.Ident) (evalFunc, error) {
	if value, predeclared := predeclaredIdents[id.Name]; predeclared {
		return c.constant(value), nil
	}
	return nil, c.errorAt(id, fmt.Errorf(
		"%w: %q is not a reference; a reference needs a node ID and a path, as in %s.output",
		ErrUnsupported, id.Name, id.Name))
}

func (c *compiler) compileRef(node ast.Expr) (evalFunc, error) {
	ref, err := c.reference(node)
	if err != nil {
		return nil, err
	}
	return func(s workflow.Store) (any, error) {
		v, ok := s.Lookup(ref)
		if !ok {
			return nil, fmt.Errorf("%w %s", ErrUndefined, ref)
		}
		return (operand{raw: v}).normalized(), nil
	}, nil
}

// reference flattens a selector and index chain into a [workflow.Ref] and
// records it.
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
	c.refs = append(c.refs, ref)
	return ref, nil
}

// flatten walks a chain such as a.output.items[0] into its root node ID and the
// path segments below it. node["any ID"] is the quoted root form for IDs that
// are not Go identifiers.
func (c *compiler) flatten(node ast.Expr) (string, []string, error) {
	switch n := node.(type) {
	case *ast.Ident:
		return c.flattenIdent(n)
	case *ast.SelectorExpr:
		return c.flattenSelector(n)
	case *ast.IndexExpr:
		return c.flattenIndex(n)
	default:
		return "", nil, c.unsupported(node, "reference may only use names and constant indexes")
	}
}

func (c *compiler) flattenIdent(node *ast.Ident) (string, []string, error) {
	if _, predeclared := predeclaredIdents[node.Name]; predeclared {
		return "", nil, c.errorAt(
			node,
			fmt.Errorf("%w: cannot select into %s", ErrUnsupported, node.Name),
		)
	}
	return node.Name, nil, nil
}

func (c *compiler) flattenSelector(node *ast.SelectorExpr) (string, []string, error) {
	root, segments, err := c.flatten(node.X)
	if err != nil {
		return "", nil, err
	}
	return root, append(segments, node.Sel.Name), nil
}

func (c *compiler) flattenIndex(node *ast.IndexExpr) (string, []string, error) {
	if namespace, ok := node.X.(*ast.Ident); ok && namespace.Name == "node" {
		root, err := c.nodeID(node.Index)
		return root, nil, err
	}
	root, segments, err := c.flatten(node.X)
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

// indexSegment reads a constant index. A Store path is text, so an index must be
// a literal rather than a computed value.
func (c *compiler) indexSegment(index ast.Expr) (string, error) {
	lit, ok := index.(*ast.BasicLit)
	if !ok {
		return "", c.unsupported(index, "index must be an integer or string literal")
	}
	switch lit.Kind {
	case token.INT:
		index, err := strconv.ParseInt(lit.Value, 0, 64)
		if err != nil {
			return "", c.errorAt(lit, fmt.Errorf("%w: index %s: %w", ErrSyntax, lit.Value, err))
		}
		return strconv.FormatInt(index, 10), nil
	case token.STRING:
		s, err := strconv.Unquote(lit.Value)
		if err != nil {
			return "", c.errorAt(lit, fmt.Errorf("%w: index %s: %w", ErrSyntax, lit.Value, err))
		}
		return s, nil
	default:
		return "", c.unsupported(lit, "index must be an integer or string literal")
	}
}

func (c *compiler) compileUnary(n *ast.UnaryExpr) (evalFunc, error) {
	eval, err := c.compile(n.X)
	if err != nil {
		return nil, err
	}
	switch n.Op {
	case token.NOT:
		return func(s workflow.Store) (any, error) {
			v, err := eval(s)
			if err != nil {
				return nil, err
			}
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("%w: ! wants bool, got %s", ErrType, (operand{raw: v}).typeName())
			}
			return !b, nil
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

func (c *compiler) compileBinary(n *ast.BinaryExpr) (evalFunc, error) {
	left, err := c.compile(n.X)
	if err != nil {
		return nil, err
	}
	right, err := c.compile(n.Y)
	if err != nil {
		return nil, err
	}

	// && and || short-circuit, so has(x) && x > 1 is a usable guard.
	switch n.Op {
	case token.LAND, token.LOR:
		stopAt := n.Op == token.LOR
		return func(s workflow.Store) (any, error) {
			lv, err := left.bool(s, n.Op)
			if err != nil {
				return nil, err
			}
			if lv == stopAt {
				return stopAt, nil
			}
			return right.bool(s, n.Op)
		}, nil
	}

	op := n.Op
	if !(binaryOperator{Token: op}).supported() {
		return nil, c.unsupported(n, "operator "+op.String())
	}
	return func(s workflow.Store) (any, error) {
		lv, err := left(s)
		if err != nil {
			return nil, err
		}
		rv, err := right(s)
		if err != nil {
			return nil, err
		}
		return (binaryOperator{Token: op}).apply(operand{raw: lv}, operand{raw: rv})
	}, nil
}

func (c *compiler) compileCall(n *ast.CallExpr) (evalFunc, error) {
	name, ok := n.Fun.(*ast.Ident)
	if !ok {
		return nil, c.unsupported(n.Fun, "call target must be a builtin name")
	}
	if len(n.Args) != 1 || n.Ellipsis != token.NoPos {
		return nil, c.errorAt(n, fmt.Errorf("%w: %s takes exactly one argument", ErrUnsupported, name.Name))
	}

	switch name.Name {
	case "has":
		// has reports whether a reference resolves, so it needs the reference
		// itself rather than the value it would fail to read.
		ref, err := c.reference(n.Args[0])
		if err != nil {
			return nil, err
		}
		return func(s workflow.Store) (any, error) {
			_, ok := s.Lookup(ref)
			return ok, nil
		}, nil
	case "len":
		arg, err := c.compile(n.Args[0])
		if err != nil {
			return nil, err
		}
		return func(s workflow.Store) (any, error) {
			v, err := arg(s)
			if err != nil {
				return nil, err
			}
			return (operand{raw: v}).length()
		}, nil
	default:
		return nil, c.errorAt(n, fmt.Errorf("%w: unknown function %q; expr provides has and len", ErrUnsupported, name.Name))
	}
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
