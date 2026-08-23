package flow_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// modulePath is this module's import path. A rename that missed this constant
// would leave no import matching it, which the count check below reports rather
// than passing as a repository with no edges.
const modulePath = "github.com/Tangerg/flow"

// moduleImports is the dependency direction of this module, written out: for each
// package, which others it may import. flow is the primitive layer and depends on
// nothing here, flowx derives combinators from it, workflow adds named state and
// serialization, expr and diagram stay optional and derived, and the internal
// packages are shared implementation detail that may not depend on any of it.
//
// Each entry states what its layer permits rather than what it currently uses, so
// the table is a rule and not a snapshot of the imports. What it forbids is the
// edge that reads as harmless in review and inverts the module: a sibling reaching
// sideways, a primitive borrowing a private helper, or an internal package
// depending on the layer it exists to serve.
var moduleImports = map[string][]string{
	".":                {},
	"internal/ctxtest": {},
	"internal/jsondoc": {},
	"internal/jsonnum": {},
	"flowx":            {"."},
	"workflow":         {".", "internal/jsondoc", "internal/jsonnum"},
	"workflow/expr":    {".", "workflow", "internal/jsondoc", "internal/jsonnum"},
	"workflow/diagram": {".", "workflow"},
	// The runnable examples are documentation, so they may use every public
	// package and none of the internal ones: an example that needed a private
	// helper would be teaching an API a caller cannot reach.
	"example": {".", "flowx", "workflow", "workflow/expr", "workflow/diagram"},
}

// TestPackageDependenciesPointOneWay is the one axiom of this module that is pure
// structure: every other one is about what a package means, and is held by tests
// of its behavior. This one is a property of the import graph, which no behavior
// can reveal — a workflow that imported flowx, or a jsondoc that imported
// workflow, would pass every test in the repository while making the layer
// boundaries fiction.
//
// Test files are held to the same layering, so the primitive layer stays testable
// without the rest of the module. They may additionally reach internal/ctxtest,
// the cancellation probe each layer's own tests use, which exists for them alone.
func TestPackageDependenciesPointOneWay(t *testing.T) {
	fileSet := token.NewFileSet()
	edges := 0
	walkRepository(t, ".go", func(name string, data []byte) {
		directory := path.Dir(name)
		allowed, known := moduleImports[directory]
		if !known {
			t.Fatalf("%s: package %s is not in moduleImports", name, directory)
		}
		if strings.HasSuffix(name, "_test.go") {
			allowed = append(allowed, "internal/ctxtest")
		}

		file, err := parser.ParseFile(fileSet, name, data, parser.ImportsOnly|parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imported := range file.Imports {
			target, ok := modulePackage(t, name, imported.Path.Value)
			if !ok {
				continue
			}
			edges++
			// A package always sees itself: an external test package imports the
			// package it tests.
			if target == directory || slices.Contains(allowed, target) {
				continue
			}
			t.Errorf(
				"%s:%d: %s may not import %s",
				name,
				fileSet.Position(imported.Pos()).Line,
				packageName(directory),
				packageName(target),
			)
		}
	})
	if edges == 0 {
		t.Fatal("no import of this module found; the walk stopped seeing the repository")
	}
}

// TestModuleImportsCoversEveryPackage keeps the table above from silently
// permitting a package it does not mention. Every entry has to name a package that
// exists, so a directory that was renamed or removed cannot leave a stale
// allowance behind that reads as a stated rule.
func TestModuleImportsCoversEveryPackage(t *testing.T) {
	found := make(map[string]struct{})
	walkRepository(t, ".go", func(name string, _ []byte) {
		found[path.Dir(name)] = struct{}{}
	})
	for _, directory := range slices.Sorted(maps.Keys(moduleImports)) {
		if _, ok := found[directory]; !ok {
			t.Errorf("moduleImports names %s, which holds no Go source", directory)
		}
		for _, allowed := range moduleImports[directory] {
			if _, ok := moduleImports[allowed]; !ok {
				t.Errorf("%s is allowed to import %s, which is not a package of this module", directory, allowed)
			}
		}
	}
}

// modulePackage converts a quoted import path into the module-relative directory
// it names. The bool reports whether the import belongs to this module at all.
func modulePackage(t *testing.T, name, quoted string) (string, bool) {
	t.Helper()
	imported, err := strconv.Unquote(quoted)
	if err != nil {
		t.Fatalf("%s: import path %s: %v", name, quoted, err)
	}
	if imported == modulePath {
		return ".", true
	}
	relative, inside := strings.CutPrefix(imported, modulePath+"/")
	return relative, inside
}

// packageName spells a directory the way the axiom does, so a failure reads as the
// rule it broke rather than as a path.
func packageName(directory string) string {
	if directory == "." {
		return "flow"
	}
	return directory
}

// wireDecodeExceptions names the types whose UnmarshalJSON cannot route through
// the shared boundary, with the reason it cannot. An exception has to be listed
// here rather than merely written differently, because a hand-rolled decoder that
// assigns before checking its error looks exactly like one that does not.
var wireDecodeExceptions = map[string]string{
	"Journal": "owns a mutex, so it cannot be replaced by value, and its decoded " +
		"records continue the revision it is already at rather than replacing it",
}

// TestEveryUnmarshalJSONDecodesThroughOneBoundary holds the wire layer to one
// decoding promise: a nil receiver reported rather than a panic, the whole
// document read before anything is assigned, and the destination replaced only
// after complete success. That promise is three separate things to get right, and
// this repository has watched it drift twice — eight types once implemented it
// independently, and two later grew their own again.
//
// The check is structural because the interesting half is not observable: a
// decoder that assigns a partial value and then fails returns the same error as one
// that does not.
//
// It asks about the exported types, which are the ones a caller hands to
// json.Unmarshal. An unexported adapter such as graphJSON is the inside of a decode
// function, reached only from within the boundary this test is about.
func TestEveryUnmarshalJSONDecodesThroughOneBoundary(t *testing.T) {
	fileSet := token.NewFileSet()
	shared := make(map[string]struct{})
	own := make(map[string]string)
	walkRepository(t, ".go", func(name string, data []byte) {
		if strings.HasSuffix(name, "_test.go") {
			return
		}
		file, err := parser.ParseFile(fileSet, name, data, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv == nil || function.Name.Name != "UnmarshalJSON" {
				continue
			}
			receiver := receiverType(function.Recv)
			if !ast.IsExported(receiver) {
				continue
			}
			if decodesThroughSharedBoundary(function.Body) {
				shared[receiver] = struct{}{}
				continue
			}
			own[receiver] = fmt.Sprintf("%s:%d", name, fileSet.Position(function.Pos()).Line)
		}
	})
	if len(shared) == 0 {
		t.Fatal("no UnmarshalJSON reaches the shared decoder; the walk stopped seeing the repository")
	}

	for _, receiver := range slices.Sorted(maps.Keys(own)) {
		if _, allowed := wireDecodeExceptions[receiver]; !allowed {
			t.Errorf(
				"%s: %s.UnmarshalJSON decodes on its own; route it through jsonDocument.decodeInto or say why it cannot",
				own[receiver],
				receiver,
			)
		}
	}
	for _, receiver := range slices.Sorted(maps.Keys(wireDecodeExceptions)) {
		if _, stale := own[receiver]; !stale {
			t.Errorf("%s is excused from the shared decoder but no longer needs to be", receiver)
		}
	}
}

// decodesThroughSharedBoundary reports whether the body reaches the one decoder,
// by either of its two spellings: workflow calls it through a generic method on
// its document, and expr calls jsondoc directly.
func decodesThroughSharedBoundary(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		ast.Inspect(call.Fun, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if ok {
				found = found || selector.Sel.Name == "decodeInto" ||
					selector.Sel.Name == "DecodeInto"
			}
			return !found
		})
		return !found
	})
	return found
}
