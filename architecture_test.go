package flow_test

import (
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
