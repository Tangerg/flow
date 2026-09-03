package flow_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"path"
	"regexp"
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

// TestEveryExportedInterfaceIsImplementableByACaller is the structural half of
// the axiom that this module has no framework base type and no hook only it may
// install. An exported interface with an unexported method is exactly such a
// hook: a caller can hold the type and can never satisfy it, so an
// implementation has to come from inside. That is a decision no behavior reveals
// — every test would pass, and only someone trying to write their own node,
// binder, or emitter would find out.
//
// The interfaces this module exports each have one exported method, which is
// what makes a caller's value a peer of a built-in one. The private
// definedStep interface is how a built-in composite recognizes its own kinds,
// and it stays unexported precisely so it is not a promise to anyone.
func TestEveryExportedInterfaceIsImplementableByACaller(t *testing.T) {
	fileSet := token.NewFileSet()
	checked := 0
	walkRepository(t, ".go", func(name string, data []byte) {
		if strings.HasSuffix(name, "_test.go") {
			return
		}
		file, err := parser.ParseFile(fileSet, name, data, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for declaration := range ast.Preorder(file) {
			spec, ok := declaration.(*ast.TypeSpec)
			if !ok || !ast.IsExported(spec.Name.Name) {
				continue
			}
			declared, ok := spec.Type.(*ast.InterfaceType)
			if !ok {
				continue
			}
			checked++
			for _, reason := range unimplementableMembers(fileSet, name, spec.Name.Name, declared) {
				t.Error(reason)
			}
		}
	})
	if checked == 0 {
		t.Fatal("no exported interface found; the walk stopped seeing the repository")
	}
}

// unimplementableMembers reports the members of one exported interface that a
// caller could not write: an unexported method, or an embedded unexported
// interface that hides one. Each reason is phrased for the member it is about,
// because which of the two it is decides what the fix looks like.
func unimplementableMembers(
	fileSet *token.FileSet,
	file, name string,
	declared *ast.InterfaceType,
) []string {
	var reasons []string
	at := func(node ast.Node) string {
		return fmt.Sprintf("%s:%d", file, fileSet.Position(node.Pos()).Line)
	}
	for _, member := range declared.Methods.List {
		for _, method := range member.Names {
			if !ast.IsExported(method.Name) {
				reasons = append(reasons, fmt.Sprintf(
					"%s: %s.%s is unexported, so only this module can implement %s",
					at(method), name, method.Name, name,
				))
			}
		}
		embedded, embeds := member.Type.(*ast.Ident)
		if embeds && len(member.Names) == 0 && !ast.IsExported(embedded.Name) {
			reasons = append(reasons, fmt.Sprintf(
				"%s: %s embeds the unexported %s, so only this module can implement it",
				at(member), name, embedded.Name,
			))
		}
	}
	return reasons
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
// json.Unmarshal. An unexported adapter such as graphJSONInput is the inside of a
// decode function, reached only from within the boundary this test is about.
//
// What it asks is that the boundary call is the entire body. Asking only that the
// body reaches it accepts a decoder that unmarshals into its receiver first and
// then calls the boundary, which keeps none of the three promises while looking
// like it keeps all of them.
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

// decodesThroughSharedBoundary reports whether the body is one statement that
// returns the one decoder, by either of its two spellings: workflow calls it
// through a generic method on its document, and expr calls jsondoc directly.
//
// Being the whole body is the question, not being somewhere in it. A decoder that
// reaches the boundary after doing something else has already had the chance to
// assign half a value, which is exactly the half no test can observe -- so
// "reaches it" would pass a body that keeps none of the promise. Every exported
// wire type here is one such statement, so nothing legitimate is refused by
// asking.
func decodesThroughSharedBoundary(body *ast.BlockStmt) bool {
	if len(body.List) != 1 {
		return false
	}
	returned, ok := body.List[0].(*ast.ReturnStmt)
	if !ok || len(returned.Results) != 1 {
		return false
	}
	found := false
	ast.Inspect(returned.Results[0], func(node ast.Node) bool {
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

// contextDerivations names every place in this module that derives a
// cancellable context, mapped to the test that proves the boundary ends what it
// derived. Ownership is structural: a derived context that outlives its boundary
// leaks cancellation into a caller's, and one that is never ended keeps its
// parent's timer alive.
//
// The map is the second statement of that rule and the walk below is the first,
// so a new derivation cannot arrive without either a test or an argument for why
// it needs none. errgroup counts: it derives on the caller's behalf, and Map
// still owns the result.
var contextDerivations = map[string]string{
	"map.go":                "TestMap_closesTheContextItDerived",
	"race.go":               "TestRace_closesTheContextItDerived",
	"workflow/run.go":       "TestEveryBoundaryClosesTheContextItDerived",
	"workflow/graph_run.go": "TestEveryBoundaryClosesTheContextItDerived",
	"workflow/stream.go":    "TestEveryBoundaryClosesTheContextItDerived",
}

// contextDerivationPattern matches the calls that produce a context whose
// lifetime someone has to own. WithValue is deliberately absent: it derives no
// cancellation and needs no ending.
var contextDerivationPattern = regexp.MustCompile(
	`(context\.With(Cancel|CancelCause|Timeout|TimeoutCause|Deadline|DeadlineCause)|errgroup\.WithContext)\(`,
)

func TestEveryContextDerivationNamesItsGuard(t *testing.T) {
	defined := definedTestNames(t)
	found := make(map[string]struct{})
	walkRepository(t, ".go", func(name string, data []byte) {
		if strings.HasSuffix(name, "_test.go") || path.Dir(name) == "example" {
			return
		}
		for index, line := range strings.Split(string(data), "\n") {
			if !contextDerivationPattern.MatchString(line) {
				continue
			}
			found[name] = struct{}{}
			if _, ok := contextDerivations[name]; !ok {
				t.Errorf(
					"%s:%d derives a context; contextDerivations must name the test that proves it ends",
					name, index+1,
				)
			}
		}
	})
	if len(found) == 0 {
		t.Fatal("no context derivation found; the walk stopped seeing the repository")
	}
	for _, file := range slices.Sorted(maps.Keys(contextDerivations)) {
		if _, ok := found[file]; !ok {
			t.Errorf("contextDerivations names %s, which derives no context", file)
		}
		if _, ok := defined[contextDerivations[file]]; !ok {
			t.Errorf("%s names test %s, which does not exist", file, contextDerivations[file])
		}
	}
}
