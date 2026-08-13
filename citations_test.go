package flow_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"os"
	"path"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// A comment that names a test is how this repository shows a rule stated twice
// is pinned rather than trusted: the reader checks the named test instead of
// taking the comment's word for it. A name that no longer resolves reads exactly
// like one that does, so renaming a test quietly turns a pinned rule back into a
// trusted one. Three of eleven citations were already broken when this test was
// written, one of them naming a benchmark that had never existed.
func TestCitedTestsResolve(t *testing.T) {
	defined := definedTestNames(t)
	cited := citedTestNames(t)
	if len(cited) == 0 {
		t.Fatal("no cited test names found; the walk below stopped seeing the repository")
	}
	// Sorted so a failure reports the same way on every run.
	for _, name := range slices.Sorted(maps.Keys(cited)) {
		if _, ok := defined[name]; !ok {
			t.Errorf("%s: no test named %s exists", strings.Join(cited[name], ", "), name)
		}
	}
}

// TestTestCommentsNameTheirOwnTest covers the other way a citation goes wrong:
// the name still resolves, so [TestCitedTestsResolve] stays green, but it names a
// different test than the one the comment is attached to. That happens by editing
// — a new test inserted into an existing comment block inherits its first lines
// and leaves the old test undocumented — and the result reads as documentation
// while describing something else.
func TestTestCommentsNameTheirOwnTest(t *testing.T) {
	fileSet := token.NewFileSet()
	documented := 0
	walkRepository(t, ".go", func(name string, data []byte) {
		if !strings.HasSuffix(name, "_test.go") {
			return
		}
		file, err := parser.ParseFile(fileSet, name, data, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || function.Doc == nil {
				continue
			}
			// Only an opening name is a claim about what follows. A name later in
			// the comment is a cross-reference, which TestCitedTestsResolve checks.
			named := openingTestName.FindString(function.Doc.Text())
			if named == "" {
				continue
			}
			documented++
			if named != function.Name.Name {
				t.Errorf(
					"%s:%d: comment opens with %s but documents %s",
					name,
					fileSet.Position(function.Pos()).Line,
					named,
					function.Name.Name,
				)
			}
		}
	})
	if documented == 0 {
		t.Fatal("no test opens its comment with its own name; the walk stopped seeing the repository")
	}
}

// TestDocumentedAPINamesResolve holds the documentation to the same rule as a
// citation: a name it uses has to exist. Fifty-odd Go snippets across the README
// and the tutorials name this module's API, and nothing compiles them -- a renamed
// or removed export leaves them reading as instructions that cannot work.
//
// It checks package-qualified names, which is where a rename shows: a method or a
// field is written on a variable whose type the check cannot know, and guessing at
// that would report names the reader never wrote.
func TestDocumentedAPINamesResolve(t *testing.T) {
	exported := map[string]map[string]struct{}{
		"flow":     exportedNames(t, "."),
		"flowx":    exportedNames(t, "flowx"),
		"workflow": exportedNames(t, "workflow"),
		"expr":     exportedNames(t, "workflow/expr"),
		"diagram":  exportedNames(t, "workflow/diagram"),
	}
	unresolved := make(map[string][]string)
	walkRepository(t, ".md", func(name string, data []byte) {
		for index, line := range strings.Split(string(data), "\n") {
			for _, match := range qualifiedNamePattern.FindAllStringSubmatch(line, -1) {
				if _, ok := exported[match[1]][match[2]]; ok {
					continue
				}
				qualified := match[1] + "." + match[2]
				unresolved[qualified] = append(
					unresolved[qualified],
					fmt.Sprintf("%s:%d", name, index+1),
				)
			}
		}
	})
	if len(unresolved) == 0 {
		return
	}
	for _, qualified := range slices.Sorted(maps.Keys(unresolved)) {
		t.Errorf("%s: %s names nothing this module exports", strings.Join(unresolved[qualified], ", "), qualified)
	}
}

// exportedNames returns every top-level name a package exports, read from its
// sources rather than from a build, so the check needs no compiler. The directory
// is read through a filesystem rooted at it, for the same reason walkRepository is.
func exportedNames(t *testing.T, dir string) map[string]struct{} {
	t.Helper()
	names := make(map[string]struct{})
	fileSet := token.NewFileSet()
	sources := os.DirFS(dir)
	entries, err := fs.ReadDir(sources, ".")
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := fs.ReadFile(sources, name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		file, err := parser.ParseFile(fileSet, name, data, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, declaration := range file.Decls {
			collectExported(names, declaration)
		}
	}
	return names
}

func collectExported(names map[string]struct{}, declaration ast.Decl) {
	switch declared := declaration.(type) {
	case *ast.FuncDecl:
		if declared.Recv == nil {
			names[declared.Name.Name] = struct{}{}
		}
	case *ast.GenDecl:
		for _, spec := range declared.Specs {
			switch value := spec.(type) {
			case *ast.TypeSpec:
				names[value.Name.Name] = struct{}{}
			case *ast.ValueSpec:
				for _, name := range value.Names {
					names[name.Name] = struct{}{}
				}
			}
		}
	}
}

// testNamePattern matches the prefixes go test recognizes followed by the capital
// that makes a name rather than a word: "Testing" is prose, "TestStore" is a
// citation. openingTestName is the same name where a doc comment claims to be
// about it.
var (
	testNamePattern = regexp.MustCompile(`\b(?:Test|Benchmark|Example|Fuzz)[A-Z]\w*`)
	openingTestName = regexp.MustCompile(`^(?:Test|Benchmark|Example|Fuzz)[A-Z]\w*`)
	// qualifiedNamePattern matches this module's packages followed by an exported
	// name, which is how documentation refers to the API.
	qualifiedNamePattern = regexp.MustCompile(`\b(flow|flowx|workflow|expr|diagram)\.([A-Z]\w*)`)
)

func definedTestNames(t *testing.T) map[string]struct{} {
	t.Helper()
	names := make(map[string]struct{})
	fileSet := token.NewFileSet()
	walkRepository(t, ".go", func(path string, data []byte) {
		if !strings.HasSuffix(path, "_test.go") {
			return
		}
		file, err := parser.ParseFile(fileSet, path, data, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && function.Recv == nil {
				names[function.Name.Name] = struct{}{}
			}
		}
	})
	return names
}

// citedTestNames collects the names comments and documentation claim, mapped to
// where each claim is made. Only comments count in Go sources: code that names a
// test is a call, which the compiler already resolves.
func citedTestNames(t *testing.T) map[string][]string {
	t.Helper()
	cited := make(map[string][]string)
	record := func(path string, line int, text string) {
		for _, name := range testNamePattern.FindAllString(text, -1) {
			cited[name] = append(cited[name], fmt.Sprintf("%s:%d", path, line))
		}
	}

	fileSet := token.NewFileSet()
	walkRepository(t, ".go", func(path string, data []byte) {
		if strings.HasSuffix(path, "_test.go") {
			return
		}
		file, err := parser.ParseFile(fileSet, path, data, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, group := range file.Comments {
			for _, comment := range group.List {
				record(path, fileSet.Position(comment.Pos()).Line, comment.Text)
			}
		}
	})
	walkRepository(t, ".md", func(path string, data []byte) {
		for index, line := range strings.Split(string(data), "\n") {
			record(path, index+1, line)
		}
	})
	return cited
}

// walkRepository visits every file with the given extension. The repository is
// the test's own working directory, read through a filesystem rooted there so the
// walk cannot address anything above it.
func walkRepository(t *testing.T, extension string, visit func(name string, data []byte)) {
	t.Helper()
	repository := os.DirFS(".")
	err := fs.WalkDir(repository, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return fs.SkipDir
			}
			return nil
		}
		if path.Ext(name) != extension {
			return nil
		}
		data, err := fs.ReadFile(repository, name)
		if err != nil {
			return err
		}
		visit(name, data)
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository: %v", err)
	}
}
