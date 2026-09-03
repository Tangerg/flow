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
// the name still resolves, so TestCitedTestsResolve stays green, but it names a
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
	// Counted because the reporting below is silent when nothing was examined:
	// a walk that stopped seeing Markdown, or a pattern that stopped matching a
	// qualified name, reads exactly like documentation with nothing wrong in it.
	resolved := 0
	walkRepository(t, ".md", func(name string, data []byte) {
		for index, line := range strings.Split(string(data), "\n") {
			for _, match := range qualifiedNamePattern.FindAllStringSubmatch(line, -1) {
				if _, ok := exported[match[1]][match[2]]; ok {
					resolved++
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
	if resolved == 0 {
		t.Fatal("no documented API name resolved; the walk stopped seeing the repository")
	}
	for _, qualified := range slices.Sorted(maps.Keys(unresolved)) {
		t.Errorf("%s: %s names nothing this module exports", strings.Join(unresolved[qualified], ", "), qualified)
	}
}

// TestGoDocLinksResolve holds a doc link to the same rule as the documentation:
// the name it points at has to exist. A link that resolves becomes a link in
// godoc; one that does not is rendered as the literal text a reader sees brackets
// around, and nothing compiles either, so a rename turns pointers into debris
// silently.
//
// Only a link that can be told from prose is checked. A comment writes
// scope[index] and records[id] too, and those are the same shape as a link to an
// unexported name, so the rule is: a qualified name whose package is this
// module's, a name that starts with a capital, and a lowercase name below a
// declared type. A single capital is a type parameter, which godoc does not link
// either.
//
// Test files are read as well. godoc renders none of them, so a broken link
// there costs a reader rather than a page — and a test comment cites API, not
// only tests: godoclint requires a link for every standard-library name in any
// comment, and the ones pointing into this module rot exactly like the rest. A
// test name is cited bare, which is what the two tests above hold.
func TestGoDocLinksResolve(t *testing.T) {
	packages := make(map[string]packageNames, len(packageDirectories))
	for _, directory := range packageDirectories {
		packages[directory] = declaredNames(t, directory)
	}
	unresolved := make(map[string][]string)
	checked := 0
	walkRepository(t, ".go", func(name string, data []byte) {
		own := packages[path.Dir(name)]
		for index, line := range strings.Split(string(data), "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "//") {
				continue
			}
			for _, match := range docLinkPattern.FindAllStringSubmatch(line, -1) {
				target := match[1]
				resolved, ok := own.resolves(packages, target)
				if !ok {
					continue
				}
				checked++
				if !resolved {
					unresolved[target] = append(
						unresolved[target],
						fmt.Sprintf("%s:%d", name, index+1),
					)
				}
			}
		}
	})
	if checked == 0 {
		t.Fatal("no doc link found; the walk stopped seeing the repository")
	}
	for _, target := range slices.Sorted(maps.Keys(unresolved)) {
		t.Errorf("%s: [%s] names nothing that exists", strings.Join(unresolved[target], ", "), target)
	}
}

// packageDirectories are the packages a doc link can name, and modulePackages maps
// the qualifier a link writes to the directory holding it.
var (
	packageDirectories = []string{
		".",
		"flowx",
		"workflow",
		"workflow/expr",
		"workflow/diagram",
		"internal/ctxtest",
		"internal/jsondoc",
		"internal/jsonnum",
		"example",
	}
	modulePackages = map[string]string{
		"flow":     ".",
		"flowx":    "flowx",
		"workflow": "workflow",
		"expr":     "workflow/expr",
		"diagram":  "workflow/diagram",
		"ctxtest":  "internal/ctxtest",
		"jsondoc":  "internal/jsondoc",
		"jsonnum":  "internal/jsonnum",
	}
	// docLinkPattern matches what a doc comment brackets. Whether the result is a
	// link at all is packageNames.resolves's decision.
	docLinkPattern = regexp.MustCompile(`\[(\*?[A-Za-z_][\w.]*)\]`)
)

// packageNames is one package's names as a doc link may write them: declared holds
// every top-level name plus every Type.Member below one, and types holds the type
// names alone, which is what separates a link to an unexported member from a
// comment indexing a map.
type packageNames struct {
	declared map[string]struct{}
	types    map[string]struct{}
}

// resolves reports whether target exists, and whether it is a doc link at all.
// A qualifier this module does not own belongs to the standard library or a
// dependency, whose API is not this test's to check.
func (p packageNames) resolves(packages map[string]packageNames, target string) (bool, bool) {
	target = strings.TrimPrefix(target, "*")
	qualifier, member, qualified := strings.Cut(target, ".")
	if directory, ours := modulePackages[qualifier]; qualified && ours {
		return packages[directory].has(member), true
	}
	if qualified && !p.hasType(qualifier) {
		// Either another package's name or, far more often, a comment writing an
		// expression: delta.key and node.ID name no type here.
		return false, false
	}
	if !qualified {
		first := rune(target[0])
		if first >= 'a' && first <= 'z' {
			// A bare lowercase name is indistinguishable from prose indexing a map.
			return false, false
		}
		if len(target) == 1 {
			return false, false // A type parameter, which godoc does not link.
		}
	}
	return p.has(target), true
}

func (p packageNames) has(name string) bool {
	_, ok := p.declared[name]
	return ok
}

func (p packageNames) hasType(name string) bool {
	_, ok := p.types[name]
	return ok
}

// exportedNames returns every top-level name a package exports, read from its
// sources rather than from a build, so the check needs no compiler. The directory
// is read through a filesystem rooted at it, for the same reason walkRepository is.
func exportedNames(t *testing.T, dir string) map[string]struct{} {
	t.Helper()
	names := make(map[string]struct{})
	parsePackage(t, dir, func(file *ast.File) {
		for _, declaration := range file.Decls {
			collectExported(names, declaration)
		}
	})
	return names
}

// declaredNames returns every name a doc link in the package may point at,
// unexported ones included: a comment explaining an internal rule links to the
// internal names it is about.
func declaredNames(t *testing.T, dir string) packageNames {
	t.Helper()
	names := packageNames{
		declared: make(map[string]struct{}),
		types:    make(map[string]struct{}),
	}
	parsePackage(t, dir, func(file *ast.File) {
		for _, declaration := range file.Decls {
			collectDeclared(names, declaration)
		}
	})
	return names
}

// parsePackage visits the package's own sources, excluding tests, from a
// filesystem rooted at its directory, for the same reason walkRepository is.
func parsePackage(t *testing.T, dir string, visit func(*ast.File)) {
	t.Helper()
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
		visit(file)
	}
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

func collectDeclared(names packageNames, declaration ast.Decl) {
	switch declared := declaration.(type) {
	case *ast.FuncDecl:
		if declared.Recv == nil {
			names.declared[declared.Name.Name] = struct{}{}
			return
		}
		names.declared[receiverType(declared.Recv)+"."+declared.Name.Name] = struct{}{}
	case *ast.GenDecl:
		for _, spec := range declared.Specs {
			switch value := spec.(type) {
			case *ast.TypeSpec:
				names.declared[value.Name.Name] = struct{}{}
				names.types[value.Name.Name] = struct{}{}
				collectMembers(names.declared, value)
			case *ast.ValueSpec:
				for _, name := range value.Names {
					names.declared[name.Name] = struct{}{}
				}
			}
		}
	}
}

// collectMembers records the fields of a struct and the methods of an interface,
// which is the other half of what a doc link can name below a type.
func collectMembers(names map[string]struct{}, spec *ast.TypeSpec) {
	var members *ast.FieldList
	switch underlying := spec.Type.(type) {
	case *ast.StructType:
		members = underlying.Fields
	case *ast.InterfaceType:
		members = underlying.Methods
	default:
		return
	}
	for _, member := range members.List {
		for _, name := range member.Names {
			names[spec.Name.Name+"."+name.Name] = struct{}{}
		}
	}
}

// receiverType names the type a method belongs to, looking through a pointer and
// through the type parameters of a generic receiver.
func receiverType(receiver *ast.FieldList) string {
	expression := receiver.List[0].Type
	if pointer, ok := expression.(*ast.StarExpr); ok {
		expression = pointer.X
	}
	switch generic := expression.(type) {
	case *ast.IndexExpr:
		expression = generic.X
	case *ast.IndexListExpr:
		expression = generic.X
	}
	if identifier, ok := expression.(*ast.Ident); ok {
		return identifier.Name
	}
	return ""
}

// testNamePattern matches the prefixes go test recognizes followed by what makes
// a name rather than a word: a capital, as in "TestStore" against the prose
// "Testing", or the underscore of the suffix form go test defines for a package
// example, as in "Example_dag". Twelve of this module's examples are named that
// way, so requiring the capital alone left every citation of one unchecked.
// openingTestName is the same name where a doc comment claims to be about it.
var (
	testNamePattern = regexp.MustCompile(`\b(?:Test|Benchmark|Example|Fuzz)(?:[A-Z]|_[a-z])\w*`)
	openingTestName = regexp.MustCompile(`^(?:Test|Benchmark|Example|Fuzz)(?:[A-Z]|_[a-z])\w*`)
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

// markdownLinkPattern matches an inline Markdown link's target. Code spans are
// stripped before it runs, because Go generics read as one: Get[string](ref)
// matches [string](ref) and would send this looking for a file called ref.
var markdownLinkPattern = regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)

var markdownCodeSpanPattern = regexp.MustCompile("`[^`]*`")

// TestMarkdownLinksResolve is TestGoDocLinksResolve's other half. The prose in
// this repository navigates by relative link — a tutorial to the example that
// runs it, the guidance file to the boundaries it cites, the changelog to a
// document explaining a change — and a target that has moved reads exactly like
// one that has not. Go doc links are checked because they point at API; these
// point at files, which rename far more often.
//
// Anchors are deliberately not resolved: the heading slug is the renderer's
// convention, not this repository's, and guessing it would fail on prose that is
// correct.
func TestMarkdownLinksResolve(t *testing.T) {
	repository := os.DirFS(".")
	unresolved := make(map[string][]string)
	checked := 0
	walkRepository(t, ".md", func(name string, data []byte) {
		fenced := false
		for index, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				fenced = !fenced
				continue
			}
			if fenced {
				continue
			}
			line = markdownCodeSpanPattern.ReplaceAllString(line, "")
			for _, match := range markdownLinkPattern.FindAllStringSubmatch(line, -1) {
				target, _, _ := strings.Cut(match[1], "#")
				if target == "" || strings.Contains(match[1], "://") ||
					strings.HasPrefix(match[1], "mailto:") {
					continue
				}
				checked++
				resolved := path.Join(path.Dir(name), target)
				if _, err := fs.Stat(repository, resolved); err != nil {
					unresolved[match[1]] = append(
						unresolved[match[1]],
						fmt.Sprintf("%s:%d", name, index+1),
					)
				}
			}
		}
	})
	if checked == 0 {
		t.Fatal("no relative Markdown links found; the walk stopped seeing the repository")
	}
	for _, target := range slices.Sorted(maps.Keys(unresolved)) {
		t.Errorf("Markdown link %q resolves to nothing, cited at %s",
			target, strings.Join(unresolved[target], ", "))
	}
}
