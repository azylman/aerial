package main

import (
	"bufio"
	"bytes"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// module is one go.mod found the same way the CI workflow's GO_MODULES find does.
type module struct {
	dir     string            // repo-relative, slash-separated ("." for a root module)
	path    string            // module path from the module directive
	replace map[string]string // module path -> repo-relative dir, local replace targets only
}

// goPackage is one directory holding .go files, with every import of every
// file regardless of build constraints (which can only over-select).
type goPackage struct {
	dir         string
	mod         *module
	imports     []string // imports of non-test files
	testImports []string // imports of _test.go files, internal and external test packages
}

type repo struct {
	modules []*module
	byPath  map[string]*module
	byDir   map[string]*module
	pkgs    map[string]*goPackage
	prefix  string // longest import-path prefix shared by every repo module
}

func loadRepo(root string) (*repo, error) {
	root = filepath.Clean(root)
	mods, err := discoverModules(root)
	if err != nil {
		return nil, err
	}
	r := &repo{modules: mods, byPath: map[string]*module{}, byDir: map[string]*module{}, pkgs: map[string]*goPackage{}}
	paths := make([]string, 0, len(mods))
	for _, m := range mods {
		r.byPath[m.path] = m
		r.byDir[m.dir] = m
		paths = append(paths, m.path)
		if err := r.scanModule(root, m); err != nil {
			return nil, err
		}
	}
	r.prefix = commonPathPrefix(paths)
	return r, nil
}

// discoverModules mirrors `find . -maxdepth 3 -name go.mod ! -path "*/.*" ! -path "*/vendor/*"`.
func discoverModules(root string) ([]*module, error) {
	root = filepath.Clean(root)
	var mods []*module
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := relSlash(root, p)
		if d.IsDir() {
			if rel != "." && (strings.HasPrefix(d.Name(), ".") || d.Name() == "vendor" || pathDepth(rel) > 2) {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() != "go.mod" {
			return nil
		}
		m, err := parseGoMod(p, path.Dir(rel))
		if err != nil {
			return err
		}
		mods = append(mods, m)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(mods, func(i, j int) bool { return mods[i].dir < mods[j].dir })
	return mods, nil
}

// parseGoMod reads the module directive and local-path replace directives.
func parseGoMod(file, dir string) (*module, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	m := &module{dir: dir, replace: map[string]string{}}
	inReplace := false
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line, _, _ := strings.Cut(sc.Text(), "//")
		fields := strings.Fields(line)
		switch {
		case len(fields) == 0:
		case inReplace && fields[0] == ")":
			inReplace = false
		case inReplace:
			m.addReplace(fields)
		case fields[0] == "module" && len(fields) == 2:
			m.path = unquote(fields[1])
		case fields[0] == "replace" && len(fields) == 2 && fields[1] == "(":
			inReplace = true
		case fields[0] == "replace":
			m.addReplace(fields[1:])
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if m.path == "" {
		return nil, fmt.Errorf("%s: no module directive", file)
	}
	return m, nil
}

// addReplace records `old [v] => new [v]` when new is a local path.
func (m *module) addReplace(fields []string) {
	arrow := -1
	for i, f := range fields {
		if f == "=>" {
			arrow = i
		}
	}
	if arrow < 1 || arrow+1 >= len(fields) {
		return
	}
	target := unquote(fields[arrow+1])
	if !strings.HasPrefix(target, "./") && !strings.HasPrefix(target, "../") && target != "." && target != ".." {
		return
	}
	m.replace[unquote(fields[0])] = path.Join(m.dir, target)
}

// scanModule records every package directory of m, skipping what the go
// tool skips (testdata, vendor, dot/underscore dirs, nested modules).
func (r *repo) scanModule(root string, m *module) error {
	base := filepath.Join(root, filepath.FromSlash(m.dir))
	fset := token.NewFileSet()
	return filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return skipPackageDir(base, p, d.Name())
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		return r.addFile(fset, m, p, relSlash(root, p))
	})
}

func skipPackageDir(base, p, name string) error {
	if p == base {
		return nil
	}
	if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata" || name == "vendor" {
		return filepath.SkipDir
	}
	if _, err := os.Stat(filepath.Join(p, "go.mod")); err == nil {
		return filepath.SkipDir
	}
	return nil
}

func (r *repo) addFile(fset *token.FileSet, m *module, file, rel string) error {
	src, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	f, err := parser.ParseFile(fset, rel, src, parser.ImportsOnly)
	if err != nil {
		return fmt.Errorf("parse %s: %w", rel, err)
	}
	dir := path.Dir(rel)
	pkg := r.pkgs[dir]
	if pkg == nil {
		pkg = &goPackage{dir: dir, mod: m}
		r.pkgs[dir] = pkg
	}
	for _, spec := range f.Imports {
		imp, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return fmt.Errorf("parse %s: import %s: %w", rel, spec.Path.Value, err)
		}
		if strings.HasSuffix(rel, "_test.go") {
			pkg.testImports = append(pkg.testImports, imp)
		} else {
			pkg.imports = append(pkg.imports, imp)
		}
	}
	return nil
}

// resolve maps an import seen while building main to a repo package dir.
// external is true for stdlib and third-party imports; ok is false for an
// import that looks like repo code but has no package directory.
func (r *repo) resolve(main *module, imp string) (dir string, external, ok bool) {
	base, rest, found := longestModuleMatch(main.replace, imp)
	if !found {
		var m *module
		m, rest, found = r.moduleFor(imp)
		if found {
			base = m.dir
		}
	}
	if !found {
		return "", !hasPathPrefix(imp, r.prefix), false
	}
	dir = path.Join(base, rest)
	_, ok = r.pkgs[dir]
	return dir, false, ok
}

func (r *repo) moduleFor(imp string) (*module, string, bool) {
	var best *module
	for p, m := range r.byPath {
		if hasPathPrefix(imp, p) && (best == nil || len(p) > len(best.path)) {
			best = m
		}
	}
	if best == nil {
		return nil, "", false
	}
	return best, strings.TrimPrefix(strings.TrimPrefix(imp, best.path), "/"), true
}

func longestModuleMatch(mods map[string]string, imp string) (dir, rest string, found bool) {
	best := ""
	for p := range mods {
		if hasPathPrefix(imp, p) && len(p) > len(best) {
			best = p
		}
	}
	if best == "" {
		return "", "", false
	}
	return mods[best], strings.TrimPrefix(strings.TrimPrefix(imp, best), "/"), true
}

// closure walks imports from roots. Test imports count only for the roots,
// matching `go list -deps -test`.
func (r *repo) closure(main *module, roots []*goPackage, withTests bool) (map[string]bool, []string) {
	seen := map[string]bool{}
	var unresolved []string
	queue := make([]string, 0, len(roots))
	for _, p := range roots {
		seen[p.dir] = true
		queue = append(queue, p.dir)
	}
	visit := func(from, imp string) {
		dir, external, ok := r.resolve(main, imp)
		switch {
		case external:
		case !ok:
			unresolved = append(unresolved, from+": "+imp)
		case !seen[dir]:
			seen[dir] = true
			queue = append(queue, dir)
		}
	}
	for _, p := range roots {
		if withTests {
			for _, imp := range p.testImports {
				visit(p.dir, imp)
			}
		}
	}
	for len(queue) > 0 {
		dir := queue[0]
		queue = queue[1:]
		for _, imp := range r.pkgs[dir].imports {
			visit(dir, imp)
		}
	}
	return seen, unresolved
}

func (r *repo) modulePackages(m *module) []*goPackage {
	var out []*goPackage
	for _, p := range r.pkgs {
		if p.mod == m {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].dir < out[j].dir })
	return out
}

// owningModule returns the module whose directory most closely encloses file.
func (r *repo) owningModule(file string) *module {
	var best *module
	bestLen := -1
	for _, m := range r.modules {
		n := len(m.dir)
		if m.dir == "." {
			n = 0
		} else if !strings.HasPrefix(file, m.dir+"/") {
			continue
		}
		if n > bestLen {
			best, bestLen = m, n
		}
	}
	return best
}

func commonPathPrefix(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	parts := strings.Split(paths[0], "/")
	for _, p := range paths[1:] {
		other := strings.Split(p, "/")
		n := 0
		for n < len(parts) && n < len(other) && parts[n] == other[n] {
			n++
		}
		parts = parts[:n]
	}
	return strings.Join(parts, "/")
}

func hasPathPrefix(s, prefix string) bool {
	return prefix != "" && (s == prefix || strings.HasPrefix(s, prefix+"/"))
}

// relSlash returns p relative to root. p always comes from walking the
// cleaned root, so it is root itself or root plus a separator and more.
func relSlash(root, p string) string {
	switch {
	case p == root:
		return "."
	case root == ".":
		return filepath.ToSlash(p)
	}
	return filepath.ToSlash(strings.TrimPrefix(p, root+string(filepath.Separator)))
}

// pathDepth counts the components of a non-root relative path.
func pathDepth(rel string) int {
	return strings.Count(rel, "/") + 1
}

func unquote(s string) string {
	if u, err := strconv.Unquote(s); err == nil {
		return u
	}
	return s
}
