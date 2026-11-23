package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
)

// image is one row of catalog.json. Every key except Select is copied
// verbatim into the build matrix by the workflow's set-matrix step.
type image struct {
	Service string     `json:"service"`
	Select  selectRule `json:"select"`
}

// selectRule lists an image's Go entry points (module dirs or package dirs)
// and every non-Go path its Dockerfile copies (exact paths or dir/**).
type selectRule struct {
	GoRoots []string `json:"go_roots"`
	Inputs  []string `json:"inputs"`
}

type gate struct {
	All      bool     `json:"all"`
	Selected []string `json:"selected"`
}

type result struct {
	Pkg     bool     `json:"pkg"`
	Images  gate     `json:"images"`
	Test    gate     `json:"test"`
	Lint    gate     `json:"lint"`
	Reasons []string `json:"reasons"`
}

// Changes to these files can alter what every build reads.
var selectAllFiles = map[string]string{
	".github/workflows/docker-publish.yml": "workflow changed",
	".dockerignore":                        "root .dockerignore changed",
}

const (
	toolDir      = "scripts/affected/"
	coverageFile = "scripts/check-coverage.sh"
	lintConfig   = ".golangci.yml"
)

func loadCatalog(file string) ([]image, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var cat []image
	if err := json.Unmarshal(data, &cat); err != nil {
		return nil, fmt.Errorf("parse %s: %w", file, err)
	}
	for _, img := range cat {
		for _, in := range img.Select.Inputs {
			if strings.Contains(strings.TrimSuffix(in, "/**"), "*") {
				return nil, fmt.Errorf("%s: input %q: only exact paths and dir/** are supported", img.Service, in)
			}
		}
	}
	return cat, nil
}

// globalReasons lists the select-everything triggers that need no import graph.
func globalReasons(changed []string, forceAll bool) []string {
	var reasons []string
	if forceAll {
		reasons = append(reasons, "force_all requested")
	}
	if len(changed) == 0 {
		reasons = append(reasons, "empty changed-file list")
	}
	for _, f := range changed {
		base := path.Base(f)
		switch {
		case selectAllFiles[f] != "":
			reasons = append(reasons, selectAllFiles[f]+": "+f)
		case strings.HasPrefix(f, toolDir) && !testOnly(f):
			reasons = append(reasons, "selection tool changed: "+f)
		case base == "go.work" || base == "go.work.sum":
			reasons = append(reasons, "workspace file changed: "+f)
		}
	}
	return reasons
}

// selectAffected decides which images to build and which modules to test and lint.
func selectAffected(root string, cat []image, changed []string, forceAll bool) result {
	res := result{Reasons: globalReasons(changed, forceAll)}
	for _, f := range changed {
		res.Pkg = res.Pkg || strings.HasPrefix(f, "pkg/")
	}
	r, err := loadRepo(root)
	var g *graph
	if err == nil {
		g, err = buildGraph(r, cat)
	}
	if err != nil {
		res.Reasons = append(res.Reasons, err.Error())
	}
	imgChanged := slices.DeleteFunc(slices.Clone(changed), testOnly)
	var affected, imgAffected map[string]bool
	if g != nil {
		res.Reasons = append(res.Reasons, g.unresolved...)
		var fileReasons []string
		affected, fileReasons = affectedPackages(r, changed)
		res.Reasons = append(res.Reasons, fileReasons...)
		imgAffected, _ = affectedPackages(r, imgChanged)
		if len(imgChanged) < len(changed) {
			for _, f := range r.testdataEmbeds {
				res.Reasons = append(res.Reasons, "non-test file embeds testdata, test-only changes may reach a binary: "+f)
			}
		}
	}
	all := len(res.Reasons) > 0
	res.Images = imageGate(cat, g, imgAffected, func(img image) []string { return inputChanges(r, img, imgChanged) }, all)
	mods := moduleDirs(r, root)
	res.Test = moduleGate(mods, g, affected, changed, all || slices.Contains(changed, coverageFile))
	res.Lint = moduleGate(mods, g, affected, changed, all || slices.Contains(changed, lintConfig))
	if res.Reasons == nil {
		res.Reasons = []string{}
	}
	return res
}

// testOnly reports whether f can only affect tests: Go and JS/TS test files,
// test shell scripts, and anything under a testdata or __tests__ dir. No
// Dockerfile runs tests, so such files never change an image.
func testOnly(f string) bool {
	if slices.ContainsFunc(strings.Split(path.Dir(f), "/"), func(seg string) bool { return seg == "testdata" || seg == "__tests__" }) {
		return true
	}
	base := path.Base(f)
	for _, suffix := range testSuffixes {
		if strings.HasSuffix(base, suffix) {
			return true
		}
	}
	return false
}

var testSuffixes = []string{
	"_test.go", ".test.sh",
	".test.ts", ".test.tsx", ".test.js", ".test.cjs", ".test.mjs",
	".spec.ts", ".spec.tsx",
}

// graph holds each image's build closure and each module's test closure.
type graph struct {
	images     map[string]map[string]bool
	modules    map[string]map[string]bool
	unresolved []string
}

func buildGraph(r *repo, cat []image) (*graph, error) {
	g := &graph{images: map[string]map[string]bool{}, modules: map[string]map[string]bool{}}
	for _, img := range cat {
		if len(img.Select.GoRoots) == 0 {
			continue
		}
		main, roots, err := r.roots(img.Select.GoRoots)
		if err != nil {
			return nil, fmt.Errorf("catalog %s: %w", img.Service, err)
		}
		set, unresolved := r.closure(main, roots, false)
		g.images[img.Service] = set
		g.addUnresolved(unresolved)
	}
	for _, m := range r.modules {
		set, unresolved := r.closure(m, r.modulePackages(m), true)
		g.modules[m.dir] = set
		g.addUnresolved(unresolved)
	}
	sort.Strings(g.unresolved)
	return g, nil
}

func (g *graph) addUnresolved(imports []string) {
	for _, imp := range imports {
		reason := "unresolvable import " + imp
		if !slices.Contains(g.unresolved, reason) {
			g.unresolved = append(g.unresolved, reason)
		}
	}
}

// mainPackages keeps the binaries of a module: only they start a build closure,
// so a package no binary imports never selects an image. A module without a
// main package (a library root) keeps every package.
func mainPackages(pkgs []*goPackage) []*goPackage {
	mains := slices.DeleteFunc(slices.Clone(pkgs), func(p *goPackage) bool { return !p.isMain })
	if len(mains) == 0 {
		return pkgs
	}
	return mains
}

// roots expands catalog go_roots: a module dir means every main package of that
// module, anything else must be a package dir. The first root's module is
// the main module whose replace directives resolve imports.
func (r *repo) roots(dirs []string) (*module, []*goPackage, error) {
	var main *module
	var out []*goPackage
	for _, d := range dirs {
		pkgs, err := r.expandRoot(d)
		if err != nil {
			return nil, nil, err
		}
		if main == nil && len(pkgs) > 0 {
			main = pkgs[0].mod
		}
		out = append(out, pkgs...)
	}
	return main, out, nil
}

func (r *repo) expandRoot(d string) ([]*goPackage, error) {
	if m := r.byDir[d]; m != nil {
		return mainPackages(r.modulePackages(m)), nil
	}
	if p := r.pkgs[d]; p != nil {
		return []*goPackage{p}, nil
	}
	prefix := d + "/"
	var matched []*goPackage
	for _, p := range r.pkgs {
		if strings.HasPrefix(p.dir, prefix) {
			matched = append(matched, p)
		}
	}
	if len(matched) == 0 {
		return nil, fmt.Errorf("go root %q is neither a module nor a package", d)
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].dir < matched[j].dir })
	return mainPackages(matched), nil
}

// affectedPackages maps changed files to package dirs. A non-test, non-Go file affects
// every enclosing package dir in its module (//go:embed reaches into
// subdirectories, testdata and assets live below their package). A go.mod or
// go.sum affects every package of its module. _test.go files never reach a
// binary or another module's tests; their own module's tests run anyway.
func affectedPackages(r *repo, changed []string) (map[string]bool, []string) {
	affected := map[string]bool{}
	var reasons []string
	for _, f := range changed {
		base := path.Base(f)
		if base == "go.mod" || base == "go.sum" {
			m := r.byDir[path.Dir(f)]
			if m == nil {
				reasons = append(reasons, "module file outside a known module: "+f)
				continue
			}
			for _, p := range r.modulePackages(m) {
				affected[p.dir] = true
			}
			continue
		}
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		if p := r.pkgs[path.Dir(f)]; p != nil && strings.HasSuffix(f, ".go") {
			affected[p.dir] = true // Go source is never //go:embed payload for an enclosing package
			continue
		}
		markEnclosing(r, f, affected)
	}
	return affected, reasons
}

func markEnclosing(r *repo, file string, affected map[string]bool) {
	m := r.owningModule(file)
	if m == nil {
		return
	}
	for d := path.Dir(file); ; d = path.Dir(d) {
		if p := r.pkgs[d]; p != nil && p.mod == m {
			affected[d] = true
		}
		if d == m.dir || d == "." {
			return
		}
	}
}

func imageGate(cat []image, g *graph, affected map[string]bool, changedFor func(image) []string, all bool) gate {
	out := gate{All: all, Selected: []string{}}
	for _, img := range cat {
		if all || matchesInputs(img.Select.Inputs, changedFor(img)) || (g != nil && intersects(g.images[img.Service], affected)) {
			out.Selected = append(out.Selected, img.Service)
		}
	}
	return out
}

// inputChanges drops the Go files of packages inside the image's Go root
// modules: the import graph decides whether those reach the binary, so a helper
// package no binary imports must not select the image through a dir/** input.
func inputChanges(r *repo, img image, changed []string) []string {
	if r == nil || len(img.Select.GoRoots) == 0 {
		return changed
	}
	_, roots, err := r.roots(img.Select.GoRoots)
	if err != nil {
		return changed
	}
	mods := map[*module]bool{}
	for _, p := range roots {
		mods[p.mod] = true
	}
	return slices.DeleteFunc(slices.Clone(changed), func(f string) bool {
		p := r.pkgs[path.Dir(f)]
		return strings.HasSuffix(f, ".go") && p != nil && mods[p.mod]
	})
}

func moduleGate(mods []string, g *graph, affected map[string]bool, changed []string, all bool) gate {
	out := gate{All: all, Selected: []string{}}
	for _, dir := range mods {
		if all || matchesInputs([]string{dir + "/**"}, changed) || (g != nil && intersects(g.modules[dir], affected)) {
			out.Selected = append(out.Selected, dir)
		}
	}
	return out
}

// moduleDirs lists module dirs from the scanned repo, or straight from
// discovery when the package scan failed.
func moduleDirs(r *repo, root string) []string {
	mods := []*module{}
	if r != nil {
		mods = r.modules
	} else if found, err := discoverModules(root); err == nil {
		mods = found
	}
	out := make([]string, 0, len(mods))
	for _, m := range mods {
		out = append(out, m.dir)
	}
	return out
}

func matchesInputs(inputs, changed []string) bool {
	for _, in := range inputs {
		prefix, isDir := strings.CutSuffix(in, "/**")
		for _, f := range changed {
			if f == in || (isDir && (prefix == "." || strings.HasPrefix(f, prefix+"/"))) {
				return true
			}
		}
	}
	return false
}

func intersects(a, b map[string]bool) bool {
	for k := range b {
		if a[k] {
			return true
		}
	}
	return false
}
