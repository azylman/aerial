package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	maxFileLines = 1500
	maxFuncLines = 150
	maxCC        = 20
	maxDepth     = 5
)

// FileMetrics records ratcheted file-level watermarks.
type FileMetrics struct {
	Lines int `json:"lines,omitempty"`
}

// FuncMetrics records ratcheted function-level watermarks.
type FuncMetrics struct {
	Lines int `json:"lines,omitempty"`
	CC    int `json:"cc,omitempty"`
	Depth int `json:"depth,omitempty"`
}

// Baseline stores grandfathered file and function complexity watermarks.
type Baseline struct {
	Files     map[string]FileMetrics `json:"files,omitempty"`
	Functions map[string]FuncMetrics `json:"functions,omitempty"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func writef(w io.Writer, format string, args ...any) {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf(format, args...))
	if _, err := w.Write(buf.Bytes()); err != nil {
		return
	}
}

func run(args []string, stdout, stderr io.Writer) int {
	fsFlags := flag.NewFlagSet("code-health", flag.ContinueOnError)
	fsFlags.SetOutput(stderr)
	rootDir := fsFlags.String("root", ".", "Repository root directory")
	baselinePath := fsFlags.String("baseline", ".code-health-baseline.json", "Baseline JSON file path")
	updateMode := fsFlags.Bool("update", false, "Write updated baseline JSON for current outliers")
	if err := fsFlags.Parse(args); err != nil {
		return 2
	}

	absRoot := filepath.Clean(*rootDir)
	targets, err := collectTargets(absRoot, fsFlags.Args())
	if err != nil {
		writef(stderr, "code-health: collect targets: %v\n", err)
		return 1
	}

	if *updateMode {
		return runUpdate(absRoot, *baselinePath, targets, stdout, stderr)
	}
	return runCheck(absRoot, *baselinePath, targets, stdout, stderr)
}

func runUpdate(absRoot, baselinePath string, targets []string, stdout, stderr io.Writer) int {
	base, err := buildBaseline(absRoot, targets)
	if err != nil {
		writef(stderr, "code-health: analyze: %v\n", err)
		return 1
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(base); err != nil {
		writef(stderr, "code-health: encode baseline: %v\n", err)
		return 1
	}
	if err := os.WriteFile(baselinePath, buf.Bytes(), 0o644); err != nil {
		writef(stderr, "code-health: write baseline: %v\n", err)
		return 1
	}
	writef(stdout, "code-health: wrote baseline to %s (%d files, %d functions)\n",
		baselinePath, len(base.Files), len(base.Functions))
	return 0
}

func runCheck(absRoot, baselinePath string, targets []string, stdout, stderr io.Writer) int {
	base, err := loadBaseline(baselinePath)
	if err != nil {
		writef(stderr, "code-health: load baseline: %v\n", err)
		return 1
	}
	violations, err := checkTargets(absRoot, targets, base)
	if err != nil {
		writef(stderr, "code-health: check: %v\n", err)
		return 1
	}
	if len(violations) > 0 {
		writef(stderr, "🚨 [Code Health] %d ratchet violation(s) detected:\n", len(violations))
		for _, v := range violations {
			writef(stderr, "   • %s\n", v)
		}
		return 1
	}
	writef(stdout, "✅ [Code Health] Checked %d Go file(s); all complexity and length ratchets passed.\n", len(targets))
	return 0
}

func collectTargets(absRoot string, explicit []string) ([]string, error) {
	if len(explicit) > 0 {
		return collectExplicitTargets(absRoot, explicit), nil
	}
	var out []string
	err := filepath.WalkDir(absRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != absRoot && isExcludedDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		rel, relErr := filepath.Rel(absRoot, path)
		if relErr == nil {
			rel = filepath.ToSlash(rel)
			if isCandidateGoFile(rel) {
				out = append(out, rel)
			}
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func collectExplicitTargets(absRoot string, explicit []string) []string {
	var out []string
	for _, arg := range explicit {
		p := arg
		if !filepath.IsAbs(p) {
			p = filepath.Join(absRoot, p)
		}
		rel, err := filepath.Rel(absRoot, p)
		if err != nil {
			continue
		}
		rel = filepath.ToSlash(rel)
		if !isCandidateGoFile(rel) {
			continue
		}
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			out = append(out, rel)
		}
	}
	sort.Strings(out)
	return out
}

func isExcludedDir(name string) bool {
	return strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules" || name == "testdata"
}

func isCandidateGoFile(rel string) bool {
	if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") ||
		strings.HasSuffix(rel, ".gen.go") || strings.HasSuffix(rel, ".pb.go") {
		return false
	}
	for _, seg := range strings.Split(rel, "/") {
		if isExcludedDir(seg) || seg == ".." {
			return false
		}
	}
	return true
}

func loadBaseline(path string) (Baseline, error) {
	b := Baseline{
		Files:     make(map[string]FileMetrics),
		Functions: make(map[string]FuncMetrics),
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return b, nil
	}
	if err != nil {
		return b, err
	}
	err = json.Unmarshal(data, &b)
	return b, err
}

func buildBaseline(absRoot string, targets []string) (Baseline, error) {
	b := Baseline{
		Files:     make(map[string]FileMetrics),
		Functions: make(map[string]FuncMetrics),
	}
	for _, rel := range targets {
		fm, fns, skipped, err := analyzeFile(filepath.Join(absRoot, rel))
		if err != nil {
			return b, err
		}
		if skipped {
			continue
		}
		if fm.Lines > maxFileLines {
			b.Files[rel] = fm
		}
		for fnName, m := range fns {
			if entry, ok := outlierFuncMetrics(m); ok {
				b.Functions[rel+"::"+fnName] = entry
			}
		}
	}
	return b, nil
}

func outlierFuncMetrics(m FuncMetrics) (FuncMetrics, bool) {
	var entry FuncMetrics
	if m.Lines > maxFuncLines {
		entry.Lines = m.Lines
	}
	if m.CC > maxCC {
		entry.CC = m.CC
	}
	if m.Depth > maxDepth {
		entry.Depth = m.Depth
	}
	return entry, entry.Lines > 0 || entry.CC > 0 || entry.Depth > 0
}

func checkTargets(absRoot string, targets []string, base Baseline) ([]string, error) {
	var violations []string
	for _, rel := range targets {
		fm, fns, skipped, err := analyzeFile(filepath.Join(absRoot, rel))
		if err != nil {
			return nil, err
		}
		if skipped {
			continue
		}
		fileLimit := max(maxFileLines, base.Files[rel].Lines)
		if fm.Lines > fileLimit {
			violations = append(violations, fmt.Sprintf("%s: file length %d exceeds limit %d (ceiling %d)", rel, fm.Lines, fileLimit, maxFileLines))
		}
		violations = append(violations, checkFuncMetrics(rel, fns, base.Functions)...)
	}
	return violations, nil
}

func checkFuncMetrics(rel string, fns map[string]FuncMetrics, baseFns map[string]FuncMetrics) []string {
	var names []string
	for k := range fns {
		names = append(names, k)
	}
	sort.Strings(names)
	var violations []string
	for _, fnName := range names {
		m := fns[fnName]
		bm := baseFns[rel+"::"+fnName]
		if lineLimit := max(maxFuncLines, bm.Lines); m.Lines > lineLimit {
			violations = append(violations, fmt.Sprintf("%s (%s): function length %d exceeds limit %d (ceiling %d)", rel, fnName, m.Lines, lineLimit, maxFuncLines))
		}
		if ccLimit := max(maxCC, bm.CC); m.CC > ccLimit {
			violations = append(violations, fmt.Sprintf("%s (%s): cyclomatic complexity %d exceeds limit %d (ceiling %d)", rel, fnName, m.CC, ccLimit, maxCC))
		}
		if depthLimit := max(maxDepth, bm.Depth); m.Depth > depthLimit {
			violations = append(violations, fmt.Sprintf("%s (%s): nesting depth %d exceeds limit %d (ceiling %d)", rel, fnName, m.Depth, depthLimit, maxDepth))
		}
	}
	return violations
}

func analyzeFile(fullPath string) (FileMetrics, map[string]FuncMetrics, bool, error) {
	src, err := os.ReadFile(fullPath)
	if err != nil {
		return FileMetrics{}, nil, false, err
	}
	if isGeneratedSource(src) {
		return FileMetrics{}, nil, true, nil
	}
	lines := bytes.Count(src, []byte("\n")) + 1
	if len(src) > 0 && src[len(src)-1] == '\n' {
		lines--
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, fullPath, src, 0)
	if err != nil {
		return FileMetrics{}, nil, false, err
	}

	fns := make(map[string]FuncMetrics)
	counts := make(map[string]int)
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		baseName := formatFuncName(fn)
		counts[baseName]++
		name := baseName
		if counts[baseName] > 1 {
			name = baseName + "#" + strconv.Itoa(counts[baseName])
		}
		fns[name] = FuncMetrics{
			Lines: fset.Position(fn.End()).Line - fset.Position(fn.Pos()).Line + 1,
			CC:    calcCC(fn.Body),
			Depth: calcDepth(fn.Body, 0),
		}
	}
	return FileMetrics{Lines: lines}, fns, false, nil
}

func isGeneratedSource(src []byte) bool {
	head := src
	if len(head) > 2048 {
		head = head[:2048]
	}
	s := string(head)
	return strings.Contains(s, "Code generated") && strings.Contains(s, "DO NOT EDIT")
}

func formatFuncName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	return exprName(fn.Recv.List[0].Type) + "." + fn.Name.Name
}

func exprName(expr ast.Expr) string {
	switch x := expr.(type) {
	case *ast.StarExpr:
		return "*" + exprName(x.X)
	case *ast.Ident:
		return x.Name
	case *ast.IndexExpr:
		return exprName(x.X)
	case *ast.IndexListExpr:
		return exprName(x.X)
	case *ast.ParenExpr:
		return exprName(x.X)
	default:
		return "recv"
	}
}

func calcCC(body *ast.BlockStmt) int {
	cc := 1
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt:
			cc++
		case *ast.CaseClause:
			if x.List != nil {
				cc++
			}
		case *ast.CommClause:
			if x.Comm != nil {
				cc++
			}
		case *ast.BinaryExpr:
			if x.Op == token.LAND || x.Op == token.LOR {
				cc++
			}
		}
		return true
	})
	return cc
}

func calcDepth(root ast.Node, cur int) int {
	maxD := cur
	ast.Inspect(root, func(n ast.Node) bool {
		if n == nil || n == root {
			return true
		}
		if ifStmt, ok := n.(*ast.IfStmt); ok {
			for curIf := ifStmt; curIf != nil; {
				maxD = max(maxD, calcDepth(curIf.Body, cur+1))
				if nextIf, isElseIf := curIf.Else.(*ast.IfStmt); isElseIf {
					curIf = nextIf
					continue
				}
				if curIf.Else != nil {
					maxD = max(maxD, calcDepth(curIf.Else, cur+1))
				}
				break
			}
			return false
		}
		if body := nestedBlockBody(n); body != nil {
			maxD = max(maxD, calcDepth(body, cur+1))
			return false
		}
		return true
	})
	return maxD
}

func nestedBlockBody(n ast.Node) *ast.BlockStmt {
	switch x := n.(type) {
	case *ast.ForStmt:
		return x.Body
	case *ast.RangeStmt:
		return x.Body
	case *ast.SwitchStmt:
		return x.Body
	case *ast.TypeSwitchStmt:
		return x.Body
	case *ast.SelectStmt:
		return x.Body
	case *ast.FuncLit:
		return x.Body
	default:
		return nil
	}
}
