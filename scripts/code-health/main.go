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
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	maxFileLines       = 1500
	maxFuncLines       = 150
	maxCC              = 20
	maxDepth           = 5
	minCloneFuncLines  = 20
	minCloneTokens     = 12
	cloneShingleSize   = 3
	maxCloneSimilarity = 0.80
)

var splitTestRegex = regexp.MustCompile(`^Test.*_(Extra|More.*|Additional.*|Uncovered.*|EdgeCases|CornerCases|Branches|AllBranches|Chunk\d+.*|Coverage.*)$`)

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

type funcCloneSig struct {
	rel    string
	pkgKey string
	name   string
	lines  int
	grams  map[string]struct{}
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
	explicit := fsFlags.Args()
	targets, err := collectTargets(absRoot, explicit)
	if err != nil {
		writef(stderr, "code-health: collect targets: %v\n", err)
		return 1
	}

	if *updateMode {
		return runUpdate(absRoot, *baselinePath, targets, stdout, stderr)
	}

	testTargets, err := collectTestTargets(absRoot, explicit)
	if err != nil {
		writef(stderr, "code-health: collect test targets: %v\n", err)
		return 1
	}
	return runCheck(absRoot, *baselinePath, targets, testTargets, stdout, stderr)
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

func runCheck(absRoot, baselinePath string, targets, testTargets []string, stdout, stderr io.Writer) int {
	base, err := loadBaseline(baselinePath)
	if err != nil {
		writef(stderr, "code-health: load baseline: %v\n", err)
		return 1
	}
	violations, err := checkTargets(absRoot, targets, testTargets, base)
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
	return collectTargetsMatching(absRoot, explicit, isCandidateGoFile)
}

func collectTestTargets(absRoot string, explicit []string) ([]string, error) {
	return collectTargetsMatching(absRoot, explicit, isCandidateTestGoFile)
}

func collectTargetsMatching(absRoot string, explicit []string, match func(string) bool) ([]string, error) {
	if len(explicit) > 0 {
		return collectExplicitTargetsMatching(absRoot, explicit, match), nil
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
			if match(rel) {
				out = append(out, rel)
			}
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func collectExplicitTargetsMatching(absRoot string, explicit []string, match func(string) bool) []string {
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
		if !match(rel) {
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

func isCandidateTestGoFile(rel string) bool {
	if !strings.HasSuffix(rel, "_test.go") ||
		strings.HasSuffix(rel, ".gen.go") || strings.HasSuffix(rel, ".pb.go") || strings.HasSuffix(rel, "_gen.go") ||
		strings.HasSuffix(rel, ".gen_test.go") || strings.HasSuffix(rel, "_gen_test.go") || strings.HasSuffix(rel, ".pb_test.go") {
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
		fm, fns, _, skipped, _, err := analyzeFile(filepath.Join(absRoot, rel), rel)
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

func checkTargets(absRoot string, targets, testTargets []string, base Baseline) ([]string, error) {
	var violations []string
	var allSigs []funcCloneSig
	for _, rel := range targets {
		fm, fns, sigs, skipped, nolintViolations, err := analyzeFile(filepath.Join(absRoot, rel), rel)
		if err != nil {
			return nil, err
		}
		if skipped {
			continue
		}
		for _, nv := range nolintViolations {
			violations = append(violations, fmt.Sprintf("%s: %s", rel, nv))
		}
		fileLimit := max(maxFileLines, base.Files[rel].Lines)
		if fm.Lines > fileLimit {
			violations = append(violations, fmt.Sprintf("%s: file length %d exceeds limit %d (ceiling %d)", rel, fm.Lines, fileLimit, maxFileLines))
		}
		violations = append(violations, checkFuncMetrics(rel, fns, base.Functions)...)
		allSigs = append(allSigs, sigs...)
	}

	violations = append(violations, checkCloneViolations(allSigs)...)

	for _, rel := range testTargets {
		testViolations, err := analyzeTestFile(filepath.Join(absRoot, filepath.FromSlash(rel)), rel)
		if err != nil {
			return nil, err
		}
		violations = append(violations, testViolations...)
	}

	return violations, nil
}

func checkCloneViolations(sigs []funcCloneSig) []string {
	pkgSigs := make(map[string][]funcCloneSig)
	for _, s := range sigs {
		pkgSigs[s.pkgKey] = append(pkgSigs[s.pkgKey], s)
	}

	var pkgKeys []string
	for k := range pkgSigs {
		pkgKeys = append(pkgKeys, k)
	}
	sort.Strings(pkgKeys)

	var violations []string
	for _, pkgKey := range pkgKeys {
		group := pkgSigs[pkgKey]
		sort.Slice(group, func(i, j int) bool {
			if group[i].rel != group[j].rel {
				return group[i].rel < group[j].rel
			}
			return group[i].name < group[j].name
		})
		for i := 0; i < len(group); i++ {
			for j := i + 1; j < len(group); j++ {
				a := group[i]
				b := group[j]
				sim := jaccardSimilarity(a.grams, b.grams)
				if sim >= maxCloneSimilarity {
					violations = append(violations, fmt.Sprintf("%s (%s) <-> %s (%s): near-miss AST clone similarity %.2f exceeds %.2f ceiling", a.rel, a.name, b.rel, b.name, sim, maxCloneSimilarity))
				}
			}
		}
	}
	return violations
}

func jaccardSimilarity(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0.0
	}
	intersection := 0
	for k := range a {
		if _, ok := b[k]; ok {
			intersection++
		}
	}
	union := len(a) + len(b) - intersection
	if union == 0 {
		return 0.0
	}
	return float64(intersection) / float64(union)
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

func analyzeFile(fullPath, rel string) (FileMetrics, map[string]FuncMetrics, []funcCloneSig, bool, []string, error) {
	src, err := os.ReadFile(fullPath)
	if err != nil {
		return FileMetrics{}, nil, nil, false, nil, err
	}
	if isGeneratedSource(src) {
		return FileMetrics{}, nil, nil, true, nil, nil
	}
	lines := bytes.Count(src, []byte("\n")) + 1
	if len(src) > 0 && src[len(src)-1] == '\n' {
		lines--
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, fullPath, src, parser.ParseComments)
	if err != nil {
		return FileMetrics{}, nil, nil, false, nil, err
	}

	nolintViolations := findBannedNolints(f, fset)

	fns := make(map[string]FuncMetrics)
	counts := make(map[string]int)
	var sigs []funcCloneSig
	pkgKey := filepath.ToSlash(filepath.Dir(rel)) + "::" + f.Name.Name
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
		fnLines := fset.Position(fn.End()).Line - fset.Position(fn.Pos()).Line + 1
		fns[name] = FuncMetrics{
			Lines: fnLines,
			CC:    calcCC(fn.Body),
			Depth: calcDepth(fn.Body, 0),
		}
		if fnLines >= minCloneFuncLines {
			tokens := buildCloneTokens(fn.Body)
			sigs = append(sigs, funcCloneSig{
				rel:    rel,
				pkgKey: pkgKey,
				name:   name,
				lines:  fnLines,
				grams:  buildCloneGrams(tokens),
			})
		}
	}
	return FileMetrics{Lines: lines}, fns, sigs, false, nolintViolations, nil
}

func buildCloneTokens(body *ast.BlockStmt) []string {
	var tokens []string
	ast.Inspect(body, func(n ast.Node) bool {
		if n == nil {
			return true
		}
		switch x := n.(type) {
		case *ast.IfStmt:
			tokens = append(tokens, "if")
			if x.Else != nil {
				tokens = append(tokens, "else")
			}
		case *ast.ForStmt:
			tokens = append(tokens, "for")
		case *ast.RangeStmt:
			tokens = append(tokens, "range")
		case *ast.SwitchStmt, *ast.TypeSwitchStmt:
			tokens = append(tokens, "switch")
		case *ast.CaseClause:
			tokens = append(tokens, "case")
		case *ast.SelectStmt:
			tokens = append(tokens, "select")
		case *ast.CommClause:
			tokens = append(tokens, "comm")
		case *ast.ReturnStmt:
			tokens = append(tokens, "return")
		case *ast.DeferStmt:
			tokens = append(tokens, "defer")
		case *ast.CallExpr:
			switch fn := x.Fun.(type) {
			case *ast.SelectorExpr:
				tokens = append(tokens, "call:."+fn.Sel.Name)
			case *ast.Ident:
				if !isGoBuiltin(fn.Name) {
					tokens = append(tokens, "call:"+fn.Name)
				}
			}
		}
		return true
	})
	return tokens
}

func buildCloneGrams(tokens []string) map[string]struct{} {
	grams := make(map[string]struct{})
	if len(tokens) >= minCloneTokens {
		for i := 0; i <= len(tokens)-cloneShingleSize; i++ {
			grams[tokens[i]+"|"+tokens[i+1]+"|"+tokens[i+2]] = struct{}{}
		}
	}
	return grams
}

func isGoBuiltin(name string) bool {
	switch name {
	case "len", "cap", "make", "new", "append", "copy", "delete", "close",
		"min", "max", "panic", "recover", "print", "println", "complex", "real", "imag", "clear":
		return true
	default:
		return false
	}
}

func findBannedNolints(f *ast.File, fset *token.FileSet) []string {
	var out []string
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if reason := checkNolintComment(c.Text); reason != "" {
				out = append(out, fmt.Sprintf("line %d: %s (%s)", fset.Position(c.Pos()).Line, strings.TrimSpace(c.Text), reason))
			}
		}
	}
	return out
}

func checkNolintComment(raw string) string {
	text := strings.TrimPrefix(raw, "//")
	if strings.HasPrefix(raw, "/*") {
		text = strings.TrimSuffix(strings.TrimPrefix(raw, "/*"), "*/")
	}
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "nolint") {
		return ""
	}
	rest := text[len("nolint"):]
	if rest != "" && rest[0] != ':' && rest[0] != ' ' && rest[0] != '\t' && !strings.HasPrefix(rest, "//") {
		return ""
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, ":") {
		return "bare //nolint directive is prohibited; specify allowed linter or handle errors"
	}
	linterPart := strings.TrimSpace(rest[1:])
	if idx := strings.Index(linterPart, "//"); idx >= 0 {
		linterPart = linterPart[:idx]
	}
	linterPart = strings.TrimSpace(linterPart)
	if linterPart == "" {
		return "empty //nolint: directive is prohibited"
	}
	for _, tok := range strings.Split(linterPart, ",") {
		fields := strings.Fields(tok)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "errcheck", "all", "dupl", "gocyclo", "gocognit", "cyclop", "funlen", "maintidx", "nestif":
			return fmt.Sprintf("//nolint:%s is prohibited", fields[0])
		}
	}
	return ""
}

func analyzeTestFile(fullPath, rel string) ([]string, error) {
	src, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, err
	}
	if isGeneratedSource(src) {
		return nil, nil
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, fullPath, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	var violations []string
	for _, nv := range findBannedNolints(f, fset) {
		violations = append(violations, fmt.Sprintf("%s: %s", rel, nv))
	}

	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if splitTestRegex.MatchString(fn.Name.Name) {
			line := fset.Position(fn.Pos()).Line
			violations = append(violations, fmt.Sprintf("%s:%d: test function %s uses banned split/catch-all suffix; fold cases into the canonical Test* table or name after specific behavior", rel, line, fn.Name.Name))
		}
	}

	ast.Inspect(f, func(n ast.Node) bool {
		st, ok := n.(*ast.StructType)
		if !ok || st.Fields == nil {
			return true
		}
		for _, field := range st.Fields.List {
			if _, isFunc := field.Type.(*ast.FuncType); !isFunc {
				continue
			}
			for _, ident := range field.Names {
				if isBannedClosureFieldName(ident.Name) {
					line := fset.Position(ident.Pos()).Line
					violations = append(violations, fmt.Sprintf("%s:%d: test struct field %q uses a banned closure func type; use declarative expectation fields instead", rel, line, ident.Name))
				}
			}
		}
		return true
	})

	return violations, nil
}

func isBannedClosureFieldName(name string) bool {
	switch name {
	case "run", "check", "extraCheck", "setup", "validate", "validator", "verify", "lookup":
		return true
	default:
		return false
	}
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
