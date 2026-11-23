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
	newHTTPPromptDispatcherFunc = "NewHTTPPromptDispatcher"
)

const (
	maxRootMainFiles         = 3
	minCrossPkgCloneGrams    = 6
	maxFileLines             = 1500
	maxDepth                 = 5
	minCloneFuncLines        = 20
	minCloneTokens           = 12
	cloneShingleSize         = 3
	maxCloneSimilarity       = 0.80
	maxCloneEditSimilarity   = 0.85
	minStructCloneFields     = 5
	maxStructCloneSimilarity = 0.85
)

var (
	splitTestRegex = regexp.MustCompile(`^Test.*(_(Extra|More.*|Additional.*|Uncovered.*|EdgeCases.*|CornerCases.*|Branches.*|AllBranches.*|Chunk\d+.*|Coverage.*)|[a-z0-9](EdgeCases.*|CornerCases.*|Branches.*|AllBranches.*|Coverage$|CoverageGaps.*|Uncovered.*))$`)
	partTestRegex  = regexp.MustCompile(`^.*_part\d+_test\.go$`)
)

// FileMetrics records ratcheted file-level watermarks.
type FileMetrics struct {
	Lines        int `json:"lines,omitempty"`
	BlankAssigns int `json:"blank_assigns,omitempty"`
}

// FuncMetrics records ratcheted function-level watermarks.
type FuncMetrics struct {
	Depth int `json:"depth,omitempty"`
}

// Baseline stores grandfathered file and function complexity watermarks.
type Baseline struct {
	Files     map[string]FileMetrics `json:"files,omitempty"`
	Functions map[string]FuncMetrics `json:"functions,omitempty"`
	RootFiles map[string]int         `json:"root_files,omitempty"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func writef(w io.Writer, format string, args ...any) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, format, args...)
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
	fixMode := fsFlags.Bool("fix", false, "Auto-sort contiguous server route and handler runs in place")
	configPath := fsFlags.String("config", "", "Optional JSON file with repo-specific thresholds, banned test patterns and skipped dirs")
	if err := fsFlags.Parse(args); err != nil {
		return 2
	}

	r, err := loadRules(*configPath)
	if err != nil {
		writef(stderr, "code-health: load config: %v\n", err)
		return 1
	}
	r.checkOrdering = r.checkOrdering || *fixMode

	absRoot := filepath.Clean(*rootDir)
	explicit := fsFlags.Args()
	targets, err := collectTargets(absRoot, explicit)
	if err != nil {
		writef(stderr, "code-health: collect targets: %v\n", err)
		return 1
	}
	targets = r.withoutSkipped(targets)

	testTargets, err := collectTestTargets(absRoot, explicit)
	if err != nil {
		writef(stderr, "code-health: collect test targets: %v\n", err)
		return 1
	}
	testTargets = r.withoutSkipped(testTargets)

	if *updateMode {
		return runUpdate(absRoot, *baselinePath, targets, r, stdout, stderr)
	}

	return runCheck(absRoot, *baselinePath, targets, testTargets, r, *fixMode, stdout, stderr)
}

func runUpdate(absRoot, baselinePath string, targets []string, r rules, stdout, stderr io.Writer) int {
	base, err := buildBaseline(absRoot, targets, r)
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

func runCheck(absRoot, baselinePath string, targets, testTargets []string, r rules, fix bool, stdout, stderr io.Writer) int {
	base, err := loadBaseline(baselinePath)
	if err != nil {
		writef(stderr, "code-health: load baseline: %v\n", err)
		return 1
	}
	violations, err := checkTargets(absRoot, targets, testTargets, base, r, fix)
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
	if !strings.HasSuffix(rel, ".go") ||
		strings.HasSuffix(rel, ".gen.go") || strings.HasSuffix(rel, ".pb.go") || strings.HasSuffix(rel, "_gen.go") ||
		strings.HasSuffix(rel, ".gen_test.go") || strings.HasSuffix(rel, "_gen_test.go") || strings.HasSuffix(rel, ".pb_test.go") {
		return false
	}
	for seg := range strings.SplitSeq(rel, "/") {
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
	for seg := range strings.SplitSeq(rel, "/") {
		if isExcludedDir(seg) || seg == ".." {
			return false
		}
	}
	return true
}

func countRootPackageFiles(targets []string) map[string]int {
	counts := make(map[string]int)
	for _, target := range targets {
		normalized := filepath.ToSlash(filepath.Clean(target))
		parts := strings.Split(normalized, "/")
		if len(parts) == 3 && (parts[0] == "services" || parts[0] == "mcp") && isCandidateGoFile(normalized) {
			dir := parts[0] + "/" + parts[1]
			counts[dir]++
		}
	}
	return counts
}

func isBannedGrabBagFilename(rel string) bool {
	base := filepath.Base(rel)
	switch base {
	case "pure.go", "pure_test.go",
		"helpers.go", "helpers_test.go",
		"utils.go", "utils_test.go",
		"common.go", "common_test.go",
		"types.go", "types_test.go",
		"frozen.go", "frozen_test.go":
		return true
	}
	return partTestRegex.MatchString(base)
}

func loadBaseline(path string) (Baseline, error) {
	b := Baseline{
		Files:     make(map[string]FileMetrics),
		Functions: make(map[string]FuncMetrics),
		RootFiles: make(map[string]int),
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return b, nil
	}
	if err != nil {
		return b, err
	}
	err = json.Unmarshal(data, &b)
	if b.Files == nil {
		b.Files = make(map[string]FileMetrics)
	}
	if b.Functions == nil {
		b.Functions = make(map[string]FuncMetrics)
	}
	if b.RootFiles == nil {
		b.RootFiles = make(map[string]int)
	}
	return b, err
}

func buildBaseline(absRoot string, targets []string, r rules) (Baseline, error) {
	b := Baseline{
		Files:     make(map[string]FileMetrics),
		Functions: make(map[string]FuncMetrics),
		RootFiles: make(map[string]int),
	}
	for _, rel := range targets {
		fa, err := analyzeFile(filepath.Join(absRoot, rel), rel)
		if err != nil {
			return b, err
		}
		if fa.skipped {
			continue
		}
		var fm FileMetrics
		if fa.metrics.Lines > r.maxFileLines {
			fm.Lines = fa.metrics.Lines
		}
		if r.testRuleEnabled(ruleBlankErrorDiscard) && fa.metrics.BlankAssigns > 0 {
			fm.BlankAssigns = fa.metrics.BlankAssigns
		}
		if fm.Lines > 0 || fm.BlankAssigns > 0 {
			b.Files[rel] = fm
		}
		for fnName, m := range fa.funcs {
			if entry, ok := outlierFuncMetrics(m, r.maxDepth); ok {
				b.Functions[rel+"::"+fnName] = entry
			}
		}
	}
	for dir, count := range countRootPackageFiles(targets) {
		if count > maxRootMainFiles {
			b.RootFiles[dir] = count
		}
	}
	return b, nil
}

func outlierFuncMetrics(m FuncMetrics, depthCeiling int) (FuncMetrics, bool) {
	var entry FuncMetrics
	if m.Depth > depthCeiling {
		entry.Depth = m.Depth
	}
	return entry, entry.Depth > 0
}

func checkRootPackageViolations(targets []string, base Baseline) []string {
	var violations []string
	rootCounts := countRootPackageFiles(targets)
	dirs := make([]string, 0, len(rootCounts))
	for dir := range rootCounts {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	for _, dir := range dirs {
		count := rootCounts[dir]
		limit := max(maxRootMainFiles, base.RootFiles[dir])
		if count > limit {
			violations = append(violations, fmt.Sprintf("%s: root package has %d Go files, exceeds limit %d (ceiling %d: main.go, main_test.go, goleak_test.go); move domain logic and tests into internal/* subpackages", dir, count, limit, maxRootMainFiles))
		}
	}
	return violations
}

func checkTargets(absRoot string, targets, testTargets []string, base Baseline, r rules, fix bool) ([]string, error) {
	var violations []string
	var allFuncSigs []funcCloneSig
	var allStructSigs []structCloneSig

	violations = append(violations, checkRootPackageViolations(targets, base)...)

	for _, rel := range targets {
		if isBannedGrabBagFilename(rel) {
			violations = append(violations, rel+": filename uses banned horizontal split or grab-bag name; name files after their domain concern or move into an internal/* subpackage")
		}
		fullPath := filepath.Join(absRoot, rel)
		if r.checkOrdering {
			orderViolations, err := checkOrFixFileOrdering(fullPath, rel, fix)
			if err != nil {
				return nil, err
			}
			violations = append(violations, orderViolations...)
		}
		fa, err := analyzeFile(fullPath, rel)
		if err != nil {
			return nil, err
		}
		if fa.skipped {
			continue
		}
		for _, nv := range fa.nolintViolations {
			violations = append(violations, fmt.Sprintf("%s: %s", rel, nv))
		}
		fileLimit := max(r.maxFileLines, base.Files[rel].Lines)
		if fa.metrics.Lines > fileLimit {
			violations = append(violations, fmt.Sprintf("%s: file length %d exceeds limit %d (ceiling %d)", rel, fa.metrics.Lines, fileLimit, r.maxFileLines))
		}
		if strings.HasSuffix(rel, "_test.go") && r.testRuleEnabled(ruleBlankErrorDiscard) && fa.metrics.BlankAssigns > base.Files[rel].BlankAssigns {
			violations = append(violations, fmt.Sprintf("%s: test blank-identifier call discard count %d exceeds limit %d (ceiling 0); assert or handle returned errors instead of assigning to '_'", rel, fa.metrics.BlankAssigns, base.Files[rel].BlankAssigns))
		}
		violations = append(violations, checkFuncMetrics(rel, fa.funcs, base.Functions, r.maxDepth)...)
		allFuncSigs = append(allFuncSigs, fa.funcSigs...)
		allStructSigs = append(allStructSigs, fa.structSigs...)
	}

	violations = append(violations, checkCloneViolations(allFuncSigs)...)
	violations = append(violations, checkStructCloneViolations(allStructSigs)...)

	for _, rel := range testTargets {
		_, testViolations, err := analyzeTestFile(filepath.Join(absRoot, filepath.FromSlash(rel)), rel, r)
		if err != nil {
			return nil, err
		}
		for _, tv := range testViolations {
			if strings.Contains(tv, "nolint") {
				continue
			}
			violations = append(violations, tv)
		}
	}

	return violations, nil
}

func checkFuncMetrics(rel string, fns map[string]FuncMetrics, baseFns map[string]FuncMetrics, depthCeiling int) []string {
	names := make([]string, 0, len(fns))
	for k := range fns {
		names = append(names, k)
	}
	sort.Strings(names)
	var violations []string
	for _, fnName := range names {
		m := fns[fnName]
		bm := baseFns[rel+"::"+fnName]
		if depthLimit := max(depthCeiling, bm.Depth); m.Depth > depthLimit {
			violations = append(violations, fmt.Sprintf("%s (%s): nesting depth %d exceeds limit %d (ceiling %d)", rel, fnName, m.Depth, depthLimit, depthCeiling))
		}
	}
	return violations
}

func countLines(src []byte) int {
	lines := bytes.Count(src, []byte("\n")) + 1
	if len(src) > 0 && src[len(src)-1] == '\n' {
		lines--
	}
	return lines
}

// fileAnalysis is everything analyzeFile extracts from one Go source file.
// Skipped files (generated code) carry only Skipped=true.
type fileAnalysis struct {
	metrics          FileMetrics
	funcs            map[string]FuncMetrics
	funcSigs         []funcCloneSig
	structSigs       []structCloneSig
	skipped          bool
	nolintViolations []string
}

func analyzeFile(fullPath, rel string) (fileAnalysis, error) {
	src, err := os.ReadFile(fullPath)
	if err != nil {
		return fileAnalysis{}, err
	}
	if isGeneratedSource(src) {
		return fileAnalysis{skipped: true}, nil
	}
	lines := countLines(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, fullPath, src, parser.ParseComments)
	if err != nil {
		return fileAnalysis{}, err
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
			Depth: calcDepth(fn.Body, 0),
		}
		if !strings.HasSuffix(rel, "_test.go") && fnLines >= minCloneFuncLines {
			tokens := buildCloneTokens(fn.Body)
			var scopedTokens []string
			if len(tokens) >= minCloneTokens {
				scopedTokens = buildScopedCloneTokens(fn.Body)
			}
			sigs = append(sigs, funcCloneSig{
				rel:          rel,
				pkgKey:       pkgKey,
				name:         name,
				lines:        fnLines,
				grams:        buildCloneGrams(tokens),
				scopedTokens: scopedTokens,
			})
		}
	}
	var structSigs []structCloneSig
	if !strings.HasSuffix(rel, "_test.go") {
		structSigs = collectStructCloneSigs(f, rel, pkgKey)
	}
	fm := FileMetrics{Lines: lines}
	if strings.HasSuffix(rel, "_test.go") {
		fm.BlankAssigns = countTestBlankCallAssigns(f)
	}
	return fileAnalysis{
		metrics:          fm,
		funcs:            fns,
		funcSigs:         sigs,
		structSigs:       structSigs,
		nolintViolations: nolintViolations,
	}, nil
}

var exemptBlankCallSelectors = map[string]bool{
	"Close":       true,
	"Encode":      true,
	"Write":       true,
	"WriteHeader": true,
	"WriteString": true,
	"Flush":       true,
	"Remove":      true,
	"RemoveAll":   true,
	"Setenv":      true,
	"Unsetenv":    true,
	"Chdir":       true,
	"Unsubscribe": true,
	"Stop":        true,
	"Cancel":      true,
}

func isCleanupCall(call *ast.CallExpr) bool {
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Cleanup" {
		return true
	}
	return false
}

func isNonExemptBlankCall(lhs, rhs ast.Expr) bool {
	ident, ok := lhs.(*ast.Ident)
	if !ok || ident.Name != "_" {
		return false
	}
	call, ok := rhs.(*ast.CallExpr)
	if !ok {
		return false
	}
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		if exemptBlankCallSelectors[sel.Sel.Name] {
			return false
		}
	}
	return true
}

func hasNonExemptBlankCallAssign(assign *ast.AssignStmt) bool {
	if len(assign.Lhs) > 0 && len(assign.Rhs) == 1 {
		return isNonExemptBlankCall(assign.Lhs[len(assign.Lhs)-1], assign.Rhs[0])
	}
	if len(assign.Rhs) == len(assign.Lhs) {
		for i := range assign.Lhs {
			if isNonExemptBlankCall(assign.Lhs[i], assign.Rhs[i]) {
				return true
			}
		}
	}
	return false
}

func countTestBlankCallAssigns(f *ast.File) int {
	var count int
	for _, decl := range f.Decls {
		ast.Inspect(decl, func(n ast.Node) bool {
			if n == nil {
				return true
			}
			if _, ok := n.(*ast.DeferStmt); ok {
				return false
			}
			if call, ok := n.(*ast.CallExpr); ok && isCleanupCall(call) {
				return false
			}
			if assign, ok := n.(*ast.AssignStmt); ok && hasNonExemptBlankCallAssign(assign) {
				count++
			}
			return true
		})
	}
	return count
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
	if after, ok := strings.CutPrefix(raw, "/*"); ok {
		text = strings.TrimSuffix(after, "*/")
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
	for tok := range strings.SplitSeq(linterPart, ",") {
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
