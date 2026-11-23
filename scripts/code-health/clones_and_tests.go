package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
)

type funcCloneSig struct {
	rel          string
	pkgKey       string
	name         string
	lines        int
	grams        map[string]struct{}
	scopedTokens []string
}

type structCloneSig struct {
	rel    string
	pkgKey string
	name   string
	fields map[string]struct{}
}

func forEachPackagePair[T any](sigs []T, pkgOf, relOf, nameOf func(T) string, fn func(a, b T)) {
	pkgSigs := make(map[string][]T)
	for _, s := range sigs {
		pkg := pkgOf(s)
		pkgSigs[pkg] = append(pkgSigs[pkg], s)
	}

	pkgKeys := make([]string, 0, len(pkgSigs))
	for k := range pkgSigs {
		pkgKeys = append(pkgKeys, k)
	}
	sort.Strings(pkgKeys)

	for _, pkgKey := range pkgKeys {
		group := pkgSigs[pkgKey]
		sort.Slice(group, func(i, j int) bool {
			relI, relJ := relOf(group[i]), relOf(group[j])
			if relI != relJ {
				return relI < relJ
			}
			return nameOf(group[i]) < nameOf(group[j])
		})
		for i := range group {
			for j := i + 1; j < len(group); j++ {
				fn(group[i], group[j])
			}
		}
	}
}

func checkPairCloneViolation(a, b funcCloneSig) string {
	if a.pkgKey == b.pkgKey {
		if sim := jaccardSimilarity(a.grams, b.grams); sim >= maxCloneSimilarity {
			return fmt.Sprintf("%s (%s) <-> %s (%s): near-miss AST clone similarity %.2f exceeds %.2f ceiling", a.rel, a.name, b.rel, b.name, sim, maxCloneSimilarity)
		}
		if editSim := editSimilarity(a.scopedTokens, b.scopedTokens); editSim >= maxCloneEditSimilarity {
			return fmt.Sprintf("%s (%s) <-> %s (%s): near-miss AST edit similarity %.2f exceeds %.2f ceiling", a.rel, a.name, b.rel, b.name, editSim, maxCloneEditSimilarity)
		}
		return ""
	}
	if a.name == "main" || b.name == "main" || len(a.grams) < minCrossPkgCloneGrams || len(b.grams) < minCrossPkgCloneGrams {
		return ""
	}
	if sim := jaccardSimilarity(a.grams, b.grams); sim >= maxCloneSimilarity {
		return fmt.Sprintf("%s (%s) <-> %s (%s): near-miss AST clone similarity %.2f exceeds %.2f ceiling", a.rel, a.name, b.rel, b.name, sim, maxCloneSimilarity)
	}
	return ""
}

func checkCloneViolations(sigs []funcCloneSig) []string {
	var violations []string
	forEachPackagePair(
		sigs,
		func(s funcCloneSig) string { return "all" },
		func(s funcCloneSig) string { return s.rel },
		func(s funcCloneSig) string { return s.name },
		func(a, b funcCloneSig) {
			if v := checkPairCloneViolation(a, b); v != "" {
				violations = append(violations, v)
			}
		},
	)
	return violations
}

func checkStructCloneViolations(sigs []structCloneSig) []string {
	var violations []string
	forEachPackagePair(
		sigs,
		func(s structCloneSig) string { return s.pkgKey },
		func(s structCloneSig) string { return s.rel },
		func(s structCloneSig) string { return s.name },
		func(a, b structCloneSig) {
			sim := jaccardSimilarity(a.fields, b.fields)
			if sim >= maxStructCloneSimilarity {
				violations = append(violations, fmt.Sprintf("%s (%s) <-> %s (%s): struct field similarity %.2f exceeds %.2f ceiling", a.rel, a.name, b.rel, b.name, sim, maxStructCloneSimilarity))
			}
		},
	)
	return violations
}

func editSimilarity(a, b []string) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0.0
	}
	m, n := len(a), len(b)
	dp := make([]int, n+1)
	for j := 0; j <= n; j++ {
		dp[j] = j
	}
	for i := 1; i <= m; i++ {
		prev := dp[0]
		dp[0] = i
		for j := 1; j <= n; j++ {
			temp := dp[j]
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			dp[j] = min(prev+cost, min(dp[j]+1, dp[j-1]+1))
			prev = temp
		}
	}
	dist := dp[n]
	maxLen := max(m, n)
	return 1.0 - float64(dist)/float64(maxLen)
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

func appendNodeCloneTokens(tokens []string, n ast.Node) []string {
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
	return tokens
}

func buildCloneTokens(body *ast.BlockStmt) []string {
	var tokens []string
	ast.Inspect(body, func(n ast.Node) bool {
		if n == nil {
			return true
		}
		tokens = appendNodeCloneTokens(tokens, n)
		return true
	})
	return tokens
}

func buildScopedCloneTokens(body *ast.BlockStmt) []string {
	if body == nil {
		return nil
	}
	var tokens []string
	var blockStack []bool
	ast.Inspect(body, func(n ast.Node) bool {
		if n == nil {
			if len(blockStack) > 0 {
				isBlock := blockStack[len(blockStack)-1]
				blockStack = blockStack[:len(blockStack)-1]
				if isBlock {
					tokens = append(tokens, "}")
				}
			}
			return true
		}
		_, isBlock := n.(*ast.BlockStmt)
		blockStack = append(blockStack, isBlock)
		if isBlock {
			tokens = append(tokens, "{")
		}
		tokens = appendNodeCloneTokens(tokens, n)
		return true
	})
	return tokens
}

func collectStructCloneSigs(f *ast.File, rel, pkgKey string) []structCloneSig {
	var sigs []structCloneSig
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}
			fields := buildStructFields(st)
			if len(fields) >= minStructCloneFields {
				sigs = append(sigs, structCloneSig{
					rel:    rel,
					pkgKey: pkgKey,
					name:   ts.Name.Name,
					fields: fields,
				})
			}
		}
	}
	return sigs
}

func buildStructFields(st *ast.StructType) map[string]struct{} {
	fields := make(map[string]struct{})
	if st == nil || st.Fields == nil {
		return fields
	}
	for _, f := range st.Fields.List {
		tagVal := ""
		if f.Tag != nil {
			tagVal = f.Tag.Value
		}
		typeStr := formatFieldType(f.Type)
		if len(f.Names) == 0 {
			fields["embed:"+typeStr+":"+tagVal] = struct{}{}
			continue
		}
		for _, name := range f.Names {
			fields[name.Name+":"+typeStr+":"+tagVal] = struct{}{}
		}
	}
	return fields
}

func formatFieldType(expr ast.Expr) string {
	switch x := expr.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.StarExpr:
		return "*" + formatFieldType(x.X)
	case *ast.SelectorExpr:
		return formatFieldType(x.X) + "." + x.Sel.Name
	case *ast.ArrayType:
		return "[]" + formatFieldType(x.Elt)
	case *ast.MapType:
		return "map[" + formatFieldType(x.Key) + "]" + formatFieldType(x.Value)
	default:
		return "other"
	}
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

func analyzeTestFile(fullPath, rel string, r rules) (FileMetrics, []string, error) {
	src, err := os.ReadFile(fullPath)
	if err != nil {
		return FileMetrics{}, nil, err
	}
	if isGeneratedSource(src) {
		return FileMetrics{}, nil, nil
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, fullPath, src, parser.ParseComments)
	if err != nil {
		return FileMetrics{}, nil, err
	}

	var violations []string
	if r.isCatchAllTestFilename(rel) {
		violations = append(violations, rel+": test filename uses banned catch-all suffix; fold cases into the canonical domain-scoped Test* table or name after specific behavior")
	}
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

	tableStructs := make(map[string]bool)
	ast.Inspect(f, func(n ast.Node) bool {
		if arr, ok := n.(*ast.ArrayType); ok && arr.Len == nil {
			if ident, ok := arr.Elt.(*ast.Ident); ok {
				tableStructs[ident.Name] = true
			}
		}
		return true
	})

	ast.Inspect(f, func(n ast.Node) bool {
		return inspectTestNode(n, fset, rel, r, tableStructs, &violations)
	})

	return FileMetrics{Lines: countLines(src)}, violations, nil
}

func inspectTestNode(n ast.Node, fset *token.FileSet, rel string, r rules, tableStructs map[string]bool, violations *[]string) bool {
	if sel, ok := n.(*ast.SelectorExpr); ok && r.testRuleEnabled(ruleHTTPDefaultClient) {
		if isSelectorNamed(sel.X, "http", "DefaultClient") && sel.Sel.Name == "CloseIdleConnections" {
			return false
		}
		if isSelectorNamed(sel, "http", "DefaultClient") {
			line := fset.Position(sel.Pos()).Line
			*violations = append(*violations, fmt.Sprintf("%s:%d: unguarded http.DefaultClient in tests is prohibited; use httptest.Server.Client() or an http.Client with a mock Transport", rel, line))
		}
	}
	if lit, ok := n.(*ast.CompositeLit); ok && r.testRuleEnabled(ruleHTTPClientLiteral) {
		if reason := checkHTTPClientLit(lit); reason != "" {
			line := fset.Position(lit.Pos()).Line
			*violations = append(*violations, fmt.Sprintf("%s:%d: %s", rel, line, reason))
		}
	}
	if call, ok := n.(*ast.CallExpr); ok {
		if rule, reason := checkBannedTestCall(call); reason != "" && r.testRuleEnabled(rule) {
			line := fset.Position(call.Pos()).Line
			*violations = append(*violations, fmt.Sprintf("%s:%d: %s", rel, line, reason))
		}
	}
	return inspectTestNodeStructs(n, fset, rel, r, tableStructs, violations)
}

func inspectTestNodeStructs(n ast.Node, fset *token.FileSet, rel string, r rules, tableStructs map[string]bool, violations *[]string) bool {
	if ts, ok := n.(*ast.TypeSpec); ok {
		if st, ok := ts.Type.(*ast.StructType); ok && st.Fields != nil {
			if tableStructs[ts.Name.Name] {
				checkTestTableStructFields(st, fset, rel, violations)
				return false
			}
			checkTestStructFields(st, fset, rel, r, violations)
			return false
		}
	}
	if arr, ok := n.(*ast.ArrayType); ok && arr.Len == nil {
		if st, ok := arr.Elt.(*ast.StructType); ok {
			checkTestTableStructFields(st, fset, rel, violations)
			return false
		}
	}
	if st, ok := n.(*ast.StructType); ok && st.Fields != nil {
		checkTestStructFields(st, fset, rel, r, violations)
	}
	return true
}

func checkTestTableStructFields(st *ast.StructType, fset *token.FileSet, rel string, violations *[]string) {
	if st == nil || st.Fields == nil {
		return
	}
	for _, field := range st.Fields.List {
		if _, isFunc := field.Type.(*ast.FuncType); !isFunc {
			continue
		}
		for _, ident := range field.Names {
			line := fset.Position(ident.Pos()).Line
			*violations = append(*violations, fmt.Sprintf("%s:%d: test struct field %q uses a banned closure func type; use declarative expectation fields instead", rel, line, ident.Name))
		}
	}
}

func checkTestStructFields(st *ast.StructType, fset *token.FileSet, rel string, r rules, violations *[]string) {
	for _, field := range st.Fields.List {
		if _, isFunc := field.Type.(*ast.FuncType); !isFunc {
			continue
		}
		for _, ident := range field.Names {
			if r.isBannedClosureFieldName(ident.Name) {
				line := fset.Position(ident.Pos()).Line
				*violations = append(*violations, fmt.Sprintf("%s:%d: test struct field %q uses a banned closure func type; use declarative expectation fields instead", rel, line, ident.Name))
			}
		}
	}
}

func isSelectorNamed(expr ast.Expr, pkgName, selName string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != selName {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == pkgName
}

func checkHTTPClientLit(lit *ast.CompositeLit) string {
	if !isSelectorNamed(lit.Type, "http", "Client") {
		return ""
	}
	for _, elt := range lit.Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Transport" && !isDisallowedTransportExpr(kv.Value) {
				return ""
			}
		}
	}
	return "http.Client literal without mock Transport in tests is prohibited; inject a mock RoundTripper or use httptest.Server.Client()"
}

func isDisallowedTransportExpr(expr ast.Expr) bool {
	if ident, ok := expr.(*ast.Ident); ok && ident.Name == "nil" {
		return true
	}
	if isSelectorNamed(expr, "http", "DefaultTransport") {
		return true
	}
	if unary, ok := expr.(*ast.UnaryExpr); ok && unary.Op == token.AND {
		return isDisallowedTransportExpr(unary.X)
	}
	if comp, ok := expr.(*ast.CompositeLit); ok && isSelectorNamed(comp.Type, "http", "Transport") {
		return true
	}
	return false
}

// checkBannedTestCall returns the violated rule ID and its message, or two
// empty strings.
func checkBannedTestCall(call *ast.CallExpr) (string, string) {
	if isSelectorNamed(call.Fun, "time", "Sleep") {
		return ruleTimeSleep, "time.Sleep in tests is prohibited; use event-driven channels, condition variables, or injected delay overrides"
	}
	if isSelectorNamed(call.Fun, "syscall", "Kill") {
		return ruleSyscallKill, "raw syscall.Kill in tests is prohibited; use dependency-injected signal or process-kill functions"
	}
	if reason := checkReservedPortCall(call); reason != "" {
		return ruleReservedPort, reason
	}
	return "", ""
}

func checkReservedPortCall(call *ast.CallExpr) string {
	callName, urlExpr := getCallTargetURL(call)
	if callName == "" || urlExpr == nil {
		return ""
	}
	lit, ok := urlExpr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return ""
	}
	val, err := strconv.Unquote(lit.Value)
	if err != nil {
		val = strings.Trim(lit.Value, "`\"")
	}
	u, err := url.Parse(val)
	if err != nil || u.Host == "" {
		u2, err2 := url.Parse("http://" + val)
		if err2 != nil {
			return ""
		}
		u = u2
	}
	host := u.Hostname()
	port := u.Port()
	if !isLoopbackHost(host) || !isReservedDaemonPort(port) {
		return ""
	}
	return fmt.Sprintf("call to %s targeting reserved live daemon port %s in tests is prohibited; use httptest.Server or mock transport instead", callName, port)
}

func getCallTargetURL(call *ast.CallExpr) (string, ast.Expr) {
	if isSelectorNamed(call.Fun, "http", "Get") {
		if len(call.Args) >= 1 {
			return "http.Get", call.Args[0]
		}
	}
	if isSelectorNamed(call.Fun, "http", "Post") {
		if len(call.Args) >= 1 {
			return "http.Post", call.Args[0]
		}
	}
	if isSelectorNamed(call.Fun, "http", "NewRequest") {
		if len(call.Args) >= 2 {
			return "http.NewRequest", call.Args[1]
		}
	}
	if isSelectorNamed(call.Fun, "http", "NewRequestWithContext") {
		if len(call.Args) >= 3 {
			return "http.NewRequestWithContext", call.Args[2]
		}
	}
	if ident, ok := call.Fun.(*ast.Ident); ok {
		switch ident.Name {
		case newHTTPPromptDispatcherFunc:
			if len(call.Args) >= 1 {
				return newHTTPPromptDispatcherFunc, call.Args[0]
			}
		case "newRequestWithContext":
			if len(call.Args) >= 3 {
				return "newRequestWithContext", call.Args[2]
			}
		}
	}
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		if sel.Sel.Name == newHTTPPromptDispatcherFunc {
			if len(call.Args) >= 1 {
				return newHTTPPromptDispatcherFunc, call.Args[0]
			}
		}
	}
	return "", nil
}

func isLoopbackHost(host string) bool {
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

func isReservedDaemonPort(port string) bool {
	switch port {
	case "8080", "8088", "4020", "8081", "8082", "8083", "8090", "8070", "9070":
		return true
	default:
		return false
	}
}
