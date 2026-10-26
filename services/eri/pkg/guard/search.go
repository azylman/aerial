package guard

import (
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
)

type toolIndexArgs struct {
	Search         string `json:"search"`
	Category       string `json:"category"`
	Name           string `json:"name"`
	IncludeSchemas bool   `json:"include_schemas"`
}

func parseToolIndexArgs(raw json.RawMessage) toolIndexArgs {
	var parsed toolIndexArgs
	if len(raw) == 0 {
		return parsed
	}
	if err := json.Unmarshal(raw, &parsed); err == nil && (parsed.Search != "" || parsed.Category != "" || parsed.Name != "" || parsed.IncludeSchemas) {
		return parsed
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil && s != "" {
		if err := json.Unmarshal([]byte(s), &parsed); err == nil {
			return parsed
		}
	}
	return parsed
}

func parseRawJSONMap(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err == nil && len(m) > 0 {
		return m
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil && s != "" {
		if err := json.Unmarshal([]byte(s), &m); err == nil {
			return m
		}
	}
	return nil
}

var sqlWildcardReplacer = strings.NewReplacer("*", " * ", ",", " , ", ".", " . ", "(", " ( ", ")", " ) ", ";", " ; ")

func skipSQLSingleQuote(sqlStr string, i int) int {
	for i++; i < len(sqlStr); i++ {
		if sqlStr[i] == '\'' {
			if i+1 < len(sqlStr) && sqlStr[i+1] == '\'' {
				i++
				continue
			}
			return i
		}
	}
	return len(sqlStr)
}

func stripSQLQuotesAndComments(sqlStr string) string {
	var b strings.Builder
	for i := 0; i < len(sqlStr); i++ {
		if sqlStr[i] == '\'' {
			i = skipSQLSingleQuote(sqlStr, i)
			b.WriteByte(' ')
			continue
		}
		if sqlStr[i] == '-' && i+1 < len(sqlStr) && sqlStr[i+1] == '-' {
			for i < len(sqlStr) && sqlStr[i] != '\n' {
				i++
			}
			b.WriteByte(' ')
			continue
		}
		b.WriteByte(sqlStr[i])
	}
	return b.String()
}

func hasSelectWildcard(sqlStr string) bool {
	cleaned := strings.ToLower(stripSQLQuotesAndComments(sqlStr))
	tokens := strings.Fields(sqlWildcardReplacer.Replace(cleaned))
	for i, tok := range tokens {
		if tok == "*" && i > 0 {
			switch tokens[i-1] {
			case "select", "distinct", ",", ".":
				return true
			}
		}
	}
	return false
}

func mapField(m map[string]any, key string) map[string]any {
	if v, ok := m[key].(map[string]any); ok {
		return v
	}
	return nil
}

func stringField(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func hasExcessiveSearchSQLLimit(sqlStr string, queryMap map[string]any) bool {
	if sz, ok := queryMap["size"].(float64); ok && sz > MaxSelectLimitRows {
		return true
	}
	cleaned := strings.ToLower(stripSQLQuotesAndComments(sqlStr))
	tokens := strings.Fields(sqlWildcardReplacer.Replace(cleaned))
	for i, tok := range tokens {
		if tok == "limit" && i+1 < len(tokens) {
			if n, err := strconv.Atoi(tokens[i+1]); err == nil && n > MaxSelectLimitRows {
				return true
			}
		}
	}
	return false
}

func shapeSearchSQLOptions(reqBody, queryMap map[string]any) bool {
	changed := false
	if queryMap != nil {
		if sz, ok := queryMap["size"].(float64); !ok || sz <= 0 {
			queryMap["size"] = MaxSelectLimitRows
			changed = true
		}
	}
	agentOpts := mapField(reqBody, "agent_options")
	fmtVal := strings.ToLower(strings.TrimSpace(stringField(agentOpts, "output_format")))
	if fmtVal == "csv" || fmtVal == "md_table" {
		agentOpts["output_format"] = "json"
		changed = true
	}
	return changed
}

func inspectOpenObserveSearchSQL(raw json.RawMessage) *Decision {
	root := parseRawJSONMap(raw)
	reqBody := mapField(root, "request_body")
	if reqBody == nil {
		return &Decision{Decision: DecisionAllow}
	}
	queryMap := mapField(reqBody, "query")
	sqlStr := stringField(queryMap, "sql")
	if hasSelectWildcard(sqlStr) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Wildcard SELECT * is prohibited in openobserve SearchSQL. Select explicit columns such as: SELECT _timestamp, service, level, message FROM docker_logs ...",
		}
	}
	if hasExcessiveSearchSQLLimit(sqlStr, queryMap) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "openobserve SearchSQL limit exceeds maximum allowed rows (50). Use LIMIT <= 50 (and query.size <= 50) to prevent context bloat.",
		}
	}
	if shapeSearchSQLOptions(reqBody, queryMap) {
		return &Decision{
			Decision:  DecisionAllow,
			Overwrite: map[string]any{"Arguments": root},
		}
	}
	return &Decision{Decision: DecisionAllow}
}

func inspectMCPToolCall(args ToolArgs) *Decision {
	mcpTool := strings.ToLower(strings.Trim(strings.TrimSpace(args.ToolName), `"`))
	if mcpTool == "streamschema" {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "openobserve StreamSchema is blocked because docker_logs contains 28,000+ historical dynamic attribute fields. Query openobserve SearchSQL directly using canonical columns: _timestamp, service, level, message, stream, container_name, host (never use SELECT *).",
		}
	}
	if mcpTool == "searchsql" {
		return inspectOpenObserveSearchSQL(args.Arguments)
	}
	if mcpTool != "tool_index" && !strings.HasSuffix(mcpTool, "_tool_index") {
		return &Decision{Decision: DecisionAllow}
	}

	idxArgs := parseToolIndexArgs(args.Arguments)
	hasFilter := strings.TrimSpace(idxArgs.Search) != "" ||
		strings.TrimSpace(idxArgs.Category) != "" ||
		strings.TrimSpace(idxArgs.Name) != ""
	if !hasFilter {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unfiltered MCP tool index call detected. Calling a tool_index endpoint without 'search', 'category', or 'name' dumps the entire tool catalog into the context window. Pass a specific 'search' keyword, 'category', or exact 'name' (or read the local tool schema JSON under <appDataDir>/mcp/<server>/<tool>.json if already listed).",
		}
	}
	if idxArgs.IncludeSchemas && strings.TrimSpace(idxArgs.Name) == "" {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unscoped MCP tool index schema dump detected: 'include_schemas: true' requires an exact 'name' parameter. Discover tool names with 'search' or 'category' (include_schemas=false) first, then request the schema for a single tool via 'name'.",
		}
	}
	return &Decision{Decision: DecisionAllow}
}

var binaryExtensions = map[string]bool{
	".so":    true,
	".a":     true,
	".o":     true,
	".dylib": true,
	".exe":   true,
	".tar":   true,
	".gz":    true,
	".tgz":   true,
	".zip":   true,
	".pyc":   true,
	".bin":   true,
	".iso":   true,
	".wasm":  true,
}

var binaryDirs = []string{
	"/bin",
	"/sbin",
	"/usr/bin",
	"/usr/sbin",
	"/usr/local/bin",
	"/usr/local/sbin",
}

var grepValueFlags = map[string]bool{
	"-A": true, "-B": true, "-C": true,
	"-g": true, "--glob": true, "--iglob": true,
	"-t": true, "--type": true, "-T": true, "--type-not": true,
	"--include": true,
}

var grepMatchLimitLongFlags = map[string]bool{
	"--max-count":           true,
	"--files-with-matches":  true,
	"--files-without-match": true,
	"--files":               true,
	"--count":               true,
	"--count-matches":       true,
	"--quiet":               true,
	"--silent":              true,
	"--help":                true,
	"--version":             true,
}

// splitPipeline splits a shell command line on ;, &&, ||, and | respecting single and double quotes.
func splitPipeline(cmd string) []string {
	cmds, _ := splitPipelineSegments(cmd)
	return cmds
}

func detectDelimiter(chars []rune, i int) (op rune, advance int) {
	r := chars[i]
	if r == '&' && i+1 < len(chars) && chars[i+1] == '&' {
		return '&', 1
	}
	if r == '|' && i+1 < len(chars) && chars[i+1] == '|' {
		return ';', 1
	}
	if r == ';' || r == '|' || r == '\n' {
		return r, 0
	}
	return 0, 0
}

func splitPipelineSegments(cmd string) ([]string, []rune) {
	var cmds []string
	var ops []rune
	var current strings.Builder
	var lastOp rune
	inSingle, inDouble, escaped := false, false, false

	flush := func(nextOp rune) {
		if s := strings.TrimSpace(current.String()); s != "" {
			cmds = append(cmds, s)
			ops = append(ops, lastOp)
			lastOp = nextOp
		} else if nextOp == '|' {
			lastOp = nextOp
		}
		current.Reset()
	}

	chars := []rune(cmd)
	for i := 0; i < len(chars); i++ {
		r := chars[i]
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\' && !inSingle:
			escaped = true
			current.WriteRune(r)
		case r == '\'' && !inDouble:
			inSingle = !inSingle
			current.WriteRune(r)
		case r == '"' && !inSingle:
			inDouble = !inDouble
			current.WriteRune(r)
		case !inSingle && !inDouble:
			if op, adv := detectDelimiter(chars, i); op != 0 {
				flush(op)
				i += adv
				continue
			}
			current.WriteRune(r)
		default:
			current.WriteRune(r)
		}
	}

	flush(0)
	return cmds, ops
}

// tokenizeQuoteAware tokenizes a single command respecting single and double quotes.
func tokenizeQuoteAware(sub string) []string {
	var tokens []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	escaped := false

	for _, r := range sub {
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' && !inSingle {
			escaped = true
			continue
		}
		if r == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if r == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}
		if !inSingle && !inDouble && (r == ' ' || r == '\t' || r == '\n') {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			continue
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// CheckSearch evaluates search and psql commands in run_command against unconstrained root, missing maxdepth,
// binary scanning, and SQL context-bloat invariants.
func CheckSearch(toolName string, args ToolArgs) *Decision {
	return CheckSearchWithSQL(toolName, args, DefaultSQLDescriber())
}

// CheckSearchWithSQL evaluates search and psql commands using the provided SQLDescriber.
func CheckSearchWithSQL(toolName string, args ToolArgs, describer SQLDescriber) *Decision {
	normalizedName := normalizeToolName(toolName)
	if normalizedName == "call_mcp_tool" {
		return inspectMCPToolCall(args)
	}
	if normalizedName != "run_command" && normalizedName != "grep_search" && normalizedName != "find_by_name" {
		return &Decision{Decision: DecisionAllow}
	}

	cmdLine := strings.TrimSpace(args.CommandLine)
	if cmdLine == "" {
		return &Decision{Decision: DecisionAllow}
	}

	commands, ops := splitPipelineSegments(cmdLine)
	for idx, cmd := range commands {
		tokens := unwrapCommandTokens(tokenizeQuoteAware(cmd))
		if len(tokens) == 0 {
			continue
		}

		bin := filepath.Base(strings.TrimLeft(tokens[0], "("))
		if (bin == "sh" || bin == "bash") && len(tokens) >= 3 && tokens[1] == "-c" {
			if dec := CheckSearchWithSQL("run_command", ToolArgs{CommandLine: tokens[2], Cwd: args.Cwd}, describer); dec != nil && dec.Decision == DecisionDeny {
				return dec
			}
			continue
		}

		// 1. Evaluate find commands
		if bin == "find" {
			if dec := inspectFindCommand(tokens, args.Cwd); dec != nil {
				return dec
			}
			continue
		}

		// 2. Evaluate grep / search / strings commands
		if isGrepOrSearchBinary(bin) {
			if dec := inspectGrepCommand(bin, tokens, ops[idx] == '|'); dec != nil {
				return dec
			}
			continue
		}
	}

	// 3. Evaluate psql invocations (schema validation, SELECT * ban, LIMIT gate, and -A shaping)
	return inspectPsqlPipeline(args.CommandLine, describer)
}

func unwrapCommandTokens(tokens []string) []string {
	tokens = stripLeadingEnv(tokens)
	if len(tokens) >= 3 && filepath.Base(strings.TrimLeft(tokens[0], "(")) == "docker" && tokens[1] == "exec" {
		i := 2
		for i < len(tokens) && strings.HasPrefix(tokens[i], "-") {
			if dockerExecValFlags[tokens[i]] {
				i += 2
			} else {
				i++
			}
		}
		if i+1 < len(tokens) {
			tokens = stripLeadingEnv(tokens[i+1:])
		}
	}
	return tokens
}

func isGrepOrSearchBinary(bin string) bool {
	switch bin {
	case "grep", "egrep", "fgrep", "rg", "ag", "ack", "strings":
		return true
	default:
		return false
	}
}

func hasShortFlag(token string, flag rune) bool {
	if !strings.HasPrefix(token, "-") || strings.HasPrefix(token, "--") {
		return false
	}
	for _, r := range token[1:] {
		if r >= '0' && r <= '9' {
			break
		}
		if r == flag {
			return true
		}
	}
	return false
}

func isGrepMatchLimitFlag(t string) (isLimit, takesArg bool) {
	if grepMatchLimitLongFlags[t] || strings.HasPrefix(t, "--max-count=") {
		return true, t == "--max-count"
	}
	for _, f := range []rune{'m', 'l', 'L', 'c', 'q'} {
		if hasShortFlag(t, f) {
			return true, f == 'm' && strings.HasSuffix(t, "m")
		}
	}
	return false, false
}

func inspectFindCommand(tokens []string, cwd string) *Decision {
	hasMaxdepth := false
	for i := 1; i < len(tokens); i++ {
		t := tokens[i]
		if t == "-help" || t == "--help" || t == "-version" || t == "--version" {
			return nil
		}
		if t == "-maxdepth" || t == "--maxdepth" {
			hasMaxdepth = true
		}
	}

	// Check target path tokens
	for i := 1; i < len(tokens); i++ {
		t := tokens[i]
		if strings.HasPrefix(t, "-") {
			// Skip flags and their arguments if flag takes an argument
			if t == "-name" || t == "-iname" || t == "-path" || t == "-ipath" || t == "-type" || t == "-maxdepth" || t == "-mindepth" || t == "-mtime" || t == "-ctime" || t == "-atime" || t == "-perm" || t == "-size" || t == "-user" || t == "-group" {
				i++
			}
			continue
		}

		// Target path token
		clean := filepath.Clean(t)
		if clean == "/" || clean == "/proc" || clean == "/sys" || clean == "/dev" || clean == "/root" {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Unconstrained root search detected. Avoid searching the root filesystem ('/') or system directories directly. Scope searches to specific package or workspace directories.",
			}
		}

		if cwd == "/" && (clean == "." || clean == "./") {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Unconstrained root search detected. Current working directory is root ('/'). Scope searches to specific workspace directories.",
			}
		}

		if clean == ".git" || strings.HasPrefix(clean, ".git/") || clean == "node_modules" || strings.HasPrefix(clean, "node_modules/") {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Direct search into large internal repository directory (.git/node_modules) detected. Exclude internal version control and dependency trees from searches.",
			}
		}
	}

	if !hasMaxdepth {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unconstrained find command without -maxdepth detected. Always specify '-maxdepth <N>' (e.g. 'find <path> -maxdepth 3 -name ...') to prevent deep recursive tree traversals into .git/objects, vendor, or cache volumes.",
		}
	}

	return nil
}

func validateGrepPathToken(pt string) *Decision {
	clean := filepath.Clean(pt)
	if clean == "/" || clean == "/proc" || clean == "/sys" || clean == "/dev" {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unconstrained root search detected. Avoid searching the root filesystem ('/') or system directories directly. Scope searches to specific package or workspace directories.",
		}
	}
	if clean == ".git" || strings.HasPrefix(clean, ".git/") || clean == "node_modules" || strings.HasPrefix(clean, "node_modules/") {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Direct search into large internal repository directory (.git/node_modules) detected. Exclude internal version control and dependency trees from searches.",
		}
	}
	if binaryExtensions[strings.ToLower(filepath.Ext(clean))] {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Binary search detected. Scanning compiled system binaries or archive files with grep/strings is prohibited. Inspect source code or documentation instead.",
		}
	}
	for _, bDir := range binaryDirs {
		if clean == bDir || strings.HasPrefix(clean, bDir+"/") {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Binary directory search detected. Scanning system binary directories is prohibited. Inspect source code or documentation instead.",
			}
		}
	}
	return nil
}

func isStandardGrep(bin string) bool {
	return bin == "grep" || bin == "egrep" || bin == "fgrep"
}

func hasBroadPathToken(pathTokens []string) bool {
	for _, pt := range pathTokens {
		switch filepath.Clean(pt) {
		case ".", "..", "/share", "/share/aerial", "/data":
			return true
		}
	}
	return false
}

var grepNoPatternLongFlags = map[string]bool{
	"--files":   true,
	"--help":    true,
	"--version": true,
}

func inspectGrepCommand(bin string, tokens []string, fromPipe bool) *Decision {
	hasExcludeDir, hasMatchLimit, hasExplicitPattern := false, false, false
	isRecursive := (bin == "rg" || bin == "ag" || bin == "ack")
	var pathTokens []string

	for i := 1; i < len(tokens); i++ {
		t := tokens[i]
		if strings.HasPrefix(t, "-") {
			if strings.HasPrefix(t, "--exclude-dir") || strings.HasPrefix(t, "--exclude") {
				hasExcludeDir = true
				if (t == "--exclude-dir" || t == "--exclude") && i+1 < len(tokens) {
					i++
				}
			}
			if isStandardGrep(bin) && (t == "--recursive" || hasShortFlag(t, 'r') || hasShortFlag(t, 'R')) {
				isRecursive = true
			}
			if limit, takesArg := isGrepMatchLimitFlag(t); limit {
				hasMatchLimit = true
				if grepNoPatternLongFlags[t] {
					hasExplicitPattern = true
				}
				if takesArg && i+1 < len(tokens) {
					i++
				}
			}
			if t == "-e" || t == "-f" || t == "--regexp" || t == "--file" {
				hasExplicitPattern = true
				i++
			} else if grepValueFlags[t] && i+1 < len(tokens) {
				i++
			}
			continue
		}
		if !hasExplicitPattern {
			hasExplicitPattern = true
			continue
		}
		pathTokens = append(pathTokens, t)
	}

	if bin == "strings" {
		pathTokens = nil
		for _, t := range tokens[1:] {
			if !strings.HasPrefix(t, "-") {
				pathTokens = append(pathTokens, t)
			}
		}
	}

	for _, pt := range pathTokens {
		if dec := validateGrepPathToken(pt); dec != nil {
			return dec
		}
	}

	if isRecursive && isStandardGrep(bin) && !hasExcludeDir && hasBroadPathToken(pathTokens) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Recursive grep without directory exclusions detected. Add '--exclude-dir=.git' (or use ripgrep/rg, which excludes .git by default) to prevent scanning raw git packfiles and binary objects.",
		}
	}

	if isRecursive && !(fromPipe && len(pathTokens) == 0) && !hasMatchLimit {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unbounded recursive text search detected. Bound output with '-m <N>' / '--max-count=<N>' (e.g. 'rg -n -m 20 <pattern> <path>'), '-l' ('--files-with-matches'), or '-c' ('--count') per the shell-filesystem-traversal skill to prevent context window bloat.",
		}
	}

	return nil
}
