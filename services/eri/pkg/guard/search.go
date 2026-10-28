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

func inspectDiscordSearchMessages(raw json.RawMessage) *Decision {
	m := parseRawJSONMap(raw)
	if lim, ok := m["limit"].(float64); ok && lim > MaxUnboundedPlanRows {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "discord_search_messages limit exceeds maximum allowed per page (25). Use limit <= 25 (default 25) and paginate via 'offset' or 'maxId' when 'has_more' is true.",
		}
	}
	return &Decision{Decision: DecisionAllow}
}

func inspectGitHubGetFileContents(raw json.RawMessage) *Decision {
	m := parseRawJSONMap(raw)
	if m == nil {
		return &Decision{Decision: DecisionAllow}
	}
	owner := strings.ToLower(strings.TrimSpace(stringField(m, "owner")))
	repo := strings.ToLower(strings.TrimSpace(stringField(m, "repo")))
	ref := strings.TrimSpace(stringField(m, "ref"))
	sha := strings.TrimSpace(stringField(m, "sha"))
	isDefaultRef := ref == "" || ref == "main" || ref == "refs/heads/main" || ref == "HEAD"
	if (owner == "" || owner == "azylman") && (repo == "aerial" || repo == "aerial-config" || repo == "mirrormere") && isDefaultRef && sha == "" {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Remote github-mcp get_file_contents on azylman/" + repo + " (default branch) is prohibited because /share/" + repo + " is mounted locally and remote fetches dump unpaginated files into the context window. Use read_file (with StartLine/EndLine) or rg/find on /share/" + repo + " instead, or pass an explicit non-main 'ref'/'sha' when inspecting an unmerged remote commit.",
		}
	}
	if _, hasFields := m["fields"]; !hasFields {
		m["fields"] = []string{"name", "type", "path", "size"}
		return &Decision{
			Decision:  DecisionAllow,
			Overwrite: map[string]any{"Arguments": m},
		}
	}
	return &Decision{Decision: DecisionAllow}
}

const (
	maxVictoriaMetricsSeriesLimit = 100
	maxGrepMatchLimit             = 20
	maxRecursiveContextLines      = 3
	maxGitLogPatchCommits         = 5
)

func hasConcreteMetricSelector(match string) bool {
	if match == "" || strings.Contains(match, "__name__=~") || strings.Contains(match, "__name__!~") {
		return false
	}
	if strings.HasPrefix(match, "{") {
		return strings.Contains(match, "__name__=")
	}
	return true
}

func inspectVictoriaMetricsSeries(raw json.RawMessage) *Decision {
	m := parseRawJSONMap(raw)
	match := strings.TrimSpace(stringField(m, "match"))
	if !hasConcreteMetricSelector(match) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unscoped victoriametrics series discovery detected: 'series' returns every label-value permutation and requires an exact metric name in 'match' (not empty, label-only '{...}', or '__name__=~' regexes). Use 'metrics' to discover metric names, or 'labels' / 'label_values' to inspect label dimensions.",
		}
	}
	lim, ok := m["limit"].(float64)
	if !ok || lim <= 0 || lim > maxVictoriaMetricsSeriesLimit {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "victoriametrics series requires an explicit 'limit' between 1 and 100 (e.g. \"limit\": 50) so result volume is bounded without silent truncation. Use 'labels' or 'label_values' for full dimension enumeration, or pass 'limit' <= 100 on 'series' to sample label combinations.",
		}
	}
	return &Decision{Decision: DecisionAllow}
}

func inspectMCPToolCall(args ToolArgs) *Decision {
	mcpTool := strings.ToLower(strings.Trim(strings.TrimSpace(args.ToolName), `"`))
	switch mcpTool {
	case "streamschema":
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "openobserve StreamSchema is blocked because docker_logs contains 28,000+ historical dynamic attribute fields. Query openobserve SearchSQL directly using canonical columns: _timestamp, service, level, message, stream, container_name, host (never use SELECT *).",
		}
	case "searchsql":
		return inspectOpenObserveSearchSQL(args.Arguments)
	case "discord_search_messages":
		return inspectDiscordSearchMessages(args.Arguments)
	case "get_file_contents":
		return inspectGitHubGetFileContents(args.Arguments)
	case "series":
		return inspectVictoriaMetricsSeries(args.Arguments)
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
	"-g": true, "--glob": true, "--iglob": true,
	"-t": true, "--type": true, "-T": true, "--type-not": true,
	"--include": true,
}

var grepContextFlags = map[string]bool{
	"-A": true, "-B": true, "-C": true,
	"--after-context": true, "--before-context": true, "--context": true,
}

var grepDiscoveryLongFlags = map[string]bool{
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

		// 2. Evaluate grep / search / strings / git grep commands
		if dec := inspectSearchSegment(bin, tokens, ops[idx] == '|'); dec != nil {
			return dec
		}
	}

	// 3. Evaluate psql invocations (schema validation, SELECT * ban, LIMIT gate, and -A shaping)
	return inspectPsqlPipeline(args.CommandLine, describer)
}

func inspectSearchSegment(bin string, tokens []string, fromPipe bool) *Decision {
	if bin == "git" {
		subcmd, subTokens := extractGitSubcommand(tokens)
		switch subcmd {
		case "grep":
			return inspectGrepCommand("git-grep", subTokens, fromPipe)
		case "log":
			return inspectGitLogCommand(subTokens)
		case "show":
			return inspectGitShowCommand(subTokens)
		}
		return nil
	}
	if isGrepOrSearchBinary(bin) {
		return inspectGrepCommand(bin, tokens, fromPipe)
	}
	return nil
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

var gitGlobalValueFlags = map[string]bool{
	"-C": true, "-c": true, "--git-dir": true, "--work-tree": true, "--namespace": true,
}

func extractGitSubcommand(tokens []string) (string, []string) {
	for i := 1; i < len(tokens); i++ {
		t := tokens[i]
		if strings.HasPrefix(t, "-") {
			if gitGlobalValueFlags[t] && i+1 < len(tokens) {
				i++
			}
			continue
		}
		return t, tokens[i:]
	}
	return "", nil
}

func atoiOrZero(s string) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return 0
}

func parseGitLogLimitFlag(t, nextToken string) (matched, consumeNext bool, count int) {
	if t == "-n" || t == "--max-count" {
		return true, nextToken != "", atoiOrZero(nextToken)
	}
	if strings.HasPrefix(t, "-n") && len(t) > 2 {
		n, err := strconv.Atoi(t[2:])
		return err == nil, false, n
	}
	if strings.HasPrefix(t, "--max-count=") {
		return true, false, atoiOrZero(strings.TrimPrefix(t, "--max-count="))
	}
	if len(t) > 1 && t[0] == '-' && t[1] >= '0' && t[1] <= '9' {
		n, err := strconv.Atoi(t[1:])
		return err == nil, false, n
	}
	return false, false, 0
}

func inspectGitLogCommand(tokens []string) *Decision {
	hasPatch := false
	hasLimit := false
	limitCount := 0
	for i := 1; i < len(tokens); i++ {
		t := tokens[i]
		if t == "--" {
			break
		}
		if t == "-p" || t == "--patch" || hasShortFlag(t, 'p') {
			hasPatch = true
		}
		nextToken := ""
		if i+1 < len(tokens) {
			nextToken = tokens[i+1]
		}
		if matched, consume, n := parseGitLogLimitFlag(t, nextToken); matched {
			hasLimit = true
			limitCount = n
			if consume {
				i++
			}
		}
	}
	if hasPatch && (!hasLimit || limitCount <= 0 || limitCount > maxGitLogPatchCommits) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unbounded or excessive 'git log -p' / '--patch' detected. Bound patch history with '-n <N>' (N <= 5, e.g. 'git log -n 3 -p -- <path>') or use '--oneline' / '--stat' first to prevent context window bloat.",
		}
	}
	return nil
}

var gitShowSafeFlags = map[string]bool{
	"-s":            true,
	"--no-patch":    true,
	"--stat":        true,
	"--numstat":     true,
	"--shortstat":   true,
	"--name-only":   true,
	"--name-status": true,
	"--oneline":     true,
	"--quiet":       true,
	"--help":        true,
}

func inspectGitShowCommand(tokens []string) *Decision {
	for i := 1; i < len(tokens); i++ {
		t := tokens[i]
		if t == "--" && i+1 < len(tokens) {
			return nil
		}
		if gitShowSafeFlags[t] || strings.HasPrefix(t, "--format=") || strings.HasPrefix(t, "--pretty=") {
			return nil
		}
		if !strings.HasPrefix(t, "-") && strings.Contains(t, ":") {
			return nil
		}
	}
	return &Decision{
		Decision: DecisionDeny,
		Reason:   "Unscoped 'git show' patch dump detected. Pass '-s' / '--stat' / '--name-only' to inspect commit metadata first, or scope patch output to specific files via 'git show <commit> -- <path>'.",
	}
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

func isGrepDiscoveryFlag(t string) bool {
	if grepDiscoveryLongFlags[t] {
		return true
	}
	for _, f := range []rune{'l', 'L', 'c', 'q'} {
		if hasShortFlag(t, f) {
			return true
		}
	}
	return false
}

func parseGrepMatchCountFlag(t, nextToken string) (matched, consumeNext bool, count int) {
	if t == "--max-count" {
		return true, nextToken != "", atoiOrZero(nextToken)
	}
	if strings.HasPrefix(t, "--max-count=") {
		return true, false, atoiOrZero(strings.TrimPrefix(t, "--max-count="))
	}
	if hasShortFlag(t, 'm') {
		idx := strings.IndexRune(t[1:], 'm') + 1
		rest := t[idx+1:]
		if rest == "" {
			return true, nextToken != "", atoiOrZero(nextToken)
		}
		return true, false, atoiOrZero(rest)
	}
	return false, false, 0
}

func parseGrepContextFlag(t, nextToken string) (matched, consumeNext bool, lines int) {
	if grepContextFlags[t] {
		return true, nextToken != "", atoiOrZero(nextToken)
	}
	if strings.HasPrefix(t, "--context=") || strings.HasPrefix(t, "--after-context=") || strings.HasPrefix(t, "--before-context=") {
		_, val, _ := strings.Cut(t, "=")
		return true, false, atoiOrZero(val)
	}
	if len(t) > 2 && t[0] == '-' && (t[1] == 'A' || t[1] == 'B' || t[1] == 'C') && t[2] >= '0' && t[2] <= '9' {
		return true, false, atoiOrZero(t[2:])
	}
	return false, false, 0
}

var findValueFlags = map[string]bool{
	"-name":     true,
	"-iname":    true,
	"-path":     true,
	"-ipath":    true,
	"-type":     true,
	"-maxdepth": true,
	"-mindepth": true,
	"-mtime":    true,
	"-ctime":    true,
	"-atime":    true,
	"-perm":     true,
	"-size":     true,
	"-user":     true,
	"-group":    true,
	"-exec":     true,
	"-execdir":  true,
	"-ok":       true,
	"-okdir":    true,
	"-regex":    true,
	"-iregex":   true,
	"-printf":   true,
}

func isFindValueFlag(t string) bool {
	return findValueFlags[t]
}

const runtimeDirDenyReason = "Searching internal runtime state (/data/runtimes) with grep/rg/find is prohibited because it scans multi-GB session transcripts. Search source repositories under /share/aerial or /share/aerial-config, or use the 'transcript-search' CLI for historical transcripts."

func isRuntimeStateDir(clean string) bool {
	return clean == "/data/runtimes" || strings.HasPrefix(clean, "/data/runtimes/")
}

func validateFindPathToken(t, cwd string) *Decision {
	clean := filepath.Clean(t)
	if clean == "/" || clean == "/proc" || clean == "/sys" || clean == "/dev" || clean == "/root" {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unconstrained root search detected. Avoid searching the root filesystem ('/') or system directories directly. Scope searches to specific package or workspace directories.",
		}
	}
	if isRuntimeStateDir(clean) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   runtimeDirDenyReason,
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

	return nil
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
			if isFindValueFlag(t) && i+1 < len(tokens) {
				i++
			}
			continue
		}

		if dec := validateFindPathToken(t, cwd); dec != nil {
			return dec
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
	if isRuntimeStateDir(clean) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   runtimeDirDenyReason,
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

var broadSearchPaths = map[string]bool{
	".":                      true,
	"..":                     true,
	"/share":                 true,
	"/share/aerial":          true,
	"/share/aerial/services": true,
	"/share/aerial/pkg":      true,
	"/share/aerial/mcp":      true,
	"/share/aerial-config":   true,
	"/share/mirrormere":      true,
	"/share/aerial-sidecars": true,
	"/data":                  true,
}

func hasBroadPathToken(pathTokens []string) bool {
	for _, pt := range pathTokens {
		if broadSearchPaths[filepath.Clean(pt)] {
			return true
		}
	}
	return false
}

func countDirectoryTargets(pathTokens []string) int {
	dirs := 0
	for _, pt := range pathTokens {
		if strings.HasSuffix(pt, "/") || filepath.Ext(filepath.Clean(pt)) == "" {
			dirs++
		}
	}
	return dirs
}

var grepNoPatternLongFlags = map[string]bool{
	"--files":   true,
	"--help":    true,
	"--version": true,
}

var grepPatternFlags = map[string]bool{
	"-e":       true,
	"-f":       true,
	"--regexp": true,
	"--file":   true,
}

func isExcludeDirFlag(t string) (matches bool, takesArg bool) {
	if t == "--exclude-dir" || t == "--exclude" {
		return true, true
	}
	if strings.HasPrefix(t, "--exclude-dir=") || strings.HasPrefix(t, "--exclude=") {
		return true, false
	}
	return false, false
}

func isStandardGrepRecursive(bin, t string) bool {
	if !isStandardGrep(bin) {
		return false
	}
	return t == "--recursive" || hasShortFlag(t, 'r') || hasShortFlag(t, 'R')
}

type grepScanFlags struct {
	hasExcludeDir      bool
	hasDiscoveryFlag   bool
	hasMatchLimit      bool
	maxMatchCount      int
	maxContextLines    int
	hasExplicitPattern bool
	isRecursive        bool
	pathTokens         []string
}

func (f *grepScanFlags) applyGrepFlag(bin, t, nextToken string) (consumeNext bool) {
	if matches, takesArg := isExcludeDirFlag(t); matches {
		f.hasExcludeDir = true
		return takesArg && nextToken != ""
	}
	if isStandardGrepRecursive(bin, t) {
		f.isRecursive = true
	}
	if isGrepDiscoveryFlag(t) {
		f.hasDiscoveryFlag = true
		f.hasMatchLimit = true
		if grepNoPatternLongFlags[t] {
			f.hasExplicitPattern = true
		}
	}
	if matched, takesArg, count := parseGrepMatchCountFlag(t, nextToken); matched {
		f.hasMatchLimit = true
		f.maxMatchCount = count
		return takesArg
	}
	if matched, takesArg, lines := parseGrepContextFlag(t, nextToken); matched {
		if lines > f.maxContextLines {
			f.maxContextLines = lines
		}
		return takesArg
	}
	if grepPatternFlags[t] {
		f.hasExplicitPattern = true
		return nextToken != ""
	}
	if grepValueFlags[t] {
		return nextToken != ""
	}
	return false
}

func collectNonFlagTokens(tokens []string) []string {
	var nonFlags []string
	for _, t := range tokens {
		if !strings.HasPrefix(t, "-") {
			nonFlags = append(nonFlags, t)
		}
	}
	return nonFlags
}

func parseGrepTokens(bin string, tokens []string) grepScanFlags {
	flags := grepScanFlags{
		isRecursive: (bin == "rg" || bin == "ag" || bin == "ack" || bin == "git-grep"),
	}

	for i := 1; i < len(tokens); i++ {
		t := tokens[i]
		if strings.HasPrefix(t, "-") {
			nextToken := ""
			if i+1 < len(tokens) {
				nextToken = tokens[i+1]
			}
			if flags.applyGrepFlag(bin, t, nextToken) {
				i++
			}
			continue
		}
		if !flags.hasExplicitPattern {
			flags.hasExplicitPattern = true
			continue
		}
		flags.pathTokens = append(flags.pathTokens, t)
	}

	if bin == "strings" {
		flags.pathTokens = collectNonFlagTokens(tokens[1:])
	}

	return flags
}

func isUnscopedRecursiveTarget(flags grepScanFlags) bool {
	if !flags.isRecursive {
		return false
	}
	return len(flags.pathTokens) == 0 || hasBroadPathToken(flags.pathTokens) || countDirectoryTargets(flags.pathTokens) > 1
}

func validateGrepLineLimits(flags grepScanFlags) *Decision {
	if flags.maxMatchCount <= 0 || flags.maxMatchCount > maxGrepMatchLimit {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Excessive grep/rg match limit detected (-m / --max-count > 20). Since '-m' bounds matches per file rather than total output, use '-m <= 20' (or use '-l' / '-c' for broad discovery).",
		}
	}
	if flags.isRecursive && flags.maxContextLines > maxRecursiveContextLines {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Excessive context lines (-C/-A/-B > 3) on recursive search detected. Large context windows multiplied across matching files cause context bloat; use -C <= 3 (or omit -C) to locate line numbers first, then use read_file with StartLine/EndLine.",
		}
	}
	if isUnscopedRecursiveTarget(flags) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Broad or multi-directory line search detected. Because '-m' limits matches per file rather than total output, searching repository roots (e.g. '.', '/share/aerial', '/share/aerial/services') or multiple directories at once requires discovery mode ('-l' / '--files-with-matches' or '-c' / '--count') first, or scoping the search to a single package/subdirectory.",
		}
	}
	return nil
}

func inspectGrepCommand(bin string, tokens []string, fromPipe bool) *Decision {
	flags := parseGrepTokens(bin, tokens)

	for _, pt := range flags.pathTokens {
		if dec := validateGrepPathToken(pt); dec != nil {
			return dec
		}
	}

	if bin == "strings" || (fromPipe && len(flags.pathTokens) == 0) {
		return nil
	}

	if flags.isRecursive && isStandardGrep(bin) && !flags.hasExcludeDir && hasBroadPathToken(flags.pathTokens) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Recursive grep without directory exclusions detected. Add '--exclude-dir=.git' (or use ripgrep/rg, which excludes .git by default) to prevent scanning raw git packfiles and binary objects.",
		}
	}

	if !flags.hasMatchLimit && !flags.hasDiscoveryFlag {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unbounded text search detected. Bound output with '-m <N>' / '--max-count=<N>' (N <= 20, e.g. 'rg -n -m 20 <pattern> <subdir>'), '-l' ('--files-with-matches'), or '-c' ('--count') per the shell-filesystem-traversal skill to prevent context window bloat.",
		}
	}

	if flags.hasDiscoveryFlag {
		return nil
	}

	return validateGrepLineLimits(flags)
}
