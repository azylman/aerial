package guard

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// DefaultBatchDir is the canonical temporary directory for batch guard conversation states.
	DefaultBatchDir = "/tmp/aerial-batch-guard"
	// DefaultBatchWindow is the sliding time window for tracking sequential tool thrashing.
	DefaultBatchWindow = 60 * time.Second
	// DefaultMaxRecords is the maximum number of invocations retained in memory/disk per conversation.
	DefaultMaxRecords = 30
	// DefaultPruneAge is the maximum age before state files are purged by opportunistic GC.
	DefaultPruneAge = 1 * time.Hour
	// DefaultMinAnchorReadLines is the minimum line span for a bounded read_file call so edit_file anchors are complete.
	DefaultMinAnchorReadLines = 120
	// DefaultPageReadLines is the full-page line window used when promoting contiguous or repeat reads on the same file.
	DefaultPageReadLines = 800
	// DefaultContiguousGapLines is the maximum line gap between two reads on the same file to treat them as contiguous creeping.
	DefaultContiguousGapLines = 50
)

var (
	gcMu                 sync.Mutex
	lastGCTime           time.Time
	pytestRegex          = regexp.MustCompile(`^(python[0-9.]*\s+-m\s+)?pytest(\s+(-v|-q|-s|-x|--tb=\S+))*\s*$`)
	goTestValFlags       = map[string]bool{"-C": true, "-timeout": true, "-tags": true, "-coverprofile": true, "-cpu": true, "-count": true, "-o": true, "-run": true, "-test.run": true}
	goTestVerbose        = map[string]bool{"-v": true, "-test.v": true, "-v=true": true, "-test.v=true": true, "--v": true, "--test.v": true, "--v=true": true, "--test.v=true": true}
	rootSharedPkgs       = map[string]bool{"httpkit": true, "ollama": true, "pgutil": true, "sanitizer": true}
	unscopedTargets      = map[string]bool{"./...": true, "brain/...": true, "services/eri/...": true, "...": true}
	gitGlobalValFlags    = map[string]bool{"-C": true, "-c": true, "--git-dir": true, "--work-tree": true, "--namespace": true, "--super-prefix": true, "--config-env": true}
	gitStatusFormatFlags = map[string]bool{"--short": true, "--porcelain": true, "--long": true, "--help": true, "-h": true, "-v": true, "--verbose": true}
	gitDiffSummaryFlags  = map[string]bool{"--stat": true, "--shortstat": true, "--numstat": true, "--summary": true, "--name-only": true, "--name-status": true, "--dirstat": true, "--check": true, "--quiet": true, "--exit-code": true, "-s": true, "--no-patch": true, "-h": true, "--help": true}
	gitDiffValFlags      = map[string]bool{"-U": true, "--unified": true, "-O": true, "-S": true, "-G": true, "--diff-filter": true, "--inter-hunk-context": true}
	scratchVarPrefixes   = []struct {
		prefix string
		envKey string
	}{
		{"${SCRATCH_DIR}", "SCRATCH_DIR"},
		{"$SCRATCH_DIR", "SCRATCH_DIR"},
		{"${SCRATCHDIR}", "SCRATCHDIR"},
		{"$SCRATCHDIR", "SCRATCHDIR"},
	}
)

func envPositiveInt(fallback int, keys ...string) int {
	for _, k := range keys {
		if raw := strings.TrimSpace(os.Getenv(k)); raw != "" {
			if n, err := strconv.Atoi(raw); err == nil && n > 0 {
				return n
			}
		}
	}
	return fallback
}

func minAnchorReadLines() int {
	return envPositiveInt(DefaultMinAnchorReadLines, "AERIAL_BATCH_MIN_ANCHOR_LINES", "AERIAL_BATCH_MIN_PAGINATED_LINES")
}

// GetBatchDir returns the active directory for batch guard state persistence.
func GetBatchDir() string {
	if env := os.Getenv("AERIAL_BATCH_GUARD_DIR"); env != "" {
		return env
	}
	return DefaultBatchDir
}

// ToolInvocation tracks a single tool execution for batch and thrash detection.
type ToolInvocation struct {
	Timestamp int64  `json:"timestamp"`
	StepIdx   int    `json:"stepIdx"`
	Turn      int    `json:"invocationNum,omitempty"`
	ToolName  string `json:"toolName"`
	Target    string `json:"target"`
	Category  string `json:"category"`
	StartLine int    `json:"startLine,omitempty"`
	EndLine   int    `json:"endLine,omitempty"`
}

// BatchState persists recent tool invocations for a conversation.
type BatchState struct {
	InvocationNum int              `json:"invocationNum,omitempty"`
	Records       []ToolInvocation `json:"records"`
}

func isUnscopedTest(cmd string) bool {
	return slices.ContainsFunc(splitPipeline(cmd), isUnscopedTestSingle)
}

func isUnscopedTestSingle(cmd string) bool {
	fields := unwrapCommandTokens(tokenizeQuoteAware(strings.TrimSpace(strings.TrimLeft(cmd, "("))))
	if len(fields) == 0 {
		return false
	}
	if fields[0] == "./scripts/verify.sh" || fields[0] == "scripts/verify.sh" {
		return isUnscopedVerify(fields[1:])
	}
	if pytestRegex.MatchString(strings.Join(fields, " ")) {
		return true
	}
	if len(fields) >= 2 && fields[0] == "go" && fields[1] == "test" {
		return isUnscopedGoTest(fields[2:])
	}
	return false
}

func isTranscriptLogPath(path string) bool {
	clean := filepath.ToSlash(strings.TrimSpace(path))
	if !strings.Contains(clean, "/.system_generated/logs/") && !strings.HasPrefix(clean, ".system_generated/logs/") {
		return false
	}
	base := filepath.Base(clean)
	return strings.HasPrefix(base, "transcript") && strings.HasSuffix(base, ".jsonl")
}

func stripLeadingEnv(fields []string) []string {
	for len(fields) > 0 && strings.Contains(fields[0], "=") && !strings.HasPrefix(fields[0], "-") {
		fields = fields[1:]
	}
	return fields
}

func isUnscopedVerify(args []string) bool {
	for _, f := range args {
		if f == "--staged" || f == "-h" || f == "--help" {
			return false
		}
	}
	return true
}

func isUnscopedGoTest(args []string) bool {
	var pkgArgs []string
	var chdirMod string
	for i := 0; i < len(args); i++ {
		f := strings.TrimRight(args[i], ")")
		if f == "" {
			continue
		}
		if strings.HasPrefix(f, "-") {
			if f == "-C" && i+1 < len(args) {
				chdirMod = strings.TrimPrefix(strings.TrimRight(args[i+1], ")"), "./")
			}
			if goTestValFlags[f] {
				i++
			}
			continue
		}
		pkgArgs = append(pkgArgs, f)
	}
	if len(pkgArgs) == 0 {
		return true
	}
	isScopedSubmodule := chdirMod != "" && chdirMod != "." && chdirMod != "services/eri"
	for _, arg := range pkgArgs {
		if arg == "./..." && isScopedSubmodule {
			continue
		}
		if unscopedTargets[arg] {
			return true
		}
	}
	return false
}

type byteSpan struct {
	start int
	end   int
	repl  string
	isPkg bool
}

func formatSubmoduleRelPkg(rest string) string {
	if rest == "" {
		return "."
	}
	return "./" + rest
}

func splitSubmoduleTestTarget(pkgArg string) (modDir, relPkg string, ok bool) {
	trimmed := strings.TrimPrefix(pkgArg, "./")
	if trimmed == "services/eri" || trimmed == "services/eri/..." {
		return "", "", false
	}
	if trimmed == "scripts/code-health" || strings.HasPrefix(trimmed, "scripts/code-health/") {
		rest := strings.TrimPrefix(strings.TrimPrefix(trimmed, "scripts/code-health"), "/")
		return "scripts/code-health", formatSubmoduleRelPkg(rest), true
	}
	parts := strings.SplitN(trimmed, "/", 3)
	if len(parts) < 2 || parts[1] == "" || parts[1] == "..." {
		return "", "", false
	}
	switch parts[0] {
	case "services", "mcp":
		rest := ""
		if len(parts) == 3 {
			rest = parts[2]
		}
		return parts[0] + "/" + parts[1], formatSubmoduleRelPkg(rest), true
	case "pkg":
		if rootSharedPkgs[parts[1]] {
			return "pkg", "./" + strings.TrimPrefix(trimmed, "pkg/"), true
		}
	}
	return "", "", false
}

func trimLeadingSpaceOffset(cmdLine string, offset int) int {
	for offset > 0 && (cmdLine[offset-1] == ' ' || cmdLine[offset-1] == '\t') {
		offset--
	}
	return offset
}

func filterGoTestSpans(raw []byteSpan, insertPos int, commonMod string, allowPkgRewrite bool) []byteSpan {
	var spans []byteSpan
	if allowPkgRewrite {
		spans = append(spans, byteSpan{start: insertPos, end: insertPos, repl: " -C " + commonMod})
	}
	for _, sp := range raw {
		if !sp.isPkg || allowPkgRewrite {
			spans = append(spans, sp)
		}
	}
	return spans
}

func collectGoTestSegmentSpans(cmdLine string, tokens []locatedToken, idx int) []byteSpan {
	var raw []byteSpan
	var commonMod string
	canRewriteMod := true
	hasPkgEdit := false

	for i := idx + 2; i < len(tokens); i++ {
		tok := tokens[i]
		cleanVal := strings.TrimRight(tok.value, ")")
		if cleanVal == "--" || cleanVal == "-args" {
			break
		}
		if cleanVal == "-C" || strings.HasPrefix(cleanVal, "-C=") {
			canRewriteMod = false
		}
		if goTestValFlags[cleanVal] {
			i++
			continue
		}
		end := tok.endOffset - (len(tok.value) - len(cleanVal))
		if goTestVerbose[cleanVal] {
			raw = append(raw, byteSpan{start: trimLeadingSpaceOffset(cmdLine, tok.startOffset), end: end})
			continue
		}
		if strings.HasPrefix(cleanVal, "-") {
			continue
		}
		modDir, relPkg, ok := splitSubmoduleTestTarget(cleanVal)
		if !ok || (commonMod != "" && commonMod != modDir) {
			canRewriteMod = false
			continue
		}
		commonMod = modDir
		hasPkgEdit = true
		raw = append(raw, byteSpan{start: tok.startOffset, end: end, repl: relPkg, isPkg: true})
	}

	return filterGoTestSpans(raw, tokens[idx+1].endOffset, commonMod, canRewriteMod && commonMod != "" && hasPkgEdit)
}

func stripGoTestVerbose(cmdLine string) string {
	var spans []byteSpan
	for _, seg := range splitPipelineWithOffsets(cmdLine) {
		tokens := tokenizeWithOffsets(seg.text, seg.offset)
		idx := skipEnvAndDockerExec(tokens)
		if idx+1 >= len(tokens) || filepath.Base(strings.TrimLeft(tokens[idx].value, "(")) != "go" || tokens[idx+1].value != "test" {
			continue
		}
		spans = append(spans, collectGoTestSegmentSpans(cmdLine, tokens, idx)...)
	}
	rewritten := cmdLine
	for _, sp := range slices.Backward(spans) {
		rewritten = rewritten[:sp.start] + sp.repl + rewritten[sp.end:]
	}
	return rewritten
}

func isUnbatchedGitMutation(cmd string) bool {
	trimmed := strings.TrimSpace(cmd)
	if trimmed == "" {
		return false
	}

	// If chained with &&, ;, ||, |, it is batched/compound
	if strings.Contains(trimmed, "&&") || strings.Contains(trimmed, ";") || strings.Contains(trimmed, "||") || strings.Contains(trimmed, "|") {
		return false
	}

	parts := strings.Fields(trimmed)
	if len(parts) >= 2 && parts[0] == "git" {
		sub := parts[1]
		if sub == "add" || sub == "commit" {
			return true
		}
	}

	return false
}

func expandScratchEnvPrefix(p string) string {
	for _, sv := range scratchVarPrefixes {
		if p == sv.prefix || strings.HasPrefix(p, sv.prefix+"/") {
			rest := strings.TrimPrefix(p, sv.prefix)
			base := filepath.ToSlash(strings.TrimSpace(os.Getenv(sv.envKey)))
			if base == "" {
				base = "/share/aerial"
			}
			return strings.TrimRight(base, "/") + rest
		}
	}
	return p
}

func scratchSegmentRepo(seg string) string {
	base := seg
	if dot := strings.IndexByte(base, '.'); dot > 0 {
		base = base[:dot]
	}
	if base == "aerial-code-scratch" || base == "aerial-pr" || strings.HasPrefix(base, "aerial-pr-") {
		return "aerial"
	}
	if strings.HasSuffix(base, "-scratch") && len(base) > len("-scratch") {
		return strings.TrimSuffix(base, "-scratch")
	}
	return ""
}

func normalizeBatchTarget(path string) string {
	p := filepath.ToSlash(strings.TrimSpace(path))
	if p == "" {
		return ""
	}
	p = expandScratchEnvPrefix(p)
	if strings.HasPrefix(p, "/") {
		p = filepath.ToSlash(filepath.Clean(p))
	}
	parts := strings.Split(p, "/")
	for i, seg := range parts {
		if repo := scratchSegmentRepo(seg); repo != "" && i+1 < len(parts) {
			return "/share/" + repo + "/" + strings.Join(parts[i+1:], "/")
		}
	}
	for _, key := range []string{"SCRATCH_DIR", "SCRATCHDIR"} {
		envDir := strings.TrimRight(filepath.ToSlash(strings.TrimSpace(os.Getenv(key))), "/")
		if envDir != "" && envDir != "/" && strings.HasPrefix(p, envDir+"/") {
			return "/share/aerial" + strings.TrimPrefix(p, envDir)
		}
	}
	return p
}

func categorizeInvocation(toolName string, args ToolArgs) (category, target string) {
	switch normalizeToolName(toolName) {
	case "view_file", "read_file":
		return "read", normalizeBatchTarget(args.FilePath())
	case "replace_file_content", "write_to_file", "edit_file", "write_file":
		return "edit", normalizeBatchTarget(args.FilePath())
	case "run_command":
		cmd := strings.TrimSpace(args.CommandLine)
		if isUnscopedTest(cmd) {
			return "unscoped_test", cmd
		}
		if isUnbatchedGitMutation(cmd) {
			return "git_mutation", cmd
		}
		return "command", cmd
	default:
		return "other", toolName
	}
}

func sanitizeID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return "default"
	}
	var sb strings.Builder
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			sb.WriteRune(r)
		}
	}
	res := sb.String()
	if res == "" {
		return "default"
	}
	return res
}

func isSameStep(rec ToolInvocation, currentStep int, currentTS int64) bool {
	if currentStep > 0 && rec.StepIdx > 0 && rec.StepIdx == currentStep {
		return true
	}
	diff := currentTS - rec.Timestamp
	if diff < 0 {
		diff = -diff
	}
	return diff <= 1
}

func removeBestEffort(path string) {
	if err := os.Remove(path); err != nil {
		return
	}
}

func saveStateBestEffort(dir, safeID string, state BatchState) {
	if err := saveState(dir, safeID, state); err != nil {
		return
	}
}

func withStateLock(dir, safeID string, fn func() error) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fn()
	}

	lockPath := filepath.Join(dir, fmt.Sprintf("state-%s.lock", safeID))
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fn()
	}
	defer func() {
		if err := f.Close(); err != nil {
			return
		}
	}()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fn()
	}
	defer func() {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
			return
		}
	}()

	return fn()
}

func loadState(dir, safeID string, now time.Time) BatchState {
	statePath := filepath.Join(dir, fmt.Sprintf("state-%s.json", safeID))
	data, err := os.ReadFile(statePath)
	if err != nil {
		return BatchState{}
	}

	var state BatchState
	if err := json.Unmarshal(data, &state); err != nil {
		removeBestEffort(statePath)
		return BatchState{}
	}

	cutoff := now.Add(-DefaultBatchWindow).Unix()
	var valid []ToolInvocation
	for _, rec := range state.Records {
		if rec.Timestamp >= cutoff {
			valid = append(valid, rec)
		}
	}
	state.Records = valid
	return state
}

func saveState(dir, safeID string, state BatchState) error {
	statePath := filepath.Join(dir, fmt.Sprintf("state-%s.json", safeID))
	tmpPath := fmt.Sprintf("%s.tmp.%d", statePath, time.Now().UnixNano())

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return err
	}

	return os.Rename(tmpPath, statePath)
}

func pruneStateFiles(dir string, now time.Time) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	cutoff := now.Add(-DefaultPruneAge)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "state-") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			removeBestEffort(filepath.Join(dir, name))
		}
	}
}

func opportunisticGC(dir string, now time.Time) {
	gcMu.Lock()
	if !lastGCTime.IsZero() && now.Sub(lastGCTime) < 10*time.Minute {
		gcMu.Unlock()
		return
	}
	lastGCTime = now
	gcMu.Unlock()

	pruneStateFiles(dir, now)
}

func readsSinceLastTargetEdit(records []ToolInvocation, target string) []ToolInvocation {
	startIdx := 0
	for i, record := range slices.Backward(records) {
		if record.Category == "edit" && record.Target == target {
			startIdx = i + 1
			break
		}
	}
	var reads []ToolInvocation
	for _, r := range records[startIdx:] {
		if r.Category == "read" && r.Target == target && r.StartLine > 0 && r.EndLine >= r.StartLine {
			reads = append(reads, r)
		}
	}
	return reads
}

func removeTargetInvocations(records []ToolInvocation, category, target string) []ToolInvocation {
	var kept []ToolInvocation
	for _, r := range records {
		if r.Category != category || r.Target != target {
			kept = append(kept, r)
		}
	}
	return kept
}

func findTouchingPriorRead(priorReads []ToolInvocation, start, end int) *ToolInvocation {
	for _, r := range slices.Backward(priorReads) {
		if start <= r.EndLine+DefaultContiguousGapLines && end >= r.StartLine-DefaultContiguousGapLines {
			return &r
		}
	}
	return nil
}

func promotePageRange(start, end int, touching *ToolInvocation) (int, int) {
	if touching != nil {
		if start >= touching.StartLine {
			newStart := touching.StartLine
			if newStart+DefaultPageReadLines-1 < end {
				newStart = start
			}
			return newStart, newStart + DefaultPageReadLines - 1
		}
		uEnd := max(end, touching.EndLine)
		newStart := min(max(1, uEnd-DefaultPageReadLines+1), start)
		return newStart, newStart + DefaultPageReadLines - 1
	}
	span := end - start + 1
	pad := (DefaultPageReadLines - span + 1) / 2
	newStart := max(1, start-pad)
	return newStart, newStart + DefaultPageReadLines - 1
}

func shapeReadFileRange(target string, startLine, endLine int, priorRecords []ToolInvocation) (effStart, effEnd int, overwritten bool) {
	if target == "" {
		return startLine, endLine, false
	}
	if startLine <= 0 && endLine <= 0 {
		return 1, DefaultPageReadLines, false
	}
	if startLine > 0 && endLine <= 0 {
		return startLine, startLine + DefaultPageReadLines - 1, false
	}
	if startLine > 0 && endLine < startLine {
		return startLine, endLine, false
	}
	start := max(1, startLine)
	end := endLine
	span := end - start + 1
	if span >= DefaultPageReadLines {
		return start, end, start != startLine
	}

	priorReads := readsSinceLastTargetEdit(priorRecords, target)
	if touching := findTouchingPriorRead(priorReads, start, end); touching != nil || len(priorReads) >= 2 {
		newStart, newEnd := promotePageRange(start, end, touching)
		return newStart, newEnd, newStart != startLine || newEnd != endLine
	}

	minAnchor := minAnchorReadLines()
	if span < minAnchor {
		pad := (minAnchor - span + 1) / 2
		newStart := max(1, start-pad)
		newEnd := newStart + minAnchor - 1
		return newStart, newEnd, newStart != startLine || newEnd != endLine
	}
	return start, end, start != startLine
}

// CheckBatch evaluates tool batching and thrash invariants using the active state directory and system time.
func CheckBatch(payload Payload, args ToolArgs) *Decision {
	return CheckBatchWithDir(payload, args, GetBatchDir(), time.Now())
}

func findGitSubcommand(tokens []locatedToken, gitIdx int) int {
	for i := gitIdx + 1; i < len(tokens); i++ {
		val := strings.TrimRight(tokens[i].value, ")")
		if gitGlobalValFlags[val] {
			i++
			continue
		}
		if strings.HasPrefix(val, "-") {
			continue
		}
		return i
	}
	return -1
}

func hasGitStatusFormatFlag(args []locatedToken) bool {
	for _, tok := range args {
		val := strings.TrimRight(tok.value, ")")
		if gitStatusFormatFlags[val] || strings.HasPrefix(val, "--porcelain=") || hasShortFlag(val, 's') || hasShortFlag(val, 'z') {
			return true
		}
	}
	return false
}

func shapeGitStatus(cmdLine string) string {
	var insertOffsets []int
	for _, seg := range splitPipelineWithOffsets(cmdLine) {
		tokens := tokenizeWithOffsets(seg.text, seg.offset)
		idx := skipEnvAndDockerExec(tokens)
		if idx >= len(tokens) || filepath.Base(strings.TrimLeft(tokens[idx].value, "(")) != "git" {
			continue
		}
		subIdx := findGitSubcommand(tokens, idx)
		if subIdx < 0 {
			continue
		}
		subTok := tokens[subIdx]
		cleanSub := strings.TrimRight(subTok.value, ")")
		if cleanSub == "status" && !hasGitStatusFormatFlag(tokens[subIdx+1:]) {
			pos := subTok.endOffset - (len(subTok.value) - len(cleanSub))
			insertOffsets = append(insertOffsets, pos)
		}
	}
	rewritten := cmdLine
	for _, pos := range slices.Backward(insertOffsets) {
		rewritten = rewritten[:pos] + " -sb" + rewritten[pos:]
	}
	return rewritten
}

func isGitLogBounded(args []locatedToken) bool {
	for _, tok := range args {
		val := strings.TrimRight(tok.value, ")")
		if val == "-n" || val == "--max-count" || val == "-h" || val == "--help" || strings.HasPrefix(val, "--max-count=") {
			return true
		}
		if strings.HasPrefix(val, "-n") && len(val) > 2 && val[2] >= '0' && val[2] <= '9' {
			return true
		}
		if len(val) >= 2 && val[0] == '-' && val[1] >= '1' && val[1] <= '9' {
			return true
		}
	}
	return false
}

func isScopedGitDiffPath(val string) bool {
	if val == "" || val == "." || val == "./" || strings.Contains(val, "..") {
		return false
	}
	if val == "HEAD" || strings.HasPrefix(val, "HEAD~") || strings.HasPrefix(val, "HEAD^") || strings.HasPrefix(val, "@") || val == "main" || val == "master" {
		return false
	}
	if strings.HasPrefix(val, "origin/") || strings.HasPrefix(val, "upstream/") || strings.HasPrefix(val, "refs/") {
		return false
	}
	return strings.Contains(val, "/") || strings.Contains(val, ".") || val == "Makefile" || val == "Dockerfile"
}

func hasScopedPathAfterDashDash(args []locatedToken) bool {
	for _, tok := range args {
		val := strings.TrimRight(tok.value, ")")
		if val != "" && val != "." && val != "./" && val != ".." {
			return true
		}
	}
	return false
}

func isGitDiffScoped(args []locatedToken) bool {
	for i := 0; i < len(args); i++ {
		val := strings.TrimRight(args[i].value, ")")
		if val == "--" {
			return hasScopedPathAfterDashDash(args[i+1:])
		}
		if gitDiffSummaryFlags[val] || strings.HasPrefix(val, "--stat=") || strings.HasPrefix(val, "--dirstat=") {
			return true
		}
		if strings.HasPrefix(val, "-") {
			if gitDiffValFlags[val] {
				i++
			}
			continue
		}
		if isScopedGitDiffPath(val) {
			return true
		}
	}
	return false
}

func inspectGitBatchCommand(cmdLine string) *Decision {
	for _, seg := range splitPipelineWithOffsets(cmdLine) {
		tokens := tokenizeWithOffsets(seg.text, seg.offset)
		idx := skipEnvAndDockerExec(tokens)
		if idx >= len(tokens) || filepath.Base(strings.TrimLeft(tokens[idx].value, "(")) != "git" {
			continue
		}
		subIdx := findGitSubcommand(tokens, idx)
		if subIdx < 0 {
			continue
		}
		sub := strings.TrimRight(tokens[subIdx].value, ")")
		rest := tokens[subIdx+1:]
		if sub == "log" && !isGitLogBounded(rest) {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Unbounded git log detected. Specify a commit count limit such as '-n <N>' or '-<N>' (e.g. 'git log -n 10 --oneline' or 'git log -5') to prevent dumping unconstrained commit history into the context window.",
			}
		}
		if sub == "diff" && !isGitDiffScoped(rest) {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Unscoped full-patch git diff detected. Pass '--stat' or '--name-only' for repository-wide summaries, or scope the diff to specific file paths (e.g. 'git diff -- <path>') to avoid dumping large patches into the context window.",
			}
		}
	}
	return nil
}

func checkStatelessBatchRules(normalizedTool string, args ToolArgs) *Decision {
	if normalizedTool == "run_command" {
		if isUnscopedTest(args.CommandLine) {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Unscoped test sweep detected. Run targeted package unit tests (e.g. 'go test ./pkg/<pkg>/...' or 'go test -run TestName ./pkg/<pkg>') during development; comprehensive monorepo sweeps are offloaded to GitHub Actions CI or './scripts/verify.sh --staged'.",
			}
		}
		if isUnbatchedGitMutation(args.CommandLine) {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Unbatched git lifecycle command detected. Combine git operations into compound one-liners (e.g. 'git add -A && git commit -m ... && git push ...') or use 'scripts/aerial-pr.sh submit <scratch_dir> [commit_msg]' for automated pre-flight verification and submission.",
			}
		}
		if dec := inspectGitBatchCommand(args.CommandLine); dec != nil {
			return dec
		}
	}
	if (normalizedTool == "read_file" || normalizedTool == "view_file") && isTranscriptLogPath(args.FilePath()) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Direct read_file/view_file on raw session transcripts (transcript*.jsonl) is prohibited because each JSONL line can exceed 30KB and floods the context window. Use the 'transcript-search' CLI skill or targeted 'jq' / 'rg -m 20' queries via run_command instead.",
		}
	}
	return nil
}

func resolvePriorRecords(state *BatchState, payload Payload, ts int64) (int, []ToolInvocation) {
	if payload.InvocationNum > 0 {
		state.InvocationNum = payload.InvocationNum
	}
	invNum := state.InvocationNum
	var prior []ToolInvocation
	for _, r := range state.Records {
		isMatchingTurn := (invNum > 0 && r.Turn > 0 && r.Turn == invNum) || ((invNum == 0 || r.Turn == 0) && isSameStep(r, payload.StepIdx, ts))
		if !isMatchingTurn {
			prior = append(prior, r)
		}
	}
	return invNum, prior
}

// CheckBatchWithDir evaluates tool batching invariants with configurable state directory and timestamp.
// Guaranteed fail-open: returns allow on unexpected errors or corrupted states.
func CheckBatchWithDir(payload Payload, args ToolArgs, dir string, now time.Time) *Decision {
	normalizedTool := normalizeToolName(payload.ToolCall.Name)
	if dec := checkStatelessBatchRules(normalizedTool, args); dec != nil {
		return dec
	}

	category, target := categorizeInvocation(payload.ToolCall.Name, args)
	safeID := sanitizeID(payload.ConversationID)

	var decision *Decision

	err := withStateLock(dir, safeID, func() error {
		state := loadState(dir, safeID, now)
		stepIdx := payload.StepIdx
		ts := now.Unix()
		invNum, priorRecords := resolvePriorRecords(&state, payload, ts)

		effStart, effEnd := args.StartLine, args.EndLine
		var readOverwritten bool
		if category == "read" {
			effStart, effEnd, readOverwritten = shapeReadFileRange(target, args.StartLine, args.EndLine, priorRecords)
		}

		// Allowed: invalidate prior reads on edit and record invocation
		if category == "edit" && target != "" {
			state.Records = removeTargetInvocations(state.Records, "read", target)
		}
		inv := ToolInvocation{
			Timestamp: ts,
			StepIdx:   stepIdx,
			Turn:      invNum,
			ToolName:  payload.ToolCall.Name,
			Target:    target,
			Category:  category,
			StartLine: effStart,
			EndLine:   effEnd,
		}
		state.Records = append(state.Records, inv)
		if len(state.Records) > DefaultMaxRecords {
			state.Records = state.Records[len(state.Records)-DefaultMaxRecords:]
		}

		saveStateBestEffort(dir, safeID, state)
		opportunisticGC(dir, now)
		decision = &Decision{Decision: DecisionAllow}
		if readOverwritten {
			decision.Overwrite = map[string]any{"StartLine": effStart, "EndLine": effEnd}
		} else if normalizedTool == "run_command" {
			if rewritten := shapeGitStatus(stripGoTestVerbose(args.CommandLine)); rewritten != args.CommandLine {
				decision.Overwrite = map[string]any{"CommandLine": rewritten}
			}
		}
		return nil
	})

	if err != nil || decision == nil {
		return &Decision{Decision: DecisionAllow}
	}
	return decision
}

// RecordPreInvocation records the current turn invocationNum.
func RecordPreInvocation(payload Payload) *InvocationResponse {
	return RecordPreInvocationWithDir(payload, GetBatchDir(), time.Now())
}

// RecordPreInvocationWithDir records the active invocationNum in state.
func RecordPreInvocationWithDir(payload Payload, dir string, now time.Time) *InvocationResponse {
	if payload.InvocationNum > 0 {
		safeID := sanitizeID(payload.ConversationID)
		if err := withStateLock(dir, safeID, func() error {
			state := loadState(dir, safeID, now)
			state.InvocationNum = payload.InvocationNum
			return saveState(dir, safeID, state)
		}); err != nil {
			return &InvocationResponse{}
		}
	}
	return &InvocationResponse{}
}

func rollbackFailedEdit(records []ToolInvocation, target string, stepIdx int) ([]ToolInvocation, bool) {
	matchIdx := -1
	for i, record := range slices.Backward(records) {
		if record.Category == "edit" && record.Target == target {
			if stepIdx <= 0 || record.StepIdx == stepIdx {
				return append(records[:i], records[i+1:]...), true
			} else if matchIdx < 0 {
				matchIdx = i
			}
		}
	}
	if matchIdx >= 0 {
		return append(records[:matchIdx], records[matchIdx+1:]...), true
	}
	return records, false
}

func isTerminalSubmitSingle(seg string) bool {
	fields := unwrapCommandTokens(tokenizeQuoteAware(strings.TrimSpace(strings.TrimLeft(seg, "("))))
	if len(fields) < 2 {
		return false
	}
	switch filepath.Base(fields[0]) {
	case "aerial-pr.sh":
		for _, arg := range fields[1:] {
			if arg == "submit" || arg == "override-roborev" || arg == "dispute-roborev" {
				return true
			}
		}
	case "aerial-build.sh":
		if slices.Contains(fields[1:], "enqueue") {
			return true
		}
	}
	return false
}

func isTerminalSubmitCommand(cmdLine string) bool {
	return slices.ContainsFunc(splitPipeline(cmdLine), isTerminalSubmitSingle)
}

func parseProcStatPPIDAndPGRP(statContent string) (ppid, pgrp int, ok bool) {
	_, after, ok := strings.CutLast(statContent, ")")
	if !ok {
		return 0, 0, false
	}
	fields := strings.Fields(after)
	if len(fields) < 3 {
		return 0, 0, false
	}
	ppidVal, err1 := strconv.Atoi(fields[1])
	pgrpVal, err2 := strconv.Atoi(fields[2])
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return ppidVal, pgrpVal, true
}

func reapOrphanedChildProcessGroups(procDir string, parentPID, selfPID int, signalFn func(int, syscall.Signal) error) {
	if parentPID <= 1 || signalFn == nil {
		return
	}
	entries, err := os.ReadDir(procDir)
	if err != nil {
		return
	}
	for _, ent := range entries {
		pid, convErr := strconv.Atoi(ent.Name())
		if convErr != nil || pid <= 1 || pid == parentPID || pid == selfPID {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(procDir, ent.Name(), "stat"))
		if readErr != nil {
			continue
		}
		if ppid, pgrp, ok := parseProcStatPPIDAndPGRP(string(data)); ok && ppid == parentPID && pgrp == pid {
			if sigErr := signalFn(-pid, syscall.SIGKILL); sigErr != nil {
				continue
			}
		}
	}
}

func resolveProcDir(dir string) string {
	if dir == DefaultBatchDir {
		return "/proc"
	}
	return filepath.Join(dir, "proc")
}

func reapOrphanedParentChildren(procDir string) {
	reapOrphanedChildProcessGroups(procDir, os.Getppid(), os.Getpid(), syscall.Kill)
}

// RecordPostBatch rolls back batch-guard edit state when an edit tool execution fails,
// and reaps orphaned background command process groups after terminal PR or build submissions.
func RecordPostBatch(payload Payload, args ToolArgs) *InvocationResponse {
	return RecordPostBatchWithDir(payload, args, GetBatchDir(), time.Now())
}

// RecordPostBatchWithDir rolls back a failed edit invocation in the specified state directory,
// and reaps orphaned background command process groups on successful terminal submit commands.
func RecordPostBatchWithDir(payload Payload, args ToolArgs, dir string, now time.Time) *InvocationResponse {
	if strings.TrimSpace(payload.Error) == "" && normalizeToolName(payload.ToolCall.Name) == "run_command" && isTerminalSubmitCommand(args.CommandLine) {
		reapOrphanedParentChildren(resolveProcDir(dir))
	}
	if cat, target := categorizeInvocation(payload.ToolCall.Name, args); strings.TrimSpace(payload.Error) != "" && cat == "edit" && target != "" {
		safeID := sanitizeID(payload.ConversationID)
		if err := withStateLock(dir, safeID, func() error {
			state := loadState(dir, safeID, now)
			if updated, removed := rollbackFailedEdit(state.Records, target, payload.StepIdx); removed {
				state.Records = updated
				return saveState(dir, safeID, state)
			}
			return nil
		}); err != nil {
			return &InvocationResponse{}
		}
	}
	return &InvocationResponse{}
}
