package guard

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
	// DefaultMinPaginatedReadLines is the default minimum line span for a prior read to qualify as multi-page pagination.
	DefaultMinPaginatedReadLines = 150
	// DefaultMaxMicroReads is the default number of allowed non-paginated read steps on the same file before blocking.
	DefaultMaxMicroReads = 3
	// DefaultMaxSequentialReads is the default number of allowed consecutive single-file read turns before requiring parallel batching.
	DefaultMaxSequentialReads = 3
)

var (
	gcMu               sync.Mutex
	lastGCTime         time.Time
	pytestRegex        = regexp.MustCompile(`^(python[0-9.]*\s+-m\s+)?pytest(\s+(-v|-q|-s|-x|--tb=\S+))*\s*$`)
	goTestValFlags     = map[string]bool{"-timeout": true, "-tags": true, "-coverprofile": true, "-cpu": true, "-count": true, "-o": true, "-run": true, "-test.run": true}
	goTestVerbose      = map[string]bool{"-v": true, "-test.v": true, "-v=true": true, "-test.v=true": true, "--v": true, "--test.v": true, "--v=true": true, "--test.v=true": true}
	unscopedTargets    = map[string]bool{"./...": true, "brain/...": true, "services/eri/...": true, "...": true}
	scratchVarPrefixes = []struct {
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

func minPaginatedReadLines() int {
	return envPositiveInt(DefaultMinPaginatedReadLines, "AERIAL_BATCH_MIN_PAGINATED_LINES")
}

func maxMicroReads() int {
	return envPositiveInt(DefaultMaxMicroReads, "AERIAL_BATCH_MAX_MICRO_READS")
}

func maxSequentialReads() int {
	return envPositiveInt(DefaultMaxSequentialReads, "AERIAL_BATCH_MAX_SEQUENTIAL_READS", "AERIAL_BATCH_MAX_SINGLE_READ_STEPS")
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
	ToolName  string `json:"toolName"`
	Target    string `json:"target"`
	Category  string `json:"category"`
	StartLine int    `json:"startLine,omitempty"`
	EndLine   int    `json:"endLine,omitempty"`
}

// BatchState persists recent tool invocations for a conversation.
type BatchState struct {
	Records []ToolInvocation `json:"records"`
}

func isUnscopedTest(cmd string) bool {
	for _, sub := range splitPipeline(cmd) {
		if isUnscopedTestSingle(sub) {
			return true
		}
	}
	return false
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
	for i := 0; i < len(args); i++ {
		f := strings.TrimRight(args[i], ")")
		if f == "" {
			continue
		}
		if strings.HasPrefix(f, "-") {
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
	for _, arg := range pkgArgs {
		if unscopedTargets[arg] {
			return true
		}
	}
	return false
}

type byteSpan struct {
	start int
	end   int
}

func stripGoTestVerbose(cmdLine string) string {
	var spans []byteSpan
	for _, seg := range splitPipelineWithOffsets(cmdLine) {
		tokens := tokenizeWithOffsets(seg.text, seg.offset)
		idx := skipEnvAndDockerExec(tokens)
		if idx+1 >= len(tokens) || filepath.Base(strings.TrimLeft(tokens[idx].value, "(")) != "go" || tokens[idx+1].value != "test" {
			continue
		}
		for i := idx + 2; i < len(tokens); i++ {
			tok := tokens[i]
			cleanVal := strings.TrimRight(tok.value, ")")
			if cleanVal == "--" || cleanVal == "-args" {
				break
			}
			if goTestValFlags[cleanVal] {
				i++
				continue
			}
			if goTestVerbose[cleanVal] {
				start := tok.startOffset
				for start > 0 && (cmdLine[start-1] == ' ' || cmdLine[start-1] == '\t') {
					start--
				}
				end := tok.endOffset - (len(tok.value) - len(cleanVal))
				spans = append(spans, byteSpan{start: start, end: end})
			}
		}
	}
	rewritten := cmdLine
	for i := len(spans) - 1; i >= 0; i-- {
		sp := spans[i]
		rewritten = rewritten[:sp.start] + rewritten[sp.end:]
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

func isSingleInspection(cmd string) bool {
	trimmed := strings.TrimSpace(cmd)
	if trimmed == "" {
		return false
	}

	if strings.Contains(trimmed, "&&") || strings.Contains(trimmed, ";") || strings.Contains(trimmed, "||") || strings.Contains(trimmed, "|") {
		return false
	}

	parts := strings.Fields(trimmed)
	if len(parts) == 0 {
		return false
	}
	bin := filepath.Base(parts[0])
	switch bin {
	case "cat", "head", "tail", "ls", "find", "which", "file", "stat", "echo":
		return true
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
		if isSingleInspection(cmd) {
			return "inspection", cmd
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

func withStateLock(dir, safeID string, fn func() error) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fn()
	}

	lockPath := filepath.Join(dir, fmt.Sprintf("state-%s.lock", safeID))
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fn()
	}
	defer f.Close() //nolint:errcheck

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fn()
	}
	defer func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) //nolint:errcheck
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
		_ = os.Remove(statePath) //nolint:errcheck
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
			_ = os.Remove(filepath.Join(dir, name)) //nolint:errcheck
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

func countTargetInvocations(records []ToolInvocation, category, target string) int {
	count := 0
	for _, step := range groupSteps(records) {
		for _, r := range step {
			if r.Category == category && r.Target == target {
				count++
				break
			}
		}
	}
	return count
}

func isPaginatedRead(priorRecords []ToolInvocation, target string, startLine, endLine int) bool {
	if startLine <= 0 || endLine < startLine {
		return false
	}
	minLines := minPaginatedReadLines()
	hasPrior := false
	for _, r := range readsSinceLastTargetEdit(priorRecords, target) {
		if r.Category != "read" || r.Target != target {
			continue
		}
		hasPrior = true
		if r.EndLine-r.StartLine+1 < minLines {
			return false
		}
		if startLine <= r.EndLine && endLine >= r.StartLine {
			return false
		}
	}
	return hasPrior
}

func readsSinceLastTargetEdit(records []ToolInvocation, target string) []ToolInvocation {
	for i := len(records) - 1; i >= 0; i-- {
		if records[i].Category == "edit" && records[i].Target == target {
			return records[i+1:]
		}
	}
	return records
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

func recordsSinceLastCommand(records []ToolInvocation) []ToolInvocation {
	for i := len(records) - 1; i >= 0; i-- {
		if records[i].Category == "command" {
			return records[i+1:]
		}
	}
	return records
}

func filterOutCategory(records []ToolInvocation, category string) []ToolInvocation {
	var kept []ToolInvocation
	for _, r := range records {
		if r.Category != category {
			kept = append(kept, r)
		}
	}
	return kept
}

func hasConsecutiveSingleSteps(records []ToolInvocation, category string, required int) bool {
	steps := groupSteps(records)
	if len(steps) < required {
		return false
	}
	for i := len(steps) - required; i < len(steps); i++ {
		if len(steps[i]) != 1 || steps[i][0].Category != category {
			return false
		}
	}
	return true
}

func groupSteps(records []ToolInvocation) [][]ToolInvocation {
	if len(records) == 0 {
		return nil
	}
	var steps [][]ToolInvocation
	var current []ToolInvocation
	currentStep := -1
	var currentTS int64 = -1

	for _, r := range records {
		if len(current) == 0 {
			current = append(current, r)
			currentStep = r.StepIdx
			currentTS = r.Timestamp
			continue
		}

		diff := r.Timestamp - currentTS
		if diff < 0 {
			diff = -diff
		}
		same := (currentStep > 0 && r.StepIdx > 0 && r.StepIdx == currentStep) || (diff <= 1)

		if same {
			current = append(current, r)
		} else {
			steps = append(steps, current)
			current = []ToolInvocation{r}
			currentStep = r.StepIdx
			currentTS = r.Timestamp
		}
	}
	if len(current) > 0 {
		steps = append(steps, current)
	}
	return steps
}

// CheckBatch evaluates tool batching and thrash invariants using the active state directory and system time.
func CheckBatch(payload Payload, args ToolArgs) *Decision {
	return CheckBatchWithDir(payload, args, GetBatchDir(), time.Now())
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
	}
	if (normalizedTool == "read_file" || normalizedTool == "view_file") && isTranscriptLogPath(args.FilePath()) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Direct read_file/view_file on raw session transcripts (transcript*.jsonl) is prohibited because each JSONL line can exceed 30KB and floods the context window. Use the 'transcript-search' CLI skill or targeted 'jq' / 'rg -m 20' queries via run_command instead.",
		}
	}
	return nil
}

func checkMicroReadViolation(category, target string, paginatedRead bool, priorRecords []ToolInvocation, state *BatchState) *Decision {
	if category != "read" || target == "" || paginatedRead {
		return nil
	}
	maxReads := maxMicroReads()
	if countTargetInvocations(readsSinceLastTargetEdit(priorRecords, target), "read", target) >= maxReads {
		state.Records = removeTargetInvocations(state.Records, "read", target)
		return &Decision{
			Decision: DecisionDeny,
			Reason:   fmt.Sprintf("Iterative micro-read thrashing detected: %d sequential reads targeting the same file across turns. Consolidate reads into larger ranges (>= %d lines) or batch parallel reads in a single turn instead of small slices.", maxReads+1, minPaginatedReadLines()),
		}
	}
	return nil
}

func checkMicroEditViolation(category, target string, priorRecords []ToolInvocation, state *BatchState) *Decision {
	if category != "edit" || target == "" {
		return nil
	}
	if countTargetInvocations(recordsSinceLastCommand(priorRecords), "edit", target) >= 2 {
		state.Records = removeTargetInvocations(state.Records, "edit", target)
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Coarse-Grained Code Editing Invariant Violated: 3 consecutive micro-edits targeting the same file across turns. Formulate the complete target diff and apply it in a single comprehensive edit, or rewrite the file via write_to_file.",
		}
	}
	return nil
}

func checkMicroCommandViolation(category string, priorRecords []ToolInvocation, state *BatchState) *Decision {
	if category == "inspection" && hasConsecutiveSingleSteps(priorRecords, "inspection", 3) {
		state.Records = filterOutCategory(state.Records, "inspection")
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Micro-command thrashing detected: 4 consecutive single inspection commands across turns. Combine commands into compound one-liners (e.g. with && or ;) or execute an ephemeral scratch script in brain/<id>/scratch/ to collect diagnostics.",
		}
	}
	return nil
}

func checkSequentialReadViolation(category string, paginatedRead bool, priorRecords []ToolInvocation, state *BatchState) *Decision {
	maxSeq := maxSequentialReads()
	if category == "read" && !paginatedRead && hasConsecutiveSingleSteps(priorRecords, "read", maxSeq) {
		state.Records = filterOutCategory(state.Records, "read")
		return &Decision{
			Decision: DecisionDeny,
			Reason:   fmt.Sprintf("Parallel Tool Batching Invariant Violated: %d consecutive sequential single-file reads detected across turns. ALWAYS emit view_file, grep_search, and read tool calls in parallel within a single turn when inspecting multiple files.", maxSeq+1),
		}
	}
	return nil
}

func evaluatePriorBatchViolations(category, target string, args ToolArgs, priorRecords []ToolInvocation, state *BatchState) *Decision {
	paginatedRead := category == "read" && target != "" && isPaginatedRead(priorRecords, target, args.StartLine, args.EndLine)

	if dec := checkMicroReadViolation(category, target, paginatedRead, priorRecords, state); dec != nil {
		return dec
	}
	if dec := checkMicroEditViolation(category, target, priorRecords, state); dec != nil {
		return dec
	}
	if dec := checkMicroCommandViolation(category, priorRecords, state); dec != nil {
		return dec
	}
	if dec := checkSequentialReadViolation(category, paginatedRead, priorRecords, state); dec != nil {
		return dec
	}
	return nil
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

		var priorRecords []ToolInvocation
		for _, r := range state.Records {
			if !isSameStep(r, stepIdx, ts) {
				priorRecords = append(priorRecords, r)
			}
		}

		if dec := evaluatePriorBatchViolations(category, target, args, priorRecords, &state); dec != nil {
			decision = dec
			_ = saveState(dir, safeID, state) //nolint:errcheck
			return nil
		}

		// Allowed: invalidate prior reads on edit and record invocation
		if category == "edit" && target != "" {
			state.Records = removeTargetInvocations(state.Records, "read", target)
		}
		inv := ToolInvocation{
			Timestamp: ts,
			StepIdx:   stepIdx,
			ToolName:  payload.ToolCall.Name,
			Target:    target,
			Category:  category,
			StartLine: args.StartLine,
			EndLine:   args.EndLine,
		}
		state.Records = append(state.Records, inv)
		if len(state.Records) > DefaultMaxRecords {
			state.Records = state.Records[len(state.Records)-DefaultMaxRecords:]
		}

		_ = saveState(dir, safeID, state) //nolint:errcheck
		opportunisticGC(dir, now)
		decision = &Decision{Decision: DecisionAllow}
		if normalizedTool == "run_command" {
			if rewritten := stripGoTestVerbose(args.CommandLine); rewritten != args.CommandLine {
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
