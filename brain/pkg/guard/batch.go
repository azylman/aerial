package guard

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
)

var (
	gcMu        sync.Mutex
	lastGCTime  time.Time
	batchDirMu  sync.RWMutex
	batchDir    = DefaultBatchDir
	pytestRegex = regexp.MustCompile(`^(python[0-9.]*\s+-m\s+)?pytest(\s+(-v|-q|-s|-x|--tb=\S+))*\s*$`)
)

// SetBatchDir overrides the directory used for batch guard state persistence (primarily for testing).
func SetBatchDir(dir string) {
	batchDirMu.Lock()
	defer batchDirMu.Unlock()
	batchDir = dir
}

// GetBatchDir returns the active directory for batch guard state persistence.
func GetBatchDir() string {
	if env := os.Getenv("AERIAL_BATCH_GUARD_DIR"); env != "" {
		return env
	}
	batchDirMu.RLock()
	defer batchDirMu.RUnlock()
	return batchDir
}

// ToolInvocation tracks a single tool execution for batch and thrash detection.
type ToolInvocation struct {
	Timestamp int64  `json:"timestamp"`
	StepIdx   int    `json:"stepIdx"`
	ToolName  string `json:"toolName"`
	Target    string `json:"target"`
	Category  string `json:"category"`
}

// BatchState persists recent tool invocations for a conversation.
type BatchState struct {
	Records []ToolInvocation `json:"records"`
}

func isUnscopedTest(cmd string) bool {
	trimmed := strings.TrimSpace(cmd)
	if trimmed == "" {
		return false
	}

	// Bare ./scripts/verify.sh without --staged
	if strings.HasPrefix(trimmed, "./scripts/verify.sh") || strings.HasPrefix(trimmed, "scripts/verify.sh") {
		if !strings.Contains(trimmed, "--staged") && !strings.Contains(trimmed, "-h") && !strings.Contains(trimmed, "--help") {
			return true
		}
	}

	// Bare pytest or python -m pytest without specific test path
	if pytestRegex.MatchString(trimmed) {
		return true
	}

	// Unscoped go test
	if strings.HasPrefix(trimmed, "go test") {
		fields := strings.Fields(trimmed)
		var pkgArgs []string
		for i := 2; i < len(fields); i++ {
			f := fields[i]
			if strings.HasPrefix(f, "-") {
				if f == "-run" || f == "-timeout" || f == "-tags" || f == "-coverprofile" || f == "-cpu" {
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
			if arg == "./..." || arg == "brain/..." || arg == "..." {
				return true
			}
		}
	}

	return false
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

func categorizeInvocation(toolName string, args ToolArgs) (category, target string) {
	switch toolName {
	case "view_file":
		return "read", args.FilePath()
	case "replace_file_content", "write_to_file":
		return "edit", args.FilePath()
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
	if currentStep > 0 && rec.StepIdx > 0 {
		return rec.StepIdx == currentStep
	}
	diff := currentTS - rec.Timestamp
	if diff < 0 {
		diff = -diff
	}
	return diff <= 1
}

func withStateLock(dir, safeID string, fn func() error) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fn()
	}

	lockPath := filepath.Join(dir, fmt.Sprintf("state-%s.lock", safeID))
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
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

	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
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
	distinctSteps := make(map[string]struct{})
	for _, r := range records {
		if r.Category == category && r.Target == target {
			key := fmt.Sprintf("%d_%d", r.StepIdx, r.Timestamp)
			distinctSteps[key] = struct{}{}
		}
	}
	return len(distinctSteps)
}

func removeTargetInvocations(records []ToolInvocation, target string) []ToolInvocation {
	var kept []ToolInvocation
	for _, r := range records {
		if r.Target != target {
			kept = append(kept, r)
		}
	}
	return kept
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

		var same bool
		if currentStep > 0 && r.StepIdx > 0 {
			same = (r.StepIdx == currentStep)
		} else {
			diff := r.Timestamp - currentTS
			if diff < 0 {
				diff = -diff
			}
			same = (diff <= 1)
		}

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

// CheckBatchWithDir evaluates tool batching invariants with configurable state directory and timestamp.
// Guaranteed fail-open: returns allow on unexpected errors or corrupted states.
func CheckBatchWithDir(payload Payload, args ToolArgs, dir string, now time.Time) *Decision {
	// Rule 1: Unscoped test sweeps
	if payload.ToolCall.Name == "run_command" && isUnscopedTest(args.CommandLine) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unscoped test sweep detected. Run targeted package unit tests (e.g. 'go test -v ./pkg/<pkg>/...') during development; comprehensive monorepo sweeps are offloaded to GitHub Actions CI or './scripts/verify.sh --staged'.",
		}
	}

	// Rule 2: Unbatched git mutations
	if payload.ToolCall.Name == "run_command" && isUnbatchedGitMutation(args.CommandLine) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unbatched git lifecycle command detected. Combine git operations into compound one-liners (e.g. 'git add -A && git commit -m ... && git push ...') or use 'scripts/aerial-pr.sh submit <scratch_dir> [commit_msg]' for automated pre-flight verification and submission.",
		}
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

		// Rule 4: Micro-reads to the same file (view_file)
		if payload.ToolCall.Name == "view_file" && target != "" {
			reads := countTargetInvocations(priorRecords, "read", target)
			if reads >= 2 {
				decision = &Decision{
					Decision: DecisionDeny,
					Reason:   "Iterative micro-read thrashing detected: 3 sequential reads targeting the same file across turns. Read the complete required range or whole file in a single view_file call (up to 800 lines) instead of small slices.",
				}
				state.Records = removeTargetInvocations(state.Records, target)
				_ = saveState(dir, safeID, state) //nolint:errcheck
				return nil
			}
		}

		// Rule 6: Micro-edits to the same file (replace_file_content / write_to_file)
		if category == "edit" && target != "" {
			edits := countTargetInvocations(priorRecords, "edit", target)
			if edits >= 2 {
				decision = &Decision{
					Decision: DecisionDeny,
					Reason:   "Coarse-Grained Code Editing Invariant Violated: 3 consecutive micro-edits targeting the same file across turns. Formulate the complete target diff and apply it in a single comprehensive edit, or rewrite the file via write_to_file.",
				}
				state.Records = removeTargetInvocations(state.Records, target)
				_ = saveState(dir, safeID, state) //nolint:errcheck
				return nil
			}
		}

		// Rule 3: Micro-command inspection thrashing (run_command)
		if category == "inspection" {
			steps := groupSteps(priorRecords)
			if len(steps) >= 3 {
				consecutive := true
				for i := len(steps) - 3; i < len(steps); i++ {
					if len(steps[i]) != 1 || steps[i][0].Category != "inspection" {
						consecutive = false
						break
					}
				}
				if consecutive {
					decision = &Decision{
						Decision: DecisionDeny,
						Reason:   "Micro-command thrashing detected: 4 consecutive single inspection commands across turns. Combine commands into compound one-liners (e.g. with && or ;) or execute an ephemeral scratch script in brain/<id>/scratch/ to collect diagnostics.",
					}
					state.Records = filterOutCategory(state.Records, "inspection")
					_ = saveState(dir, safeID, state) //nolint:errcheck
					return nil
				}
			}
		}

		// Rule 5: Sequential single-file reads across turns (view_file)
		if payload.ToolCall.Name == "view_file" {
			steps := groupSteps(priorRecords)
			if len(steps) >= 2 {
				consecutive := true
				for i := len(steps) - 2; i < len(steps); i++ {
					if len(steps[i]) != 1 || steps[i][0].ToolName != "view_file" {
						consecutive = false
						break
					}
				}
				if consecutive {
					decision = &Decision{
						Decision: DecisionDeny,
						Reason:   "Parallel Tool Batching Invariant Violated: 3 consecutive sequential single-file reads detected across turns. ALWAYS emit view_file, grep_search, and read tool calls in parallel within a single turn when inspecting multiple files.",
					}
					state.Records = filterOutCategory(state.Records, "read")
					_ = saveState(dir, safeID, state) //nolint:errcheck
					return nil
				}
			}
		}

		// Allowed: record invocation
		inv := ToolInvocation{
			Timestamp: ts,
			StepIdx:   stepIdx,
			ToolName:  payload.ToolCall.Name,
			Target:    target,
			Category:  category,
		}
		state.Records = append(state.Records, inv)
		if len(state.Records) > DefaultMaxRecords {
			state.Records = state.Records[len(state.Records)-DefaultMaxRecords:]
		}

		_ = saveState(dir, safeID, state) //nolint:errcheck
		opportunisticGC(dir, now)
		decision = &Decision{Decision: DecisionAllow}
		return nil
	})

	if err != nil || decision == nil {
		return &Decision{Decision: DecisionAllow}
	}
	return decision
}
