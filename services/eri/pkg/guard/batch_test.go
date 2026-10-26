package guard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type batchHookInput struct {
	tool  string
	path  string
	cmd   string
	start int
	end   int
}

func TestCheckBatch(t *testing.T) {
	// Not t.Parallel() because it exercises SetBatchDir and t.Setenv
	now := time.Now()

	turn := func(items ...batchHookInput) []batchHookInput { return items }
	turns := func(ts ...[]batchHookInput) [][]batchHookInput { return ts }

	stepTests := []struct {
		name             string
		steps            [][]batchHookInput
		wantLastDecision DecisionType
		wantReasonSubstr string
		wantOverwriteCmd string
	}{
		{"unscoped go test", turns(turn(batchHookInput{tool: "run_command", cmd: "go test ./..."})), DecisionDeny, "Unscoped test sweep", ""},
		{"unscoped test brain/...", turns(turn(batchHookInput{tool: "run_command", cmd: "go test brain/..."})), DecisionDeny, "Unscoped test sweep", ""},
		{"unscoped test services/eri/...", turns(turn(batchHookInput{tool: "run_command", cmd: "go test services/eri/..."})), DecisionDeny, "Unscoped test sweep", ""},
		{"unscoped pytest", turns(turn(batchHookInput{tool: "run_command", cmd: "pytest"})), DecisionDeny, "Unscoped test sweep", ""},
		{"unscoped verify.sh", turns(turn(batchHookInput{tool: "run_command", cmd: "./scripts/verify.sh"})), DecisionDeny, "Unscoped test sweep", ""},
		{"scoped test allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "go test ./pkg/guard"})), DecisionAllow, "", ""},
		{"verbose test stripped and allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "go test -v ./pkg/guard"})), DecisionAllow, "", "go test ./pkg/guard"},
		{"verbose test with run stripped and allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "go test -v -run TestCheckBatch ./pkg/guard"})), DecisionAllow, "", "go test -run TestCheckBatch ./pkg/guard"},
		{"compound unscoped test denied", turns(turn(batchHookInput{tool: "run_command", cmd: "cd /tmp && go test ./..."})), DecisionDeny, "Unscoped test sweep", ""},
		{"scoped verify staged", turns(turn(batchHookInput{tool: "run_command", cmd: "./scripts/verify.sh --staged"})), DecisionAllow, "", ""},
		{"verify help allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "./scripts/verify.sh --help"})), DecisionAllow, "", ""},
		{"empty command in batch allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "   "})), DecisionAllow, "", ""},
		{"unbatched git add", turns(turn(batchHookInput{tool: "run_command", cmd: "git add main.go"})), DecisionDeny, "Unbatched git lifecycle", ""},
		{"unbatched git commit", turns(turn(batchHookInput{tool: "run_command", cmd: "git commit -m 'fix'"})), DecisionDeny, "Unbatched git lifecycle", ""},
		{"bare git status rewritten to -sb", turns(turn(batchHookInput{tool: "run_command", cmd: "git status"})), DecisionAllow, "", "git status -sb"},
		{"git -C status rewritten to -sb", turns(turn(batchHookInput{tool: "run_command", cmd: "git -C /tmp/repo status pkg/guard"})), DecisionAllow, "", "git -C /tmp/repo status -sb pkg/guard"},
		{"subshell git status rewritten to -sb", turns(turn(batchHookInput{tool: "run_command", cmd: "(cd /tmp/repo && git status)"})), DecisionAllow, "", "(cd /tmp/repo && git status -sb)"},
		{"git status -s unchanged", turns(turn(batchHookInput{tool: "run_command", cmd: "git status -s"})), DecisionAllow, "", ""},
		{"git status --porcelain unchanged", turns(turn(batchHookInput{tool: "run_command", cmd: "git status --porcelain"})), DecisionAllow, "", ""},
		{"unbounded git log denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git log"})), DecisionDeny, "Unbounded git log detected", ""},
		{"unbounded git log --oneline denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git log --oneline"})), DecisionDeny, "Unbounded git log detected", ""},
		{"bounded git log -n 5 allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git log -n 5 --oneline"})), DecisionAllow, "", ""},
		{"bounded git log -n5 allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git log -n5"})), DecisionAllow, "", ""},
		{"bounded git log -3 allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git log -3"})), DecisionAllow, "", ""},
		{"unscoped bare git diff denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff"})), DecisionDeny, "Unscoped full-patch git diff detected", ""},
		{"unscoped git diff --cached denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff --cached"})), DecisionDeny, "Unscoped full-patch git diff detected", ""},
		{"unscoped git diff HEAD denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff HEAD"})), DecisionDeny, "Unscoped full-patch git diff detected", ""},
		{"unscoped git diff origin/main denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git --no-pager diff origin/main"})), DecisionDeny, "Unscoped full-patch git diff detected", ""},
		{"unscoped git diff dot denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff -- ."})), DecisionDeny, "Unscoped full-patch git diff detected", ""},
		{"git version flag without subcommand allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git --version"})), DecisionAllow, "", ""},
		{"git diff --stat allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff --stat"})), DecisionAllow, "", ""},
		{"git diff --name-only allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff --cached --name-only"})), DecisionAllow, "", ""},
		{"git diff scoped with dash-dash allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff HEAD -- services/eri/pkg/guard/batch.go"})), DecisionAllow, "", ""},
		{"git diff scoped with file path allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff -U 3 services/eri/pkg/guard/batch.go"})), DecisionAllow, "", ""},
		{"read_file on transcript.jsonl denied", turns(turn(batchHookInput{tool: "read_file", path: "/data/runtimes/discord/.gemini/antigravity-cli/brain/361f6998/.system_generated/logs/transcript.jsonl"})), DecisionDeny, "transcript-search", ""},
		{"view_file on transcript_full.jsonl denied", turns(turn(batchHookInput{tool: "view_file", path: ".system_generated/logs/transcript_full.jsonl"})), DecisionDeny, "transcript-search", ""},
		{"docker exec go test -v stripped and allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "docker exec -i eri go test -v ./pkg/guard"})), DecisionAllow, "", "docker exec -i eri go test ./pkg/guard"},
		{"docker exec unscoped go test denied", turns(turn(batchHookInput{tool: "run_command", cmd: "docker exec eri go test ./..."})), DecisionDeny, "Unscoped test sweep", ""},
		{"micro inspection thrashing", turns(turn(batchHookInput{tool: "run_command", cmd: "cat a.txt"}), turn(batchHookInput{tool: "run_command", cmd: "head b.txt"}), turn(batchHookInput{tool: "run_command", cmd: "tail c.txt"}), turn(batchHookInput{tool: "run_command", cmd: "which d.txt"})), DecisionDeny, "Micro-command thrashing detected", ""},
		{"three micro reads same file allowed", turns(turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"})), DecisionAllow, "", ""},
		{"micro read thrashing same file on 4th", turns(turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"})), DecisionDeny, "Iterative micro-read thrashing", ""},
		{"micro read_file thrashing same file on 4th", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go"})), DecisionDeny, "Iterative micro-read thrashing", ""},
		{"micro read thrashing across share and scratch dir", turns(turn(batchHookInput{tool: "read_file", path: "/share/aerial/pkg/foo.go"}), turn(batchHookInput{tool: "read_file", path: "/data/scratch/aerial-code-scratch.abc123/pkg/foo.go"}), turn(batchHookInput{tool: "read_file", path: "$SCRATCHDIR/pkg/foo.go"}), turn(batchHookInput{tool: "read_file", path: "${SCRATCH_DIR}/pkg/foo.go"})), DecisionDeny, "Iterative micro-read thrashing", ""},
		{"edit invalidates prior reads on same file", turns(turn(batchHookInput{tool: "read_file", path: "/share/aerial/pkg/foo.go"}), turn(batchHookInput{tool: "read_file", path: "/share/aerial/pkg/foo.go"}), turn(batchHookInput{tool: "read_file", path: "/share/aerial/pkg/foo.go"}), turn(batchHookInput{tool: "edit_file", path: "/data/scratch/aerial-code-scratch.abc123/pkg/foo.go"}), turn(batchHookInput{tool: "read_file", path: "/data/scratch/aerial-code-scratch.abc123/pkg/foo.go"}), turn(batchHookInput{tool: "read_file", path: "$SCRATCHDIR/pkg/foo.go"}), turn(batchHookInput{tool: "read_file", path: "/share/aerial/pkg/foo.go"})), DecisionAllow, "", ""},
		{"three sequential single file reads allowed", turns(turn(batchHookInput{tool: "view_file", path: "/tmp/a.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/b.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/c.go"})), DecisionAllow, "", ""},
		{"sequential single file reads denied on 4th", turns(turn(batchHookInput{tool: "view_file", path: "/tmp/a.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/b.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/c.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/d.go"})), DecisionDeny, "Parallel Tool Batching Invariant Violated", ""},
		{"sequential single read_file calls denied on 4th", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/a.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/b.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/c.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/d.go"})), DecisionDeny, "Parallel Tool Batching Invariant Violated", ""},
		{"parallel reads allowed", turns(turn(batchHookInput{tool: "view_file", path: "/tmp/a.go"}, batchHookInput{tool: "view_file", path: "/tmp/b.go"}, batchHookInput{tool: "view_file", path: "/tmp/c.go"})), DecisionAllow, "", ""},
		{"micro edit thrashing", turns(turn(batchHookInput{tool: "replace_file_content", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "replace_file_content", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "replace_file_content", path: "/tmp/mod.go"})), DecisionDeny, "Coarse-Grained Code Editing Invariant Violated", ""},
		{"micro edit_file thrashing", turns(turn(batchHookInput{tool: "edit_file", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "write_file", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "edit_file", path: "/tmp/mod.go"})), DecisionDeny, "Coarse-Grained Code Editing Invariant Violated", ""},
		{"post-test edit after go test allowed", turns(turn(batchHookInput{tool: "write_file", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "run_command", cmd: "go test ./pkg/guard"}), turn(batchHookInput{tool: "edit_file", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "edit_file", path: "/tmp/mod.go"})), DecisionAllow, "", ""},
		{"read different file allowed", turns(turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"}, batchHookInput{tool: "view_file", path: "/tmp/extra1.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"}, batchHookInput{tool: "view_file", path: "/tmp/extra2.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/other.go"}, batchHookInput{tool: "view_file", path: "/tmp/extra1.go"})), DecisionAllow, "", ""},
		{"parallel same-file reads in turn 1 plus turn 2 read allowed", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/big.go", start: 10, end: 60}, batchHookInput{tool: "read_file", path: "/tmp/big.go", start: 800, end: 860}), turn(batchHookInput{tool: "read_file", path: "/tmp/big.go", start: 861, end: 950})), DecisionAllow, "", ""},
		{"non-overlapping 150-line paginated read_file allowed across 4 turns", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 1, end: 150}), turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 151, end: 300}), turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 301, end: 450}), turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 451, end: 600})), DecisionAllow, "", ""},
		{"overlapping paginated read_file denied on 4th", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 1, end: 500}, batchHookInput{tool: "read_file", path: "/tmp/o1.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 501, end: 1000}, batchHookInput{tool: "read_file", path: "/tmp/o2.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 400, end: 900}, batchHookInput{tool: "read_file", path: "/tmp/o3.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 450, end: 950})), DecisionDeny, "Iterative micro-read thrashing", ""},
		{"small non-overlapping micro-reads denied on 4th", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 1, end: 50}, batchHookInput{tool: "read_file", path: "/tmp/o1.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 51, end: 100}, batchHookInput{tool: "read_file", path: "/tmp/o2.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 101, end: 150}, batchHookInput{tool: "read_file", path: "/tmp/o3.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/huge.go", start: 151, end: 200})), DecisionDeny, "Iterative micro-read thrashing", ""},
	}

	for _, tc := range stepTests {
		t.Run("step_"+tc.name, func(t *testing.T) {
			dir := t.TempDir()
			convID := "test-session"
			var lastDec *Decision

			stepIdx := 0
			for turnIdx, turn := range tc.steps {
				stepTime := now.Add(time.Duration(turnIdx) * 2 * time.Second)
				for _, inv := range turn {
					stepIdx++
					payload := Payload{
						ConversationID: convID,
						StepIdx:        stepIdx,
						ToolCall:       ToolCall{Name: inv.tool},
					}
					args := ToolArgs{
						TargetFile:  inv.path,
						StartLine:   inv.start,
						EndLine:     inv.end,
						CommandLine: inv.cmd,
					}
					lastDec = CheckBatchWithDir(payload, args, dir, stepTime)
				}
			}

			if lastDec == nil {
				t.Fatalf("no decisions returned")
			}
			if lastDec.Decision != tc.wantLastDecision {
				t.Fatalf("got decision=%q, want %q (reason: %s)", lastDec.Decision, tc.wantLastDecision, lastDec.Reason)
			}
			if tc.wantReasonSubstr != "" && !strings.Contains(lastDec.Reason, tc.wantReasonSubstr) {
				t.Fatalf("expected reason containing %q, got %q", tc.wantReasonSubstr, lastDec.Reason)
			}
			if strings.Contains(lastDec.Reason, " -v ") {
				t.Fatalf("unexpected -v recommendation in reason: %q", lastDec.Reason)
			}
			gotOverwrite, _ := lastDec.Overwrite["CommandLine"].(string)
			if gotOverwrite != tc.wantOverwriteCmd {
				t.Fatalf("Overwrite[CommandLine] = %q, want %q", gotOverwrite, tc.wantOverwriteCmd)
			}
		})
	}

	// Fault Injection: Corrupted state file recovery
	dir := t.TempDir()
	convID := "corrupted-session"
	statePath := filepath.Join(dir, fmt.Sprintf("state-%s.json", sanitizeID(convID)))
	if err := os.WriteFile(statePath, []byte("{invalid-json-data"), 0o600); err != nil {
		t.Fatalf("failed to write corrupted state: %v", err)
	}
	dec := CheckBatchWithDir(Payload{ConversationID: convID, StepIdx: 1, ToolCall: ToolCall{Name: "view_file"}}, ToolArgs{TargetFile: "/tmp/foo.txt"}, dir, now)
	if dec.Decision != DecisionAllow {
		t.Fatalf("expected allow after corrupt state recovery, got: %v", dec)
	}

	// Concurrency test: 10 concurrent goroutines calling CheckBatchWithDir
	var wg sync.WaitGroup
	concurrentSession := "concurrent-session"
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p := Payload{
				ConversationID: concurrentSession,
				StepIdx:        idx + 1,
				ToolCall:       ToolCall{Name: "view_file"},
			}
			d := CheckBatchWithDir(p, ToolArgs{TargetFile: fmt.Sprintf("/tmp/concurrent_%d.go", idx)}, dir, now)
			if d == nil {
				t.Errorf("got nil decision in concurrent call")
			}
		}(i)
	}
	wg.Wait()

	// Max records truncation (DefaultMaxRecords = 30)
	truncDir := t.TempDir()
	truncSession := "trunc-session"
	for i := 0; i < 35; i++ {
		p := Payload{
			ConversationID: truncSession,
			StepIdx:        i + 1,
			ToolCall:       ToolCall{Name: "run_command"},
		}
		d := CheckBatchWithDir(p, ToolArgs{CommandLine: fmt.Sprintf("echo rec-%d && true", i)}, truncDir, now)
		if d.Decision != DecisionAllow {
			t.Fatalf("call %d expected allow, got %v", i, d)
		}
	}
	st := loadState(truncDir, truncSession, now)
	if len(st.Records) > DefaultMaxRecords {
		t.Fatalf("records length = %d, want <= %d", len(st.Records), DefaultMaxRecords)
	}

	// Uncreatable directory fail-open
	dFail := CheckBatchWithDir(Payload{ToolCall: ToolCall{Name: "view_file"}}, ToolArgs{TargetFile: "/a.go"}, "/dev/null/impossible", now)
	if dFail.Decision != DecisionAllow {
		t.Fatalf("expected allow on uncreatable dir, got %v", dFail)
	}

	// Wrapper and Env checks
	if defDir := GetBatchDir(); defDir != DefaultBatchDir {
		t.Fatalf("GetBatchDir() default = %q, want %q", defDir, DefaultBatchDir)
	}
	t.Setenv("AERIAL_BATCH_GUARD_DIR", "/tmp/custom-batch-guard-dir")
	if envDir := GetBatchDir(); envDir != "/tmp/custom-batch-guard-dir" {
		t.Fatalf("GetBatchDir() = %q, want /tmp/custom-batch-guard-dir", envDir)
	}

	customDir := t.TempDir()
	t.Setenv("AERIAL_BATCH_GUARD_DIR", customDir)
	dDefault := CheckBatch(Payload{ConversationID: "custom-batch-session", StepIdx: 1, ToolCall: ToolCall{Name: "view_file"}}, ToolArgs{TargetFile: "/tmp/fresh.txt"})
	if dDefault == nil || dDefault.Decision != DecisionAllow {
		t.Fatalf("expected allow from CheckBatch, got %v", dDefault)
	}

	// Tunable environment variable overrides and SCRATCH_DIR resolution
	t.Setenv("AERIAL_BATCH_MIN_PAGINATED_LINES", "80")
	t.Setenv("AERIAL_BATCH_MAX_MICRO_READS", "2")
	t.Setenv("AERIAL_BATCH_MAX_SEQUENTIAL_READS", "2")
	t.Setenv("SCRATCH_DIR", "/tmp/custom-worktree")
	if got := minPaginatedReadLines(); got != 80 {
		t.Fatalf("minPaginatedReadLines() = %d, want 80", got)
	}
	if got := maxMicroReads(); got != 2 {
		t.Fatalf("maxMicroReads() = %d, want 2", got)
	}
	if got := maxSequentialReads(); got != 2 {
		t.Fatalf("maxSequentialReads() = %d, want 2", got)
	}
	if got := normalizeBatchTarget("/tmp/custom-worktree/pkg/a.go"); got != "/share/aerial/pkg/a.go" {
		t.Fatalf("normalizeBatchTarget(custom-worktree) = %q, want /share/aerial/pkg/a.go", got)
	}
	if got := normalizeBatchTarget("$SCRATCH_DIR/pkg/a.go"); got != "/share/aerial/pkg/a.go" {
		t.Fatalf("normalizeBatchTarget($SCRATCH_DIR) = %q, want /share/aerial/pkg/a.go", got)
	}
	t.Setenv("AERIAL_BATCH_MIN_PAGINATED_LINES", "invalid")
	if got := minPaginatedReadLines(); got != DefaultMinPaginatedReadLines {
		t.Fatalf("minPaginatedReadLines() fallback = %d, want %d", got, DefaultMinPaginatedReadLines)
	}

	// Prune state files and opportunistic GC
	gcDir := t.TempDir()
	oldFile := filepath.Join(gcDir, "state-old-session.json")
	newFile := filepath.Join(gcDir, "state-new-session.json")
	if err := os.WriteFile(oldFile, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newFile, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldTime := now.Add(-3 * time.Hour)
	if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(gcDir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gcDir, "ignore.txt"), []byte("txt"), 0o600); err != nil {
		t.Fatal(err)
	}

	pruneStateFiles(gcDir, now)
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Fatalf("expected old file to be pruned")
	}
	if _, err := os.Stat(newFile); err != nil {
		t.Fatalf("expected new file to remain")
	}
	opportunisticGC(gcDir, now)
}

func TestBatchHelpers(t *testing.T) {
	t.Parallel()

	unscopedTests := []struct {
		cmd  string
		want bool
	}{
		{"go test ./...", true},
		{"go test brain/...", true},
		{"go test services/eri/...", true},
		{"go test", true},
		{"pytest", true},
		{"python3 -m pytest", true},
		{"scripts/verify.sh", true},
		{"./scripts/verify.sh", true},
		{"go test ./pkg/guard", false},
		{"go test -v ./pkg/guard", false},
		{"go test -v -run TestFoo ./pkg/guard", false},
		{"go test -v=true -run=TestFoo -timeout 30s ./pkg/guard", false},
		{"cd services/eri && go test ./...", true},
		{"(cd services/eri && go test ./...)", true},
		{"CGO_ENABLED=0 go test ./...", true},
		{"scripts/verify.sh --staged", false},
		{"scripts/verify.sh -h", false},
		{"", false},
	}
	for _, tc := range unscopedTests {
		t.Run("unscoped_"+tc.cmd, func(t *testing.T) {
			t.Parallel()
			if got := isUnscopedTest(tc.cmd); got != tc.want {
				t.Fatalf("isUnscopedTest(%q) = %v, want %v", tc.cmd, got, tc.want)
			}
		})
	}

	stripVerboseTests := []struct {
		cmd  string
		want string
	}{
		{"go test ./pkg/guard", "go test ./pkg/guard"},
		{"go test -v ./pkg/guard", "go test ./pkg/guard"},
		{"go test -test.v ./pkg/guard", "go test ./pkg/guard"},
		{"go test -v=true -test.v=true --v --test.v ./pkg/guard", "go test ./pkg/guard"},
		{"go test -v -run TestFoo ./pkg/guard", "go test -run TestFoo ./pkg/guard"},
		{"cd /tmp && CGO_ENABLED=1 go test -v -race ./pkg/guard", "cd /tmp && CGO_ENABLED=1 go test -race ./pkg/guard"},
		{"(cd /tmp && go test ./pkg/guard -v)", "(cd /tmp && go test ./pkg/guard)"},
		{"(go test -v ./pkg/guard)", "(go test ./pkg/guard)"},
		{"go test -v ./pkg/guard -- -v", "go test ./pkg/guard -- -v"},
		{"go test -run -v -v ./pkg/guard", "go test -run -v ./pkg/guard"},
	}
	for _, tc := range stripVerboseTests {
		t.Run("strip_verbose_"+tc.cmd, func(t *testing.T) {
			t.Parallel()
			if got := stripGoTestVerbose(tc.cmd); got != tc.want {
				t.Fatalf("stripGoTestVerbose(%q) = %q, want %q", tc.cmd, got, tc.want)
			}
		})
	}

	gitMutationTests := []struct {
		cmd  string
		want bool
	}{
		{"git add .", true},
		{"git commit -m 'msg'", true},
		{"git status", false},
		{"git diff", false},
		{"git log", false},
		{"echo git add", false},
		{"", false},
	}
	for _, tc := range gitMutationTests {
		t.Run("git_mutation_"+tc.cmd, func(t *testing.T) {
			t.Parallel()
			if got := isUnbatchedGitMutation(tc.cmd); got != tc.want {
				t.Fatalf("isUnbatchedGitMutation(%q) = %v, want %v", tc.cmd, got, tc.want)
			}
		})
	}

	singleInspectionTests := []struct {
		cmd  string
		want bool
	}{
		{"cat file.go", true},
		{"head -n 10 file.go", true},
		{"tail file.go", true},
		{"which agy", true},
		{"stat file.go", true},
		{"file binary", true},
		{"echo hello", true},
		{"cat a.txt && cat b.txt", false},
		{"cat a.txt | grep foo", false},
		{"", false},
		{"   ", false},
	}
	for _, tc := range singleInspectionTests {
		t.Run("inspection_"+tc.cmd, func(t *testing.T) {
			t.Parallel()
			if got := isSingleInspection(tc.cmd); got != tc.want {
				t.Fatalf("isSingleInspection(%q) = %v, want %v", tc.cmd, got, tc.want)
			}
		})
	}

	categoryTests := []struct {
		name    string
		tool    string
		args    ToolArgs
		wantCat string
		wantTgt string
	}{
		{"view file", "view_file", ToolArgs{TargetFile: "/tmp/a.go"}, "read", "/tmp/a.go"},
		{"view file empty path", "view_file", ToolArgs{}, "read", ""},
		{"view file config scratch", "view_file", ToolArgs{TargetFile: "/data/scratch/aerial-config-scratch.999999/config.yaml"}, "read", "/share/aerial-config/config.yaml"},
		{"view file aerial-pr scratch", "view_file", ToolArgs{TargetFile: "/tmp/aerial-pr-123456/main.go"}, "read", "/share/aerial/main.go"},
		{"replace content", "replace_file_content", ToolArgs{TargetFile: "/tmp/a.go"}, "edit", "/tmp/a.go"},
		{"write to file", "write_to_file", ToolArgs{TargetFile: "/tmp/a.go"}, "edit", "/tmp/a.go"},
		{"unscoped test cmd", "run_command", ToolArgs{CommandLine: "go test ./..."}, "unscoped_test", "go test ./..."},
		{"git mutation cmd", "run_command", ToolArgs{CommandLine: "git commit -m 'a'"}, "git_mutation", "git commit -m 'a'"},
		{"inspection cmd", "run_command", ToolArgs{CommandLine: "cat a.txt"}, "inspection", "cat a.txt"},
		{"generic cmd", "run_command", ToolArgs{CommandLine: "make build"}, "command", "make build"},
		{"other tool", "custom_mcp_tool", ToolArgs{}, "other", "custom_mcp_tool"},
		{"default_api prefix", "default_api:run_command", ToolArgs{CommandLine: "cat a.txt"}, "inspection", "cat a.txt"},
	}
	for _, tc := range categoryTests {
		t.Run("category_"+tc.name, func(t *testing.T) {
			t.Parallel()
			cat, tgt := categorizeInvocation(tc.tool, tc.args)
			if cat != tc.wantCat || tgt != tc.wantTgt {
				t.Fatalf("categorizeInvocation = (%q, %q), want (%q, %q)", cat, tgt, tc.wantCat, tc.wantTgt)
			}
		})
	}

	sanitizeTests := []struct {
		input string
		want  string
	}{
		{"", "default"},
		{"   ", "default"},
		{"session-123_abc", "session-123_abc"},
		{"session/../../../etc/passwd", "sessionetcpasswd"},
		{"!@#$%^&*()", "default"},
	}
	for _, tc := range sanitizeTests {
		t.Run("sanitize_"+tc.input, func(t *testing.T) {
			t.Parallel()
			if got := sanitizeID(tc.input); got != tc.want {
				t.Fatalf("sanitizeID(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}

	// Step grouping and timestamp check
	rec := ToolInvocation{Timestamp: 100, StepIdx: 0}
	if !isSameStep(rec, 0, 99) {
		t.Fatalf("expected true for within 1s diff")
	}
	if isSameStep(rec, 0, 50) {
		t.Fatalf("expected false for diff > 1s")
	}

	if steps := groupSteps(nil); steps != nil {
		t.Fatalf("expected nil for empty records")
	}
	invs := []ToolInvocation{
		{Timestamp: 100, StepIdx: 0},
		{Timestamp: 99, StepIdx: 0},
		{Timestamp: 50, StepIdx: 0},
	}
	steps := groupSteps(invs)
	if len(steps) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(steps))
	}

	// Filtering and counting helpers
	list := []ToolInvocation{
		{Category: "read", Target: "a.go", StepIdx: 1, Timestamp: 100},
		{Category: "read", Target: "a.go", StepIdx: 2, Timestamp: 200},
		{Category: "read", Target: "b.go", StepIdx: 3, Timestamp: 300},
		{Category: "edit", Target: "a.go", StepIdx: 4, Timestamp: 400},
	}
	if count := countTargetInvocations(list, "read", "a.go"); count != 2 {
		t.Fatalf("countTargetInvocations = %d, want 2", count)
	}
	afterRemove := removeTargetInvocations(list, "read", "a.go")
	if len(afterRemove) != 2 || afterRemove[0].Target != "b.go" || afterRemove[1].Category != "edit" {
		t.Fatalf("removeTargetInvocations failed: %+v", afterRemove)
	}
	afterFilter := filterOutCategory(list, "read")
	if len(afterFilter) != 1 || afterFilter[0].Category != "edit" {
		t.Fatalf("filterOutCategory failed: %+v", afterFilter)
	}
}
