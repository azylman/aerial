package widget

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

func TestFormatChunkActiveStep(t *testing.T) {
	t.Parallel()
	const flux = "https://flux.example.com"
	tests := []struct {
		name         string
		chunk        model.VanadisChunk
		fluxURL      string
		wantContains []string
	}{
		{"open with passing and pending checks", model.VanadisChunk{ID: "c1", Status: "open", CIChecks: map[string]string{"lint": "passed", "test": "pending"}}, "", []string{"1/2 passed"}},
		{"open with failed check is empty", model.VanadisChunk{ID: "c1-fail", Status: "open", CIChecks: map[string]string{"lint": "passed", "test": "failed"}}, "", nil},
		{"deploying without rollout state", model.VanadisChunk{ID: "c2", Status: "deploying"}, "", []string{"Deploying to cluster"}},
		{"pending is empty", model.VanadisChunk{ID: "c3", Status: "pending"}, "", nil},
		{"open without checks is empty", model.VanadisChunk{ID: "c-open", Status: "open"}, "", nil},
		{"open with only blank checks is empty", model.VanadisChunk{ID: "c-blank", Status: "open", CIChecks: map[string]string{"lint": ""}}, "", nil},
		{"merged with failing check is empty", model.VanadisChunk{ID: "c-fail", Status: "merged", CIChecks: map[string]string{"lint": "passed", "test": "failure"}}, "", nil},
		{"merged with kustomizations but not deploying skips rollout", model.VanadisChunk{ID: "c-kust", Status: "merged", RolloutState: &pubsub.PRRolloutState{KustomizationStatuses: map[string]string{"k1": "healthy"}}}, "", nil},
		{"deploying with blank rollout candidates deploys", model.VanadisChunk{ID: "c-dep", Status: "deploying", RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"", "  "}}}, "", []string{"Deploying to cluster"}},
		{"case-insensitive duplicate build services dedupe", model.VanadisChunk{ID: "c-dupe", Status: "merged", CIChecks: map[string]string{"Build & Push Images (Eri)": "passed", "Build and Push Images (ERI)": "passed", "lint": ""}}, "", []string{"`ERI`: 🔄 starting"}},
		{"deploying with kustomizations", model.VanadisChunk{ID: "c-kust2", Status: statusDeploying, RolloutState: &pubsub.PRRolloutState{KustomizationStatuses: map[string]string{"k1": statusDeployed}}}, flux, []string{"✅ rolled out"}},
		{"deploying with jobs", model.VanadisChunk{ID: "c-jobs", Status: statusDeploying, RolloutState: &pubsub.PRRolloutState{
			ExpectedJobs: []string{"svc-a", "svc-b", "svc-c", "svc-d"}, StartedJobs: []string{"svc-b", "svc-c"}, DeployedJobs: []string{"svc-a"},
			JobStates: map[string]string{"svc-c": "verifying_health"},
		}}, "", []string{"`svc-a`: ✅ healthy", "`svc-b`: 🔄 starting", "`svc-c`: 🔄 verifying health", "`svc-d`: ⏳ building image"}},
		{"merged with container image builds", model.VanadisChunk{ID: "c-images", Status: statusMerged, CIChecks: map[string]string{"build-check-1": "passed", "build-check-2": "passed"}}, "", []string{"Building container images (2/2 completed)"}},
		{"merged with matched build job service", model.VanadisChunk{ID: "c-svc", Status: statusMerged, CIChecks: map[string]string{"Build & Push Images (eri)": "passed"}}, "", []string{"`eri`: 🔄 starting"}},
		{"open with merge queue pos 1 and all passed", model.VanadisChunk{ID: "c-mq1", Status: "open", CIChecks: map[string]string{"lint": "passed", "test": "passed", "_merge_queue": "1/3"}}, "", []string{"Updating branch with main (#1 of 3 · 2/2 passed)"}},
		{"open with merge queue pos 2 and all passed", model.VanadisChunk{ID: "c-mq2", Status: "open", CIChecks: map[string]string{"lint": "passed", "test": "passed", "_merge_queue": "2/3"}}, "", []string{"Waiting in merge queue (#2 of 3 · 2/2 passed)"}},
		{"open with merge queue and pending checks", model.VanadisChunk{ID: "c-mq-pend", Status: "open", CIChecks: map[string]string{"lint": "passed", "test": "pending", "_merge_queue": "2/4"}}, "", []string{"Waiting on CI (1/2 passed · #2 of 4 in merge queue)"}},
		{"open with unqueued merge queue 1/1", model.VanadisChunk{ID: "c-mq-unqueued", Status: "open", CIChecks: map[string]string{"lint": "passed", "test": "pending", "_merge_queue": "1/1"}}, "", []string{"Waiting on CI (1/2 passed)"}},
		{"open with failed check and merge queue is empty", model.VanadisChunk{ID: "c-mq-fail", Status: "open", CIChecks: map[string]string{"lint": "failed", "_merge_queue": "1/2"}}, "", nil},
		{"open with only underscore metadata is empty", model.VanadisChunk{ID: "c-mq-only", Status: "open", CIChecks: map[string]string{"_merge_queue": "1/2", "_head_sha": "abc"}}, "", nil},
		{"merged with underscore keys not counted in image builds", model.VanadisChunk{ID: "c-mq-img", Status: statusMerged, CIChecks: map[string]string{"_merge_queue": "1/2", "build-check-1": "passed"}}, "", []string{"Building container images (1/1 completed)"}},
		{"ci_failed without active step uses banner", model.VanadisChunk{ID: "c-cifail", Status: "ci_failed", Banner: "Lint failed"}, "", []string{"Lint failed — remediation in progress"}},
		{"ci_failed with active step appends step", model.VanadisChunk{ID: "c-cifail-step", Status: "ci_failed", Banner: "Lint failed", ActiveStep: "Fixing formatting"}, "", []string{"Lint failed — remediation in progress", "Fixing formatting"}},
		{"conflict without active step uses default banner", model.VanadisChunk{ID: "c-conf", Status: "conflict"}, "", []string{"Remediation in progress"}},
		{"conflict with active step appends step", model.VanadisChunk{ID: "c-conf-step", Status: "conflict", Banner: "Merge conflict", ActiveStep: "Rebasing onto main"}, "", []string{"Merge conflict — remediation in progress", "Rebasing onto main"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got string
			if tc.fluxURL != "" {
				got = FormatChunkActiveStep(tc.chunk, tc.fluxURL)
			} else {
				got = FormatChunkActiveStep(tc.chunk)
			}
			if len(tc.wantContains) == 0 {
				assert.Empty(t, got)
			}
			for _, w := range tc.wantContains {
				assert.Contains(t, got, w)
			}
		})
	}
}

func TestNormalizeWidgetStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{input: "coding", want: "running"},
		{input: "running", want: "running"},
		{input: "pr_opened", want: "open"},
		{input: "open", want: "open"},
		{input: "completed", want: "completed"},
		{input: "deployed", want: "completed"},
		{input: "merged", want: "merged"},
		{input: "failed", want: "failed"},
		{input: "crashed", want: "failed"},
		{input: "timeout", want: "failed"},
		{input: "cancelled", want: "cancelled"},
		{input: "custom_status", want: "custom_status"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got := NormalizeWidgetStatus(tc.input)
			if got != tc.want {
				t.Errorf("NormalizeWidgetStatus(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestFormatDeployTargetLabel(t *testing.T) {
	t.Parallel()
	const flux = "https://flux.example.com/"
	tests := []struct {
		name, target, kind, fluxURL, want string
	}{
		{"no flux url", "default-svc", "job", "", "`default-svc`"},
		{"git-sync", "git-sync", "job", flux, "[`git-sync`](https://flux.example.com/c/main/flux/sources)"},
		{"github-runner", "github-runner", "job", flux, "[`github-runner`](https://flux.example.com/c/main/flux/helmreleases)"},
		{"deployment", "my-app", "job", flux, "[`my-app`](https://flux.example.com/c/main/deployments/default/my-app)"},
		{"kustomization", "kust-1", "kustomization", flux, "[`kust-1`](https://flux.example.com/c/main/flux/kustomize/kustomizations/flux-system/kust-1)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, formatDeployTargetLabel(tc.target, tc.kind, tc.fluxURL))
		})
	}
}

func TestFormatKustomizationRolloutStatus(t *testing.T) {
	t.Parallel()
	const flux = "https://flux.example.com"
	tests := []struct {
		name         string
		status       string
		kusts        map[string]string
		fluxURL      string
		wantOK       bool
		wantContains []string
	}{
		{"merged is not a rollout", statusMerged, map[string]string{"kust": "healthy"}, "", false, nil},
		{"deploying lists each kustomization", statusDeploying, map[string]string{"k1": statusDeployed, "k2": "progressing"}, flux, true, []string{
			"[`k1`](https://flux.example.com/c/main/flux/kustomize/kustomizations/flux-system/k1): ✅ rolled out",
			"[`k2`](https://flux.example.com/c/main/flux/kustomize/kustomizations/flux-system/k2): 🔄 rolling out",
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s string
			var ok bool
			if tc.fluxURL != "" {
				s, ok = FormatKustomizationRolloutStatus(tc.status, tc.kusts, tc.fluxURL)
			} else {
				s, ok = FormatKustomizationRolloutStatus(tc.status, tc.kusts)
			}
			assert.Equal(t, tc.wantOK, ok)
			if !tc.wantOK {
				assert.Empty(t, s)
			}
			for _, w := range tc.wantContains {
				assert.Contains(t, s, w)
			}
		})
	}
}

func TestBuildSyncWidgetRequest(t *testing.T) {
	t.Parallel()
	build := model.VanadisBuild{
		ID: "b1", Title: "Build 1", Objective: "Ship it", Status: "coding",
		ThreadID: "th-1", StatusMessageID: "msg-1", Tier: 2, Quiet: true,
	}
	chunks := []model.VanadisChunk{
		{ID: "c1", ChunkIndex: 1, Mode: "pr", Repo: "", Title: "Chunk 1", Status: "pending"},
		{ID: "c2", ChunkIndex: 2, Mode: "action", Title: "Chunk 2", Status: "running"},
	}
	req := BuildSyncWidgetRequest(build, chunks)
	assert.Equal(t, "th-1", req.ChannelID)
	assert.Equal(t, "msg-1", req.MessageID)
	assert.Equal(t, "b1", req.ID)
	assert.Equal(t, 2, req.Tier)
	assert.Equal(t, "Build 1", req.Title)
	assert.Equal(t, "running", req.Status)
	assert.Equal(t, "Ship it", req.Banner)
	assert.True(t, req.SkipSink)
	if len(req.Chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(req.Chunks))
	}
	// The running action chunk renders above the pending PR chunk.
	assert.Equal(t, "c2", req.Chunks[0].ID)
	assert.Equal(t, "", req.Chunks[0].Repo)
	assert.Equal(t, "c1", req.Chunks[1].ID)
	assert.Equal(t, "aerial", req.Chunks[1].Repo)
}

var widgetOrderBase = time.Date(2026, 11, 19, 9, 0, 0, 0, time.UTC)

// orderChunk builds a fixture chunk; startedMin/completedMin < 0 leave the timestamp zero.
func orderChunk(id string, idx int, status string, startedMin, completedMin int, deps ...string) model.VanadisChunk {
	c := model.VanadisChunk{ID: id, ChunkIndex: idx, Status: status, DependsOn: deps}
	if startedMin >= 0 {
		c.StartedAt = widgetOrderBase.Add(time.Duration(startedMin) * time.Minute)
	}
	if completedMin >= 0 {
		c.CompletedAt = widgetOrderBase.Add(time.Duration(completedMin) * time.Minute)
	}
	return c
}

// regressionOrderChunks reproduces an appended-DAG build: 0-14 completed, 15 running,
// 16 pending verify (depends on 21), 17-20 completed later, 21 pending (depends on 15).
func regressionOrderChunks() []model.VanadisChunk {
	chunks := make([]model.VanadisChunk, 0, 22)
	for i := range 15 {
		chunks = append(chunks, orderChunk(fmt.Sprintf("c%02d", i), i, "completed", i, i+1))
	}
	chunks = append(chunks,
		orderChunk("c15", 15, "running", 30, -1),
		orderChunk("c16", 16, "pending", -1, -1, "c21"),
	)
	for i := 17; i <= 20; i++ {
		chunks = append(chunks, orderChunk(fmt.Sprintf("c%02d", i), i, "merged", i, i+20))
	}
	return append(chunks, orderChunk("c21", 21, "pending", -1, -1, "c15"))
}

func TestOrderWidgetChunks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		chunks []model.VanadisChunk
		want   []string
	}{
		{
			name:   "empty input",
			chunks: nil,
			want:   []string{},
		},
		{
			name:   "appended dag regression orders done by completion then running then pending by depth",
			chunks: regressionOrderChunks(),
			want: []string{
				"c00", "c01", "c02", "c03", "c04", "c05", "c06", "c07", "c08", "c09",
				"c10", "c11", "c12", "c13", "c14", "c17", "c18", "c19", "c20",
				"c15", "c21", "c16",
			},
		},
		{
			name: "failed chunk stays in progress stage ahead of pending",
			chunks: []model.VanadisChunk{
				orderChunk("p", 0, "pending", -1, -1),
				orderChunk("f", 1, "failed", 5, -1),
				orderChunk("r", 2, "coding", 2, -1),
				orderChunk("d", 3, "deployed", 1, 3),
			},
			want: []string{"d", "r", "f", "p"},
		},
		{
			name: "cancelled closed synced and success are done",
			chunks: []model.VanadisChunk{
				orderChunk("open", 0, "pr_opened", 1, -1),
				orderChunk("cancel", 1, "cancelled", -1, 4),
				orderChunk("close", 2, "closed", -1, 3),
				orderChunk("sync", 3, "synced", -1, 2),
				orderChunk("ok", 4, "success", -1, 1),
			},
			want: []string{"ok", "sync", "close", "cancel", "open"},
		},
		{
			name: "pending dag depth beats chunk index",
			chunks: []model.VanadisChunk{
				orderChunk("deep", 0, "pending", -1, -1, "mid"),
				orderChunk("mid", 1, "pending", -1, -1, "root"),
				orderChunk("root", 2, "pending", -1, -1),
				orderChunk("blank", 3, "", -1, -1),
			},
			want: []string{"root", "blank", "mid", "deep"},
		},
		{
			name: "unknown depends on ids are treated as roots",
			chunks: []model.VanadisChunk{
				orderChunk("child", 0, "pending", -1, -1, "orphan-parent"),
				orderChunk("leaf", 1, "pending", -1, -1, "child"),
				orderChunk("root", 2, "pending", -1, -1, "missing"),
			},
			want: []string{"child", "root", "leaf"},
		},
		{
			name: "dependency cycle terminates and falls back to chunk index",
			chunks: []model.VanadisChunk{
				orderChunk("a", 0, "pending", -1, -1, "b"),
				orderChunk("b", 1, "pending", -1, -1, "a"),
			},
			want: []string{"b", "a"},
		},
		{
			name: "zero timestamps sort after set ones then fall back to chunk index",
			chunks: []model.VanadisChunk{
				orderChunk("done-zero-b", 1, "completed", -1, -1),
				orderChunk("done-zero-a", 0, "completed", -1, -1),
				orderChunk("done-set", 2, "completed", -1, 9),
				orderChunk("run-zero", 3, "running", -1, -1),
				orderChunk("run-set", 4, "running", 7, -1),
			},
			want: []string{"done-set", "done-zero-a", "done-zero-b", "run-set", "run-zero"},
		},
		{
			name: "identical keys tiebreak on id",
			chunks: []model.VanadisChunk{
				orderChunk("zeta", 0, "pending", -1, -1),
				orderChunk("alpha", 0, "pending", -1, -1),
			},
			want: []string{"alpha", "zeta"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := slices.Clone(tc.chunks)
			got := orderWidgetChunks(tc.chunks)
			ids := make([]string, 0, len(got))
			for _, c := range got {
				ids = append(ids, c.ID)
			}
			assert.Equal(t, tc.want, ids)
			assert.Equal(t, before, tc.chunks, "input slice must not be mutated")
		})
	}
}

func TestBuildStatusWidgetChunk(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		chunk           model.VanadisChunk
		wantMode        string
		wantRepo        string
		wantStepContain string
		wantLive        bool
	}{
		{"PRChunkKeepsRepo", model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial-config", Status: "pending"}, "pr", "aerial-config", "", false},
		{"PRChunkWithoutRepoDefaultsToAerial", model.VanadisChunk{ID: "c2", Mode: "pr", Status: "pending"}, "pr", "aerial", "", false},
		{"ActionChunkWithPRKeepsRepo", model.VanadisChunk{ID: "c4", Mode: "action", Repo: "aerial", PRNumber: 2431, Status: "running"}, "action", "aerial", "", false},
		{"ActionChunkWithPRKeepsOtherRepo", model.VanadisChunk{ID: "c5", Mode: "action", Repo: "aerial-config", PRNumber: 7, Status: "running"}, "action", "aerial-config", "", false},
		{"ActionChunkWithPRNoRepoDefaults", model.VanadisChunk{ID: "c6", Mode: "action", PRNumber: 7, Status: "running"}, "action", "aerial", "", false},
		{"ActionChunkHasNoRepo", model.VanadisChunk{ID: "c3", Mode: "action", Status: "pending"}, "action", "", "", false},
		{"CIFailedStaysLive", model.VanadisChunk{ID: "c7", Mode: "pr", Repo: "aerial", Status: "ci_failed", Banner: `CI check "lint" failed`}, "pr", "aerial", "remediation", true},
		{"CIFailedWithActiveStep", model.VanadisChunk{ID: "c7-step", Mode: "pr", Repo: "aerial", Status: "ci_failed", Banner: `CI check "lint" failed`, ActiveStep: "Fixing linter error"}, "pr", "aerial", "Fixing linter error", true},
		{"ConflictStaysLive", model.VanadisChunk{ID: "c8", Mode: "pr", Repo: "aerial", Status: "conflict", Banner: "Merge conflict detected"}, "pr", "aerial", "remediation", true},
		{"ConflictWithActiveStep", model.VanadisChunk{ID: "c8-step", Mode: "pr", Repo: "aerial", Status: "conflict", Banner: "Merge conflict detected", ActiveStep: "Resolving git conflict"}, "pr", "aerial", "Resolving git conflict", true},
		{"FailedIsTerminal", model.VanadisChunk{ID: "c9", Mode: "pr", Repo: "aerial", Status: "failed"}, "pr", "aerial", "", false},
		{"ActiveStepOnLiveChunk", model.VanadisChunk{ID: "c10", Mode: "action", Status: "running", ActiveStep: "Compiling binary..."}, "action", "", "Compiling binary...", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := BuildStatusWidgetChunk(tc.chunk)
			assert.Equal(t, tc.wantMode, got.Mode)
			assert.Equal(t, tc.wantRepo, got.Repo)
			assert.Contains(t, got.ActiveStep, tc.wantStepContain)
			if tc.wantStepContain != "" {
				assert.Equal(t, tc.wantLive, !isChunkTerminal(got.Status), "ci_failed/conflict are recoverable: remediation runs and the next head flips back to pr_opened")
			}
		})
	}
	assert.True(t, isChunkTerminal("failed"))
}

func TestNormalizeRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		owner     string
		repo      string
		wantFull  string
		wantShort string
	}{
		{name: "bare repo gets owner", owner: "acme", repo: "aerial", wantFull: "acme/aerial", wantShort: "aerial"},
		{name: "bare repo trimmed", owner: "acme", repo: " aerial/ ", wantFull: "acme/aerial", wantShort: "aerial"},
		{name: "qualified repo kept", owner: "acme", repo: "other/x", wantFull: "other/x", wantShort: "x"},
		{name: "empty repo", owner: "acme", repo: "  ", wantFull: "", wantShort: ""},
		{name: "empty owner keeps bare repo", owner: "", repo: "aerial", wantFull: "aerial", wantShort: "aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			full, short := NormalizeRepo(tc.owner, tc.repo)
			assert.Equal(t, tc.wantFull, full)
			assert.Equal(t, tc.wantShort, short)
		})
	}
}

func TestParseMergeQueuePosition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		raw       string
		wantPos   int
		wantTotal int
		wantOK    bool
	}{
		{name: "empty", raw: "", wantPos: 0, wantTotal: 0, wantOK: false},
		{name: "missing slash", raw: "123", wantPos: 0, wantTotal: 0, wantOK: false},
		{name: "extra slash", raw: "1/2/3", wantPos: 0, wantTotal: 0, wantOK: false},
		{name: "non-numeric pos", raw: "a/2", wantPos: 0, wantTotal: 0, wantOK: false},
		{name: "non-numeric total", raw: "1/b", wantPos: 0, wantTotal: 0, wantOK: false},
		{name: "total less than two", raw: "1/1", wantPos: 0, wantTotal: 0, wantOK: false},
		{name: "pos less than one", raw: "0/2", wantPos: 0, wantTotal: 0, wantOK: false},
		{name: "pos greater than total", raw: "3/2", wantPos: 0, wantTotal: 0, wantOK: false},
		{name: "valid head of queue", raw: "1/2", wantPos: 1, wantTotal: 2, wantOK: true},
		{name: "valid tail of queue", raw: "3/3", wantPos: 3, wantTotal: 3, wantOK: true},
		{name: "valid with whitespace", raw: " 2 / 5 ", wantPos: 2, wantTotal: 5, wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pos, total, ok := parseMergeQueuePosition(tc.raw)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantPos, pos)
			assert.Equal(t, tc.wantTotal, total)
		})
	}
}
