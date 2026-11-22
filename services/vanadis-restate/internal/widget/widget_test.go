package widget

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

func TestWidgetFormatting(t *testing.T) {
	t.Parallel()
	c := model.VanadisChunk{
		ID:     "c1",
		Status: "open",
		CIChecks: map[string]string{
			"lint": "passed",
			"test": "pending",
		},
	}
	step := FormatChunkActiveStep(c)
	if !strings.Contains(step, "1/2 passed") {
		t.Errorf("FormatChunkActiveStep = %q, want '1/2 passed'", step)
	}

	cFailed := model.VanadisChunk{
		ID:     "c1-fail",
		Status: "open",
		CIChecks: map[string]string{
			"lint": "passed",
			"test": "failed",
		},
	}
	if stepFailed := FormatChunkActiveStep(cFailed); stepFailed != "" {
		t.Errorf("FormatChunkActiveStep failed = %q, want empty", stepFailed)
	}

	cDeploying := model.VanadisChunk{ID: "c2", Status: "deploying"}
	stepDeploy := FormatChunkActiveStep(cDeploying)
	if stepDeploy != "Deploying to cluster" {
		t.Errorf("FormatChunkActiveStep deploying = %q, want 'Deploying to cluster'", stepDeploy)
	}

	cEmpty := model.VanadisChunk{ID: "c3", Status: "pending"}
	if stepEmpty := FormatChunkActiveStep(cEmpty); stepEmpty != "" {
		t.Errorf("FormatChunkActiveStep pending = %q, want empty", stepEmpty)
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

func TestFormatKustomizationAndJobRollout(t *testing.T) {
	t.Parallel()

	// Deploy target label tests
	assert.Equal(t, "`default-svc`", formatDeployTargetLabel("default-svc", "job", ""))
	assert.Equal(t, "[`git-sync`](https://flux.example.com/c/main/flux/sources)", formatDeployTargetLabel("git-sync", "job", "https://flux.example.com/"))
	assert.Equal(t, "[`github-runner`](https://flux.example.com/c/main/flux/helmreleases)", formatDeployTargetLabel("github-runner", "job", "https://flux.example.com/"))
	assert.Equal(t, "[`my-app`](https://flux.example.com/c/main/deployments/default/my-app)", formatDeployTargetLabel("my-app", "job", "https://flux.example.com/"))
	assert.Equal(t, "[`kust-1`](https://flux.example.com/c/main/flux/kustomize/kustomizations/flux-system/kust-1)", formatDeployTargetLabel("kust-1", "kustomization", "https://flux.example.com/"))

	// Kustomization rollout status
	s, ok := FormatKustomizationRolloutStatus(statusMerged, map[string]string{"kust": "healthy"})
	assert.False(t, ok)
	assert.Empty(t, s)

	s, ok = FormatKustomizationRolloutStatus(statusDeploying, map[string]string{
		"k1": statusDeployed,
		"k2": "progressing",
	}, "https://flux.example.com")
	assert.True(t, ok)
	assert.Contains(t, s, "[`k1`](https://flux.example.com/c/main/flux/kustomize/kustomizations/flux-system/k1): ✅ rolled out")
	assert.Contains(t, s, "[`k2`](https://flux.example.com/c/main/flux/kustomize/kustomizations/flux-system/k2): 🔄 rolling out")

	// FormatChunkActiveStep with kustomizations
	chunkKust := model.VanadisChunk{
		ID:     "c-kust",
		Status: statusDeploying,
		RolloutState: &pubsub.PRRolloutState{
			KustomizationStatuses: map[string]string{"k1": statusDeployed},
		},
	}
	stepKust := FormatChunkActiveStep(chunkKust, "https://flux.example.com")
	assert.Contains(t, stepKust, "✅ rolled out")

	// FormatChunkActiveStep with jobs
	chunkJobs := model.VanadisChunk{
		ID:     "c-jobs",
		Status: statusDeploying,
		RolloutState: &pubsub.PRRolloutState{
			ExpectedJobs: []string{"svc-a", "svc-b", "svc-c", "svc-d"},
			StartedJobs:  []string{"svc-b", "svc-c"},
			DeployedJobs: []string{"svc-a"},
			JobStates:    map[string]string{"svc-c": "verifying_health"},
		},
	}
	stepJobs := FormatChunkActiveStep(chunkJobs)
	assert.Contains(t, stepJobs, "`svc-a`: ✅ healthy")
	assert.Contains(t, stepJobs, "`svc-b`: 🔄 starting")
	assert.Contains(t, stepJobs, "`svc-c`: 🔄 verifying health")
	assert.Contains(t, stepJobs, "`svc-d`: ⏳ building image")

	// FormatChunkActiveStep with container image builds on merged
	chunkImages := model.VanadisChunk{
		ID:     "c-images",
		Status: statusMerged,
		CIChecks: map[string]string{
			"build-check-1": "passed",
			"build-check-2": "passed",
		},
	}
	stepImages := FormatChunkActiveStep(chunkImages)
	assert.Contains(t, stepImages, "Building container images (2/2 completed)")

	// FormatChunkActiveStep with matched build job service on merged
	chunkService := model.VanadisChunk{
		ID:     "c-svc",
		Status: statusMerged,
		CIChecks: map[string]string{
			"Build & Push Images (eri)": "passed",
		},
	}
	stepService := FormatChunkActiveStep(chunkService)
	assert.Contains(t, stepService, "`eri`: 🔄 starting")
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

func TestChunkActiveStepBranches(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		chunk model.VanadisChunk
		want  string
	}{
		{
			name:  "open without checks is empty",
			chunk: model.VanadisChunk{ID: "c-open", Status: "open"},
			want:  "",
		},
		{
			name:  "open with only blank checks is empty",
			chunk: model.VanadisChunk{ID: "c-blank", Status: "open", CIChecks: map[string]string{"lint": ""}},
			want:  "",
		},
		{
			name: "merged with failing check is empty",
			chunk: model.VanadisChunk{ID: "c-fail", Status: "merged", CIChecks: map[string]string{
				"lint": "passed",
				"test": "failure",
			}},
			want: "",
		},
		{
			name: "merged with kustomizations but not deploying skips rollout",
			chunk: model.VanadisChunk{ID: "c-kust", Status: "merged", RolloutState: &pubsub.PRRolloutState{
				KustomizationStatuses: map[string]string{"k1": "healthy"},
			}},
			want: "",
		},
		{
			name: "deploying with blank rollout candidates deploys",
			chunk: model.VanadisChunk{ID: "c-dep", Status: "deploying", RolloutState: &pubsub.PRRolloutState{
				ExpectedJobs: []string{"", "  "},
			}},
			want: "Deploying to cluster",
		},
		{
			name: "case-insensitive duplicate build services dedupe",
			chunk: model.VanadisChunk{ID: "c-dupe", Status: "merged", CIChecks: map[string]string{
				"Build & Push Images (Eri)":   "passed",
				"Build and Push Images (ERI)": "passed",
				"lint":                        "",
			}},
			want: "`ERI`: 🔄 starting",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, FormatChunkActiveStep(tc.chunk))
		})
	}
}

func TestBuildStatusWidgetChunkCIFailedStaysLive(t *testing.T) {
	t.Parallel()
	got := BuildStatusWidgetChunk(model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial", Status: "ci_failed", Banner: `CI check "lint" failed`})
	assert.Contains(t, got.ActiveStep, "remediation")
	assert.False(t, isChunkTerminal(got.Status), "ci_failed is recoverable: remediation runs and the next head flips it back to pr_opened")
	got = BuildStatusWidgetChunk(model.VanadisChunk{ID: "c2", Mode: "pr", Repo: "aerial", Status: "conflict", Banner: "Merge conflict detected"})
	assert.Contains(t, got.ActiveStep, "remediation")
	assert.False(t, isChunkTerminal(got.Status))
	assert.True(t, isChunkTerminal("failed"))
}

func TestBuildStatusWidgetChunk(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		chunk    model.VanadisChunk
		wantMode string
		wantRepo string
	}{
		{"PRChunkKeepsRepo", model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial-config", Status: "pending"}, "pr", "aerial-config"},
		{"PRChunkWithoutRepoDefaultsToAerial", model.VanadisChunk{ID: "c2", Mode: "pr", Status: "pending"}, "pr", "aerial"},
		{"ActionChunkHasNoRepo", model.VanadisChunk{ID: "c3", Mode: "action", Status: "pending"}, "action", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := BuildStatusWidgetChunk(tc.chunk)
			if got.Mode != tc.wantMode || got.Repo != tc.wantRepo {
				t.Errorf("BuildStatusWidgetChunk() mode=%q repo=%q, want mode=%q repo=%q", got.Mode, got.Repo, tc.wantMode, tc.wantRepo)
			}
		})
	}
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
