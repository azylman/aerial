package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestFormatCIChecksActiveStep(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		prStatus     string
		ciChecksJSON string
		want         string
	}{
		{name: "non-open status", prStatus: "merged", ciChecksJSON: `{"lint":"passed"}`, want: ""},
		{name: "empty json", prStatus: "open", ciChecksJSON: "", want: ""},
		{name: "empty object", prStatus: "open", ciChecksJSON: "{}", want: ""},
		{name: "invalid json", prStatus: "open", ciChecksJSON: "invalid", want: ""},
		{name: "single passed", prStatus: "open", ciChecksJSON: `{"lint":"passed"}`, want: "Waiting on CI (1/1 passed)"},
		{name: "mixed passed and pending", prStatus: "open", ciChecksJSON: `{"lint":"passed","test":"pending"}`, want: "Waiting on CI (1/2 passed)"},
		{name: "has failed", prStatus: "open", ciChecksJSON: `{"lint":"passed","test":"failed"}`, want: ""},
		{name: "all pending", prStatus: "open", ciChecksJSON: `{"lint":"pending","test":"pending"}`, want: "Waiting on CI (0/2 passed)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatCIChecksActiveStep(tc.prStatus, tc.ciChecksJSON)
			if got != tc.want {
				t.Fatalf("formatCIChecksActiveStep(%q, %q) = %q, want %q", tc.prStatus, tc.ciChecksJSON, got, tc.want)
			}
		})
	}
}

func TestFormatPerJobRolloutStatus(t *testing.T) {
	t.Parallel()
	allJobs := []string{"eri", "synapse", "haro"}
	startedJobs := []string{"synapse"}
	deployedJobs := []string{"eri"}
	jobStates := map[string]string{"synapse": "verifying_health"}
	passedBuildJobs := map[string]bool{"haro": true}

	got := formatPerJobRolloutStatus(allJobs, "deploying", startedJobs, deployedJobs, jobStates, passedBuildJobs)
	wantLines := []string{
		"`eri`: ✅ healthy",
		"`synapse`: 🔄 verifying health",
		"`haro`: 🔄 starting",
	}
	for _, line := range wantLines {
		if !strings.Contains(got, line) {
			t.Errorf("expected %q in output, got:\n%s", line, got)
		}
	}
}

func TestFormatKustomizationRolloutStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		status   string
		statuses map[string]string
		wantOK   bool
		wantSub  []string
	}{
		{
			name:     "not deploying",
			status:   "merged",
			statuses: map[string]string{"aerial-core": "deployed"},
			wantOK:   false,
		},
		{
			name:     "empty statuses",
			status:   "deploying",
			statuses: nil,
			wantOK:   false,
		},
		{
			name:   "deploying with kustomizations",
			status: "deploying",
			statuses: map[string]string{
				"aerial-core": "deployed",
				"marida-core": "progressing",
			},
			wantOK: true,
			wantSub: []string{
				"`aerial-core`: ✅ rolled out",
				"`marida-core`: 🔄 rolling out",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := formatKustomizationRolloutStatus(tc.status, tc.statuses)
			if ok != tc.wantOK {
				t.Fatalf("formatKustomizationRolloutStatus() ok = %v, want %v", ok, tc.wantOK)
			}
			for _, sub := range tc.wantSub {
				if !strings.Contains(got, sub) {
					t.Errorf("expected %q in output, got:\n%s", sub, got)
				}
			}
		})
	}
}

func TestFormatChunkActiveStep(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		chunk VanadisChunk
		want  string
	}{
		{
			name: "pr_opened with checks",
			chunk: VanadisChunk{
				Status:   "pr_opened",
				CIChecks: map[string]string{"lint": "passed", "test": "pending"},
			},
			want: "Waiting on CI (1/2 passed)",
		},
		{
			name: "pr_opened without checks",
			chunk: VanadisChunk{
				Status: "pr_opened",
			},
			want: "",
		},
		{
			name: "deploying with kustomization rollout",
			chunk: VanadisChunk{
				Status: "deploying",
				RolloutState: &pubsub.PRRolloutState{
					KustomizationStatuses: map[string]string{
						"aerial-core": "deployed",
					},
				},
			},
			want: "`aerial-core`: ✅ rolled out",
		},
		{
			name: "merged with container image counts",
			chunk: VanadisChunk{
				Status: "merged",
				CIChecks: map[string]string{
					"check1": "passed",
					"check2": "passed",
				},
			},
			want: "Building container images (2/2 completed)",
		},
		{
			name: "merged with build check service matches",
			chunk: VanadisChunk{
				Status: "merged",
				CIChecks: map[string]string{
					"Build & Push Images (eri)": "passed",
				},
			},
			want: "`eri`: 🔄 starting",
		},
		{
			name: "failed checks returns empty",
			chunk: VanadisChunk{
				Status: "merged",
				CIChecks: map[string]string{
					"lint": "failed",
				},
			},
			want: "",
		},
		{
			name: "other status returns empty",
			chunk: VanadisChunk{
				Status: "pending",
			},
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatChunkActiveStep(tc.chunk)
			if got != tc.want {
				t.Fatalf("FormatChunkActiveStep() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResolveCandidateJobs(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"default"}, resolveCandidateJobs(nil, []string{"default"}))

	rsEmpty := &pubsub.PRRolloutState{}
	assert.Equal(t, []string{"fallback"}, resolveCandidateJobs(rsEmpty, []string{"fallback"}))

	rs := &pubsub.PRRolloutState{
		ExpectedJobs: []string{"synapse", "eri"},
		StartedJobs:  []string{"eri"},
		DeployedJobs: []string{"haro"},
	}
	got := resolveCandidateJobs(rs, []string{"fallback"})
	assert.Equal(t, []string{"eri", "haro", "synapse"}, got)
}

func TestFormatSingleJobRolloutState_Extended(t *testing.T) {
	t.Parallel()

	// started without verifying_health
	got1 := formatSingleJobRolloutState("eri", "deploying", nil, map[string]bool{"eri": true}, map[string]string{"eri": "running"}, nil, true)
	assert.Contains(t, got1, "`eri`: 🔄 starting")

	// !hasStartedJobs && cleanStatus == "deploying"
	got2 := formatSingleJobRolloutState("eri", "deploying", nil, nil, nil, nil, false)
	assert.Contains(t, got2, "`eri`: 🔄 starting")

	// default building image
	got3 := formatSingleJobRolloutState("eri", "merged", nil, nil, nil, nil, false)
	assert.Contains(t, got3, "`eri`: ⏳ building image")
}

func TestDedupeTrimmedStrings_EmptyItems(t *testing.T) {
	t.Parallel()
	in := []string{"", "  ", "a", "A", "b"}
	out := dedupeTrimmedStrings(in)
	assert.Equal(t, []string{"a", "b"}, out)
}

func TestAnalyzeChunkBuildChecks_InProgress(t *testing.T) {
	t.Parallel()
	checks := map[string]string{
		"Build & Push Container Images (eri)": "in_progress",
	}
	jobs, passedJobs, total, passed, failed := analyzeChunkBuildChecks(checks)
	assert.Equal(t, []string{"eri"}, jobs)
	assert.False(t, passedJobs["eri"])
	assert.Equal(t, 1, total)
	assert.Equal(t, 0, passed)
	assert.Equal(t, 0, failed)
}

func TestFormatChunkActiveStep_RolloutState_And_Fallbacks(t *testing.T) {
	t.Parallel()

	// chunk with RolloutState containing jobs
	c1 := VanadisChunk{
		Status: "deploying",
		RolloutState: &pubsub.PRRolloutState{
			ExpectedJobs: []string{"eri"},
			StartedJobs:  []string{"eri"},
			DeployedJobs: []string{"eri"},
			JobStates:    map[string]string{"eri": "healthy"},
		},
	}
	step1 := FormatChunkActiveStep(c1)
	assert.Contains(t, step1, "`eri`: ✅ healthy")

	// merged with empty checks returns ""
	c2 := VanadisChunk{Status: "merged"}
	assert.Empty(t, FormatChunkActiveStep(c2))

	// merged with failed checks returns ""
	c3 := VanadisChunk{
		Status:   "merged",
		CIChecks: map[string]string{"test": "failed"},
	}
	assert.Empty(t, FormatChunkActiveStep(c3))
}

func TestBuildSyncWidgetRequest_SkipSink(t *testing.T) {
	t.Parallel()
	reqQuiet := buildSyncWidgetRequest(VanadisBuild{Quiet: true}, nil, SyncChunkPRWidgetRequest{})
	assert.True(t, reqQuiet.SkipSink)
}

func TestFormatDeployTargetLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		target      string
		kind        string
		deployUIURL string
		want        string
	}{
		{
			name:        "empty base url preserves code block",
			target:      "core-services",
			kind:        "kustomization",
			deployUIURL: "",
			want:        "`core-services`",
		},
		{
			name:        "whitespace base url preserves code block",
			target:      "core-services",
			kind:        "kustomization",
			deployUIURL: "   ",
			want:        "`core-services`",
		},
		{
			name:        "kustomization with trailing slash in base url",
			target:      "core-services",
			kind:        "kustomization",
			deployUIURL: "https://flux.lan.zylman.com/",
			want:        "[`core-services`](https://flux.lan.zylman.com/c/main/flux/kustomize/kustomizations/flux-system/core-services)",
		},
		{
			name:        "standard job deep link",
			target:      "eri",
			kind:        "job",
			deployUIURL: "https://flux.lan.zylman.com",
			want:        "[`eri`](https://flux.lan.zylman.com/c/main/deployments/default/eri)",
		},
		{
			name:        "special git-sync route",
			target:      "git-sync",
			kind:        "job",
			deployUIURL: "https://flux.lan.zylman.com",
			want:        "[`git-sync`](https://flux.lan.zylman.com/c/main/flux/sources)",
		},
		{
			name:        "special github-runner route",
			target:      "github-runner",
			kind:        "job",
			deployUIURL: "https://flux.lan.zylman.com",
			want:        "[`github-runner`](https://flux.lan.zylman.com/c/main/flux/helmreleases)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatDeployTargetLabel(tc.target, tc.kind, tc.deployUIURL)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestVanadisServer_DeployUIURL(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "vanadis.yaml")
	_ = os.WriteFile(cfgPath, []byte("deploy_ui_url: \"https://flux-file.lan.zylman.com/\"\n"), 0o600)

	var nilSrv *VanadisServer
	assert.Empty(t, nilSrv.deployUIURL())

	srvConfigured := &VanadisServer{
		cfg: Config{DeployUIURL: "https://flux.lan.zylman.com/"},
	}
	assert.Equal(t, "https://flux.lan.zylman.com", srvConfigured.deployUIURL())

	srvFileFallback := &VanadisServer{
		cfg: Config{ConfigPath: cfgPath},
	}
	assert.Equal(t, "https://flux-file.lan.zylman.com", srvFileFallback.deployUIURL())

	srvEmpty := &VanadisServer{
		cfg: Config{},
	}
	assert.Empty(t, srvEmpty.deployUIURL())
}

func TestFormatRolloutStatus_WithDeployUIURL(t *testing.T) {
	t.Parallel()

	baseURL := "https://flux.lan.zylman.com"

	// Kustomization status with URL
	kustStatus := map[string]string{
		"aerial-core": "deployed",
		"sidecars":    "progressing",
	}
	gotKust, ok := formatKustomizationRolloutStatus("deploying", kustStatus, baseURL)
	assert.True(t, ok)
	assert.Contains(t, gotKust, "[`aerial-core`](https://flux.lan.zylman.com/c/main/flux/kustomize/kustomizations/flux-system/aerial-core): ✅ rolled out")
	assert.Contains(t, gotKust, "[`sidecars`](https://flux.lan.zylman.com/c/main/flux/kustomize/kustomizations/flux-system/sidecars): 🔄 rolling out")

	// Single job rollout with URL
	gotJob := formatSingleJobRolloutState("git-sync", "deploying", nil, map[string]bool{"git-sync": true}, nil, nil, true, baseURL)
	assert.Equal(t, "[`git-sync`](https://flux.lan.zylman.com/c/main/flux/sources): 🔄 starting", gotJob)

	gotRunner := formatSingleJobRolloutState("github-runner", "merged", nil, nil, nil, nil, false, baseURL)
	assert.Equal(t, "[`github-runner`](https://flux.lan.zylman.com/c/main/flux/helmreleases): ⏳ building image", gotRunner)

	gotDeploy := formatSingleJobRolloutState("eri", "deploying", map[string]bool{"eri": true}, nil, nil, nil, true, baseURL)
	assert.Equal(t, "[`eri`](https://flux.lan.zylman.com/c/main/deployments/default/eri): ✅ healthy", gotDeploy)

	// FormatChunkActiveStep with baseURL
	chunk := VanadisChunk{
		Status: "deploying",
		RolloutState: &pubsub.PRRolloutState{
			KustomizationStatuses: map[string]string{
				"aerial-core": "healthy",
			},
		},
	}
	gotStep := FormatChunkActiveStep(chunk, baseURL)
	assert.Equal(t, "[`aerial-core`](https://flux.lan.zylman.com/c/main/flux/kustomize/kustomizations/flux-system/aerial-core): ✅ rolled out", gotStep)

	// FormatChunkActiveStep backward compatibility (empty deployUIURL)
	gotLegacyStep := FormatChunkActiveStep(chunk)
	assert.Equal(t, "`aerial-core`: ✅ rolled out", gotLegacyStep)
}
