package main

import (
	"maps"
	"slices"
	"testing"
	"time"
)

// mkK8sCont builds a container exactly as k8s mode synthesizes it from a Deployment.
func mkK8sCont(name, image, status string, uptimeSec int64) DockerContainerJSON {
	return synthesizeContainersFromServices([]ServiceStatus{{Name: name, Status: status, UptimeSeconds: uptimeSec, Commit: imageCommitSHA(image), Image: image}}, testRefTime)[0]
}

func TestImageCommitSHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{name: "full sha tag", image: "ghcr.io/azylman/aerial-synapse:de22ef4a238e56b6f9fb67cb7eb676f39d6596c3", want: "de22ef4a238e56b6f9fb67cb7eb676f39d6596c3"},
		{name: "uppercase sha tag lowercased", image: "ghcr.io/azylman/aerial-eri:DE22EF4A238E56B6F9FB67CB7EB676F39D6596C3", want: "de22ef4a238e56b6f9fb67cb7eb676f39d6596c3"},
		{name: "registry with port", image: "host:5000/img:de22ef4a238e56b6f9fb67cb7eb676f39d6596c3", want: "de22ef4a238e56b6f9fb67cb7eb676f39d6596c3"},
		{name: "registry with port and no tag", image: "host:5000/img", want: ""},
		{name: "digest", image: "ghcr.io/azylman/aerial-eri@sha256:de22ef4a238e56b6f9fb67cb7eb676f39d6596c3de22ef4a238e56b6", want: ""},
		{name: "latest", image: "ghcr.io/azylman/aerial-eri:latest", want: ""},
		{name: "semver", image: "grafana/grafana:11.3.0", want: ""},
		{name: "date tag", image: "ghcr.io/azylman/aerial-eri:20261120", want: ""},
		{name: "short hex", image: "ghcr.io/azylman/aerial-eri:de22ef4", want: ""},
		{name: "41 hex chars", image: "ghcr.io/azylman/aerial-eri:de22ef4a238e56b6f9fb67cb7eb676f39d6596c3a", want: ""},
		{name: "40 non-hex chars", image: "ghcr.io/azylman/aerial-eri:zz22ef4a238e56b6f9fb67cb7eb676f39d6596c3", want: ""},
		{name: "empty", image: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := imageCommitSHA(tc.image); got != tc.want {
				t.Errorf("imageCommitSHA(%q) = %q, want %q", tc.image, got, tc.want)
			}
		})
	}
}

func TestImageName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{name: "ghcr with sha tag", image: "ghcr.io/azylman/aerial-synapse:de22ef4a238e56b6f9fb67cb7eb676f39d6596c3", want: "aerial-synapse"},
		{name: "registry with port", image: "host:5000/team/img:v1", want: "img"},
		{name: "digest", image: "ghcr.io/azylman/aerial-eri@sha256:abcd", want: "aerial-eri"},
		{name: "bare name", image: "nginx", want: "nginx"},
		{name: "bare name with tag", image: "nginx:1.27", want: "nginx"},
		{name: "empty", image: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := imageName(tc.image); got != tc.want {
				t.Errorf("imageName(%q) = %q, want %q", tc.image, got, tc.want)
			}
		})
	}
}

func TestBuiltImages(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		jobs []GitHubJob
		want []string
	}{
		{name: "no jobs", jobs: nil, want: nil},
		{name: "successful builds sorted", jobs: []GitHubJob{mkJob(1, 1, "Build & Push Images to GHCR (synapse)", "completed", "success", 0, 0), mkJob(2, 1, "Build & Push Images to GHCR (eri-restate)", "completed", "success", 0, 0)}, want: []string{"aerial-eri-restate", "aerial-synapse"}},
		{name: "failed and skipped builds excluded", jobs: []GitHubJob{mkJob(1, 1, "Build & Push Images to GHCR (eri)", "completed", "failure", 0, 0), mkJob(2, 1, "Build & Push Images to GHCR (docs)", "completed", "skipped", 0, 0)}, want: nil},
		{name: "legacy tuple job name uses first field", jobs: []GitHubJob{mkJob(1, 1, "Build & Push Images to GHCR (brain, ., ./brain/Dockerfile, aerial-brain)", "completed", "success", 0, 0)}, want: []string{"aerial-brain"}},
		{name: "duplicate service deduped", jobs: []GitHubJob{mkJob(1, 1, "Build & Push Images to GHCR (eri)", "completed", "success", 0, 0), mkJob(2, 1, "Build & Push Images to GHCR (eri)", "completed", "success", 0, 0)}, want: []string{"aerial-eri"}},
		{name: "non build jobs and sidecar builds ignored", jobs: []GitHubJob{mkJob(1, 1, "Run Service Unit Tests", "completed", "success", 0, 0), mkJob(2, 1, "Build & Push Sidecar Images to GHCR (aura)", "completed", "success", 0, 0), mkJob(3, 1, "Build & Push Images to GHCR ()", "completed", "success", 0, 0)}, want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := builtImages(tc.jobs); !slices.Equal(got, tc.want) {
				t.Errorf("builtImages() = %v, want %v", got, tc.want)
			}
		})
	}
}

func mergeClusterBatch5Cases() []mergeClusterTestCase {
	const (
		shaNew   = "de22ef4a238e56b6f9fb67cb7eb676f39d6596c3"
		shaOld   = "1111222233334444555566667777888899990000"
		shaNext  = "abcdef0123456789abcdef0123456789abcdef01"
		synNew   = "ghcr.io/azylman/aerial-synapse:" + shaNew
		synOld   = "ghcr.io/azylman/aerial-synapse:" + shaOld
		synNext  = "ghcr.io/azylman/aerial-synapse:" + shaNext
		eriOld   = "ghcr.io/azylman/aerial-eri:" + shaOld
		buildSyn = "Build & Push Images to GHCR (synapse)"
		buildEri = "Build & Push Images to GHCR (eri)"
	)
	run := mkRun(2001, "azylman/aerial", shaNew, "completed", "success", "", -10*time.Minute, -5*time.Minute)
	nextRun := mkRun(2002, "azylman/aerial", shaNext, "completed", "success", "", -4*time.Minute, -2*time.Minute)
	synJobs := map[int64][]GitHubJob{2001: {mkJob(1, 2001, "Run Service Unit Tests", "completed", "success", 0, 0), mkJob(2, 2001, buildSyn, "completed", "success", 0, 0)}}
	eriSynJobs := map[int64][]GitHubJob{2001: {mkJob(1, 2001, buildEri, "completed", "success", 0, 0), mkJob(2, 2001, buildSyn, "completed", "success", 0, 0)}}
	restateJobs := map[int64][]GitHubJob{2001: {mkJob(1, 2001, "Build & Push Images to GHCR (eri-restate)", "completed", "success", 0, 0), mkJob(2, 2001, buildSyn, "completed", "success", 0, 0)}}
	docsJobs := map[int64][]GitHubJob{2001: {mkJob(1, 2001, "Run Service Unit Tests", "completed", "success", 0, 0), mkJob(2, 2001, buildSyn, "completed", "skipped", 0, 0)}}
	bothJobs := map[int64][]GitHubJob{2001: {mkJob(1, 2001, buildSyn, "completed", "success", 0, 0)}, 2002: {mkJob(2, 2002, buildSyn, "completed", "success", 0, 0)}}
	console := mkK8sCont("console", "ghcr.io/azylman/aerial-console:"+shaOld, "healthy", 7200)
	grafana := mkK8sCont("grafana", "grafana/grafana:11.3.0", "unhealthy", 7200)
	liveSteps := []string{"completed", "completed", "completed", "completed", "completed"}

	return []mergeClusterTestCase{
		// 67. Synapse-only build fully rolled to both stacks long ago is live
		{name: "K8sRollout_SynapseBothStacksOnSHA_Live", runs: mkRuns(run), jobs: synJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "healthy", 900), mkK8sCont("synapse-agent-alpha", synNew, "healthy", 900), console, grafana}, currentCommit: "0000000", wantList: []wantDep{{Commit: "de22ef4", Stage: "live", Progress: 100, StepStatuses: liveSteps}}},

		// 68. Sibling stack still on the old SHA keeps the card swapping
		{name: "K8sRollout_SiblingOnOldSHA_Swapping", runs: mkRuns(run), jobs: synJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "healthy", 900), mkK8sCont("synapse-agent-alpha", synOld, "healthy", 7200), console}, currentCommit: "0000000", wantList: []wantDep{{Stage: "swapping", Progress: 72, StepStatuses: []string{"completed", "completed", "completed", "active", "active"}, HasContainerChip: true}}},

		// 69. eri and synapse built, only synapse rolled: not live
		{name: "K8sRollout_PartialServiceRollout_Swapping", runs: mkRuns(run), jobs: eriSynJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "healthy", 900), mkK8sCont("eri", eriOld, "healthy", 7200)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "swapping"}}},

		// 70. Built image with no Deployment (RestateDeployment) is ignored
		{name: "K8sRollout_BuiltImageWithoutDeploymentIgnored_Live", runs: mkRuns(run), jobs: restateJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "healthy", 60)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "live", Progress: 100}}},

		// 71. Docs-only run is live on CI success
		{name: "K8sRollout_DocsOnlyRun_Live", runs: mkRuns(run), jobs: docsJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synOld, "healthy", 7200)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "live", Progress: 100}}},

		// 72. Jobs missing after a console restart: healthy group on the SHA is live regardless of uptime
		{name: "K8sRollout_JobsMissingHealthyGroup_Live", runs: mkRuns(run), containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "healthy", 900), console, grafana}, currentCommit: "0000000", wantList: []wantDep{{Stage: "live", Progress: 100}}},

		// 73. No container on the SHA after 5 min waits in awaiting_pull
		{name: "K8sRollout_NotRolledAfter5Min_AwaitingPull", runs: mkRuns(run), jobs: synJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synOld, "healthy", 7200)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "awaiting_pull", StepStatusMap: map[int]string{2: "active"}}}},

		// 74. Jobs missing and nothing on the SHA also waits in awaiting_pull
		{name: "K8sRollout_JobsMissingNotRolled_AwaitingPull", runs: mkRuns(run), containers: []DockerContainerJSON{mkK8sCont("synapse", synOld, "healthy", 7200)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "awaiting_pull"}}},

		// 75. Deployment on the SHA still starting is swapping
		{name: "K8sRollout_StartingOnSHA_Swapping", runs: mkRuns(run), jobs: synJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "starting", 20)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "swapping", Progress: 60}}},

		// 76. Unhealthy Deployment running a built image degrades the card
		{name: "K8sRollout_UnhealthyOnSHA_Degraded", runs: mkRuns(run), jobs: synJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "unhealthy", 300), mkK8sCont("synapse-agent-alpha", synNew, "healthy", 300)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "degraded", Progress: 85, StepStatusMap: map[int]string{3: "completed", 4: "failed"}}}},

		// 77. Containers already on a newer main run's SHA count as rolled for the older card
		{name: "K8sRollout_NewerRunSHACountsAsRolled_BothLive", runs: mkRuns(run, nextRun), jobs: bothJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNext, "healthy", 60)}, currentCommit: "0000000", wantList: []wantDep{{Commit: "abcdef0", Stage: "live"}, {Commit: "de22ef4", Stage: "live"}}},
	}
}

func TestSynthesizeContainersFromServices(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	const sha = "de22ef4a238e56b6f9fb67cb7eb676f39d6596c3"
	tests := []struct {
		name        string
		svc         ServiceStatus
		wantState   string
		wantCreated int64
		wantLabels  map[string]string
	}{
		{name: "k8s service with sha image stamps commit and image", svc: ServiceStatus{Name: "synapse-agent-alpha", Status: "healthy", UptimeSeconds: 900, Commit: sha, Image: "ghcr.io/azylman/aerial-synapse:" + sha}, wantState: "running", wantCreated: now.Unix() - 900, wantLabels: map[string]string{"aerial.service": "synapse-agent-alpha", "aerial.commit_sha": sha, "aerial.image": "aerial-synapse"}},
		{name: "non-sha image stamps image only", svc: ServiceStatus{Name: "grafana", Status: "starting", UptimeSeconds: 5, Image: "grafana/grafana:11.3.0"}, wantState: "running", wantCreated: now.Unix() - 5, wantLabels: map[string]string{"aerial.service": "grafana", "aerial.image": "grafana"}},
		{name: "unhealthy service without image is exited and unstamped", svc: ServiceStatus{Name: "eri", Status: "unhealthy"}, wantState: "exited", wantCreated: now.Unix(), wantLabels: map[string]string{"aerial.service": "eri"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := synthesizeContainersFromServices([]ServiceStatus{tc.svc}, now)
			if len(got) != 1 {
				t.Fatalf("expected 1 container, got %d", len(got))
			}
			c := got[0]
			if c.State != tc.wantState || c.Created != tc.wantCreated {
				t.Errorf("state/created = %q/%d, want %q/%d", c.State, c.Created, tc.wantState, tc.wantCreated)
			}
			if !maps.Equal(c.Labels, tc.wantLabels) {
				t.Errorf("labels = %v, want %v", c.Labels, tc.wantLabels)
			}
		})
	}
}
