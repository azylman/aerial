package flux

import (
	"slices"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

type translateFluxAlertCase struct {
	name       string
	alert      model.FluxAlertEvent
	wantOK     bool
	wantEvent  string
	wantStatus string
	wantJob    string
	wantRepo   string
	wantSHA    string
	wantDetail string
	wantKind   string
}

func translateFluxAlertGitRepoCases(now time.Time) []translateFluxAlertCase {
	return []translateFluxAlertCase{
		{
			name: "GitRepository NewArtifact maps to sync_success",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "GitRepository", Name: "aerial-config", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "NewArtifact",
				Message:        "stored new artifact for commit 'main@sha1:1234567890abcdef'",
				Metadata:       map[string]string{"revision": "main@sha1:1234567890abcdef"},
			},
			wantOK:     true,
			wantKind:   "GitRepository",
			wantEvent:  "sync_success",
			wantStatus: "success",
			wantJob:    "git-sync",
			wantRepo:   "aerial",
			wantSHA:    "1234567890abcdef",
			wantDetail: "stored new artifact for commit 'main@sha1:1234567890abcdef'",
		},
		{
			name: "GitRepository GitOperationSucceeded maps to sync_success",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "GitOperationSucceeded",
				Message:        "sync succeeded",
				Metadata:       map[string]string{"revision": "sha1:abcdef1234567890"},
			},
			wantOK:     true,
			wantKind:   "GitRepository",
			wantEvent:  "sync_success",
			wantStatus: "success",
			wantJob:    "git-sync",
			wantRepo:   "aerial",
			wantSHA:    "abcdef1234567890",
			wantDetail: "sync succeeded",
		},
		{
			name: "GitRepository GitOperationFailed maps to sync_failed",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"},
				Severity:       "error",
				Timestamp:      now,
				Reason:         "GitOperationFailed",
				Message:        "authentication failed",
			},
			wantOK:     true,
			wantKind:   "GitRepository",
			wantEvent:  "sync_failed",
			wantStatus: "failed",
			wantJob:    "git-sync",
			wantRepo:   "aerial",
			wantDetail: "authentication failed",
		},
		{
			name: "GitRepository severity error without reason maps to sync_failed",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"},
				Severity:       "error",
				Timestamp:      now,
				Reason:         "UnknownFailure",
				Message:        "clone timeout",
			},
			wantOK:     true,
			wantKind:   "GitRepository",
			wantEvent:  "sync_failed",
			wantStatus: "failed",
			wantJob:    "git-sync",
			wantRepo:   "aerial",
			wantDetail: "clone timeout",
		},
		{
			name: "GitRepository unknown reason ignored",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "Progressing",
			},
			wantOK: false,
		},
	}
}

func translateFluxAlertKustomizationSuccessCases(now time.Time) []translateFluxAlertCase {
	return []translateFluxAlertCase{
		{
			name: "Kustomization Progressing maps to deploy_started",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "Progressing",
				Message:        "reconciling",
				Metadata:       map[string]string{"revision": "main@sha1:1111222233334444"},
			},
			wantOK:     true,
			wantKind:   "Kustomization",
			wantEvent:  "deploy_started",
			wantStatus: "started",
			wantJob:    "synapse",
			wantRepo:   "aerial",
			wantSHA:    "1111222233334444",
			wantDetail: "reconciling",
		},
		{
			name: "Kustomization ReconciliationSucceeded maps to deploy_success",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "ReconciliationSucceeded",
				Message:        "applied revision",
				Metadata:       map[string]string{"revision": "main@sha1:5555666677778888"},
			},
			wantOK:     true,
			wantKind:   "Kustomization",
			wantEvent:  "deploy_success",
			wantStatus: "success",
			wantJob:    "synapse",
			wantRepo:   "aerial",
			wantSHA:    "5555666677778888",
			wantDetail: "applied revision",
		},
	}
}

func translateFluxAlertKustomizationFailureCases(now time.Time) []translateFluxAlertCase {
	return []translateFluxAlertCase{
		{
			name: "Kustomization ReconciliationFailed maps to deploy_failed",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
				Severity:       "error",
				Timestamp:      now,
				Reason:         "ReconciliationFailed",
				Message:        "dry-run failed",
			},
			wantOK:     true,
			wantKind:   "Kustomization",
			wantEvent:  "deploy_failed",
			wantStatus: "failed",
			wantJob:    "synapse",
			wantRepo:   "aerial",
			wantDetail: "dry-run failed",
		},
		{
			name: "Kustomization HealthCheckFailed maps to deploy_failed",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "HealthCheckFailed",
				Message:        "readiness probe timed out",
			},
			wantOK:     true,
			wantKind:   "Kustomization",
			wantEvent:  "deploy_failed",
			wantStatus: "failed",
			wantJob:    "synapse",
			wantRepo:   "aerial",
			wantDetail: "readiness probe timed out",
		},
		{
			name: "Kustomization BuildFailed maps to deploy_failed",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "BuildFailed",
				Message:        "kustomize build error",
			},
			wantOK:     true,
			wantKind:   "Kustomization",
			wantEvent:  "deploy_failed",
			wantStatus: "failed",
			wantJob:    "synapse",
			wantRepo:   "aerial",
			wantDetail: "kustomize build error",
		},
		{
			name: "Kustomization ArtifactFailed maps to deploy_failed",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "ArtifactFailed",
				Message:        "artifact not found",
			},
			wantOK:     true,
			wantKind:   "Kustomization",
			wantEvent:  "deploy_failed",
			wantStatus: "failed",
			wantJob:    "synapse",
			wantRepo:   "aerial",
			wantDetail: "artifact not found",
		},
		{
			name: "Kustomization ValidationFailed maps to deploy_failed",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "ValidationFailed",
				Message:        "schema validation error",
			},
			wantOK:     true,
			wantKind:   "Kustomization",
			wantEvent:  "deploy_failed",
			wantStatus: "failed",
			wantJob:    "synapse",
			wantRepo:   "aerial",
			wantDetail: "schema validation error",
		},
		{
			name: "Kustomization severity error without failure reason maps to deploy_failed",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
				Severity:       "error",
				Timestamp:      now,
				Reason:         "CustomErrorReason",
				Message:        "unspecified error",
			},
			wantOK:     true,
			wantKind:   "Kustomization",
			wantEvent:  "deploy_failed",
			wantStatus: "failed",
			wantJob:    "synapse",
			wantRepo:   "aerial",
			wantDetail: "unspecified error",
		},
		{
			name: "Kustomization unknown reason ignored",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "UnknownReason",
			},
			wantOK: false,
		},
		{
			name: "Unsupported Kind ignored",
			alert: model.FluxAlertEvent{
				InvolvedObject: model.FluxInvolvedObject{Kind: "Receiver", Name: "generic", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "NewArtifact",
			},
			wantOK: false,
		},
	}
}

func translateFluxAlertKustomizationCases(now time.Time) []translateFluxAlertCase {
	return append(translateFluxAlertKustomizationSuccessCases(now), translateFluxAlertKustomizationFailureCases(now)...)
}

func assertTranslatedDeployEvent(t *testing.T, tc translateFluxAlertCase, got model.DeployEvent) {
	t.Helper()
	if got.Event != tc.wantEvent {
		t.Errorf("Event = %q, want %q", got.Event, tc.wantEvent)
	}
	if got.Status != tc.wantStatus {
		t.Errorf("Status = %q, want %q", got.Status, tc.wantStatus)
	}
	if got.JobName != tc.wantJob {
		t.Errorf("JobName = %q, want %q", got.JobName, tc.wantJob)
	}
	if got.Repo != tc.wantRepo {
		t.Errorf("Repo = %q, want %q", got.Repo, tc.wantRepo)
	}
	if got.CommitSHA != tc.wantSHA {
		t.Errorf("CommitSHA = %q, want %q", got.CommitSHA, tc.wantSHA)
	}
	if got.Details != tc.wantDetail {
		t.Errorf("Details = %q, want %q", got.Details, tc.wantDetail)
	}
	if got.SourceKind != tc.wantKind {
		t.Errorf("SourceKind = %q, want %q", got.SourceKind, tc.wantKind)
	}
	if got.Timestamp.IsZero() {
		t.Errorf("Timestamp is zero, expected set timestamp")
	}
}

func TestTranslateFluxAlertToDeployEvent(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 6, 18, 0, 0, 0, time.UTC)
	tests := append(translateFluxAlertGitRepoCases(now), translateFluxAlertKustomizationCases(now)...)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := TranslateFluxAlertToDeployEvent(tc.alert)
			if ok != tc.wantOK {
				t.Fatalf("TranslateFluxAlertToDeployEvent() ok = %v, wantOK = %v", ok, tc.wantOK)
			}
			if tc.wantOK {
				assertTranslatedDeployEvent(t, tc, got)
			}
		})
	}

	// Zero timestamp fallback
	zeroAlert := model.FluxAlertEvent{
		InvolvedObject: model.FluxInvolvedObject{Kind: "GitRepository", Name: "aerial"},
		Reason:         "NewArtifact",
	}
	evt, ok := TranslateFluxAlertToDeployEvent(zeroAlert)
	if !ok || evt.Timestamp.IsZero() {
		t.Errorf("expected ok=true and non-zero timestamp for zero-time alert")
	}
}

func TestAlertPayloadRepoAndResolveFluxRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		alert    model.FluxAlertEvent
		wantP    string
		wantRepo string
	}{
		{
			name:     "explicit repo field",
			alert:    model.FluxAlertEvent{Repo: "acme/custom"},
			wantP:    "acme/custom",
			wantRepo: "acme/custom",
		},
		{
			name:     "metadata repo field",
			alert:    model.FluxAlertEvent{Metadata: map[string]string{"repo": "acme/meta"}},
			wantP:    "acme/meta",
			wantRepo: "acme/meta",
		},
		{
			name:     "empty repo falls back to the bare default repo",
			alert:    model.FluxAlertEvent{},
			wantP:    "",
			wantRepo: "aerial",
		},
		{
			name:     "bare payload repo passes through",
			alert:    model.FluxAlertEvent{Repo: " mirrormere "},
			wantP:    "mirrormere",
			wantRepo: "mirrormere",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotP := AlertPayloadRepo(tc.alert)
			if gotP != tc.wantP {
				t.Errorf("AlertPayloadRepo() = %q, want %q", gotP, tc.wantP)
			}
			gotRepo := ResolveFluxRepo(tc.alert)
			if gotRepo != tc.wantRepo {
				t.Errorf("ResolveFluxRepo() = %q, want %q", gotRepo, tc.wantRepo)
			}
		})
	}
}

func TestIsFluxFailureReason(t *testing.T) {
	t.Parallel()
	failures := []string{"ReconciliationFailed", "HealthCheckFailed", "BuildFailed", "ArtifactFailed", "ValidationFailed"}
	for _, reason := range failures {
		if !IsFluxFailureReason(reason) {
			t.Errorf("expected IsFluxFailureReason(%q) = true", reason)
		}
	}
	if IsFluxFailureReason("Progressing") || IsFluxFailureReason("ReconciliationSucceeded") || IsFluxFailureReason("") {
		t.Errorf("expected IsFluxFailureReason to return false for non-failures")
	}
}

func TestImageTag(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{name: "plain tag", image: "ghcr.io/example/aerial-synapse:abc123", want: "abc123"},
		{name: "registry with port and tag", image: "registry.lan:5000/aerial/haro:def456", want: "def456"},
		{name: "registry with port untagged", image: "registry.lan:5000/aerial/haro"},
		{name: "tag with digest", image: "ghcr.io/example/aerial-synapse:abc123@sha256:beef", want: "abc123"},
		{name: "digest only", image: "ghcr.io/example/aerial-synapse@sha256:beef"},
		{name: "untagged", image: "busybox"},
		{name: "bare name with tag", image: "postgres:18", want: "18"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ImageTag(tc.image); got != tc.want {
				t.Errorf("ImageTag(%q) = %q, want %q", tc.image, got, tc.want)
			}
		})
	}
}

func TestLinkManagedWorkloads(t *testing.T) {
	t.Parallel()
	mixed := []model.WorkloadRollout{
		{Kind: "Deployment", Name: "dep"},
		{Kind: "RestateCluster", Name: "restate-cluster", Namespace: "ns1"},
		{Kind: "StatefulSet", Name: "restate", Namespace: "ns1"},
		{Kind: "StatefulSet", Name: "restate-cluster", Namespace: "ns1"},
		{Kind: "StatefulSet", Name: "other", Namespace: "ns1"},
	}
	tests := []struct {
		name        string
		workloads   []model.WorkloadRollout
		calls       int
		cluster     int
		wantManaged []string
	}{
		{name: "cluster links its own and restate statefulsets", workloads: mixed, calls: 1, cluster: 1, wantManaged: []string{"restate", "restate-cluster"}},
		{name: "relinking is idempotent", calls: 2, wantManaged: []string{"restate"}, workloads: []model.WorkloadRollout{{Kind: "RestateCluster", Namespace: "default", Name: "restate"}, {Kind: "StatefulSet", Namespace: "default", Name: "restate"}}},
		{name: "unnamespaced cluster uses its name as namespace", calls: 1, wantManaged: []string{"restate"}, workloads: []model.WorkloadRollout{{Kind: "RestateCluster", Name: "restate"}, {Kind: "StatefulSet", Namespace: "restate", Name: "restate"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for range tc.calls {
				LinkManagedWorkloads(tc.workloads)
			}
			var got []string
			for _, m := range tc.workloads[tc.cluster].ManagedWorkloads {
				got = append(got, m.Name)
			}
			if !slices.Equal(got, tc.wantManaged) {
				t.Errorf("managed = %v, want %v", got, tc.wantManaged)
			}
		})
	}
}

func TestReloadsOn(t *testing.T) {
	t.Parallel()
	listed := model.WorkloadRollout{Annotations: map[string]string{reloaderConfigMapAnnotation: "cm1, cm2"}, ConfigMapRefs: []string{"cm1"}}
	auto := model.WorkloadRollout{Annotations: map[string]string{reloaderAutoAnnotation: "true"}, ConfigMapRefs: []string{"cm1"}}
	tests := []struct {
		name string
		w    model.WorkloadRollout
		cm   string
		want bool
	}{
		{"listed first", listed, "cm1", true},
		{"listed trimmed", listed, "cm2", true},
		{"not listed", listed, "cm3", false},
		{"auto referenced", auto, "cm1", true},
		{"auto unreferenced", auto, "cm2", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ReloadsOn(tc.w, tc.cm); got != tc.want {
				t.Errorf("ReloadsOn(%q) = %v, want %v", tc.cm, got, tc.want)
			}
		})
	}
}

func TestWorkloadRolloutComplete(t *testing.T) {
	t.Parallel()
	synapseImage := "ghcr.io/example/aerial-synapse:0123abcd"

	deployRollout := func(name, image string, gen, observed int64, replicas, updated, available, total int32, deadline bool) model.WorkloadRollout {
		return model.WorkloadRollout{Kind: "Deployment", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, UpdatedReplicas: updated, AvailableReplicas: available, StatusReplicas: total, ProgressDeadlineExceeded: deadline}
	}
	stsRollout := func(name, image string, gen, observed int64, replicas, available int32, updateRev, currentRev string) model.WorkloadRollout {
		return model.WorkloadRollout{Kind: "StatefulSet", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, AvailableReplicas: available, UpdateRevision: updateRev, CurrentRevision: currentRev}
	}
	dsRollout := func(name, image string, gen, observed int64, desired, updated, available int32) model.WorkloadRollout {
		return model.WorkloadRollout{Kind: "DaemonSet", Namespace: "kube-system", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, DesiredNumberScheduled: desired, UpdatedNumberScheduled: updated, NumberAvailable: available}
	}
	restateDeployRollout := func(name, image string, gen, observed int64, replicas, ready, available int32, deployID string, readyCond bool) model.WorkloadRollout {
		return model.WorkloadRollout{Kind: "RestateDeployment", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, ReadyReplicas: ready, AvailableReplicas: available, DeploymentID: deployID, ReadyConditionTrue: readyCond}
	}
	restateClusterRollout := func(name, image string, gen, observed int64, readyCond bool) model.WorkloadRollout {
		return model.WorkloadRollout{Kind: "RestateCluster", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: 1, ReadyConditionTrue: readyCond}
	}
	restateClusterRolloutWithManaged := func(name, image string, gen, observed int64, readyCond bool, managed ...model.WorkloadRollout) model.WorkloadRollout {
		return model.WorkloadRollout{Kind: "RestateCluster", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: 1, ReadyConditionTrue: readyCond, ManagedWorkloads: managed}
	}

	tests := []struct {
		name       string
		workload   model.WorkloadRollout
		wantDone   bool
		wantFailed bool
		wantReason string
	}{
		{name: "zero cluster child residual StatusReplicas", workload: model.WorkloadRollout{Kind: "RestateCluster", ManagedWorkloads: []model.WorkloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, StatusReplicas: 1}}}, wantReason: "managed StatefulSet default/restate: waiting for all replicas to stop"},
		{name: "zero cluster child residual ReadyReplicas", workload: model.WorkloadRollout{Kind: "RestateCluster", ManagedWorkloads: []model.WorkloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, ReadyReplicas: 1}}}, wantReason: "managed StatefulSet default/restate: waiting for all replicas to stop"},
		{name: "zero cluster child residual AvailableReplicas", workload: model.WorkloadRollout{Kind: "RestateCluster", ManagedWorkloads: []model.WorkloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, AvailableReplicas: 1}}}, wantReason: "managed StatefulSet default/restate: waiting for all replicas to stop"},
		{name: "zero cluster child residual UpdatedReplicas", workload: model.WorkloadRollout{Kind: "RestateCluster", ManagedWorkloads: []model.WorkloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, UpdatedReplicas: 1}}}, wantReason: "managed StatefulSet default/restate: waiting for all replicas to stop"},
		{name: "zero cluster stale child", workload: model.WorkloadRollout{Kind: "RestateCluster", ManagedWorkloads: []model.WorkloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 1}}}, wantReason: "managed StatefulSet default/restate: generation 2 not yet observed"},
		{name: "RestateDeployment stopped without child evidence", workload: model.WorkloadRollout{Kind: "RestateDeployment", Generation: 2, ObservedGeneration: 2}, wantReason: "waiting for stopped child evidence"},
		{name: "RestateDeployment stopped stale generation", workload: model.WorkloadRollout{Kind: "RestateDeployment", Generation: 2, ObservedGeneration: 1}, wantReason: "generation 2 not yet observed"},
		{name: "StatefulSet stopped with inactive status", workload: model.WorkloadRollout{Kind: "StatefulSet", Generation: 2, ObservedGeneration: 2, UpdateRevision: "new", CurrentRevision: "old"}, wantDone: true},
		{name: "StatefulSet stopped stale generation", workload: model.WorkloadRollout{Kind: "StatefulSet", Generation: 2, ObservedGeneration: 1}, wantReason: "generation 2 not yet observed"},
		{name: "StatefulSet stopped residual StatusReplicas", workload: model.WorkloadRollout{Kind: "StatefulSet", Generation: 2, ObservedGeneration: 2, StatusReplicas: 1}, wantReason: "waiting for all replicas to stop"},
		{name: "zero cluster verifies stopped child without Ready", workload: model.WorkloadRollout{Kind: "RestateCluster", ManagedWorkloads: []model.WorkloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, UpdateRevision: "new", CurrentRevision: "old"}}}, wantDone: true},
		{name: "zero cluster missing child", workload: model.WorkloadRollout{Kind: "RestateCluster"}, wantReason: "waiting for managed statefulset"},
		{name: "zero cluster child desired replicas mismatch", workload: model.WorkloadRollout{Kind: "RestateCluster", ManagedWorkloads: []model.WorkloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Replicas: 1}}}, wantReason: "managed StatefulSet default/restate replicas 1 do not match cluster replicas 0"},
		{name: "zero cluster Images mismatch", workload: model.WorkloadRollout{Kind: "RestateCluster", Images: []string{"new"}, ManagedWorkloads: []model.WorkloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, UpdateRevision: "new", CurrentRevision: "old"}}}, wantReason: "managed StatefulSet default/restate images [] do not match cluster image new"},
		{name: "zero cluster Env mismatch", workload: model.WorkloadRollout{Kind: "RestateCluster", Env: map[string]string{"A": "1"}, ManagedWorkloads: []model.WorkloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, UpdateRevision: "new", CurrentRevision: "old"}}}, wantReason: "managed StatefulSet default/restate env A=\"\" does not match cluster \"1\""},
		{name: "zero cluster Resources mismatch", workload: model.WorkloadRollout{Kind: "RestateCluster", Resources: model.KubeResourceRequirements{Limits: map[string]string{"cpu": "1"}}, ManagedWorkloads: []model.WorkloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, UpdateRevision: "new", CurrentRevision: "old"}}}, wantReason: "managed StatefulSet default/restate resource limit cpu=\"\" does not match cluster \"1\""},
		{name: "deployment rolled out", workload: deployRollout("synapse", synapseImage, 3, 3, 2, 2, 2, 2, false), wantDone: true},
		{name: "deployment with old replicas still present", workload: deployRollout("synapse", synapseImage, 3, 3, 1, 1, 1, 2, false), wantReason: "1/1 updated, 1/1 available, 2 total"},
		{name: "deployment new replica unavailable", workload: deployRollout("synapse", synapseImage, 3, 3, 1, 1, 0, 1, false), wantReason: "1/1 updated, 0/1 available, 1 total"},
		{name: "deployment with stale observed generation", workload: deployRollout("synapse", synapseImage, 4, 3, 1, 1, 1, 1, false), wantReason: "generation 4 not yet observed"},
		{name: "deployment progress deadline exceeded", workload: deployRollout("synapse", synapseImage, 3, 3, 1, 1, 0, 2, true), wantFailed: true, wantReason: "progress deadline exceeded"},
		{
			name: "deployment terminating with finalizers fails fast on stale observed generation",
			workload: func() model.WorkloadRollout {
				w := deployRollout("synapse", synapseImage, 4, 3, 1, 1, 1, 1, false)
				w.DeletionTimestamp = "2026-11-20T18:00:00Z"
				w.Finalizers = []string{"deployments.restate.dev"}
				return w
			}(),
			wantFailed: true,
			wantReason: "terminating since 2026-11-20T18:00:00Z (blocked by finalizers [deployments.restate.dev]; generation 4 not observed)",
		},
		{
			name: "deployment terminating without finalizers fails fast on stale observed generation",
			workload: func() model.WorkloadRollout {
				w := deployRollout("synapse", synapseImage, 4, 3, 1, 1, 1, 1, false)
				w.DeletionTimestamp = "2026-11-20T18:00:00Z"
				return w
			}(),
			wantFailed: true,
			wantReason: "terminating since 2026-11-20T18:00:00Z (generation 4 not observed)",
		},
		{name: "statefulset rolled out", workload: stsRollout("db", synapseImage, 2, 2, 2, 2, "db-2", "db-2"), wantDone: true},
		{name: "statefulset with old revision pods", workload: stsRollout("db", synapseImage, 2, 2, 2, 2, "db-2", "db-1"), wantReason: "revision db-1 of db-2, 2/2 available"},
		{name: "daemonset rolled out", workload: dsRollout("agent", synapseImage, 5, 5, 3, 3, 3), wantDone: true},
		{name: "daemonset unavailable", workload: dsRollout("agent", synapseImage, 5, 5, 3, 3, 2), wantReason: "3/3 updated, 2/3 available"},
		{name: "restatedeployment rolled out", workload: restateDeployRollout("restate-worker", synapseImage, 3, 3, 2, 2, 2, "dp_12345", true), wantDone: true},
		{name: "restatedeployment ready replicas unavailable", workload: restateDeployRollout("restate-worker", synapseImage, 3, 3, 2, 1, 2, "dp_12345", true), wantReason: "1/2 ready, 2/2 available, deploymentId=dp_12345, ready=true"},
		{name: "restatecluster ready condition false", workload: restateClusterRollout("restate", synapseImage, 2, 0, false), wantReason: "ready=false"},
		{name: "restatecluster without managed statefulset pending", workload: restateClusterRollout("restate", synapseImage, 2, 0, true), wantReason: "waiting for managed statefulset"},
		{
			name:       "restatecluster with managed statefulset matching spec rolled out",
			workload:   restateClusterRolloutWithManaged("restate", "docker.io/restatedev/restate:1.6", 2, 0, true, stsRollout("restate", "docker.io/restatedev/restate:1.6", 2, 2, 1, 1, "restate-1", "restate-1")),
			wantDone:   true,
			wantReason: "",
		},
		{name: "unsupported kind", workload: model.WorkloadRollout{Kind: "Job"}, wantReason: "unsupported workload kind"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			done, failed, reason := WorkloadRolloutComplete(tc.workload)
			if done != tc.wantDone || failed != tc.wantFailed || reason != tc.wantReason {
				t.Errorf("WorkloadRolloutComplete() = (%v, %v, %q), want (%v, %v, %q)", done, failed, reason, tc.wantDone, tc.wantFailed, tc.wantReason)
			}
		})
	}
}

func TestPRRolloutStatus(t *testing.T) {
	t.Parallel()
	rolloutClock := func(hh, mm, ss int) time.Time {
		return time.Date(2026, time.November, 8, hh, mm, ss, 0, time.UTC)
	}
	withChange := func(w model.WorkloadRollout, changedAt time.Time, annotations map[string]string, configMapRefs ...string) model.WorkloadRollout {
		w.ChangedAt, w.Annotations, w.ConfigMapRefs = changedAt, annotations, configMapRefs
		return w
	}
	deployRollout := func(name, image string, gen, observed int64, replicas, updated, available, total int32, deadline bool) model.WorkloadRollout {
		return model.WorkloadRollout{Kind: "Deployment", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, UpdatedReplicas: updated, AvailableReplicas: available, StatusReplicas: total, ProgressDeadlineExceeded: deadline}
	}
	stsRollout := func(name, image string, gen, observed int64, replicas, available int32, updateRev, currentRev string) model.WorkloadRollout {
		return model.WorkloadRollout{Kind: "StatefulSet", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, AvailableReplicas: available, UpdateRevision: updateRev, CurrentRevision: currentRev}
	}

	const otherImage = "ghcr.io/example/aerial-haro:feedface"
	const rolloutSHA = "0123abcd"
	const synapseImage = "ghcr.io/example/aerial-synapse:" + rolloutSHA
	since := rolloutClock(10, 0, 0)
	auto := map[string]string{"reloader.stakater.com/auto": "true"}
	eriDone := deployRollout("eri", otherImage, 2, 2, 1, 1, 1, 1, false)
	eriRolling := deployRollout("eri", otherImage, 2, 2, 1, 1, 0, 2, false)
	eriConfig := []model.ConfigMapChange{{Namespace: "default", Name: "eri-config", ChangedAt: rolloutClock(10, 1, 0)}}

	tests := []struct {
		name            string
		workloads       []model.WorkloadRollout
		configMaps      []model.ConfigMapChange
		commitSHA       string
		includedCommits []string
		expectedJobs    []string
		now             time.Time
		wantDone        bool
		wantFailed      bool
		wantReason      string
	}{
		{name: "no workloads is done", commitSHA: rolloutSHA, wantDone: true},
		{name: "image match pending", workloads: []model.WorkloadRollout{deployRollout("synapse", synapseImage, 3, 3, 1, 1, 0, 2, false)}, commitSHA: rolloutSHA, wantReason: "Deployment default/synapse (1/1 updated, 0/1 available, 2 total)"},
		{
			name: "terminating workload with new generation fails PR rollout status fast",
			workloads: []model.WorkloadRollout{func() model.WorkloadRollout {
				w := deployRollout("synapse", synapseImage, 4, 3, 1, 1, 1, 1, false)
				w.DeletionTimestamp = "2026-11-20T18:00:00Z"
				w.Finalizers = []string{"deployments.restate.dev"}
				return w
			}()},
			commitSHA:  rolloutSHA,
			wantFailed: true,
			wantReason: "Deployment default/synapse: terminating since 2026-11-20T18:00:00Z (blocked by finalizers [deployments.restate.dev]; generation 4 not observed)",
		},
		{name: "image match done", workloads: []model.WorkloadRollout{deployRollout("synapse", synapseImage, 3, 3, 1, 1, 1, 1, false), stsRollout("db", "ghcr.io/example/aerial-db:"+rolloutSHA+"@sha256:beef", 2, 2, 1, 1, "db-2", "db-2")}, commitSHA: rolloutSHA, wantDone: true},
		{name: "image match failed", workloads: []model.WorkloadRollout{deployRollout("synapse", synapseImage, 3, 3, 1, 1, 0, 2, true)}, commitSHA: rolloutSHA, wantFailed: true, wantReason: "Deployment default/synapse: progress deadline exceeded"},
		{name: "manifest-only change done", workloads: []model.WorkloadRollout{withChange(deployRollout("haro", otherImage, 3, 3, 1, 1, 1, 1, false), rolloutClock(10, 1, 0), nil)}, commitSHA: rolloutSHA, wantDone: true},
		{name: "reloader pending restart", workloads: []model.WorkloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), auto, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 3, 0), wantReason: "Deployment default/eri (awaiting Reloader restart for ConfigMap default/eri-config)"},
		{name: "reloader restarted and rolling out", workloads: []model.WorkloadRollout{withChange(eriRolling, rolloutClock(10, 1, 5), auto, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 3, 0), wantReason: "Deployment default/eri (1/1 updated, 0/1 available, 2 total)"},
		{name: "reloader restarted and rolled out", workloads: []model.WorkloadRollout{withChange(eriDone, rolloutClock(10, 1, 0), auto, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 30, 0), wantDone: true},
		{name: "reloader restart timeout fails", workloads: []model.WorkloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), auto, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 6, 1), wantFailed: true, wantReason: "Deployment default/eri: not restarted by Reloader within 5m0s of ConfigMap default/eri-config changing"},
		{name: "reloader reload annotation names configmap", workloads: []model.WorkloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), map[string]string{"configmap.reloader.stakater.com/reload": "eri-flags, eri-config"})}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 3, 0), wantReason: "Deployment default/eri (awaiting Reloader restart for ConfigMap default/eri-config)"},
		{name: "reloader configmap auto annotation gates", workloads: []model.WorkloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), map[string]string{"configmap.reloader.stakater.com/auto": "true"}, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 3, 0), wantReason: "Deployment default/eri (awaiting Reloader restart for ConfigMap default/eri-config)"},
		{
			name: "expected jobs ignores unready unrelated workload changed after since",
			workloads: []model.WorkloadRollout{
				deployRollout("synapse", synapseImage, 3, 3, 1, 1, 1, 1, false),
				withChange(deployRollout("haro", otherImage, 3, 3, 1, 1, 0, 2, false), rolloutClock(10, 1, 0), nil),
			},
			commitSHA:    rolloutSHA,
			expectedJobs: []string{"synapse"},
			wantDone:     true,
		},
		{
			name: "expected jobs awaits image tag when matching workload runs older ancestor tag",
			workloads: []model.WorkloadRollout{
				deployRollout("synapse", otherImage, 3, 3, 1, 1, 1, 1, false),
			},
			commitSHA:    rolloutSHA,
			expectedJobs: []string{"synapse"},
			wantReason:   "Deployment default/synapse (awaiting image tag 0123abcd)",
		},
		{
			name: "included commits completes when matching workload runs descendant commit tag",
			workloads: []model.WorkloadRollout{
				deployRollout("synapse", "ghcr.io/example/aerial-synapse:descendant-sha", 3, 3, 1, 1, 1, 1, false),
			},
			commitSHA:       rolloutSHA,
			includedCommits: []string{"descendant-sha"},
			expectedJobs:    []string{"synapse"},
			wantDone:        true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			done, failed, reason := PRRolloutStatus(PRRolloutInput{
				Workloads:       tc.workloads,
				ConfigMaps:      tc.configMaps,
				CommitSHA:       tc.commitSHA,
				IncludedCommits: tc.includedCommits,
				ExpectedJobs:    tc.expectedJobs,
				Since:           since,
				Now:             tc.now,
			})
			if done != tc.wantDone || failed != tc.wantFailed || reason != tc.wantReason {
				t.Errorf("PRRolloutStatus() = (%v, %v, %q), want (%v, %v, %q)", done, failed, reason, tc.wantDone, tc.wantFailed, tc.wantReason)
			}
		})
	}
}

func TestImageServiceCandidates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		image string
		want  []string
	}{
		{name: "empty image", image: "", want: nil},
		{name: "aerial prefix stripped", image: "ghcr.io/example/aerial-eri:sha-42", want: []string{"aerial-eri", "eri"}},
		{name: "aerial-sidecar prefix stripped before aerial", image: "ghcr.io/example/aerial-sidecar-cast-watcher:sha-42", want: []string{"aerial-sidecar-cast-watcher", "cast-watcher"}},
		{name: "mirrormere prefix stripped", image: "ghcr.io/example/mirrormere-kiosk:sha-42", want: []string{"mirrormere-kiosk", "kiosk"}},
		{name: "mirrormere without hyphen keeps base", image: "ghcr.io/example/mirrormere:sha-42", want: []string{"mirrormere"}},
		{name: "aerial without stripped suffix", image: "ghcr.io/example/aerial-:sha-42", want: []string{"aerial-"}},
		{name: "short name without prefix", image: "eri:sha-42", want: []string{"eri"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ImageServiceCandidates(tc.image)
			if !slices.Equal(got, tc.want) {
				t.Errorf("ImageServiceCandidates(%q) = %v, want %v", tc.image, got, tc.want)
			}
		})
	}
}

func TestReadyForCommitServices(t *testing.T) {
	t.Parallel()
	rolloutClock := func(hh, mm, ss int) time.Time {
		return time.Date(2026, time.November, 8, hh, mm, ss, 0, time.UTC)
	}
	deployRollout := func(name, image string, gen, observed int64, replicas, updated, available, total int32, deadline bool) model.WorkloadRollout {
		return model.WorkloadRollout{Kind: "Deployment", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, UpdatedReplicas: updated, AvailableReplicas: available, StatusReplicas: total, ProgressDeadlineExceeded: deadline}
	}
	withChange := func(w model.WorkloadRollout, changedAt time.Time, annotations map[string]string, configMapRefs ...string) model.WorkloadRollout {
		w.ChangedAt, w.Annotations, w.ConfigMapRefs = changedAt, annotations, configMapRefs
		return w
	}

	const rolloutSHA = "0123abcd"
	const synapseImage = "ghcr.io/example/aerial-synapse:" + rolloutSHA
	const eriImage = "ghcr.io/example/aerial-eri:" + rolloutSHA
	const otherImage = "ghcr.io/example/aerial-haro:other-sha"
	since := rolloutClock(10, 0, 0)
	auto := map[string]string{"reloader.stakater.com/auto": "true"}

	tests := []struct {
		name            string
		workloads       []model.WorkloadRollout
		configMaps      []model.ConfigMapChange
		commitSHA       string
		includedCommits []string
		expectedJobs    []string
		want            []string
	}{
		{name: "empty commitSHA returns nil", workloads: []model.WorkloadRollout{deployRollout("synapse", synapseImage, 3, 3, 1, 1, 1, 1, false)}, commitSHA: "", want: nil},
		{name: "empty workloads returns nil", workloads: nil, commitSHA: rolloutSHA, want: nil},
		{name: "single ready returns candidate service names", workloads: []model.WorkloadRollout{deployRollout("synapse", synapseImage, 3, 3, 1, 1, 1, 1, false)}, commitSHA: rolloutSHA, want: []string{"aerial-synapse", "synapse"}},
		{name: "multi-workload same image partial ready returns unready", workloads: []model.WorkloadRollout{
			deployRollout("eri-1", eriImage, 3, 3, 1, 1, 1, 1, false),
			deployRollout("eri-2", eriImage, 3, 3, 1, 1, 0, 2, false),
		}, commitSHA: rolloutSHA, want: nil},
		{name: "multi-workload same image all ready returns candidates", workloads: []model.WorkloadRollout{
			deployRollout("eri-1", eriImage, 3, 3, 1, 1, 1, 1, false),
			deployRollout("eri-2", eriImage, 3, 3, 1, 1, 1, 1, false),
		}, commitSHA: rolloutSHA, want: []string{"aerial-eri", "eri"}},
		{name: "terminating duplicate workload skipped without blocking ready candidates", workloads: []model.WorkloadRollout{
			deployRollout("eri-1", eriImage, 3, 3, 1, 1, 1, 1, false),
			func() model.WorkloadRollout {
				w := deployRollout("eri-terminating", eriImage, 4, 3, 1, 1, 0, 1, false)
				w.DeletionTimestamp = "2026-11-20T18:00:00Z"
				w.Finalizers = []string{"deployments.restate.dev"}
				return w
			}(),
		}, commitSHA: rolloutSHA, want: []string{"aerial-eri", "eri"}},
		{name: "awaiting Reloader restart excluded", workloads: []model.WorkloadRollout{
			withChange(deployRollout("synapse", synapseImage, 3, 3, 1, 1, 1, 1, false), rolloutClock(9, 0, 0), auto, "synapse-config"),
		}, configMaps: []model.ConfigMapChange{{Namespace: "default", Name: "synapse-config", ChangedAt: rolloutClock(10, 1, 0)}}, commitSHA: rolloutSHA, want: nil},
		{name: "non-commit-SHA workload ignored", workloads: []model.WorkloadRollout{
			deployRollout("haro", otherImage, 3, 3, 1, 1, 1, 1, false),
			deployRollout("synapse", synapseImage, 3, 3, 1, 1, 1, 1, false),
		}, commitSHA: rolloutSHA, want: []string{"aerial-synapse", "synapse"}},
		{
			name: "expected jobs skips unrelated unready workload",
			workloads: []model.WorkloadRollout{
				deployRollout("synapse", synapseImage, 3, 3, 1, 1, 1, 1, false),
				deployRollout("eri", eriImage, 3, 3, 1, 1, 0, 2, false),
			},
			commitSHA:    rolloutSHA,
			expectedJobs: []string{"synapse"},
			want:         []string{"aerial-synapse", "synapse"},
		},
		{
			name: "included commits reports ready candidate for descendant commit tag",
			workloads: []model.WorkloadRollout{
				deployRollout("synapse", "ghcr.io/example/aerial-synapse:descendant-sha", 3, 3, 1, 1, 1, 1, false),
			},
			commitSHA:       rolloutSHA,
			includedCommits: []string{"descendant-sha"},
			expectedJobs:    []string{"synapse"},
			want:            []string{"aerial-synapse", "synapse"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ReadyForCommitServices(PRRolloutInput{
				Workloads:       tc.workloads,
				ConfigMaps:      tc.configMaps,
				CommitSHA:       tc.commitSHA,
				IncludedCommits: tc.includedCommits,
				ExpectedJobs:    tc.expectedJobs,
				Since:           since,
				Now:             rolloutClock(10, 5, 0),
			})
			if !slices.Equal(got, tc.want) {
				t.Errorf("ReadyForCommitServices() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestServiceJobs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		jobs     []string
		requests map[string]prwire.KustomizationRequest
		want     []string
	}{
		{
			name:     "nil jobs returns nil",
			jobs:     nil,
			requests: nil,
			want:     nil,
		},
		{
			name:     "empty strings dropped",
			jobs:     []string{"", "  ", "\t"},
			requests: nil,
			want:     nil,
		},
		{
			name: "kustomization names filtered",
			jobs: []string{"k8s-aerial", "synapse", " aerial "},
			requests: map[string]prwire.KustomizationRequest{
				"k8s-aerial": {},
				"aerial":     {},
			},
			want: []string{"synapse"},
		},
		{
			name: "trims and deduplicates preserving first seen",
			jobs: []string{" synapse ", "eri", "synapse", "  eri  ", "haro"},
			requests: map[string]prwire.KustomizationRequest{
				"manifests": {},
			},
			want: []string{"synapse", "eri", "haro"},
		},
		{
			name: "all jobs match kustomization requests returns nil",
			jobs: []string{"k8s-aerial", "aerial-config"},
			requests: map[string]prwire.KustomizationRequest{
				"k8s-aerial":    {},
				"aerial-config": {},
			},
			want: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ServiceJobs(tc.jobs, tc.requests)
			if !slices.Equal(got, tc.want) {
				t.Errorf("ServiceJobs() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWorkloadMatchesJobs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		workload     model.WorkloadRollout
		expectedJobs []string
		want         bool
	}{
		{
			name:         "empty expectedJobs returns true",
			workload:     model.WorkloadRollout{Kind: "Deployment", Name: "synapse"},
			expectedJobs: nil,
			want:         true,
		},
		{
			name:         "workload name exact match",
			workload:     model.WorkloadRollout{Kind: "Deployment", Name: "synapse"},
			expectedJobs: []string{"synapse"},
			want:         true,
		},
		{
			name:         "workload name case-insensitive and trimmed",
			workload:     model.WorkloadRollout{Kind: "Deployment", Name: " Synapse "},
			expectedJobs: []string{"synapse"},
			want:         true,
		},
		{
			name: "image service candidate matches stripped prefix",
			workload: model.WorkloadRollout{
				Kind:   "Deployment",
				Name:   "worker",
				Images: []string{"ghcr.io/example/aerial-synapse:sha-1"},
			},
			expectedJobs: []string{"synapse"},
			want:         true,
		},
		{
			name: "image service candidate matches full name",
			workload: model.WorkloadRollout{
				Kind:   "Deployment",
				Name:   "worker",
				Images: []string{"ghcr.io/example/aerial-synapse:sha-1"},
			},
			expectedJobs: []string{"aerial-synapse"},
			want:         true,
		},
		{
			name: "sidecar image does not match unrelated job",
			workload: model.WorkloadRollout{
				Kind:   "Deployment",
				Name:   "worker",
				Images: []string{"fluent/fluent-bit:v1.9"},
			},
			expectedJobs: []string{"synapse"},
			want:         false,
		},
		{
			name: "restate cluster managed child matches by name",
			workload: model.WorkloadRollout{
				Kind: "RestateCluster",
				Name: "restate-cluster",
				ManagedWorkloads: []model.WorkloadRollout{
					{Kind: "StatefulSet", Name: "restate"},
				},
			},
			expectedJobs: []string{"restate"},
			want:         true,
		},
		{
			name: "restate cluster managed child matches by image",
			workload: model.WorkloadRollout{
				Kind: "RestateCluster",
				Name: "cluster-x",
				ManagedWorkloads: []model.WorkloadRollout{
					{Kind: "StatefulSet", Name: "sts-x", Images: []string{"ghcr.io/example/aerial-restate:tag"}},
				},
			},
			expectedJobs: []string{"restate"},
			want:         true,
		},
		{
			name: "no match returns false",
			workload: model.WorkloadRollout{
				Kind:   "Deployment",
				Name:   "haro",
				Images: []string{"ghcr.io/example/aerial-haro:sha"},
			},
			expectedJobs: []string{"synapse"},
			want:         false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := WorkloadMatchesJobs(tc.workload, tc.expectedJobs)
			if got != tc.want {
				t.Errorf("WorkloadMatchesJobs() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCandidateDescendantTags(t *testing.T) {
	t.Parallel()
	const prSHA = "0123abcd"
	tests := []struct {
		name         string
		workloads    []model.WorkloadRollout
		commitSHA    string
		expectedJobs []string
		want         []string
	}{
		{
			name:         "empty commitSHA returns nil",
			workloads:    []model.WorkloadRollout{{Kind: "Deployment", Name: "synapse", Images: []string{"ghcr.io/example/aerial-synapse:new-sha"}}},
			commitSHA:    "",
			expectedJobs: []string{"synapse"},
			want:         nil,
		},
		{
			name:         "empty expectedJobs returns nil",
			workloads:    []model.WorkloadRollout{{Kind: "Deployment", Name: "synapse", Images: []string{"ghcr.io/example/aerial-synapse:new-sha"}}},
			commitSHA:    prSHA,
			expectedJobs: nil,
			want:         nil,
		},
		{
			name:         "empty workloads returns nil",
			workloads:    nil,
			commitSHA:    prSHA,
			expectedJobs: []string{"synapse"},
			want:         nil,
		},
		{
			name: "matching job extracts descendant tag and ignores prSHA and latest",
			workloads: []model.WorkloadRollout{
				{
					Kind:   "Deployment",
					Name:   "synapse",
					Images: []string{"ghcr.io/example/aerial-synapse:descendant-1", "ghcr.io/example/aerial-synapse:" + prSHA, "ghcr.io/example/aerial-synapse:latest"},
				},
			},
			commitSHA:    prSHA,
			expectedJobs: []string{"synapse"},
			want:         []string{"descendant-1"},
		},
		{
			name: "sidecar image excluded",
			workloads: []model.WorkloadRollout{
				{
					Kind:   "Deployment",
					Name:   "synapse",
					Images: []string{"ghcr.io/example/aerial-synapse:descendant-1", "docker.io/fluent/fluent-bit:v1.9"},
				},
			},
			commitSHA:    prSHA,
			expectedJobs: []string{"synapse"},
			want:         []string{"descendant-1"},
		},
		{
			name: "deleting workload ignored",
			workloads: []model.WorkloadRollout{
				{
					Kind:              "Deployment",
					Name:              "synapse",
					Images:            []string{"ghcr.io/example/aerial-synapse:descendant-1"},
					DeletionTimestamp: "2026-11-20T18:00:00Z",
				},
			},
			commitSHA:    prSHA,
			expectedJobs: []string{"synapse"},
			want:         nil,
		},
		{
			name: "restate cluster links and extracts child image tag",
			workloads: []model.WorkloadRollout{
				{
					Kind:      "RestateCluster",
					Namespace: "default",
					Name:      "restate",
				},
				{
					Kind:      "StatefulSet",
					Namespace: "default",
					Name:      "restate",
					Images:    []string{"ghcr.io/example/aerial-restate:descendant-restate"},
				},
			},
			commitSHA:    prSHA,
			expectedJobs: []string{"restate"},
			want:         []string{"descendant-restate"},
		},
		{
			name: "multiple workloads deduplicate in first seen order",
			workloads: []model.WorkloadRollout{
				{
					Kind:   "Deployment",
					Name:   "synapse-1",
					Images: []string{"ghcr.io/example/aerial-synapse:tag-a", "ghcr.io/example/aerial-synapse:tag-b"},
				},
				{
					Kind:   "Deployment",
					Name:   "synapse-2",
					Images: []string{"ghcr.io/example/aerial-synapse:tag-a", "ghcr.io/example/aerial-synapse:tag-c"},
				},
			},
			commitSHA:    prSHA,
			expectedJobs: []string{"synapse"},
			want:         []string{"tag-a", "tag-b", "tag-c"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CandidateDescendantTags(tc.workloads, tc.commitSHA, tc.expectedJobs)
			if !slices.Equal(got, tc.want) {
				t.Errorf("CandidateDescendantTags() = %v, want %v", got, tc.want)
			}
		})
	}
}
