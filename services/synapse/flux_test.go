package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func scanPreImagePRRow(dest ...any) error {
	if len(dest) < 3 {
		return nil
	}
	if p, ok := dest[0].(*int); ok {
		*p = 896
	}
	if b, ok := dest[1].(*string); ok {
		*b = "feat/test"
	}
	if t, ok := dest[2].(*string); ok {
		*t = "100000000000000001"
	}
	return nil
}

func scanPreImageStatusRow(status string, dest ...any) error {
	if len(dest) < 2 {
		return nil
	}
	if s, ok := dest[0].(*string); ok {
		*s = status
	}
	if b, ok := dest[1].(*[]byte); ok {
		*b = []byte(`{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`)
	}
	return nil
}

func newMockPreImageKustomizationPool(status string) *mockSynapsePgxPool {
	return &mockSynapsePgxPool{
		queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "SELECT pr_number, branch, target_id FROM pr_registry") {
				return mockSynapsePgxRow{scanFn: scanPreImagePRRow}
			}
			if strings.Contains(sql, "SELECT status, COALESCE(metadata, '{}'::jsonb)") {
				return mockSynapsePgxRow{scanFn: func(dest ...any) error {
					return scanPreImageStatusRow(status, dest...)
				}}
			}
			return mockSynapsePgxRow{}
		},
	}
}

type translateFluxAlertCase struct {
	name       string
	alert      FluxAlertEvent
	wantOK     bool
	wantEvent  string
	wantStatus string
	wantJob    string
	wantRepo   string
	wantSHA    string
	wantDetail string
	wantKind   string
}

func runTranslateFluxAlertCase(t *testing.T, tc translateFluxAlertCase) {
	t.Helper()
	got, ok := TranslateFluxAlertToDeployEvent(tc.alert)
	if ok != tc.wantOK {
		t.Fatalf("TranslateFluxAlertToDeployEvent() ok = %v, wantOK = %v", ok, tc.wantOK)
	}
	if !tc.wantOK {
		return
	}
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

type handleFluxWebhookCase struct {
	name           string
	endpoint       string
	payload        string
	withBus        bool
	busFail        bool
	downstreamFail bool
	pgReg          *PostgresPRRegistry
	wantStatusCode int
	wantStatus     string
	wantEvent      string
	wantJobName    string
	wantPRNumber   int
	wantBusJobs    int
}

func assertFluxResponse(t *testing.T, body []byte, wantStatus, wantEvent, wantJob string, wantPR int) {
	t.Helper()
	if wantStatus == "" {
		return
	}
	var res map[string]any
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if gotStatus, _ := res["status"].(string); gotStatus != wantStatus {
		t.Errorf("status = %q, want %q", gotStatus, wantStatus)
	}
	if wantEvent != "" {
		if gotEvent, _ := res["event"].(string); gotEvent != wantEvent {
			t.Errorf("event = %q, want %q", gotEvent, wantEvent)
		}
	}
	if wantJob != "" {
		if gotJob, _ := res["job_name"].(string); gotJob != wantJob {
			t.Errorf("job_name = %q, want %q", gotJob, wantJob)
		}
	}
	if wantPR != 0 {
		if gotPR, _ := res["pr_number"].(float64); int(gotPR) != wantPR {
			t.Errorf("pr_number = %v, want %d", res["pr_number"], wantPR)
		}
	}
}

func runHandleFluxWebhookCase(t *testing.T, tc handleFluxWebhookCase) {
	t.Helper()
	srv := newTestRouter(Config{SystemChannelID: "123456789012345678"}, nil, tc.pgReg, nil)
	if tc.downstreamFail {
		srv.SetDispatcher(&mockOutboundDispatcher{directMessageErr: errors.New("dispatch failed")})
	}
	var mockBus *mockEventBus
	if tc.withBus {
		mockBus = &mockEventBus{}
		if tc.busFail {
			mockBus.err = errors.New("bus insert failed")
		}
		srv.SetEventBus(mockBus)
	}

	req := httptest.NewRequest(http.MethodPost, tc.endpoint, strings.NewReader(tc.payload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Routes().ServeHTTP(w, req)

	if w.Code != tc.wantStatusCode {
		t.Fatalf("status code = %d, want %d (body: %s)", w.Code, tc.wantStatusCode, w.Body.String())
	}

	assertFluxResponse(t, w.Body.Bytes(), tc.wantStatus, tc.wantEvent, tc.wantJobName, tc.wantPRNumber)

	if tc.withBus && mockBus != nil {
		if len(mockBus.insertedDeployJobs) != tc.wantBusJobs {
			t.Errorf("inserted bus jobs = %d, want %d", len(mockBus.insertedDeployJobs), tc.wantBusJobs)
		}
	}
}

func translateFluxAlertGitRepoCases(now time.Time) []translateFluxAlertCase {
	return []translateFluxAlertCase{
		{
			name: "GitRepository NewArtifact without payload repo maps to sync_success on the default repo",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial-config", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "NewArtifact",
				Message:        "stored artifact for commit 'feat: test'",
				Metadata: map[string]string{
					"revision": "main@sha1:abcdef1234567890abcdef1234567890abcdef12",
				},
			},
			wantOK:     true,
			wantKind:   "GitRepository",
			wantEvent:  "sync_success",
			wantStatus: "success",
			wantJob:    "git-sync",
			wantRepo:   "azylman/aerial",
			wantSHA:    "abcdef1234567890abcdef1234567890abcdef12",
			wantDetail: "stored artifact for commit 'feat: test'",
		},
		{
			name: "GitRepository GitOperationSucceeded maps to sync_success",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "GitOperationSucceeded",
				Message:        "git sync ok",
				Metadata: map[string]string{
					"revision": "sha1:9876543210987654321098765432109876543210",
				},
			},
			wantOK:     true,
			wantKind:   "GitRepository",
			wantEvent:  "sync_success",
			wantStatus: "success",
			wantJob:    "git-sync",
			wantRepo:   "azylman/aerial",
			wantSHA:    "9876543210987654321098765432109876543210",
			wantDetail: "git sync ok",
		},
		{
			name: "GitRepository GitOperationFailed maps to sync_failed",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"},
				Severity:       "error",
				Timestamp:      now,
				Reason:         "GitOperationFailed",
				Message:        "failed to clone",
				Metadata: map[string]string{
					"revision": "main@sha1:1111222233334444555566667777888899990000",
				},
			},
			wantOK:     true,
			wantKind:   "GitRepository",
			wantEvent:  "sync_failed",
			wantStatus: "failed",
			wantJob:    "git-sync",
			wantRepo:   "azylman/aerial",
			wantSHA:    "1111222233334444555566667777888899990000",
			wantDetail: "failed to clone",
		},
		{
			name: "GitRepository severity error without GitOperationFailed maps to sync_failed",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"},
				Severity:       "error",
				Timestamp:      now,
				Reason:         "AuthError",
				Message:        "authentication failure",
			},
			wantOK:     true,
			wantKind:   "GitRepository",
			wantEvent:  "sync_failed",
			wantStatus: "failed",
			wantJob:    "git-sync",
			wantRepo:   "azylman/aerial",
			wantDetail: "authentication failure",
		},
		{
			name: "GitRepository unhandled reason ignored",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "OtherReason",
			},
			wantOK: false,
		},
	}
}

func translateFluxAlertKustomizationCases(now time.Time) []translateFluxAlertCase {
	return []translateFluxAlertCase{
		{
			name: "Kustomization Progressing maps to deploy_started",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "Kustomization", Name: "webhooks-router", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "Progressing",
				Message:        "reconciliation in progress",
				Metadata: map[string]string{
					"revision": "main@sha1:abcdef1234",
					"repo":     "azylman/aerial",
				},
			},
			wantOK:     true,
			wantKind:   "Kustomization",
			wantEvent:  "deploy_started",
			wantStatus: "started",
			wantJob:    "webhooks-router",
			wantRepo:   "azylman/aerial",
			wantSHA:    "abcdef1234",
			wantDetail: "reconciliation in progress",
		},
	}
}

func translateFluxAlertOtherCases(now time.Time) []translateFluxAlertCase {
	return []translateFluxAlertCase{
		{
			name: "HelmRelease ReconciliationSucceeded maps to deploy_success",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "HelmRelease", Name: "coredns", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "ReconciliationSucceeded",
				Message:        "release reconciled",
				Repo:           "azylman/aerial-config",
				Metadata: map[string]string{
					"revision": "sha1:ffffffff",
				},
			},
			wantOK:     true,
			wantKind:   "HelmRelease",
			wantEvent:  "deploy_success",
			wantStatus: "success",
			wantJob:    "coredns",
			wantRepo:   "azylman/aerial-config",
			wantSHA:    "ffffffff",
			wantDetail: "release reconciled",
		},
		{
			name: "Kustomization ReconciliationFailed maps to deploy_failed",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "Kustomization", Name: "brain", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "ReconciliationFailed",
				Message:        "dry-run failed",
				Metadata: map[string]string{
					"revision": "abcdef",
				},
			},
			wantOK:     true,
			wantKind:   "Kustomization",
			wantEvent:  "deploy_failed",
			wantStatus: "failed",
			wantJob:    "brain",
			wantRepo:   "azylman/aerial",
			wantSHA:    "abcdef",
			wantDetail: "dry-run failed",
		},
		{
			name: "Kustomization HealthCheckFailed maps to deploy_failed",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "HealthCheckFailed",
				Message:        "pod unhealthy",
			},
			wantOK:     true,
			wantKind:   "Kustomization",
			wantEvent:  "deploy_failed",
			wantStatus: "failed",
			wantJob:    "synapse",
			wantRepo:   "azylman/aerial",
			wantDetail: "pod unhealthy",
		},
		{
			name: "Kustomization BuildFailed maps to deploy_failed",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
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
			wantRepo:   "azylman/aerial",
			wantDetail: "kustomize build error",
		},
		{
			name: "Kustomization ArtifactFailed maps to deploy_failed",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
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
			wantRepo:   "azylman/aerial",
			wantDetail: "artifact not found",
		},
		{
			name: "Kustomization ValidationFailed maps to deploy_failed",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
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
			wantRepo:   "azylman/aerial",
			wantDetail: "schema validation error",
		},
		{
			name: "Kustomization severity error without failure reason maps to deploy_failed",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
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
			wantRepo:   "azylman/aerial",
			wantDetail: "unspecified error",
		},
		{
			name: "Kustomization unknown reason ignored",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "Kustomization", Name: "synapse", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "UnknownReason",
			},
			wantOK: false,
		},
		{
			name: "Unsupported Kind ignored",
			alert: FluxAlertEvent{
				InvolvedObject: FluxInvolvedObject{Kind: "Receiver", Name: "generic", Namespace: "flux-system"},
				Severity:       "info",
				Timestamp:      now,
				Reason:         "NewArtifact",
			},
			wantOK: false,
		},
	}
}

func translateFluxAlertCases(now time.Time) []translateFluxAlertCase {
	return append(append(translateFluxAlertGitRepoCases(now), translateFluxAlertKustomizationCases(now)...), translateFluxAlertOtherCases(now)...)
}

func TestTranslateFluxAlertToDeployEvent(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 6, 18, 0, 0, 0, time.UTC)
	tests := translateFluxAlertCases(now)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTranslateFluxAlertCase(t, tc)
		})
	}
}

func TestHandleFluxWebhook(t *testing.T) {
	t.Parallel()

	tests := []handleFluxWebhookCase{
		{
			name:           "POST /webhooks/flux valid kustomization without bus",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"synapse","namespace":"flux-system"},"severity":"info","reason":"ReconciliationSucceeded","message":"reconciliation ok"}`,
			wantStatusCode: http.StatusOK,
			wantStatus:     "accepted",
			wantEvent:      "deploy_success",
			wantJobName:    "synapse",
		},
		{
			name:           "POST /api/webhooks/flux valid gitrepository without bus",
			endpoint:       "/api/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"GitRepository","name":"aerial-config","namespace":"flux-system"},"severity":"info","reason":"NewArtifact","message":"sync ok","metadata":{"revision":"main@sha1:abcdef1234"}}`,
			wantStatusCode: http.StatusOK,
			wantStatus:     "accepted",
			wantEvent:      "sync_success",
			wantJobName:    "git-sync",
		},
		{
			name:           "POST /webhooks/flux unhandled alert returns 200 ignored",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Bucket","name":"test","namespace":"flux-system"},"severity":"info","reason":"Progressing"}`,
			wantStatusCode: http.StatusOK,
			wantStatus:     "ignored",
		},
		{
			name:           "POST /webhooks/flux invalid json returns 400",
			endpoint:       "/webhooks/flux",
			payload:        `{invalid json`,
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "POST /webhooks/flux with bus inserts job",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"brain","namespace":"flux-system"},"severity":"info","reason":"Progressing","message":"progressing","metadata":{"revision":"sha1:12345"}}`,
			withBus:        true,
			wantStatusCode: http.StatusAccepted,
			wantStatus:     "accepted",
			wantEvent:      "deploy_started",
			wantJobName:    "brain",
			wantBusJobs:    1,
		},
		{
			name:           "POST /webhooks/flux with bus failure returns 500",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"brain","namespace":"flux-system"},"severity":"info","reason":"Progressing"}`,
			withBus:        true,
			busFail:        true,
			wantStatusCode: http.StatusInternalServerError,
		},
		{
			name:           "POST /webhooks/flux without bus error processing event returns 200 with result",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"webhooks-router","namespace":"flux-system"},"severity":"error","reason":"ReconciliationFailed","message":"broken deploy"}`,
			downstreamFail: true,
			wantStatusCode: http.StatusOK,
			wantStatus:     "accepted",
			wantEvent:      "deploy_failed",
			wantJobName:    "webhooks-router",
		},
		{
			name:           "POST /webhooks/flux ignores pre-image kustomization deploy_started event without inserting bus jobs",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"aerial-core","namespace":"flux-system"},"severity":"info","reason":"Progressing","message":"progressing","metadata":{"revision":"main@sha1:12345"}}`,
			withBus:        true,
			pgReg:          NewPostgresPRRegistry(newMockPreImageKustomizationPool("merged")),
			wantStatusCode: http.StatusOK,
			wantStatus:     "ignored",
			wantEvent:      "deploy_started",
			wantJobName:    "aerial-core",
			wantPRNumber:   896,
			wantBusJobs:    0,
		},
		{
			name:           "POST /webhooks/flux ignores pre-image kustomization deploy_success event without inserting bus jobs",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"aerial-core","namespace":"flux-system"},"severity":"info","reason":"ReconciliationSucceeded","message":"reconciled","metadata":{"revision":"main@sha1:12345"}}`,
			withBus:        true,
			pgReg:          NewPostgresPRRegistry(newMockPreImageKustomizationPool("synced")),
			wantStatusCode: http.StatusOK,
			wantStatus:     "ignored",
			wantEvent:      "deploy_success",
			wantJobName:    "aerial-core",
			wantPRNumber:   896,
			wantBusJobs:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runHandleFluxWebhookCase(t, tc)
		})
	}
}

func TestCleanRevisionSHAAndRepoHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantSHA string
	}{
		{name: "branch@sha1 prefix", input: "main@sha1:1234567890abcdef", wantSHA: "1234567890abcdef"},
		{name: "sha1 prefix", input: "sha1:1234567890abcdef", wantSHA: "1234567890abcdef"},
		{name: "branch@ prefix", input: "main@1234567890abcdef", wantSHA: "1234567890abcdef"},
		{name: "bare sha", input: "1234567890abcdef", wantSHA: "1234567890abcdef"},
		{name: "empty revision", input: "", wantSHA: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := cleanRevisionSHA(tc.input)
			if got != tc.wantSHA {
				t.Errorf("cleanRevisionSHA(%q) = %q, want %q", tc.input, got, tc.wantSHA)
			}
		})
	}

	// Test zero timestamp fallback in TranslateFluxAlertToDeployEvent
	zeroTimeAlert := FluxAlertEvent{
		InvolvedObject: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial"},
		Reason:         "NewArtifact",
	}
	evt, ok := TranslateFluxAlertToDeployEvent(zeroTimeAlert)
	if !ok || evt.Timestamp.IsZero() {
		t.Errorf("expected ok=true and non-zero timestamp for zero-time alert")
	}
}

func TestHandleFluxWebhook_Forwarding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cfgForward    bool
		wantForwarded bool
	}{
		{
			name:          "forwards raw webhook to peer when configured",
			cfgForward:    true,
			wantForwarded: true,
		},
		{
			name:          "no forward when WebhookForwardURLs is empty",
			cfgForward:    false,
			wantForwarded: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var receivedBody string
			var receivedHeader http.Header
			peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				receivedBody = string(b)
				receivedHeader = r.Header.Clone()
				w.WriteHeader(http.StatusOK)
			}))
			defer peerServer.Close()

			cfg := Config{
				BotID:        "aerial",
				DefaultOwner: true,
			}
			if tc.cfgForward {
				cfg.WebhookForwardURLs = map[string]string{
					"aerial":  "http://self:4020",
					"eri-sub": peerServer.URL,
				}
			}

			srv := newTestRouter(cfg, peerServer.Client(), nil, nil)
			payload := `{"involvedObject":{"kind":"GitRepository","name":"aerial","namespace":"flux-system"},"severity":"info","reason":"NewArtifact","message":"git commit"}`
			req := httptest.NewRequest(http.MethodPost, "/webhooks/flux", strings.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Custom-Header", "custom-val")
			w := httptest.NewRecorder()

			srv.Routes().ServeHTTP(w, req)

			if tc.wantForwarded {
				if receivedBody != payload {
					t.Fatalf("forwarded body = %q, want %q", receivedBody, payload)
				}
				if receivedHeader.Get("X-Custom-Header") != "custom-val" {
					t.Errorf("forwarded header X-Custom-Header = %q, want custom-val", receivedHeader.Get("X-Custom-Header"))
				}
			} else if receivedBody != "" {
				t.Fatalf("expected no forward, but got body %q", receivedBody)
			}
		})
	}
}

func TestKustomizationsForImages(t *testing.T) {
	t.Parallel()
	core := fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"}
	marida := fluxKustomizationRef{Namespace: "flux-system", Name: "marida-core"}
	appsMarida := fluxKustomizationRef{Namespace: "apps", Name: "marida-core"}
	owned := func(name, namespace string, images ...string) workloadImageRef {
		labels := map[string]string{"kustomize.toolkit.fluxcd.io/name": name}
		if namespace != "" {
			labels["kustomize.toolkit.fluxcd.io/namespace"] = namespace
		}
		return workloadImageRef{Labels: labels, Images: images}
	}

	tests := []struct {
		name       string
		workloads  []workloadImageRef
		imageNames []string
		want       []fluxKustomizationRef
	}{
		{
			name: "two kustomizations sharing an image are both returned sorted",
			workloads: []workloadImageRef{
				owned("marida-core", "flux-system", "ghcr.io/azylman/aerial-synapse:abc"),
				owned("aerial-core", "flux-system", "ghcr.io/azylman/aerial-synapse:abc"),
			},
			imageNames: []string{"aerial-synapse"},
			want:       []fluxKustomizationRef{core, marida},
		},
		{
			name: "tag and digest are stripped before matching",
			workloads: []workloadImageRef{
				owned("aerial-core", "flux-system", "registry.local:5000/team/aerial-eri:v1@sha256:deadbeef"),
				owned("marida-core", "flux-system", "aerial-eri@sha256:deadbeef"),
			},
			imageNames: []string{"aerial-eri"},
			want:       []fluxKustomizationRef{core, marida},
		},
		{
			name: "unlabeled workload is ignored",
			workloads: []workloadImageRef{
				{Labels: map[string]string{"app": "eri"}, Images: []string{"ghcr.io/azylman/aerial-eri:abc"}},
				{Images: []string{"ghcr.io/azylman/aerial-eri:abc"}},
			},
			imageNames: []string{"aerial-eri"},
		},
		{
			name:       "no matching image returns nil",
			workloads:  []workloadImageRef{owned("aerial-core", "flux-system", "ghcr.io/azylman/aerial-eri:abc")},
			imageNames: []string{"aerial-haro"},
		},
		{
			name: "duplicate owners de-duplicate and namespace label defaults to flux-system",
			workloads: []workloadImageRef{
				owned("aerial-core", "", "ghcr.io/azylman/aerial-eri:abc"),
				owned("aerial-core", "flux-system", "ghcr.io/azylman/aerial-eri:def"),
				owned("marida-core", "apps", "ghcr.io/azylman/aerial-eri:abc"),
			},
			imageNames: []string{"aerial-eri"},
			want:       []fluxKustomizationRef{appsMarida, core},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := kustomizationsForImages(tc.workloads, tc.imageNames); !slices.Equal(got, tc.want) {
				t.Errorf("kustomizationsForImages() = %v, want %v", got, tc.want)
			}
		})
	}
}

const (
	rolloutSHA   = "0123abcd"
	synapseImage = "ghcr.io/azylman/aerial-synapse:" + rolloutSHA
)

// deployRollout builds a Deployment rollout state.
func deployRollout(name, image string, gen, observed int64, replicas, updated, available, total int32, deadline bool) workloadRollout {
	return workloadRollout{Kind: "Deployment", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, UpdatedReplicas: updated, AvailableReplicas: available, StatusReplicas: total, ProgressDeadlineExceeded: deadline}
}

// stsRollout builds a StatefulSet rollout state.
func stsRollout(name, image string, gen, observed int64, replicas, available int32, updateRev, currentRev string) workloadRollout {
	return workloadRollout{Kind: "StatefulSet", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, AvailableReplicas: available, UpdateRevision: updateRev, CurrentRevision: currentRev}
}

// dsRollout builds a DaemonSet rollout state.
func dsRollout(name, image string, gen, observed int64, desired, updated, available int32) workloadRollout {
	return workloadRollout{Kind: "DaemonSet", Namespace: "kube-system", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, DesiredNumberScheduled: desired, UpdatedNumberScheduled: updated, NumberAvailable: available}
}

// restateDeployRollout builds a RestateDeployment rollout state.
func restateDeployRollout(name, image string, gen, observed int64, replicas, ready, available int32, deployID string, readyCond bool) workloadRollout {
	return workloadRollout{Kind: "RestateDeployment", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, ReadyReplicas: ready, AvailableReplicas: available, DeploymentID: deployID, ReadyConditionTrue: readyCond}
}

// restateClusterRollout builds a RestateCluster rollout state.
func restateClusterRollout(name, image string, gen, observed int64, readyCond bool) workloadRollout {
	return workloadRollout{Kind: "RestateCluster", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: 1, ReadyConditionTrue: readyCond}
}

func restateClusterRolloutWithManaged(name, image string, gen, observed int64, readyCond bool, managed ...workloadRollout) workloadRollout {
	return workloadRollout{Kind: "RestateCluster", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: 1, ReadyConditionTrue: readyCond, ManagedWorkloads: managed}
}

func TestImageTag(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{name: "plain tag", image: "ghcr.io/azylman/aerial-synapse:abc123", want: "abc123"},
		{name: "registry with port and tag", image: "registry.lan:5000/aerial/haro:def456", want: "def456"},
		{name: "registry with port untagged", image: "registry.lan:5000/aerial/haro"},
		{name: "tag with digest", image: "ghcr.io/azylman/aerial-synapse:abc123@sha256:beef", want: "abc123"},
		{name: "digest only", image: "ghcr.io/azylman/aerial-synapse@sha256:beef"},
		{name: "untagged", image: "busybox"},
		{name: "bare name with tag", image: "postgres:18", want: "18"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := imageTag(tc.image); got != tc.want {
				t.Errorf("imageTag(%q) = %q, want %q", tc.image, got, tc.want)
			}
		})
	}
}

func TestDecodeWorkloadRollouts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		kind    string
		body    string
		want    []workloadRollout
		wantErr bool
	}{
		{name: "RestateDeployment explicit zero", kind: "RestateDeployment", body: `{"items":[{"metadata":{"generation":2},"spec":{"replicas":0},"status":{"observedGeneration":2,"conditions":[{"type":"Ready","status":"False"}]}}]}`, want: []workloadRollout{{Kind: "RestateDeployment", Images: []string{}, Generation: 2, ObservedGeneration: 2, Replicas: 0}}},
		{name: "RestateDeployment omitted defaults one", kind: "RestateDeployment", body: `{"items":[{"metadata":{"generation":2},"spec":{},"status":{"observedGeneration":2,"conditions":[{"type":"Ready","status":"False"}]}}]}`, want: []workloadRollout{{Kind: "RestateDeployment", Images: []string{}, Generation: 2, ObservedGeneration: 2, Replicas: 1}}},
		{name: "StatefulSet explicit zero", kind: "StatefulSet", body: `{"items":[{"metadata":{"generation":2},"spec":{"replicas":0},"status":{"observedGeneration":2,"conditions":[{"type":"Ready","status":"False"}]}}]}`, want: []workloadRollout{{Kind: "StatefulSet", Images: []string{}, Generation: 2, ObservedGeneration: 2, Replicas: 0}}},
		{name: "StatefulSet omitted defaults one", kind: "StatefulSet", body: `{"items":[{"metadata":{"generation":2},"spec":{},"status":{"observedGeneration":2,"conditions":[{"type":"Ready","status":"False"}]}}]}`, want: []workloadRollout{{Kind: "StatefulSet", Images: []string{}, Generation: 2, ObservedGeneration: 2, Replicas: 1}}},
		{name: "RestateCluster explicit zero", kind: "RestateCluster", body: `{"items":[{"metadata":{"generation":2},"spec":{"compute":{"replicas":0}},"status":{"observedGeneration":2,"conditions":[{"type":"Ready","status":"False"}]}}]}`, want: []workloadRollout{{Kind: "RestateCluster", Images: []string{}, Generation: 2, ObservedGeneration: 2, Replicas: 0}}},
		{name: "RestateCluster omitted defaults one", kind: "RestateCluster", body: `{"items":[{"metadata":{"generation":2},"spec":{},"status":{"observedGeneration":2,"conditions":[{"type":"Ready","status":"False"}]}}]}`, want: []workloadRollout{{Kind: "RestateCluster", Images: []string{}, Generation: 2, ObservedGeneration: 2, Replicas: 1}}},
		{
			name: "deployment defaults replicas to one and detects the progress deadline",
			kind: "Deployment",
			body: `{"items":[{"metadata":{"name":"synapse","namespace":"default","generation":4},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:0123abcd"}],"initContainers":[{"image":"busybox"}]}}},"status":{"observedGeneration":4,"replicas":2,"updatedReplicas":1,"availableReplicas":1,"conditions":[{"type":"Available","status":"True"},{"type":"Progressing","status":"False","reason":"ProgressDeadlineExceeded"}]}}]}`,
			want: []workloadRollout{{Kind: "Deployment", Namespace: "default", Name: "synapse", Images: []string{"ghcr.io/azylman/aerial-synapse:0123abcd", "busybox"}, Generation: 4, ObservedGeneration: 4, Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1, StatusReplicas: 2, ProgressDeadlineExceeded: true}},
		},
		{
			name: "deployment progressing normally",
			kind: "Deployment",
			body: `{"items":[{"metadata":{"name":"haro","namespace":"default","generation":2},"spec":{"replicas":3,"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-haro:0123abcd"}]}}},"status":{"observedGeneration":2,"replicas":3,"updatedReplicas":3,"availableReplicas":3,"conditions":[{"type":"Progressing","status":"True","reason":"NewReplicaSetAvailable"}]}}]}`,
			want: []workloadRollout{{Kind: "Deployment", Namespace: "default", Name: "haro", Images: []string{"ghcr.io/azylman/aerial-haro:0123abcd"}, Generation: 2, ObservedGeneration: 2, Replicas: 3, UpdatedReplicas: 3, AvailableReplicas: 3, StatusReplicas: 3}},
		},
		{
			name: "statefulset revisions",
			kind: "StatefulSet",
			body: `{"items":[{"metadata":{"name":"db","namespace":"default","generation":7},"spec":{"replicas":2,"template":{"spec":{"containers":[{"image":"postgres:18"}]}}},"status":{"observedGeneration":7,"replicas":2,"availableReplicas":2,"updateRevision":"db-2","currentRevision":"db-1"}}]}`,
			want: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "db", Images: []string{"postgres:18"}, Generation: 7, ObservedGeneration: 7, Replicas: 2, AvailableReplicas: 2, StatusReplicas: 2, UpdateRevision: "db-2", CurrentRevision: "db-1"}},
		},
		{
			name: "daemonset scheduling counts",
			kind: "DaemonSet",
			body: `{"items":[{"metadata":{"name":"fluent-bit","namespace":"kube-system","generation":3},"spec":{"template":{"spec":{"containers":[{"image":"fluent/fluent-bit:4"}]}}},"status":{"observedGeneration":3,"desiredNumberScheduled":3,"updatedNumberScheduled":2,"numberAvailable":3}}]}`,
			want: []workloadRollout{{Kind: "DaemonSet", Namespace: "kube-system", Name: "fluent-bit", Images: []string{"fluent/fluent-bit:4"}, Generation: 3, ObservedGeneration: 3, Replicas: 1, DesiredNumberScheduled: 3, UpdatedNumberScheduled: 2, NumberAvailable: 3}},
		},
		{
			name: "change time skips subresource writes and configmap refs cover every source",
			kind: "Deployment",
			body: `{"items":[{"metadata":{"name":"eri","namespace":"default","generation":5,"annotations":{"reloader.stakater.com/auto":"true"},"managedFields":[{"manager":"kustomize-controller","operation":"Apply","time":"2026-11-08T10:01:00Z"},{"manager":"kube-controller-manager","operation":"Update","subresource":"status","time":"2026-11-08T10:09:00Z"},{"manager":"reloader","operation":"Update","time":"2026-11-08T10:02:00Z"},{"manager":"kubectl","operation":"Update"}]},"spec":{"replicas":1,"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-eri:0123abcd","env":[{"name":"A","valueFrom":{"configMapKeyRef":{"name":"eri-flags","key":"a"}}},{"name":"B","value":"b"},{"name":"C","valueFrom":{"secretKeyRef":{"name":"eri-secret","key":"c"}}}],"envFrom":[{"configMapRef":{"name":"eri-env"}},{"secretRef":{"name":"eri-secret"}}]}],"initContainers":[{"image":"busybox","envFrom":[{"configMapRef":{"name":"eri-env"}}]}],"volumes":[{"name":"cfg","configMap":{"name":"eri-config"}},{"name":"data","emptyDir":{}},{"name":"bundle","projected":{"sources":[{"configMap":{"name":"ca-bundle"}},{"secret":{"name":"eri-secret"}}]}}]}}},"status":{"observedGeneration":5,"replicas":1,"updatedReplicas":1,"availableReplicas":1}}]}`,
			want: []workloadRollout{{Kind: "Deployment", Namespace: "default", Name: "eri", Images: []string{"ghcr.io/azylman/aerial-eri:0123abcd", "busybox"}, ChangedAt: rolloutClock(10, 2, 0), Annotations: map[string]string{"reloader.stakater.com/auto": "true"}, ConfigMapRefs: []string{"ca-bundle", "eri-config", "eri-env", "eri-flags"}, Generation: 5, ObservedGeneration: 5, Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1, StatusReplicas: 1}},
		},
		{
			name: "restatedeployment replicas and ready condition",
			kind: "RestateDeployment",
			body: `{"items":[{"metadata":{"name":"restate-worker","namespace":"default","generation":3},"spec":{"replicas":2,"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-restate-worker:0123abcd"}]}}},"status":{"observedGeneration":3,"replicas":2,"readyReplicas":2,"availableReplicas":2,"deploymentId":"dp_12345","conditions":[{"type":"Ready","status":"True"}]}}]}`,
			want: []workloadRollout{{Kind: "RestateDeployment", Namespace: "default", Name: "restate-worker", Images: []string{"ghcr.io/azylman/aerial-restate-worker:0123abcd"}, Generation: 3, ObservedGeneration: 3, Replicas: 2, ReadyReplicas: 2, AvailableReplicas: 2, StatusReplicas: 2, DeploymentID: "dp_12345", ReadyConditionTrue: true}},
		},
		{
			name: "restatecluster compute image and ready condition",
			kind: "RestateCluster",
			body: `{"items":[{"metadata":{"name":"restate","namespace":"default","generation":2},"spec":{"compute":{"image":"docker.io/restatedev/restate:1.6"}},"status":{"observedGeneration":2,"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			want: []workloadRollout{{Kind: "RestateCluster", Namespace: "default", Name: "restate", Images: []string{"docker.io/restatedev/restate:1.6"}, Generation: 2, ObservedGeneration: 2, Replicas: 1, ReadyConditionTrue: true}},
		},
		{
			name: "restatecluster with compute env and resources",
			kind: "RestateCluster",
			body: `{"items":[{"metadata":{"name":"restate","namespace":"default","generation":2},"spec":{"compute":{"image":"docker.io/restatedev/restate:1.6","replicas":2,"env":[{"name":"A","value":"1"}],"resources":{"limits":{"cpu":"1"},"requests":{"memory":"512Mi"}}}},"status":{"observedGeneration":2,"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			want: []workloadRollout{{Kind: "RestateCluster", Namespace: "default", Name: "restate", Images: []string{"docker.io/restatedev/restate:1.6"}, Generation: 2, ObservedGeneration: 2, Replicas: 2, ReadyConditionTrue: true, Env: map[string]string{"A": "1"}, Resources: kubeResourceRequirements{Limits: map[string]string{"cpu": "1"}, Requests: map[string]string{"memory": "512Mi"}}}},
		},
		{
			name: "statefulset with container env and resources",
			kind: "StatefulSet",
			body: `{"items":[{"metadata":{"name":"restate","namespace":"default","generation":2},"spec":{"replicas":2,"template":{"spec":{"containers":[{"image":"docker.io/restatedev/restate:1.6","env":[{"name":"A","value":"1"}],"resources":{"limits":{"cpu":"1"}}}]}}},"status":{"observedGeneration":2,"replicas":2,"availableReplicas":2,"currentRevision":"r-1","updateRevision":"r-1"}}]}`,
			want: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Images: []string{"docker.io/restatedev/restate:1.6"}, Generation: 2, ObservedGeneration: 2, Replicas: 2, AvailableReplicas: 2, StatusReplicas: 2, CurrentRevision: "r-1", UpdateRevision: "r-1", Env: map[string]string{"A": "1"}, Resources: kubeResourceRequirements{Limits: map[string]string{"cpu": "1"}}}},
		},
		{name: "empty list", kind: "Deployment", body: `{"items":[]}`, want: []workloadRollout{}},
		{name: "malformed list", kind: "Deployment", body: `{"items":`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodeWorkloadRollouts(tc.kind, []byte(tc.body))
			if (err != nil) != tc.wantErr {
				t.Fatalf("decodeWorkloadRollouts() err = %v, wantErr %v", err, tc.wantErr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("decodeWorkloadRollouts() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestWorkloadRolloutComplete(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		workload   workloadRollout
		wantDone   bool
		wantFailed bool
		wantReason string
	}{
		{name: "zero cluster child residual StatusReplicas", workload: workloadRollout{Kind: "RestateCluster", ManagedWorkloads: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, StatusReplicas: 1}}}, wantReason: "managed StatefulSet default/restate: waiting for all replicas to stop"},
		{name: "zero cluster child residual ReadyReplicas", workload: workloadRollout{Kind: "RestateCluster", ManagedWorkloads: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, ReadyReplicas: 1}}}, wantReason: "managed StatefulSet default/restate: waiting for all replicas to stop"},
		{name: "zero cluster child residual AvailableReplicas", workload: workloadRollout{Kind: "RestateCluster", ManagedWorkloads: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, AvailableReplicas: 1}}}, wantReason: "managed StatefulSet default/restate: waiting for all replicas to stop"},
		{name: "zero cluster child residual UpdatedReplicas", workload: workloadRollout{Kind: "RestateCluster", ManagedWorkloads: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, UpdatedReplicas: 1}}}, wantReason: "managed StatefulSet default/restate: waiting for all replicas to stop"},
		{name: "zero cluster stale child", workload: workloadRollout{Kind: "RestateCluster", ManagedWorkloads: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 1}}}, wantReason: "managed StatefulSet default/restate: generation 2 not yet observed"},
		{name: "RestateDeployment stopped with inactive status", workload: workloadRollout{Kind: "RestateDeployment", Generation: 2, ObservedGeneration: 2}, wantDone: true},
		{name: "RestateDeployment stopped stale generation", workload: workloadRollout{Kind: "RestateDeployment", Generation: 2, ObservedGeneration: 1}, wantReason: "generation 2 not yet observed"},
		{name: "RestateDeployment stopped residual StatusReplicas", workload: workloadRollout{Kind: "RestateDeployment", Generation: 2, ObservedGeneration: 2, StatusReplicas: 1}, wantReason: "waiting for all replicas to stop"},
		{name: "RestateDeployment stopped residual ReadyReplicas", workload: workloadRollout{Kind: "RestateDeployment", Generation: 2, ObservedGeneration: 2, ReadyReplicas: 1}, wantReason: "waiting for all replicas to stop"},
		{name: "RestateDeployment stopped residual AvailableReplicas", workload: workloadRollout{Kind: "RestateDeployment", Generation: 2, ObservedGeneration: 2, AvailableReplicas: 1}, wantReason: "waiting for all replicas to stop"},
		// The portable stopped-rollout contract checks every decoded counter, even
		// when an operator version currently omits updatedReplicas.
		{name: "RestateDeployment stopped residual UpdatedReplicas", workload: workloadRollout{Kind: "RestateDeployment", Generation: 2, ObservedGeneration: 2, UpdatedReplicas: 1}, wantReason: "waiting for all replicas to stop"},
		{name: "StatefulSet stopped with inactive status", workload: workloadRollout{Kind: "StatefulSet", Generation: 2, ObservedGeneration: 2, UpdateRevision: "new", CurrentRevision: "old"}, wantDone: true},
		{name: "StatefulSet stopped stale generation", workload: workloadRollout{Kind: "StatefulSet", Generation: 2, ObservedGeneration: 1}, wantReason: "generation 2 not yet observed"},
		{name: "StatefulSet stopped residual StatusReplicas", workload: workloadRollout{Kind: "StatefulSet", Generation: 2, ObservedGeneration: 2, StatusReplicas: 1}, wantReason: "waiting for all replicas to stop"},
		{name: "StatefulSet stopped residual ReadyReplicas", workload: workloadRollout{Kind: "StatefulSet", Generation: 2, ObservedGeneration: 2, ReadyReplicas: 1}, wantReason: "waiting for all replicas to stop"},
		{name: "StatefulSet stopped residual AvailableReplicas", workload: workloadRollout{Kind: "StatefulSet", Generation: 2, ObservedGeneration: 2, AvailableReplicas: 1}, wantReason: "waiting for all replicas to stop"},
		{name: "StatefulSet stopped residual UpdatedReplicas", workload: workloadRollout{Kind: "StatefulSet", Generation: 2, ObservedGeneration: 2, UpdatedReplicas: 1}, wantReason: "waiting for all replicas to stop"},
		{name: "zero cluster verifies stopped child without Ready", workload: workloadRollout{Kind: "RestateCluster", ManagedWorkloads: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, UpdateRevision: "new", CurrentRevision: "old"}}}, wantDone: true},
		{name: "zero cluster missing child", workload: workloadRollout{Kind: "RestateCluster"}, wantReason: "waiting for managed statefulset"},
		{name: "zero cluster child desired replicas mismatch", workload: workloadRollout{Kind: "RestateCluster", ManagedWorkloads: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Replicas: 1}}}, wantReason: "managed StatefulSet default/restate replicas 1 do not match cluster replicas 0"},
		{name: "zero cluster Images mismatch", workload: workloadRollout{Kind: "RestateCluster", Images: []string{"new"}, ManagedWorkloads: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, UpdateRevision: "new", CurrentRevision: "old"}}}, wantReason: "managed StatefulSet default/restate images [] do not match cluster image new"},
		{name: "zero cluster Env mismatch", workload: workloadRollout{Kind: "RestateCluster", Env: map[string]string{"A": "1"}, ManagedWorkloads: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, UpdateRevision: "new", CurrentRevision: "old"}}}, wantReason: "managed StatefulSet default/restate env A=\"\" does not match cluster \"1\""},
		{name: "zero cluster Resources mismatch", workload: workloadRollout{Kind: "RestateCluster", Resources: kubeResourceRequirements{Limits: map[string]string{"cpu": "1"}}, ManagedWorkloads: []workloadRollout{{Kind: "StatefulSet", Namespace: "default", Name: "restate", Generation: 2, ObservedGeneration: 2, UpdateRevision: "new", CurrentRevision: "old"}}}, wantReason: "managed StatefulSet default/restate resource limit cpu=\"\" does not match cluster \"1\""},
		{name: "deployment rolled out", workload: deployRollout("synapse", synapseImage, 3, 3, 2, 2, 2, 2, false), wantDone: true},
		{name: "deployment with old replicas still present", workload: deployRollout("synapse", synapseImage, 3, 3, 1, 1, 1, 2, false), wantReason: "1/1 updated, 1/1 available, 2 total"},
		{name: "deployment new replica unavailable", workload: deployRollout("synapse", synapseImage, 3, 3, 1, 1, 0, 1, false), wantReason: "1/1 updated, 0/1 available, 1 total"},
		{name: "deployment with stale observed generation", workload: deployRollout("synapse", synapseImage, 4, 3, 1, 1, 1, 1, false), wantReason: "generation 4 not yet observed"},
		{name: "deployment progress deadline exceeded", workload: deployRollout("synapse", synapseImage, 3, 3, 1, 1, 0, 2, true), wantFailed: true, wantReason: "progress deadline exceeded"},
		{name: "deployment deadline from a previous generation is not yet a failure", workload: deployRollout("synapse", synapseImage, 4, 3, 1, 1, 0, 2, true), wantReason: "generation 4 not yet observed"},
		{name: "statefulset rolled out", workload: stsRollout("db", synapseImage, 2, 2, 2, 2, "db-2", "db-2"), wantDone: true},
		{name: "statefulset with old revision pods", workload: stsRollout("db", synapseImage, 2, 2, 2, 2, "db-2", "db-1"), wantReason: "revision db-1 of db-2, 2/2 available"},
		{name: "statefulset unavailable", workload: stsRollout("db", synapseImage, 2, 2, 2, 1, "db-2", "db-2"), wantReason: "revision db-2 of db-2, 1/2 available"},
		{name: "statefulset with stale observed generation", workload: stsRollout("db", synapseImage, 3, 2, 2, 2, "db-2", "db-2"), wantReason: "generation 3 not yet observed"},
		{name: "daemonset rolled out", workload: dsRollout("agent", synapseImage, 5, 5, 3, 3, 3), wantDone: true},
		{name: "daemonset with old pods scheduled", workload: dsRollout("agent", synapseImage, 5, 5, 3, 2, 3), wantReason: "2/3 updated, 3/3 available"},
		{name: "daemonset unavailable", workload: dsRollout("agent", synapseImage, 5, 5, 3, 3, 2), wantReason: "3/3 updated, 2/3 available"},
		{name: "daemonset with stale observed generation", workload: dsRollout("agent", synapseImage, 6, 5, 3, 3, 3), wantReason: "generation 6 not yet observed"},
		{name: "restatedeployment rolled out", workload: restateDeployRollout("restate-worker", synapseImage, 3, 3, 2, 2, 2, "dp_12345", true), wantDone: true},
		{name: "restatedeployment ready replicas unavailable", workload: restateDeployRollout("restate-worker", synapseImage, 3, 3, 2, 1, 2, "dp_12345", true), wantReason: "1/2 ready, 2/2 available, deploymentId=dp_12345, ready=true"},
		{name: "restatedeployment available replicas unavailable", workload: restateDeployRollout("restate-worker", synapseImage, 3, 3, 2, 2, 1, "dp_12345", true), wantReason: "2/2 ready, 1/2 available, deploymentId=dp_12345, ready=true"},
		{name: "restatedeployment missing deploymentId", workload: restateDeployRollout("restate-worker", synapseImage, 3, 3, 2, 2, 2, "", true), wantReason: "2/2 ready, 2/2 available, deploymentId=, ready=true"},
		{name: "restatedeployment ready condition false", workload: restateDeployRollout("restate-worker", synapseImage, 3, 3, 2, 2, 2, "dp_12345", false), wantReason: "2/2 ready, 2/2 available, deploymentId=dp_12345, ready=false"},
		{name: "restatecluster ready condition false", workload: restateClusterRollout("restate", synapseImage, 2, 0, false), wantReason: "ready=false"},
		{name: "restatecluster without managed statefulset pending", workload: restateClusterRollout("restate", synapseImage, 2, 0, true), wantReason: "waiting for managed statefulset"},
		{
			name:       "restatecluster with managed statefulset matching spec rolled out",
			workload:   restateClusterRolloutWithManaged("restate", "docker.io/restatedev/restate:1.6", 2, 0, true, stsRollout("restate", "docker.io/restatedev/restate:1.6", 2, 2, 1, 1, "restate-1", "restate-1")),
			wantDone:   true,
			wantReason: "",
		},
		{
			name:       "restatecluster with managed statefulset image mismatch pending",
			workload:   restateClusterRolloutWithManaged("restate", "docker.io/restatedev/restate:1.6", 2, 0, true, stsRollout("restate", "docker.io/restatedev/restate:1.5", 2, 2, 1, 1, "restate-1", "restate-1")),
			wantReason: "managed StatefulSet default/restate images [docker.io/restatedev/restate:1.5] do not match cluster image docker.io/restatedev/restate:1.6",
		},
		{
			name: "restatecluster with managed statefulset env mismatch pending",
			workload: func() workloadRollout {
				w := restateClusterRolloutWithManaged("restate", "docker.io/restatedev/restate:1.6", 2, 0, true, stsRollout("restate", "docker.io/restatedev/restate:1.6", 2, 2, 1, 1, "restate-1", "restate-1"))
				w.Env = map[string]string{"RESTATE_AUTO_PROVISION": "false"}
				return w
			}(),
			wantReason: "managed StatefulSet default/restate env RESTATE_AUTO_PROVISION=\"\" does not match cluster \"false\"",
		},
		{
			name: "restatecluster with managed statefulset resource mismatch pending",
			workload: func() workloadRollout {
				w := restateClusterRolloutWithManaged("restate", "docker.io/restatedev/restate:1.6", 2, 0, true, stsRollout("restate", "docker.io/restatedev/restate:1.6", 2, 2, 1, 1, "restate-1", "restate-1"))
				w.Resources = kubeResourceRequirements{Limits: map[string]string{"cpu": "1"}}
				return w
			}(),
			wantReason: "managed StatefulSet default/restate resource limit cpu=\"\" does not match cluster \"1\"",
		},
		{
			name: "restatecluster metadata-only change with managed statefulset matching spec is done",
			workload: func() workloadRollout {
				m := stsRollout("restate", "docker.io/restatedev/restate:1.6", 2, 2, 1, 1, "restate-1", "restate-1")
				m.ChangedAt = rolloutClock(10, 0, 0)
				w := restateClusterRolloutWithManaged("restate", "docker.io/restatedev/restate:1.6", 2, 0, true, m)
				w.ChangedAt = rolloutClock(10, 5, 0)
				return w
			}(),
			wantDone: true,
		},
		{
			name:       "restatecluster with managed statefulset rolling out pending",
			workload:   restateClusterRolloutWithManaged("restate", "docker.io/restatedev/restate:1.6", 2, 0, true, stsRollout("restate", "docker.io/restatedev/restate:1.6", 2, 2, 1, 0, "restate-1", "restate-1")),
			wantReason: "managed StatefulSet default/restate: revision restate-1 of restate-1, 0/1 available",
		},
		{
			name:       "restatecluster with managed statefulset unobserved generation pending",
			workload:   restateClusterRolloutWithManaged("restate", "docker.io/restatedev/restate:1.6", 2, 0, true, stsRollout("restate", "docker.io/restatedev/restate:1.6", 3, 2, 1, 1, "restate-1", "restate-1")),
			wantReason: "managed StatefulSet default/restate: generation 3 not yet observed",
		},
		{name: "unsupported kind", workload: workloadRollout{Kind: "Job"}, wantReason: "unsupported workload kind"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			done, failed, reason := workloadRolloutComplete(tc.workload)
			if done != tc.wantDone || failed != tc.wantFailed || reason != tc.wantReason {
				t.Errorf("workloadRolloutComplete() = (%v, %v, %q), want (%v, %v, %q)", done, failed, reason, tc.wantDone, tc.wantFailed, tc.wantReason)
			}
		})
	}
}

// rolloutClock returns 2026-11-08 at hh:mm:ss UTC, the day of the rollout fixtures.
func rolloutClock(hh, mm, ss int) time.Time {
	return time.Date(2026, time.November, 8, hh, mm, ss, 0, time.UTC)
}

// withChange sets when w last changed, its annotations and the ConfigMaps its pod template consumes.
func withChange(w workloadRollout, changedAt time.Time, annotations map[string]string, configMapRefs ...string) workloadRollout {
	w.ChangedAt, w.Annotations, w.ConfigMapRefs = changedAt, annotations, configMapRefs
	return w
}

func TestPRRolloutStatus(t *testing.T) {
	t.Parallel()
	const otherImage = "ghcr.io/azylman/aerial-haro:feedface"
	since := rolloutClock(10, 0, 0)
	auto := map[string]string{"reloader.stakater.com/auto": "true"}
	eriDone := deployRollout("eri", otherImage, 2, 2, 1, 1, 1, 1, false)
	eriRolling := deployRollout("eri", otherImage, 2, 2, 1, 1, 0, 2, false)
	eriConfig := []configMapChange{{Namespace: "default", Name: "eri-config", ChangedAt: rolloutClock(10, 1, 0)}}
	tests := []struct {
		name       string
		workloads  []workloadRollout
		configMaps []configMapChange
		commitSHA  string
		now        time.Time
		wantDone   bool
		wantFailed bool
		wantReason string
	}{
		{name: "changed stopped deployment completes without registration", workloads: []workloadRollout{withChange(restateDeployRollout("worker", otherImage, 2, 2, 0, 0, 0, "", false), rolloutClock(10, 1, 0), nil)}, commitSHA: rolloutSHA, wantDone: true},
		{name: "changed stopped statefulset ignores revisions", workloads: []workloadRollout{withChange(stsRollout("restate", otherImage, 2, 2, 0, 0, "new", "old"), rolloutClock(10, 1, 0), nil)}, commitSHA: rolloutSHA, wantDone: true},
		{name: "changed zero cluster links stopped child", workloads: []workloadRollout{withChange(workloadRollout{Kind: "RestateCluster", Name: "restate", Images: []string{otherImage}}, rolloutClock(10, 1, 0), nil), {Kind: "StatefulSet", Namespace: "restate", Name: "restate", Images: []string{otherImage}, Generation: 2, ObservedGeneration: 2, UpdateRevision: "new", CurrentRevision: "old"}}, commitSHA: rolloutSHA, wantDone: true},

		{name: "changed zero cluster missing child pending", workloads: []workloadRollout{withChange(workloadRollout{Kind: "RestateCluster", Name: "restate"}, rolloutClock(10, 1, 0), nil)}, commitSHA: rolloutSHA, wantReason: "RestateCluster /restate (waiting for managed statefulset)"},

		{name: "no workloads is done", commitSHA: rolloutSHA, wantDone: true},
		{name: "image match pending", workloads: []workloadRollout{deployRollout("synapse", synapseImage, 3, 3, 1, 1, 0, 2, false), dsRollout("agent", synapseImage, 5, 5, 3, 2, 3)}, commitSHA: rolloutSHA, wantReason: "Deployment default/synapse (1/1 updated, 0/1 available, 2 total); DaemonSet kube-system/agent (2/3 updated, 3/3 available)"},
		{name: "image match done", workloads: []workloadRollout{deployRollout("synapse", synapseImage, 3, 3, 1, 1, 1, 1, false), stsRollout("db", "ghcr.io/azylman/aerial-db:"+rolloutSHA+"@sha256:beef", 2, 2, 1, 1, "db-2", "db-2")}, commitSHA: rolloutSHA, wantDone: true},
		{name: "image match failed", workloads: []workloadRollout{deployRollout("haro", otherImage, 2, 2, 1, 0, 0, 1, false), deployRollout("synapse", synapseImage, 3, 3, 1, 1, 0, 2, true)}, commitSHA: rolloutSHA, wantFailed: true, wantReason: "Deployment default/synapse: progress deadline exceeded"},
		{name: "image match on an init container gates", workloads: []workloadRollout{{Kind: "Deployment", Namespace: "default", Name: "eri", Images: []string{"busybox", synapseImage}, Generation: 1, ObservedGeneration: 1, Replicas: 1, StatusReplicas: 1}}, commitSHA: rolloutSHA, wantReason: "Deployment default/eri (0/1 updated, 0/1 available, 1 total)"},
		{name: "manifest-only change pending", workloads: []workloadRollout{withChange(deployRollout("haro", otherImage, 3, 3, 1, 1, 0, 2, false), rolloutClock(10, 1, 0), nil)}, commitSHA: rolloutSHA, wantReason: "Deployment default/haro (1/1 updated, 0/1 available, 2 total)"},
		{name: "manifest-only change with unobserved generation pending", workloads: []workloadRollout{withChange(deployRollout("haro", otherImage, 4, 3, 1, 1, 1, 1, false), rolloutClock(10, 1, 0), nil)}, commitSHA: rolloutSHA, wantReason: "Deployment default/haro (generation 4 not yet observed)"},
		{name: "manifest-only change done", workloads: []workloadRollout{withChange(deployRollout("haro", otherImage, 3, 3, 1, 1, 1, 1, false), rolloutClock(10, 1, 0), nil)}, commitSHA: rolloutSHA, wantDone: true},
		{name: "manifest-only change failed", workloads: []workloadRollout{withChange(deployRollout("haro", otherImage, 3, 3, 1, 1, 0, 2, true), rolloutClock(10, 1, 0), nil)}, commitSHA: rolloutSHA, wantFailed: true, wantReason: "Deployment default/haro: progress deadline exceeded"},
		{name: "untouched broken workload ignored", workloads: []workloadRollout{withChange(deployRollout("haro", otherImage, 2, 2, 1, 0, 0, 2, true), rolloutClock(9, 0, 0), nil), deployRollout("synapse", synapseImage, 3, 3, 1, 1, 1, 1, false)}, commitSHA: rolloutSHA, wantDone: true},
		{name: "change exactly at the skew boundary gates", workloads: []workloadRollout{withChange(deployRollout("haro", otherImage, 3, 3, 1, 1, 0, 2, false), rolloutClock(9, 59, 30), nil)}, commitSHA: rolloutSHA, wantReason: "Deployment default/haro (1/1 updated, 0/1 available, 2 total)"},
		{name: "change just before the skew boundary ignored", workloads: []workloadRollout{withChange(deployRollout("haro", otherImage, 3, 3, 1, 1, 0, 2, true), rolloutClock(9, 59, 29), nil)}, commitSHA: rolloutSHA, wantDone: true},
		{name: "unknown change time and empty commit ignored", workloads: []workloadRollout{deployRollout("cache", "valkey", 1, 1, 1, 0, 0, 1, false)}, wantDone: true},
		{name: "reloader pending restart", workloads: []workloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), auto, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 3, 0), wantReason: "Deployment default/eri (awaiting Reloader restart for ConfigMap default/eri-config)"},
		{name: "reloader restarted and rolling out", workloads: []workloadRollout{withChange(eriRolling, rolloutClock(10, 1, 5), auto, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 3, 0), wantReason: "Deployment default/eri (1/1 updated, 0/1 available, 2 total)"},
		{name: "reloader restarted and rolled out", workloads: []workloadRollout{withChange(eriDone, rolloutClock(10, 1, 0), auto, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 30, 0), wantDone: true},
		{name: "reloader restart timeout fails", workloads: []workloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), auto, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 6, 1), wantFailed: true, wantReason: "Deployment default/eri: not restarted by Reloader within 5m0s of ConfigMap default/eri-config changing"},
		{name: "reloader restart at exactly the timeout still pending", workloads: []workloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), auto, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 6, 0), wantReason: "Deployment default/eri (awaiting Reloader restart for ConfigMap default/eri-config)"},
		{name: "reloader reload annotation names the configmap", workloads: []workloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), map[string]string{"configmap.reloader.stakater.com/reload": "eri-flags, eri-config"})}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 3, 0), wantReason: "Deployment default/eri (awaiting Reloader restart for ConfigMap default/eri-config)"},
		{name: "reloader configmap auto annotation gates", workloads: []workloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), map[string]string{"configmap.reloader.stakater.com/auto": "true"}, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 3, 0), wantReason: "Deployment default/eri (awaiting Reloader restart for ConfigMap default/eri-config)"},
		{name: "reloader auto workload not consuming the configmap ignored", workloads: []workloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), auto, "eri-flags")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 30, 0), wantDone: true},
		{name: "consumer without reloader ignored", workloads: []workloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), nil, "eri-config")}, configMaps: eriConfig, commitSHA: rolloutSHA, now: rolloutClock(10, 30, 0), wantDone: true},
		{name: "configmap in another namespace ignored", workloads: []workloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), auto, "eri-config")}, configMaps: []configMapChange{{Namespace: "apps", Name: "eri-config", ChangedAt: rolloutClock(10, 1, 0)}}, commitSHA: rolloutSHA, now: rolloutClock(10, 30, 0), wantDone: true},
		{name: "configmap changed before the merge ignored", workloads: []workloadRollout{withChange(eriDone, rolloutClock(9, 0, 0), auto, "eri-config")}, configMaps: []configMapChange{{Namespace: "default", Name: "eri-config", ChangedAt: rolloutClock(9, 30, 0)}}, commitSHA: rolloutSHA, now: rolloutClock(10, 30, 0), wantDone: true},
		{
			name: "restatecluster manifest change with managed statefulset pending operator rollout",
			workloads: []workloadRollout{
				withChange(restateClusterRollout("restate", "docker.io/restatedev/restate:1.6", 2, 0, true), rolloutClock(10, 1, 0), nil),
				stsRollout("restate", "docker.io/restatedev/restate:1.5", 1, 1, 1, 1, "restate-1", "restate-1"),
			},
			commitSHA:  rolloutSHA,
			wantReason: "RestateCluster default/restate (managed StatefulSet default/restate images [docker.io/restatedev/restate:1.5] do not match cluster image docker.io/restatedev/restate:1.6)",
		},
		{
			name: "restatecluster manifest change with managed statefulset rolled out is done",
			workloads: []workloadRollout{
				withChange(restateClusterRollout("restate", "docker.io/restatedev/restate:1.6", 2, 0, true), rolloutClock(10, 1, 0), nil),
				withChange(stsRollout("restate", "docker.io/restatedev/restate:1.6", 2, 2, 1, 1, "restate-2", "restate-2"), rolloutClock(10, 1, 5), nil),
			},
			commitSHA: rolloutSHA,
			wantDone:  true,
		},
		{
			name: "cluster-scoped restatecluster links managed statefulset in cluster-named namespace",
			workloads: []workloadRollout{
				withChange(workloadRollout{Kind: "RestateCluster", Name: "restate-marida", Images: []string{"docker.io/restatedev/restate:1.6"}, Generation: 2, Replicas: 1, ReadyConditionTrue: true}, rolloutClock(10, 1, 0), nil),
				{Kind: "StatefulSet", Namespace: "restate-marida", Name: "restate", Images: []string{"docker.io/restatedev/restate:1.6"}, Generation: 2, ObservedGeneration: 2, Replicas: 1, AvailableReplicas: 1, UpdateRevision: "restate-2", CurrentRevision: "restate-2"},
			},
			commitSHA: rolloutSHA,
			wantDone:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			done, failed, reason := prRolloutStatus(prRolloutInput{Workloads: tc.workloads, ConfigMaps: tc.configMaps, CommitSHA: tc.commitSHA, Since: since, Now: tc.now})
			if done != tc.wantDone || failed != tc.wantFailed || reason != tc.wantReason {
				t.Errorf("prRolloutStatus() = (%v, %v, %q), want (%v, %v, %q)", done, failed, reason, tc.wantDone, tc.wantFailed, tc.wantReason)
			}
		})
	}
}
