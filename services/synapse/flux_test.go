package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTranslateFluxAlertToDeployEvent(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 6, 18, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		alert      FluxAlertEvent
		wantOK     bool
		wantEvent  string
		wantStatus string
		wantJob    string
		wantRepo   string
		wantSHA    string
		wantDetail string
	}{
		{
			name: "GitRepository NewArtifact maps to sync_success",
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
			wantEvent:  "sync_success",
			wantStatus: "success",
			wantJob:    "git-sync",
			wantRepo:   "azylman/aerial-config",
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
			wantEvent:  "deploy_started",
			wantStatus: "started",
			wantJob:    "webhooks-router",
			wantRepo:   "azylman/aerial",
			wantSHA:    "abcdef1234",
			wantDetail: "reconciliation in progress",
		},
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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
			if got.Timestamp.IsZero() {
				t.Errorf("Timestamp is zero, expected set timestamp")
			}
		})
	}
}

func TestHandleFluxWebhook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		endpoint       string
		payload        string
		withRiver      bool
		riverFail      bool
		downstreamFail bool
		wantStatusCode int
		wantStatus     string
		wantEvent      string
		wantJobName    string
		wantRiverJobs  int
	}{
		{
			name:           "POST /webhooks/flux valid kustomization without river",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"synapse","namespace":"flux-system"},"severity":"info","reason":"ReconciliationSucceeded","message":"reconciliation ok"}`,
			wantStatusCode: http.StatusOK,
			wantStatus:     "accepted",
			wantEvent:      "deploy_success",
			wantJobName:    "synapse",
		},
		{
			name:           "POST /api/webhooks/flux valid gitrepository without river",
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
			name:           "POST /webhooks/flux with river inserts job",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"brain","namespace":"flux-system"},"severity":"info","reason":"Progressing","message":"progressing","metadata":{"revision":"sha1:12345"}}`,
			withRiver:      true,
			wantStatusCode: http.StatusAccepted,
			wantStatus:     "accepted",
			wantEvent:      "deploy_started",
			wantJobName:    "brain",
			wantRiverJobs:  1,
		},
		{
			name:           "POST /webhooks/flux with river failure returns 500",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"brain","namespace":"flux-system"},"severity":"info","reason":"Progressing"}`,
			withRiver:      true,
			riverFail:      true,
			wantStatusCode: http.StatusInternalServerError,
		},
		{
			name:           "POST /webhooks/flux without river error processing event returns 200 with result",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"webhooks-router","namespace":"flux-system"},"severity":"error","reason":"ReconciliationFailed","message":"broken deploy"}`,
			downstreamFail: true,
			wantStatusCode: http.StatusOK,
			wantStatus:     "accepted",
			wantEvent:      "deploy_failed",
			wantJobName:    "webhooks-router",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewRouterServer(Config{SystemChannelID: "123456789012345678"}, nil)
			if tc.downstreamFail {
				srv.SetDispatcher(&mockOutboundDispatcher{directMessageErr: errors.New("dispatch failed")})
			}
			var mockRiver *mockRiverInserter
			if tc.withRiver {
				mockRiver = &mockRiverInserter{}
				if tc.riverFail {
					mockRiver.err = errors.New("river insert failed")
				}
				srv.SetRiverClient(mockRiver)
			}

			req := httptest.NewRequest(http.MethodPost, tc.endpoint, strings.NewReader(tc.payload))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			srv.Routes().ServeHTTP(w, req)

			if w.Code != tc.wantStatusCode {
				t.Fatalf("status code = %d, want %d (body: %s)", w.Code, tc.wantStatusCode, w.Body.String())
			}

			if tc.wantStatus != "" {
				var res map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}
				if gotStatus, _ := res["status"].(string); gotStatus != tc.wantStatus {
					t.Errorf("status = %q, want %q", gotStatus, tc.wantStatus)
				}
				if tc.wantEvent != "" {
					if gotEvent, _ := res["event"].(string); gotEvent != tc.wantEvent {
						t.Errorf("event = %q, want %q", gotEvent, tc.wantEvent)
					}
				}
				if tc.wantJobName != "" {
					if gotJob, _ := res["job_name"].(string); gotJob != tc.wantJobName {
						t.Errorf("job_name = %q, want %q", gotJob, tc.wantJobName)
					}
				}
			}

			if tc.withRiver && mockRiver != nil {
				if len(mockRiver.insertedDeployJobs) != tc.wantRiverJobs {
					t.Errorf("inserted river jobs = %d, want %d", len(mockRiver.insertedDeployJobs), tc.wantRiverJobs)
				}
			}
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
