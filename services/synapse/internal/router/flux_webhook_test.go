package router

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type handleFluxWebhookCase struct {
	name           string
	endpoint       string
	payload        string
	withBus        bool
	busFail        bool
	downstreamFail bool
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
	srv := newTestRouter(Config{SystemChannelID: "123456789012345678"}, nil, nil)
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runHandleFluxWebhookCase(t, tc)
		})
	}
}
