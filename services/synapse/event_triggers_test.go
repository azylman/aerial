package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCheckEventTriggers_HaroHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilRegistry bool
		source      string
		targets     []string
		eventType   string
		httpStatus  int
		httpBody    string
		wantMatched bool
		wantErr     bool
	}{
		{name: "check nil registry returns false", nilRegistry: true, source: "custom_source", targets: []string{"device.alpha"}, eventType: "ready"},
		{name: "check empty targets returns false", source: "custom_source", targets: nil, eventType: "ready"},
		{name: "check 500 error returns error", source: "custom_source", targets: []string{"device.alpha"}, eventType: "ready", httpStatus: http.StatusInternalServerError, httpBody: `{"error":"db"}`, wantErr: true},
		{name: "check matched true", source: "custom_source", targets: []string{"device.alpha"}, eventType: "ready", httpStatus: http.StatusOK, httpBody: `{"status":"ok","matched":true}`, wantMatched: true},
		{name: "check matched false", source: "custom_source", targets: []string{"device.alpha"}, eventType: "ready", httpStatus: http.StatusOK, httpBody: `{"status":"ok","matched":false}`, wantMatched: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reg *PostgresPRRegistry
			if !tc.nilRegistry {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/event-triggers/check" {
						http.NotFound(w, r)
						return
					}
					status := tc.httpStatus
					if status == 0 {
						status = http.StatusOK
					}
					w.WriteHeader(status)
					_, _ = w.Write([]byte(tc.httpBody))
				}))
				defer ts.Close()
				reg = NewPostgresPRRegistry(nil, WithHaroClient(ts.URL, ts.Client()))
			}
			matched, err := reg.CheckEventTriggers(context.Background(), tc.source, tc.targets, tc.eventType)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CheckEventTriggers() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if matched != tc.wantMatched {
				t.Fatalf("CheckEventTriggers() matched = %v, want %v", matched, tc.wantMatched)
			}
		})
	}
}

func TestFireEventTriggers_HaroHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		nilRegistry   bool
		source        string
		targets       []string
		eventType     string
		contextHeader string
		httpStatus    int
		httpBody      string
		wantCount     int
		wantErr       bool
	}{
		{name: "fire nil registry returns nil", nilRegistry: true, source: "github", targets: []string{"azylman/aerial"}, eventType: "ci_passed"},
		{name: "fire empty targets returns nil", source: "github", targets: nil, eventType: "ci_passed"},
		{name: "fire whitespace targets returns nil", source: "github", targets: []string{"   ", ""}, eventType: "ci_passed"},
		{name: "fire empty source returns nil", source: "", targets: []string{"azylman/aerial"}, eventType: "ci_passed"},
		{name: "fire empty eventType returns nil", source: "github", targets: []string{"azylman/aerial"}, eventType: ""},
		{name: "fire server 500 error returns error", source: "github", targets: []string{"azylman/aerial"}, eventType: "ci_passed", httpStatus: http.StatusInternalServerError, httpBody: `{"error":"db error"}`, wantErr: true},
		{name: "fire invalid json returns error", source: "github", targets: []string{"azylman/aerial"}, eventType: "ci_passed", httpStatus: http.StatusOK, httpBody: `{invalid`, wantErr: true},
		{name: "fire successful with multiple rows and context_header", source: "github", targets: []string{"azylman/aerial", "azylman/aerial:main"}, eventType: "pr_merged", contextHeader: "GitHub pr_merged", httpStatus: http.StatusOK, httpBody: `{"status":"ok","fired":2,"triggers":[{"id":"trig-1","channel_id":"1555405874565091380","source":"github","target":"azylman/aerial","event_type":"pr_merged","prompt":"Continue","effort":"high"},{"id":"trig-2","channel_id":"1555405874565091380","source":"github","target":"azylman/aerial:main","event_type":"pr_merged","prompt":"Report","effort":"medium"}]}`, wantCount: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reg *PostgresPRRegistry
			if !tc.nilRegistry {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/event-triggers/fire" {
						http.NotFound(w, r)
						return
					}
					status := tc.httpStatus
					if status == 0 {
						status = http.StatusOK
					}
					w.WriteHeader(status)
					_, _ = w.Write([]byte(tc.httpBody))
				}))
				defer ts.Close()
				reg = NewPostgresPRRegistry(nil, WithHaroClient(ts.URL, ts.Client()))
			}
			got, err := reg.FireEventTriggers(context.Background(), tc.source, tc.targets, tc.eventType, tc.contextHeader)
			if (err != nil) != tc.wantErr {
				t.Fatalf("FireEventTriggers() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("FireEventTriggers() returned %d items, want %d", len(got), tc.wantCount)
			}
		})
	}
}

func TestPostgresPRRegistry_HaroClientOptions(t *testing.T) {
	t.Parallel()
	WithHaroClient("http://custom.haro", nil)(nil)
	var nilReg *PostgresPRRegistry
	if c, u := nilReg.getHaroClient(); c == nil || u != "http://haro.aerial" {
		t.Fatalf("nilReg.getHaroClient() = %v, %q", c, u)
	}
	emptyReg := &PostgresPRRegistry{}
	if c, u := emptyReg.getHaroClient(); c == nil || u != "http://haro.aerial" {
		t.Fatalf("emptyReg.getHaroClient() = %v, %q", c, u)
	}
}

func TestFailVanadisChunkDetailsByPR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilRegistry bool
		repo        string
		prNumber    int
		httpStatus  int
		httpBody    string
		buildID     string
		chunkID     string
		wantFailed  bool
		wantErr     bool
	}{
		{
			name:        "nil registry returns false",
			nilRegistry: true,
			repo:        "azylman/aerial",
			prNumber:    10,
		},
		{
			name:     "empty repo returns false",
			repo:     "",
			prNumber: 10,
		},
		{
			name:     "invalid prNumber returns false",
			repo:     "azylman/aerial",
			prNumber: 0,
		},
		{
			name:       "server 500 error returns error",
			repo:       "azylman/aerial",
			prNumber:   10,
			httpStatus: http.StatusInternalServerError,
			httpBody:   `{"error":"internal err"}`,
			wantErr:    true,
		},
		{
			name:       "invalid json returns error",
			repo:       "azylman/aerial",
			prNumber:   10,
			httpStatus: http.StatusOK,
			httpBody:   `{invalid-json`,
			wantErr:    true,
		},
		{
			name:       "not found / not failed returns false",
			repo:       "azylman/aerial",
			prNumber:   10,
			httpStatus: http.StatusOK,
			httpBody:   `{"failed":false}`,
			wantFailed: false,
		},
		{
			name:       "successful chunk fail updates and returns true",
			repo:       "azylman/aerial",
			prNumber:   10,
			httpStatus: http.StatusOK,
			httpBody:   `{"build_id":"build-123","chunk_id":"chunk-1","failed":true}`,
			buildID:    "build-123",
			chunkID:    "chunk-1",
			wantFailed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reg *PostgresPRRegistry
			if !tc.nilRegistry {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/chunks/fail-pr" {
						http.NotFound(w, r)
						return
					}
					status := tc.httpStatus
					if status == 0 {
						status = http.StatusOK
					}
					w.WriteHeader(status)
					_, _ = w.Write([]byte(tc.httpBody))
				}))
				defer ts.Close()
				reg = NewPostgresPRRegistry(nil, WithVanadisClient(ts.URL, ts.Client()))
			}

			bID, cID, failed, err := reg.FailVanadisChunkDetailsByPR(context.Background(), tc.repo, tc.prNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("FailVanadisChunkDetailsByPR() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if failed != tc.wantFailed {
				t.Fatalf("FailVanadisChunkDetailsByPR() failed = %v, want %v", failed, tc.wantFailed)
			}
			if tc.wantFailed {
				if bID != tc.buildID || cID != tc.chunkID {
					t.Fatalf("FailVanadisChunkDetailsByPR() got bID=%q, cID=%q; want %q, %q", bID, cID, tc.buildID, tc.chunkID)
				}
			}
		})
	}
}

func TestHandleEventsWebhook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		body          string
		withRiver     bool
		riverErr      bool
		checkMatched  bool
		checkErr      bool
		fireErr       bool
		wantStatus    int
		wantRiverJobs int
		wantFired     int
	}{
		{
			name:       "invalid json returns 400",
			body:       `{invalid`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing required fields returns 400",
			body:       `{"source":"custom_source"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:          "check returns 200 and matched false drops event without river enqueue",
			body:          `{"source":"custom_source","target":"device.alpha","event_type":"running"}`,
			withRiver:     true,
			checkMatched:  false,
			wantStatus:    http.StatusOK,
			wantRiverJobs: 0,
		},
		{
			name:          "check returns matched true enqueues into river with 202",
			body:          `{"source":"Custom_Source","target":"device.alpha","event_type":"IDLE","details":"Cycle complete"}`,
			withRiver:     true,
			checkMatched:  true,
			wantStatus:    http.StatusAccepted,
			wantRiverJobs: 1,
		},
		{
			name:          "haro check error fails open and enqueues into river with 202",
			body:          `{"source":"custom_source","targets":["device.alpha"],"event_type":"idle","context_header":"Custom header"}`,
			withRiver:     true,
			checkErr:      true,
			wantStatus:    http.StatusAccepted,
			wantRiverJobs: 1,
		},
		{
			name:         "river insert error returns 500",
			body:         `{"source":"custom_source","target":"device.alpha","event_type":"idle"}`,
			withRiver:    true,
			checkMatched: true,
			riverErr:     true,
			wantStatus:   http.StatusInternalServerError,
		},
		{
			name:         "no river synchronous fire succeeds with 200",
			body:         `{"source":"custom_source","target":"device.alpha","event_type":"idle"}`,
			withRiver:    false,
			checkMatched: true,
			wantStatus:   http.StatusOK,
			wantFired:    1,
		},
		{
			name:         "no river synchronous fire error returns 502",
			body:         `{"source":"custom_source","target":"device.alpha","event_type":"idle"}`,
			withRiver:    false,
			checkMatched: true,
			fireErr:      true,
			wantStatus:   http.StatusBadGateway,
			wantFired:    1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{
				checkTriggersRes: tc.checkMatched,
			}
			if tc.checkErr {
				mockReg.checkTriggersErr = errors.New("haro unreachable")
			}
			if tc.fireErr {
				mockReg.claimTriggersErr = errors.New("haro fire 502")
			}
			mockRiver := &mockRiverInserter{}
			if tc.riverErr {
				mockRiver.err = errors.New("river db down")
			}

			srv := NewRouterServer(Config{}, nil)
			srv.SetRegistry(mockReg)
			if tc.withRiver {
				srv.SetRiverClient(mockRiver)
			}

			req := httptest.NewRequest(http.MethodPost, "/webhooks/events", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			srv.Routes().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("got status %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if len(mockRiver.insertedEventJobs) != tc.wantRiverJobs {
				t.Fatalf("got %d river event jobs, want %d", len(mockRiver.insertedEventJobs), tc.wantRiverJobs)
			}
			if len(mockReg.claimTriggersCalls) != tc.wantFired {
				t.Fatalf("got %d fire calls, want %d", len(mockReg.claimTriggersCalls), tc.wantFired)
			}
		})
	}
}

func TestProcessEventTriggerWebhook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilServer   bool
		nilRegistry bool
		fireErr     bool
		args        EventTriggerWebhookArgs
		wantFired   int
		wantErr     bool
	}{
		{
			name:      "nil server returns nil",
			nilServer: true,
			args:      EventTriggerWebhookArgs{Source: "github", Targets: []string{"azylman/aerial"}, EventType: "ci_passed"},
			wantErr:   false,
		},
		{
			name:        "nil registry returns nil",
			nilRegistry: true,
			args:        EventTriggerWebhookArgs{Source: "github", Targets: []string{"azylman/aerial"}, EventType: "ci_passed"},
			wantErr:     false,
		},
		{
			name: "empty source returns nil",
			args: EventTriggerWebhookArgs{Source: "  ", Targets: []string{"azylman/aerial"}, EventType: "ci_passed"},
		},
		{
			name: "empty event type returns nil",
			args: EventTriggerWebhookArgs{Source: "github", Targets: []string{"azylman/aerial"}, EventType: "  "},
		},
		{
			name: "empty targets returns nil",
			args: EventTriggerWebhookArgs{Source: "github", Targets: []string{"   ", ""}, EventType: "ci_passed"},
		},
		{
			name:      "successful fire returns nil and triggers haro",
			args:      EventTriggerWebhookArgs{Source: "github", Targets: []string{"azylman/aerial", "azylman/aerial"}, EventType: "ci_passed", ContextHeader: "CI passed"},
			wantFired: 1,
		},
		{
			name:      "haro fire error propagated",
			fireErr:   true,
			args:      EventTriggerWebhookArgs{Source: "github", Targets: []string{"azylman/aerial"}, EventType: "ci_passed"},
			wantFired: 1,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{}
			if tc.fireErr {
				mockReg.claimTriggersErr = errors.New("simulated fire error")
			}
			var srv *RouterServer
			if !tc.nilServer {
				srv = NewRouterServer(Config{}, nil)
				if !tc.nilRegistry {
					srv.SetRegistry(mockReg)
				}
			}

			err := srv.ProcessEventTriggerWebhook(context.Background(), tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ProcessEventTriggerWebhook() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if len(mockReg.claimTriggersCalls) != tc.wantFired {
				t.Fatalf("got %d fire calls, want %d", len(mockReg.claimTriggersCalls), tc.wantFired)
			}
		})
	}
}

func TestFireEventTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		nilRegistry      bool
		targets          []string
		claimErr         bool
		contextHeader    string
		wantFired        int
		wantHeaderSubstr string
	}{
		{
			name:        "nil registry is no-op",
			nilRegistry: true,
			targets:     []string{"azylman/aerial"},
		},
		{
			name:    "empty targets is no-op",
			targets: nil,
		},
		{
			name:    "whitespace targets is no-op",
			targets: []string{"   ", ""},
		},
		{
			name:      "fire error is handled gracefully",
			targets:   []string{"azylman/aerial"},
			claimErr:  true,
			wantFired: 1,
		},
		{
			name:             "fires triggers via haro with context header",
			targets:          []string{"azylman/aerial"},
			contextHeader:    "GitHub ci_passed on azylman/aerial",
			wantFired:        1,
			wantHeaderSubstr: "GitHub ci_passed on azylman/aerial",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{}
			if tc.claimErr {
				mockReg.claimTriggersErr = errors.New("simulated fire error")
			}

			srv := NewRouterServer(Config{}, nil)
			if !tc.nilRegistry {
				srv.SetRegistry(mockReg)
			}

			srv.fireEventTriggers(context.Background(), "github", tc.targets, "ci_passed", tc.contextHeader)

			if len(mockReg.claimTriggersCalls) != tc.wantFired {
				t.Fatalf("fireEventTriggers() fired %d times, want %d", len(mockReg.claimTriggersCalls), tc.wantFired)
			}
			if tc.wantHeaderSubstr != "" && len(mockReg.claimTriggersCalls) > 0 {
				if !strings.Contains(mockReg.claimTriggersCalls[0].contextHeader, tc.wantHeaderSubstr) {
					t.Fatalf("contextHeader = %q, want substr %q", mockReg.claimTriggersCalls[0].contextHeader, tc.wantHeaderSubstr)
				}
			}
		})
	}
}

func TestEvaluateHangarEventTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		event          string
		repo           string
		details        string
		wantFired      bool
		wantTarget     string
		wantEventType  string
		wantPromptPart string
	}{
		{
			name:      "unsupported event ignored",
			event:     "deploy_started",
			repo:      "azylman/aerial",
			wantFired: false,
		},
		{
			name:      "empty repo ignored",
			event:     "sync_success",
			repo:      "",
			wantFired: false,
		},
		{
			name:           "sync_success with details fires hangar trigger",
			event:          "sync_success",
			repo:           "aerial",
			details:        "synced 5 files",
			wantFired:      true,
			wantTarget:     "azylman/aerial",
			wantEventType:  "sync_success",
			wantPromptPart: "Hangar git sync sync_success for azylman/aerial: synced 5 files",
		},
		{
			name:           "sync_failed without details fires hangar trigger",
			event:          "sync_failed",
			repo:           "azylman/aerial-config",
			details:        "",
			wantFired:      true,
			wantTarget:     "azylman/aerial-config",
			wantEventType:  "sync_failed",
			wantPromptPart: "Hangar git sync sync_failed for azylman/aerial-config",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{}
			srv := NewRouterServer(Config{}, nil)
			srv.SetRegistry(mockReg)

			srv.evaluateHangarEventTriggers(context.Background(), HangarDeployEvent{
				Event:   tc.event,
				Repo:    tc.repo,
				Details: tc.details,
			})

			if !tc.wantFired {
				if len(mockReg.claimTriggersCalls) != 0 {
					t.Fatalf("expected 0 fire calls, got %d", len(mockReg.claimTriggersCalls))
				}
				return
			}
			if len(mockReg.claimTriggersCalls) != 1 {
				t.Fatalf("expected 1 fire call, got %d", len(mockReg.claimTriggersCalls))
			}
			call := mockReg.claimTriggersCalls[0]
			if call.source != "hangar" || call.eventType != tc.wantEventType || len(call.targets) != 1 || call.targets[0] != tc.wantTarget {
				t.Fatalf("unexpected fire call: %+v, want source=hangar target=%s event=%s", call, tc.wantTarget, tc.wantEventType)
			}
			if !strings.Contains(call.contextHeader, tc.wantPromptPart) {
				t.Fatalf("contextHeader %q does not contain %q", call.contextHeader, tc.wantPromptPart)
			}
		})
	}
}

func TestEvaluateNomadEventTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		jobID         string
		eventType     string
		refID         string
		details       string
		wantFired     bool
		wantTarget    string
		wantEventType string
	}{
		{
			name:      "empty jobID ignored",
			jobID:     "   ",
			eventType: "deploy_success",
			wantFired: false,
		},
		{
			name:          "deploy_success normalizes and fires nomad trigger",
			jobID:         " Brain ",
			eventType:     "deploy_success",
			refID:         "dep-123",
			details:       "",
			wantFired:     true,
			wantTarget:    "brain",
			wantEventType: "deploy_success",
		},
		{
			name:          "deploy_failed with details fires nomad trigger",
			jobID:         "Synapse",
			eventType:     "deploy_failed",
			refID:         "alloc-456",
			details:       "crashed with oom",
			wantFired:     true,
			wantTarget:    "synapse",
			wantEventType: "deploy_failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{
				claimTriggersRes: []ClaimedEventTrigger{
					{
						ID:        "nomad-trig",
						ChannelID: "1555405874565091380",
						Source:    "nomad",
						Target:    tc.wantTarget,
						EventType: tc.wantEventType,
						Prompt:    "Nomad event",
					},
				},
			}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(Config{}, nil)
			srv.SetRegistry(mockReg)
			srv.SetDispatcher(mockDisp)

			srv.evaluateNomadEventTriggers(context.Background(), tc.jobID, tc.eventType, tc.refID, tc.details)

			if !tc.wantFired {
				if len(mockReg.claimTriggersCalls) != 0 {
					t.Fatalf("expected 0 claim calls, got %d", len(mockReg.claimTriggersCalls))
				}
				return
			}
			if len(mockReg.claimTriggersCalls) != 1 {
				t.Fatalf("expected 1 claim call, got %d", len(mockReg.claimTriggersCalls))
			}
			call := mockReg.claimTriggersCalls[0]
			if call.source != "nomad" || call.eventType != tc.wantEventType || len(call.targets) != 1 || call.targets[0] != tc.wantTarget {
				t.Fatalf("unexpected claim call: %+v", call)
			}
		})
	}
}

func TestEvaluateVanadisAdvancementTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		res             *VanadisAdvancementResult
		wantChunkTarget string
		wantBuildTarget string
		wantChunkFired  bool
		wantBuildFired  bool
	}{
		{
			name: "nil result is no-op",
			res:  nil,
		},
		{
			name: "unmatched result is no-op",
			res: &VanadisAdvancementResult{
				Matched:  false,
				Advanced: false,
			},
		},
		{
			name: "not advanced is no-op",
			res: &VanadisAdvancementResult{
				Matched:  true,
				Advanced: false,
			},
		},
		{
			name: "advanced with chunk resolved from Chunks slice",
			res: &VanadisAdvancementResult{
				Matched:          true,
				Advanced:         true,
				BuildID:          "build-abc",
				MergedChunkIndex: 1,
				Chunks: []VanadisChunkSummary{
					{ChunkIndex: 0, ID: "build-abc-chunk-1"},
					{ChunkIndex: 1, ID: "build-abc-chunk-custom-2"},
				},
			},
			wantChunkFired:  true,
			wantChunkTarget: "build-abc-chunk-custom-2",
		},
		{
			name: "advanced with chunk fallback from index",
			res: &VanadisAdvancementResult{
				Matched:          true,
				Advanced:         true,
				BuildID:          "build-xyz",
				MergedChunkIndex: 2,
				Chunks:           nil,
			},
			wantChunkFired:  true,
			wantChunkTarget: "build-xyz-chunk-3",
		},
		{
			name: "build completed fires both chunk and build triggers",
			res: &VanadisAdvancementResult{
				Matched:          true,
				Advanced:         true,
				BuildCompleted:   true,
				BuildID:          "build-full",
				MergedChunkIndex: 0,
				Chunks: []VanadisChunkSummary{
					{ChunkIndex: 0, ID: "build-full-chunk-1"},
				},
			},
			wantChunkFired:  true,
			wantChunkTarget: "build-full-chunk-1",
			wantBuildFired:  true,
			wantBuildTarget: "build-full",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(Config{}, nil)
			srv.SetRegistry(mockReg)
			srv.SetDispatcher(mockDisp)

			srv.evaluateVanadisAdvancementTriggers(context.Background(), "azylman/aerial", 42, tc.res)

			var chunkFired, buildFired bool
			for _, call := range mockReg.claimTriggersCalls {
				if call.source == "vanadis_chunk" && call.eventType == "merged" && len(call.targets) == 1 && call.targets[0] == tc.wantChunkTarget {
					chunkFired = true
				}
				if call.source == "vanadis_build" && call.eventType == "completed" && len(call.targets) == 1 && call.targets[0] == tc.wantBuildTarget {
					buildFired = true
				}
			}
			if chunkFired != tc.wantChunkFired {
				t.Fatalf("chunkFired = %v, want %v", chunkFired, tc.wantChunkFired)
			}
			if buildFired != tc.wantBuildFired {
				t.Fatalf("buildFired = %v, want %v", buildFired, tc.wantBuildFired)
			}
		})
	}
}

func TestEvaluateVanadisFailureTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		buildID        string
		chunkID        string
		reason         string
		wantChunkFired bool
		wantBuildFired bool
	}{
		{
			name:           "both empty is no-op",
			buildID:        "",
			chunkID:        "",
			wantChunkFired: false,
			wantBuildFired: false,
		},
		{
			name:           "chunkID only fires chunk failure",
			chunkID:        "build-1-chunk-2",
			reason:         "ci failed",
			wantChunkFired: true,
			wantBuildFired: false,
		},
		{
			name:           "buildID only fires build failure",
			buildID:        "build-1",
			reason:         "build cancelled",
			wantChunkFired: false,
			wantBuildFired: true,
		},
		{
			name:           "both IDs fire both failure triggers",
			buildID:        "build-1",
			chunkID:        "build-1-chunk-1",
			reason:         "remediation cap reached",
			wantChunkFired: true,
			wantBuildFired: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(Config{}, nil)
			srv.SetRegistry(mockReg)
			srv.SetDispatcher(mockDisp)

			srv.evaluateVanadisFailureTriggers(context.Background(), tc.buildID, tc.chunkID, tc.reason)

			var chunkFired, buildFired bool
			for _, call := range mockReg.claimTriggersCalls {
				if call.source == "vanadis_chunk" && call.eventType == "failed" {
					chunkFired = true
				}
				if call.source == "vanadis_build" && call.eventType == "failed" {
					buildFired = true
				}
			}
			if chunkFired != tc.wantChunkFired {
				t.Fatalf("chunkFired = %v, want %v", chunkFired, tc.wantChunkFired)
			}
			if buildFired != tc.wantBuildFired {
				t.Fatalf("buildFired = %v, want %v", buildFired, tc.wantBuildFired)
			}
		})
	}
}

func TestFailVanadisChunk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		nilRegistry  bool
		prNumber     int
		failErr      bool
		failed       bool
		buildID      string
		chunkID      string
		wantTriggers bool
		wantErr      bool
	}{
		{
			name:        "nil registry is no-op",
			nilRegistry: true,
			prNumber:    10,
		},
		{
			name:     "invalid prNumber is no-op",
			prNumber: 0,
		},
		{
			name:     "registry error returns error",
			prNumber: 10,
			failErr:  true,
			wantErr:  true,
		},
		{
			name:         "failed is false does not fire triggers",
			prNumber:     10,
			failed:       false,
			wantTriggers: false,
		},
		{
			name:         "failed is true fires triggers",
			prNumber:     10,
			failed:       true,
			buildID:      "build-101",
			chunkID:      "chunk-101",
			wantTriggers: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{
				failVanadisRes:     tc.failed,
				failVanadisBuildID: tc.buildID,
				failVanadisChunkID: tc.chunkID,
			}
			if tc.failErr {
				mockReg.failVanadisErr = errors.New("simulated fail err")
			}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(Config{}, nil)
			if !tc.nilRegistry {
				srv.SetRegistry(mockReg)
			}
			srv.SetDispatcher(mockDisp)

			err := srv.failVanadisChunk(context.Background(), "azylman/aerial", tc.prNumber, "test_reason")
			if (err != nil) != tc.wantErr {
				t.Fatalf("failVanadisChunk() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantTriggers {
				if len(mockReg.claimTriggersCalls) != 2 {
					t.Fatalf("expected 2 failure claim calls (chunk + build), got %d", len(mockReg.claimTriggersCalls))
				}
			} else if !tc.wantErr && !tc.nilRegistry && tc.prNumber > 0 {
				if len(mockReg.claimTriggersCalls) != 0 {
					t.Fatalf("expected 0 failure claim calls, got %d", len(mockReg.claimTriggersCalls))
				}
			}
		})
	}
}

func TestExtractCheckRunHeadBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantBranch string
	}{
		{
			name:       "empty body returns empty",
			body:       "",
			wantBranch: "",
		},
		{
			name:       "invalid json returns empty",
			body:       "{invalid",
			wantBranch: "",
		},
		{
			name:       "valid check_run body extracts branch with refs/heads/ stripped",
			body:       `{"check_run":{"check_suite":{"head_branch":"refs/heads/feat/awesome-feature"}}}`,
			wantBranch: "feat/awesome-feature",
		},
		{
			name:       "valid check_run body extracts bare branch",
			body:       `{"check_run":{"check_suite":{"head_branch":"main"}}}`,
			wantBranch: "main",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractCheckRunHeadBranch([]byte(tc.body))
			if got != tc.wantBranch {
				t.Fatalf("extractCheckRunHeadBranch() = %q, want %q", got, tc.wantBranch)
			}
		})
	}
}

func TestResolveGitHubTriggerEventType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		res       *ResolvedGitHubEvent
		wantEvent string
	}{
		{
			name:      "nil res returns empty",
			res:       nil,
			wantEvent: "",
		},
		{
			name: "pull_request closed with merged true returns pr_merged",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				Merged:   true,
				MergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
			},
			wantEvent: "pr_merged",
		},
		{
			name: "pull_request closed unmerged with merge_sha returns empty",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				Merged:   false,
				MergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
			},
			wantEvent: "",
		},
		{
			name: "pull_request closed without merge_sha returns empty",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				Merged:   false,
				MergeSHA: "",
			},
			wantEvent: "",
		},
		{
			name: "pull_request opened returns empty",
			res: &ResolvedGitHubEvent{
				Event:  "pull_request",
				Action: "opened",
			},
			wantEvent: "",
		},
		{
			name: "check_run completed success returns ci_passed",
			res: &ResolvedGitHubEvent{
				Event:      "check_run",
				Action:     "completed",
				Conclusion: "success",
			},
			wantEvent: "ci_passed",
		},
		{
			name: "check_run completed failure returns ci_failed",
			res: &ResolvedGitHubEvent{
				Event:      "check_run",
				Action:     "completed",
				Conclusion: "failure",
			},
			wantEvent: "ci_failed",
		},
		{
			name: "check_run completed timed_out returns ci_failed",
			res: &ResolvedGitHubEvent{
				Event:      "check_run",
				Action:     "completed",
				Conclusion: "timed_out",
			},
			wantEvent: "ci_failed",
		},
		{
			name: "check_run in_progress returns empty",
			res: &ResolvedGitHubEvent{
				Event:  "check_run",
				Action: "in_progress",
			},
			wantEvent: "",
		},
		{
			name: "workflow_run completed success returns ci_passed",
			res: &ResolvedGitHubEvent{
				Event:      "workflow_run",
				Action:     "completed",
				Conclusion: "success",
			},
			wantEvent: "ci_passed",
		},
		{
			name: "workflow_run completed cancelled returns empty",
			res: &ResolvedGitHubEvent{
				Event:      "workflow_run",
				Action:     "completed",
				Conclusion: "cancelled",
			},
			wantEvent: "",
		},
		{
			name: "ping event returns empty",
			res: &ResolvedGitHubEvent{
				Event: "ping",
			},
			wantEvent: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := resolveGitHubTriggerEventType(tc.res)
			if got != tc.wantEvent {
				t.Fatalf("resolveGitHubTriggerEventType() = %q, want %q", got, tc.wantEvent)
			}
		})
	}
}

func TestEvaluateGitHubEventTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		res           *ResolvedGitHubEvent
		body          string
		wantFired     bool
		wantSource    string
		wantEventType string
		wantTargets   []string
	}{
		{
			name:      "nil res is no-op",
			res:       nil,
			wantFired: false,
		},
		{
			name: "empty repo is no-op",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				MergeSHA: "123",
				Repo:     "",
			},
			wantFired: false,
		},
		{
			name: "unsupported event type is no-op",
			res: &ResolvedGitHubEvent{
				Event: "ping",
				Repo:  "azylman/aerial",
			},
			wantFired: false,
		},
		{
			name: "pull_request closed with branch builds repo and branch targets",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				Merged:   true,
				MergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
				Repo:     "azylman/aerial",
				Branch:   "refs/heads/feat/trigger-eval",
			},
			wantFired:     true,
			wantSource:    "github",
			wantEventType: "pr_merged",
			wantTargets:   []string{"azylman/aerial", "azylman/aerial:feat/trigger-eval"},
		},
		{
			name: "pull_request closed unmerged with test merge_sha does not fire",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				Merged:   false,
				MergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
				Repo:     "azylman/aerial",
				Branch:   "refs/heads/feat/trigger-eval",
			},
			wantFired: false,
		},
		{
			name: "check_run extracts branch from body when res.Branch is empty",
			res: &ResolvedGitHubEvent{
				Event:      "check_run",
				Action:     "completed",
				Conclusion: "success",
				Repo:       "aerial",
				Branch:     "",
			},
			body:          `{"check_run":{"check_suite":{"head_branch":"refs/heads/feat/auto-extract"}}}`,
			wantFired:     true,
			wantSource:    "github",
			wantEventType: "ci_passed",
			wantTargets:   []string{"azylman/aerial", "azylman/aerial:feat/auto-extract"},
		},
		{
			name: "workflow_run completed failure with bare repo",
			res: &ResolvedGitHubEvent{
				Event:      "workflow_run",
				Action:     "completed",
				Conclusion: "failure",
				Repo:       "azylman/aerial",
				Branch:     "",
			},
			wantFired:     true,
			wantSource:    "github",
			wantEventType: "ci_failed",
			wantTargets:   []string{"azylman/aerial"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(Config{}, nil)
			srv.SetRegistry(mockReg)
			srv.SetDispatcher(mockDisp)

			srv.evaluateGitHubEventTriggers(context.Background(), tc.res, []byte(tc.body))

			if !tc.wantFired {
				if len(mockReg.claimTriggersCalls) != 0 {
					t.Fatalf("expected 0 claim calls, got %d", len(mockReg.claimTriggersCalls))
				}
				return
			}
			if len(mockReg.claimTriggersCalls) != 1 {
				t.Fatalf("expected 1 claim call, got %d", len(mockReg.claimTriggersCalls))
			}
			call := mockReg.claimTriggersCalls[0]
			if call.source != tc.wantSource || call.eventType != tc.wantEventType {
				t.Fatalf("call = %+v, want source=%s eventType=%s", call, tc.wantSource, tc.wantEventType)
			}
			if len(call.targets) != len(tc.wantTargets) {
				t.Fatalf("call.targets = %v, want %v", call.targets, tc.wantTargets)
			}
			for i, tgt := range tc.wantTargets {
				if call.targets[i] != tgt {
					t.Fatalf("call.targets[%d] = %q, want %q", i, call.targets[i], tgt)
				}
			}
		})
	}
}

func TestIntegrations_EventTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		testScenario  string
		wantSource    string
		wantEventType string
	}{
		{
			name:          "ProcessHangarEvent sync_success fires hangar triggers",
			testScenario:  "hangar_sync_success",
			wantSource:    "hangar",
			wantEventType: "sync_success",
		},
		{
			name:          "ProcessGitHubEvent pr closed merged fires github triggers",
			testScenario:  "github_pr_merged",
			wantSource:    "github",
			wantEventType: "pr_merged",
		},
		{
			name:          "ProcessGitHubEvent closed unmerged fires vanadis failure triggers",
			testScenario:  "github_pr_closed_unmerged",
			wantSource:    "vanadis_chunk",
			wantEventType: "failed",
		},
		{
			name:          "ProcessGitHubEvent closed unmerged with test merge sha fires vanadis failure triggers but not github merged",
			testScenario:  "github_pr_closed_unmerged_with_test_merge_sha",
			wantSource:    "vanadis_chunk",
			wantEventType: "failed",
		},
		{
			name:          "notifyRemediationCap fires vanadis failure triggers",
			testScenario:  "notify_remediation_cap",
			wantSource:    "vanadis_chunk",
			wantEventType: "failed",
		},
		{
			name:          "HandleNomadJobSuccess fires nomad deploy_success triggers",
			testScenario:  "nomad_success",
			wantSource:    "nomad",
			wantEventType: "deploy_success",
		},
		{
			name:          "HandleNomadJobFailure fires nomad deploy_failed triggers",
			testScenario:  "nomad_failure",
			wantSource:    "nomad",
			wantEventType: "deploy_failed",
		},
		{
			name:          "advanceVanadisChunk fires vanadis_chunk merged triggers",
			testScenario:  "vanadis_advance",
			wantSource:    "vanadis_chunk",
			wantEventType: "merged",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{
				failVanadisRes:     true,
				failVanadisBuildID: "build-integ-1",
				failVanadisChunkID: "chunk-integ-1",
				advanceVanadisRes: &VanadisAdvancementResult{
					Matched:          true,
					Advanced:         true,
					BuildID:          "build-integ-1",
					MergedChunkIndex: 0,
					Chunks: []VanadisChunkSummary{
						{ChunkIndex: 0, ID: "chunk-integ-1"},
					},
				},
			}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(Config{}, nil)
			srv.SetRegistry(mockReg)
			srv.SetDispatcher(mockDisp)

			ctx := context.Background()
			switch tc.testScenario {
			case "hangar_sync_success":
				_, _ = srv.ProcessHangarEvent(ctx, HangarDeployEvent{
					Event: "sync_success",
					Repo:  "azylman/aerial",
				})
			case "github_pr_merged":
				body := []byte(`{"action":"closed","number":1,"repository":{"full_name":"azylman/aerial"},"pull_request":{"number":1,"merged":true,"merge_commit_sha":"sha-merge-123","head":{"ref":"feat/test","sha":"sha-head-123"}}}`)
				_, _ = srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-merged", body)
			case "github_pr_closed_unmerged":
				body := []byte(`{"action":"closed","number":1,"repository":{"full_name":"azylman/aerial"},"pull_request":{"number":1,"merged":false,"head":{"ref":"feat/test","sha":"sha-head-123"}}}`)
				_, _ = srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-closed", body)
			case "github_pr_closed_unmerged_with_test_merge_sha":
				body := []byte(`{"action":"closed","number":1,"repository":{"full_name":"azylman/aerial"},"pull_request":{"number":1,"merged":false,"merge_commit_sha":"test-merge-sha-123","head":{"ref":"feat/test","sha":"sha-head-123"}}}`)
				_, _ = srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-closed-test-merge", body)
			case "notify_remediation_cap":
				srv.notifyRemediationCap(ctx, &ResolvedGitHubEvent{
					Repo:     "azylman/aerial",
					PRNumber: 1,
					TargetID: "1555405874565091380",
				}, 4)
			case "nomad_success":
				_ = srv.HandleNomadJobSuccess(ctx, "brain", "dep-succ")
			case "nomad_failure":
				_ = srv.HandleNomadJobFailure(ctx, "brain", "dep-fail", "failed", "oom")
			case "vanadis_advance":
				_ = srv.advanceVanadisChunk(ctx, "azylman/aerial", 1)
			}

			var found bool
			for _, call := range mockReg.claimTriggersCalls {
				if call.source == tc.wantSource && call.eventType == tc.wantEventType {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("expected claim call for source=%s eventType=%s, calls = %+v", tc.wantSource, tc.wantEventType, mockReg.claimTriggersCalls)
			}
			if tc.testScenario == "github_pr_closed_unmerged_with_test_merge_sha" {
				for _, call := range mockReg.claimTriggersCalls {
					if call.source == "github" && call.eventType == "pr_merged" {
						t.Fatalf("unexpected claim call for github pr_merged on unmerged PR: %+v", call)
					}
				}
			}
		})
	}
}

func TestProcessGitHubEvent_TriggersEvaluatedOnlyOnSuccess(t *testing.T) {
	t.Parallel()

	mockReg := &mockPRRegistry{
		err: errors.New("database connection lost"),
	}
	mockDisp := &mockOutboundDispatcher{}
	srv := NewRouterServer(Config{}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	ctx := context.Background()
	body := []byte(`{"action":"closed","number":1,"repository":{"full_name":"azylman/aerial"},"pull_request":{"number":1,"merged":true,"merge_commit_sha":"sha-merge-123","head":{"ref":"feat/test","sha":"sha-head-123"}}}`)

	res, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-merged-err", body)
	if err == nil {
		t.Fatalf("expected error from ProcessGitHubEvent, got res=%v", res)
	}

	if len(mockReg.claimTriggersCalls) != 0 {
		t.Fatalf("expected no claimTriggersCalls on ProcessGitHubEvent error, got %d calls: %+v", len(mockReg.claimTriggersCalls), mockReg.claimTriggersCalls)
	}
}
