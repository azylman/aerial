package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
)

func newSequentialMockServer(t *testing.T, expectedPath string, defaultStatus int, statuses []int, body string) *httptest.Server {
	t.Helper()
	var reqIdx int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != expectedPath {
			http.NotFound(w, r)
			return
		}
		status := defaultStatus
		if len(statuses) > 0 {
			if reqIdx < len(statuses) {
				status = statuses[reqIdx]
			} else {
				status = statuses[len(statuses)-1]
			}
		}
		reqIdx++
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(ts.Close)
	return ts
}

func newVanadisTestRegistry(t *testing.T, nilRegistry bool, expectedPath string, defaultStatus int, statuses []int, body string) *PostgresPRRegistry {
	t.Helper()
	if nilRegistry {
		return nil
	}
	ts := newSequentialMockServer(t, expectedPath, defaultStatus, statuses, body)
	reg := NewPostgresPRRegistry(nil, WithVanadisClient(ts.URL, ts.Client()))
	reg.vanadisRetryDelay = 0
	return reg
}

type failVanadisChunkCase struct {
	name         string
	nilRegistry  bool
	repo         string
	prNumber     int
	httpStatus   int
	httpStatuses []int
	httpBody     string
	buildID      string
	chunkID      string
	wantFailed   bool
	wantErr      bool
}

func runFailVanadisChunkDetailsCase(t *testing.T, tc failVanadisChunkCase) {
	t.Helper()
	reg := newVanadisTestRegistry(t, tc.nilRegistry, "/chunks/fail-pr", tc.httpStatus, tc.httpStatuses, tc.httpBody)
	bID, cID, failed, err := reg.FailVanadisChunkDetailsByPR(context.Background(), tc.repo, tc.prNumber)
	if (err != nil) != tc.wantErr {
		t.Fatalf("FailVanadisChunkDetailsByPR() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if failed != tc.wantFailed {
		t.Fatalf("FailVanadisChunkDetailsByPR() failed = %v, want %v", failed, tc.wantFailed)
	}
	if tc.wantFailed && (bID != tc.buildID || cID != tc.chunkID) {
		t.Fatalf("FailVanadisChunkDetailsByPR() got bID=%q, cID=%q; want %q, %q", bID, cID, tc.buildID, tc.chunkID)
	}
}

func TestFailVanadisChunkDetailsByPR(t *testing.T) {
	t.Parallel()

	tests := []failVanadisChunkCase{
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
			name:       "server 400 non-transient error returns error",
			repo:       "azylman/aerial",
			prNumber:   10,
			httpStatus: http.StatusBadRequest,
			httpBody:   `{"error":"bad request"}`,
			wantErr:    true,
		},
		{
			name:         "server retry 502 then 200 succeeds",
			repo:         "azylman/aerial",
			prNumber:     10,
			httpStatuses: []int{http.StatusBadGateway, http.StatusOK},
			httpBody:     `{"build_id":"build-123","chunk_id":"chunk-1","failed":true}`,
			buildID:      "build-123",
			chunkID:      "chunk-1",
			wantFailed:   true,
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
			runFailVanadisChunkDetailsCase(t, tc)
		})
	}
}

func TestFireEventTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		targets          []string
		contextHeader    string
		wantFired        int
		wantHeaderSubstr string
	}{
		{
			name:    "empty targets is no-op",
			targets: nil,
		},
		{
			name:    "whitespace targets is no-op",
			targets: []string{"   ", ""},
		},
		{
			name:             "fires triggers via bus with context header",
			targets:          []string{"azylman/aerial"},
			contextHeader:    "GitHub ci_passed on azylman/aerial",
			wantFired:        1,
			wantHeaderSubstr: "GitHub ci_passed on azylman/aerial",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recBus := &recordingBus{}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.SetEventBus(recBus)

			srv.fireEventTriggers(context.Background(), "github", tc.targets, "ci_passed", tc.contextHeader)

			if recBus.publishCount() != tc.wantFired {
				t.Fatalf("fireEventTriggers() fired %d times, want %d", recBus.publishCount(), tc.wantFired)
			}
			if tc.wantHeaderSubstr != "" && recBus.publishCount() > 0 {
				ev, ok := recBus.lastEvent()
				if !ok || !strings.Contains(ev.ContextHeader, tc.wantHeaderSubstr) {
					t.Fatalf("contextHeader = %q, want substr %q", ev.ContextHeader, tc.wantHeaderSubstr)
				}
			}
		})
	}
}

func TestEvaluateFluxEventTriggers(t *testing.T) {
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
			name:           "sync_success with details fires flux trigger",
			event:          "sync_success",
			repo:           "aerial",
			details:        "synced 5 files",
			wantFired:      true,
			wantTarget:     "azylman/aerial",
			wantEventType:  "sync_success",
			wantPromptPart: "Flux git sync sync_success for azylman/aerial: synced 5 files",
		},
		{
			name:           "sync_failed without details fires flux trigger",
			event:          "sync_failed",
			repo:           "azylman/aerial-config",
			details:        "",
			wantFired:      true,
			wantTarget:     "azylman/aerial-config",
			wantEventType:  "sync_failed",
			wantPromptPart: "Flux git sync sync_failed for azylman/aerial-config",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recBus := &recordingBus{}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.SetEventBus(recBus)

			srv.evaluateFluxEventTriggers(context.Background(), DeployEvent{
				Event:   tc.event,
				Repo:    tc.repo,
				Details: tc.details,
			})

			if !tc.wantFired {
				if recBus.publishCount() != 0 {
					t.Fatalf("expected 0 fire calls, got %d", recBus.publishCount())
				}
				return
			}
			if recBus.publishCount() != 1 {
				t.Fatalf("expected 1 fire call, got %d", recBus.publishCount())
			}
			ev, ok := recBus.lastEvent()
			if !ok {
				t.Fatalf("expected recorded event")
			}
			if ev.Source != "flux" || ev.EventType != tc.wantEventType || len(ev.Targets) != 1 || ev.Targets[0] != tc.wantTarget {
				t.Fatalf("unexpected fire call: %+v, want source=flux target=%s event=%s", ev, tc.wantTarget, tc.wantEventType)
			}
			if !strings.Contains(ev.ContextHeader, tc.wantPromptPart) {
				t.Fatalf("contextHeader %q does not contain %q", ev.ContextHeader, tc.wantPromptPart)
			}
		})
	}
}

type vanadisAdvancementTriggerCase struct {
	name            string
	res             *VanadisAdvancementResult
	wantChunkTarget string
	wantBuildTarget string
	wantChunkFired  bool
	wantBuildFired  bool
}

func runVanadisAdvancementTriggerCase(t *testing.T, tc vanadisAdvancementTriggerCase) {
	t.Helper()
	recBus := &recordingBus{}
	mockDisp := &mockOutboundDispatcher{}
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	srv.SetEventBus(recBus)
	srv.SetDispatcher(mockDisp)

	srv.evaluateVanadisAdvancementTriggers(context.Background(), "azylman/aerial", 42, tc.res)

	var chunkFired, buildFired bool
	recBus.mu.Lock()
	events := append([]pubsub.ExternalEvent(nil), recBus.events...)
	recBus.mu.Unlock()
	for _, ev := range events {
		if ev.Source == "vanadis_chunk" && ev.EventType == "merged" && len(ev.Targets) == 1 && ev.Targets[0] == tc.wantChunkTarget {
			chunkFired = true
		}
		if ev.Source == "vanadis_build" && ev.EventType == "completed" && len(ev.Targets) == 1 && ev.Targets[0] == tc.wantBuildTarget {
			buildFired = true
		}
	}
	if chunkFired != tc.wantChunkFired {
		t.Fatalf("chunkFired = %v, want %v", chunkFired, tc.wantChunkFired)
	}
	if buildFired != tc.wantBuildFired {
		t.Fatalf("buildFired = %v, want %v", buildFired, tc.wantBuildFired)
	}
}

func TestEvaluateVanadisAdvancementTriggers(t *testing.T) {
	t.Parallel()

	tests := []vanadisAdvancementTriggerCase{
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
			runVanadisAdvancementTriggerCase(t, tc)
		})
	}
}

type vanadisFailureTriggerCase struct {
	name           string
	buildID        string
	chunkID        string
	reason         string
	wantChunkFired bool
	wantBuildFired bool
}

func runVanadisFailureTriggerCase(t *testing.T, tc vanadisFailureTriggerCase) {
	t.Helper()
	recBus := &recordingBus{}
	mockDisp := &mockOutboundDispatcher{}
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	srv.SetEventBus(recBus)
	srv.SetDispatcher(mockDisp)

	srv.evaluateVanadisFailureTriggers(context.Background(), tc.buildID, tc.chunkID, tc.reason)

	var chunkFired, buildFired bool
	recBus.mu.Lock()
	events := append([]pubsub.ExternalEvent(nil), recBus.events...)
	recBus.mu.Unlock()
	for _, ev := range events {
		if ev.Source == "vanadis_chunk" && ev.EventType == "failed" {
			chunkFired = true
		}
		if ev.Source == "vanadis_build" && ev.EventType == "failed" {
			buildFired = true
		}
	}
	if chunkFired != tc.wantChunkFired {
		t.Fatalf("chunkFired = %v, want %v", chunkFired, tc.wantChunkFired)
	}
	if buildFired != tc.wantBuildFired {
		t.Fatalf("buildFired = %v, want %v", buildFired, tc.wantBuildFired)
	}
}

func TestEvaluateVanadisFailureTriggers(t *testing.T) {
	t.Parallel()

	tests := []vanadisFailureTriggerCase{
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
			runVanadisFailureTriggerCase(t, tc)
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
			recBus := &recordingBus{}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.SetEventBus(recBus)
			if !tc.nilRegistry {
				srv.SetRegistry(mockReg)
			}
			srv.SetDispatcher(mockDisp)

			err := srv.failVanadisChunk(context.Background(), "azylman/aerial", tc.prNumber, "test_reason")
			if (err != nil) != tc.wantErr {
				t.Fatalf("failVanadisChunk() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantTriggers {
				if recBus.publishCount() != 2 {
					t.Fatalf("expected 2 failure claim calls (chunk + build), got %d", recBus.publishCount())
				}
			} else if !tc.wantErr && !tc.nilRegistry && tc.prNumber > 0 {
				if recBus.publishCount() != 0 {
					t.Fatalf("expected 0 failure claim calls, got %d", recBus.publishCount())
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
			recBus := &recordingBus{}
			mockReg := &mockPRRegistry{}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.SetEventBus(recBus)
			srv.SetRegistry(mockReg)
			srv.SetDispatcher(mockDisp)

			srv.evaluateGitHubEventTriggers(context.Background(), tc.res, []byte(tc.body))

			if !tc.wantFired {
				if recBus.publishCount() != 0 {
					t.Fatalf("expected 0 bus events, got %d", recBus.publishCount())
				}
				return
			}
			if recBus.publishCount() != 1 {
				t.Fatalf("expected 1 bus event, got %d", recBus.publishCount())
			}
			ev, ok := recBus.lastEvent()
			if !ok {
				t.Fatalf("expected recorded bus event")
			}
			if ev.Source != tc.wantSource || ev.EventType != tc.wantEventType {
				t.Fatalf("ev = %+v, want source=%s eventType=%s", ev, tc.wantSource, tc.wantEventType)
			}
			if len(ev.Targets) != len(tc.wantTargets) {
				t.Fatalf("ev.Targets = %v, want %v", ev.Targets, tc.wantTargets)
			}
			for i, tgt := range tc.wantTargets {
				if ev.Targets[i] != tgt {
					t.Fatalf("ev.Targets[%d] = %q, want %q", i, ev.Targets[i], tgt)
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
			name:          "ProcessDeployEvent sync_success fires flux triggers",
			testScenario:  "flux_sync_success",
			wantSource:    "flux",
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
			name:          "advanceVanadisChunk fires vanadis_chunk merged triggers",
			testScenario:  "vanadis_advance",
			wantSource:    "vanadis_chunk",
			wantEventType: "merged",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recBus := &recordingBus{}
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
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.SetEventBus(recBus)
			srv.SetRegistry(mockReg)
			srv.SetDispatcher(mockDisp)

			ctx := context.Background()
			switch tc.testScenario {
			case "flux_sync_success":
				_, _ = srv.ProcessDeployEvent(ctx, DeployEvent{
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
					TargetID: "100000000000000001",
				}, 4)
			case "vanadis_advance":
				_ = srv.advanceVanadisChunk(ctx, "azylman/aerial", 1)
			}

			recBus.mu.Lock()
			events := append([]pubsub.ExternalEvent(nil), recBus.events...)
			recBus.mu.Unlock()

			var found bool
			for _, ev := range events {
				if ev.Source == tc.wantSource && ev.EventType == tc.wantEventType {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("expected bus event for source=%s eventType=%s, events = %+v", tc.wantSource, tc.wantEventType, events)
			}
			if tc.testScenario == "github_pr_closed_unmerged_with_test_merge_sha" {
				for _, ev := range events {
					if ev.Source == "github" && ev.EventType == "pr_merged" {
						t.Fatalf("unexpected bus event for github pr_merged on unmerged PR: %+v", ev)
					}
				}
			}
		})
	}
}

func TestProcessGitHubEvent_TriggersEvaluatedOnlyOnSuccess(t *testing.T) {
	t.Parallel()

	recBus := &recordingBus{}
	mockReg := &mockPRRegistry{
		err: errors.New("database connection lost"),
	}
	mockDisp := &mockOutboundDispatcher{}
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	srv.SetEventBus(recBus)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	ctx := context.Background()
	body := []byte(`{"action":"closed","number":1,"repository":{"full_name":"azylman/aerial"},"pull_request":{"number":1,"merged":true,"merge_commit_sha":"sha-merge-123","head":{"ref":"feat/test","sha":"sha-head-123"}}}`)

	res, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-merged-err", body)
	if err == nil {
		t.Fatalf("expected error from ProcessGitHubEvent, got res=%v", res)
	}

	if recBus.publishCount() != 0 {
		t.Fatalf("expected no bus events on ProcessGitHubEvent error, got %d events: %+v", recBus.publishCount(), recBus.events)
	}
}
