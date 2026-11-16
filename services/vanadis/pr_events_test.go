package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

type recordedRequest struct {
	Path string
	Body map[string]any
	Raw  []byte
}

type eventRecordingBus struct {
	pubsub.Bus

	mu     sync.Mutex
	events []pubsub.ExternalEvent
}

func (b *eventRecordingBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	if strings.HasPrefix(subject, "aerial.events.") {
		if ev, ok := data.(pubsub.ExternalEvent); ok {
			b.mu.Lock()
			b.events = append(b.events, ev)
			b.mu.Unlock()
		}
	}
	return b.Bus.Publish(ctx, subject, msgID, data)
}

func (b *eventRecordingBus) getEvents() []pubsub.ExternalEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]pubsub.ExternalEvent(nil), b.events...)
}

func setupTestServer(t *testing.T) (*VanadisServer, *memoryVanadisStore, *eventRecordingBus, *[]recordedRequest) {
	t.Helper()
	var reqs []recordedRequest
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		reqs = append(reqs, recordedRequest{Path: r.URL.Path, Body: body})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	t.Cleanup(ts.Close)

	store := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	recBus := &eventRecordingBus{Bus: memBus}

	srv := NewVanadisServer(Config{
		BrainURL:     ts.URL,
		EriURL:       ts.URL,
		GitHubAPIURL: ts.URL,
	}, store, ts.Client())
	srv.restateCfg.RestateIngressURL = ts.URL
	srv.SetBus(recBus)

	return srv, store, recBus, &reqs
}

func TestHandlePRLifecycleEvent_Validation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv, _, _, _ := setupTestServer(t)

	// nil server or store
	nilSrv := &VanadisServer{}
	assert.Error(t, nilSrv.HandlePRLifecycleEvent(ctx, &memoryMsg{data: []byte(`{}`)}))

	// invalid JSON
	assert.NoError(t, srv.HandlePRLifecycleEvent(ctx, &memoryMsg{data: []byte(`bad-json`)}))

	// empty repo or pr number <= 0
	msgEmptyRepo, _ := json.Marshal(pubsub.PRLifecycleEvent{Event: "opened", Repo: "", PRNumber: 10})
	assert.NoError(t, srv.HandlePRLifecycleEvent(ctx, &memoryMsg{data: msgEmptyRepo}))

	msgZeroPR, _ := json.Marshal(pubsub.PRLifecycleEvent{Event: "opened", Repo: "azylman/aerial", PRNumber: 0})
	assert.NoError(t, srv.HandlePRLifecycleEvent(ctx, &memoryMsg{data: msgZeroPR}))
}

func TestHandlePROpened(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("untracked PR auto-creates 1-chunk build", func(t *testing.T) {
		t.Parallel()
		srv, store, _, _ := setupTestServer(t)
		ev := pubsub.PRLifecycleEvent{
			Event:    "opened",
			Repo:     "azylman/aerial",
			PRNumber: 123,
			Branch:   "feat/cool",
			HeadSHA:  "sha123",
			Title:    "Cool feature",
			TargetID: "123456789012345678",
		}

		err := srv.ProcessPRLifecycleEvent(ctx, ev)
		require.NoError(t, err)

		lookup, err := store.GetChunkByPR(ctx, "azylman/aerial", 123)
		require.NoError(t, err)
		require.True(t, lookup.Matched)

		chunk, build, err := store.GetChunk(ctx, lookup.ChunkID)
		require.NoError(t, err)
		assert.Equal(t, 0, build.Tier)
		assert.Equal(t, "123456789012345678", build.ThreadID)
		assert.False(t, build.Quiet)
		assert.Equal(t, "pr_opened", chunk.Status)
		assert.Equal(t, 123, chunk.PRNumber)
	})

	t.Run("untracked dependabot PR creates quiet build", func(t *testing.T) {
		t.Parallel()
		srv, store, _, _ := setupTestServer(t)
		ev := pubsub.PRLifecycleEvent{
			Event:    "opened",
			Repo:     "azylman/aerial",
			PRNumber: 124,
			Branch:   "dependabot/go_modules/pkg-1.2.3",
			TargetID: "123456789012345678",
		}

		err := srv.ProcessPRLifecycleEvent(ctx, ev)
		require.NoError(t, err)

		lookup, err := store.GetChunkByPR(ctx, "azylman/aerial", 124)
		require.NoError(t, err)
		require.True(t, lookup.Matched)

		_, build, err := store.GetChunk(ctx, lookup.ChunkID)
		require.NoError(t, err)
		assert.True(t, build.Quiet)
	})

	t.Run("untracked PR without valid TargetID is skipped", func(t *testing.T) {
		t.Parallel()
		srv, store, _, _ := setupTestServer(t)
		ev := pubsub.PRLifecycleEvent{
			Event:    "opened",
			Repo:     "azylman/aerial",
			PRNumber: 125,
			TargetID: "not-a-snowflake",
		}

		err := srv.ProcessPRLifecycleEvent(ctx, ev)
		require.NoError(t, err)

		lookup, err := store.GetChunkByPR(ctx, "azylman/aerial", 125)
		require.NoError(t, err)
		assert.False(t, lookup.Matched)
	})

	t.Run("already tracked PR coalesces widget without duplicate build", func(t *testing.T) {
		t.Parallel()
		srv, store, _, _ := setupTestServer(t)
		build := mkBuild("b-exist", "123456789012345678", "Build", "Obj", "running")
		chunk := mkChunk("c-exist", "b-exist", 0, "Chunk", "Instr", "medium", "pr_opened")
		chunk.PRNumber = 200
		chunk.Repo = "azylman/aerial"
		require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

		ev := pubsub.PRLifecycleEvent{
			Event:    "opened",
			Repo:     "azylman/aerial",
			PRNumber: 200,
			TargetID: "123456789012345678",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

		builds, _ := store.ListBuilds(ctx, 10)
		assert.Len(t, builds, 1)
	})
}

func TestHandlePRSynchronize(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv, store, _, _ := setupTestServer(t)

	build := mkBuild("b-sync", "123456789012345678", "Build", "Obj", "failed")
	chunk := mkChunk("c-sync", "b-sync", 0, "Chunk", "Instr", "medium", "failed")
	chunk.PRNumber = 300
	chunk.Repo = "azylman/aerial"
	chunk.CIFailCount = 2
	chunk.Banner = "some failure"
	chunk.CIChecks = map[string]string{"lint": "failed"}
	require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

	ev := pubsub.PRLifecycleEvent{
		Event:    "synchronize",
		Repo:     "azylman/aerial",
		PRNumber: 300,
	}
	require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

	updatedChunk, updatedBuild, err := store.GetChunk(ctx, "c-sync")
	require.NoError(t, err)
	assert.Equal(t, 0, updatedChunk.CIFailCount)
	assert.Empty(t, updatedChunk.Banner)
	assert.Empty(t, updatedChunk.CIChecks)
	assert.Equal(t, "pr_opened", updatedChunk.Status)
	assert.Equal(t, "running", updatedBuild.Status)
}

func TestHandlePRCheckUpdated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name               string
		initialStatus      string
		initialBanner      string
		initialChecks      map[string]string
		checkName          string
		checkState         string
		expectedStatus     string
		expectedBanner     string
		expectedCheckState string
	}{
		{
			name:               "initial pr_opened records check state",
			initialStatus:      "pr_opened",
			initialBanner:      "",
			initialChecks:      nil,
			checkName:          "lint",
			checkState:         "passed",
			expectedStatus:     "pr_opened",
			expectedBanner:     "",
			expectedCheckState: "passed",
		},
		{
			name:               "ci_failed chunk with single check passing recovers to pr_opened",
			initialStatus:      "ci_failed",
			initialBanner:      "CI check \"lint\" failed",
			initialChecks:      map[string]string{"lint": "failed"},
			checkName:          "lint",
			checkState:         "passed",
			expectedStatus:     "pr_opened",
			expectedBanner:     "",
			expectedCheckState: "passed",
		},
		{
			name:               "ci_failed chunk with multiple checks stays ci_failed if another check remains failed",
			initialStatus:      "ci_failed",
			initialBanner:      "CI check \"lint\" failed",
			initialChecks:      map[string]string{"lint": "failed", "test": "failed"},
			checkName:          "lint",
			checkState:         "passed",
			expectedStatus:     "ci_failed",
			expectedBanner:     "CI check \"lint\" failed",
			expectedCheckState: "passed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, store, _, _ := setupTestServer(t)

			bID := "b-" + tc.name
			cID := "c-" + tc.name
			build := mkBuild(bID, "123456789012345678", "Build", "Obj", "running")
			chunk := mkChunk(cID, bID, 0, "Chunk", "Instr", "medium", tc.initialStatus)
			chunk.PRNumber = 400
			chunk.Repo = "azylman/aerial"
			chunk.Banner = tc.initialBanner
			if tc.initialChecks != nil {
				chunk.CIChecks = make(map[string]string)
				maps.Copy(chunk.CIChecks, tc.initialChecks)
			}
			require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

			ev := pubsub.PRLifecycleEvent{
				Event:      "check_updated",
				Repo:       "azylman/aerial",
				PRNumber:   400,
				CheckName:  tc.checkName,
				CheckState: tc.checkState,
			}
			require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

			updatedChunk, _, err := store.GetChunk(ctx, cID)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedCheckState, updatedChunk.CIChecks[tc.checkName])
			assert.Equal(t, tc.expectedStatus, updatedChunk.Status)
			assert.Equal(t, tc.expectedBanner, updatedChunk.Banner)
		})
	}
}

func TestHandlePRRolloutUpdated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv, store, _, _ := setupTestServer(t)

	build := mkBuild("b-roll", "123456789012345678", "Build", "Obj", "running")
	chunk := mkChunk("c-roll", "b-roll", 0, "Chunk", "Instr", "medium", "pr_opened")
	chunk.PRNumber = 500
	chunk.Repo = "azylman/aerial"
	require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

	rs := &pubsub.PRRolloutState{
		ExpectedJobs: []string{"eri"},
		StartedJobs:  []string{"eri"},
	}
	ev := pubsub.PRLifecycleEvent{
		Event:        "deploy_started",
		Repo:         "azylman/aerial",
		PRNumber:     500,
		RolloutState: rs,
	}
	require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

	updatedChunk, _, err := store.GetChunk(ctx, "c-roll")
	require.NoError(t, err)
	assert.Equal(t, "deploying", updatedChunk.Status)
	require.NotNil(t, updatedChunk.RolloutState)
	assert.Equal(t, []string{"eri"}, updatedChunk.RolloutState.ExpectedJobs)
}

func TestCiRemediationDispatchID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		chunkID   string
		headSHA   string
		failCount int
		expected  string
	}{
		{
			name:      "sha present",
			chunkID:   "c1",
			headSHA:   "abc1234",
			failCount: 1,
			expected:  "vanadis-c1-cifail-abc1234-1",
		},
		{
			name:      "sha with whitespace trimmed",
			chunkID:   "c1",
			headSHA:   "  def5678  \n",
			failCount: 2,
			expected:  "vanadis-c1-cifail-def5678-2",
		},
		{
			name:      "empty sha fallback",
			chunkID:   "c2",
			headSHA:   "",
			failCount: 3,
			expected:  "vanadis-c2-cifail-3",
		},
		{
			name:      "whitespace-only sha fallback",
			chunkID:   "c3",
			headSHA:   "   \t  ",
			failCount: 1,
			expected:  "vanadis-c3-cifail-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, ciRemediationDispatchID(tc.chunkID, tc.headSHA, tc.failCount))
		})
	}
}

func TestConflictRemediationDispatchID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		chunkID  string
		headSHA  string
		expected string
	}{
		{
			name:     "sha longer than 7 characters",
			chunkID:  "c1",
			headSHA:  "abcdef123456",
			expected: "vanadis-conflict-c1-abcdef1",
		},
		{
			name:     "sha exactly 7 characters",
			chunkID:  "c1",
			headSHA:  "abc1234",
			expected: "vanadis-conflict-c1-abc1234",
		},
		{
			name:     "sha shorter than 7 characters",
			chunkID:  "c1",
			headSHA:  "abc12",
			expected: "vanadis-conflict-c1-abc12",
		},
		{
			name:     "sha with whitespace trimmed",
			chunkID:  "c1",
			headSHA:  "  def5678abc  \n",
			expected: "vanadis-conflict-c1-def5678",
		},
		{
			name:     "empty sha fallback",
			chunkID:  "c2",
			headSHA:  "",
			expected: "vanadis-conflict-c2",
		},
		{
			name:     "whitespace-only sha fallback",
			chunkID:  "c3",
			headSHA:  "   \t  ",
			expected: "vanadis-conflict-c3",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, conflictRemediationDispatchID(tc.chunkID, tc.headSHA))
		})
	}
}

func TestHandlePRCIFailed_Remediation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("attempt <= 3 dispatches brain remediation", func(t *testing.T) {
		t.Parallel()
		srv, store, recBus, reqs := setupTestServer(t)
		build := mkBuild("b-ci1", "123456789012345678", "Build", "Obj", "running")
		chunk := mkChunk("c-ci1", "b-ci1", 0, "Chunk", "Instr", "medium", "pr_opened")
		chunk.PRNumber = 601
		chunk.Repo = "azylman/aerial"
		chunk.CIFailCount = 1
		require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

		ev := pubsub.PRLifecycleEvent{
			Event:     "ci_failed",
			Repo:      "azylman/aerial",
			PRNumber:  601,
			Branch:    "feat/test",
			HeadSHA:   "sha601",
			CheckName: "Go Unit Tests",
			Details:   "Detailed test failure:\n--- FAIL: TestA",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

		updatedChunk, _, err := store.GetChunk(ctx, "c-ci1")
		require.NoError(t, err)
		assert.Equal(t, 2, updatedChunk.CIFailCount)
		assert.Equal(t, "ci_failed", updatedChunk.Status)
		assert.Contains(t, updatedChunk.Banner, "CI check \"Go Unit Tests\" failed")
		assert.Equal(t, "failed", updatedChunk.CIChecks["Go Unit Tests"])

		var foundDispatch bool
		for _, r := range *reqs {
			if r.Path != "/dispatch" {
				continue
			}
			foundDispatch = true
			assert.Equal(t, "vanadis-c-ci1-cifail-sha601-2", r.Body["id"])
			assert.Equal(t, "vanadis-c-ci1", r.Body["session_id"])
			assert.Equal(t, "123456789012345678", r.Body["target_id"])
			assert.Equal(t, true, r.Body["skip_if_active"])
			assert.Contains(t, r.Body["prompt"], "Go Unit Tests")
			assert.Contains(t, r.Body["prompt"], "--- FAIL: TestA")
		}
		assert.True(t, foundDispatch, "expected POST /dispatch to be called")
		assert.Empty(t, recBus.getEvents(), "no external events expected on non-terminal remediation")

		rerunEv := pubsub.PRLifecycleEvent{
			Event:      "check_updated",
			Repo:       "azylman/aerial",
			PRNumber:   601,
			CheckName:  "Go Unit Tests",
			CheckState: "passed",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, rerunEv))

		recoveredChunk, _, err := store.GetChunk(ctx, "c-ci1")
		require.NoError(t, err)
		assert.Equal(t, "pr_opened", recoveredChunk.Status)
		assert.Empty(t, recoveredChunk.Banner)
		assert.Equal(t, "passed", recoveredChunk.CIChecks["Go Unit Tests"])
	})
}

func TestHandlePRCIFailed_DispatchIDVariants(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("empty or whitespace head sha falls back to legacy dispatch id", func(t *testing.T) {
		t.Parallel()
		srv, store, recBus, reqs := setupTestServer(t)
		build := mkBuild("b-ci-empty", "123456789012345678", "Build", "Obj", "running")
		chunk := mkChunk("c-ci-empty", "b-ci-empty", 0, "Chunk", "Instr", "medium", "pr_opened")
		chunk.PRNumber = 602
		chunk.Repo = "azylman/aerial"
		chunk.CIFailCount = 0
		require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

		ev := pubsub.PRLifecycleEvent{
			Event:     "ci_failed",
			Repo:      "azylman/aerial",
			PRNumber:  602,
			Branch:    "feat/test",
			HeadSHA:   "   ",
			CheckName: "Go Unit Tests",
			Details:   "Failure details",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

		var foundDispatch bool
		for _, r := range *reqs {
			if r.Path != "/dispatch" {
				continue
			}
			foundDispatch = true
			assert.Equal(t, "vanadis-c-ci-empty-cifail-1", r.Body["id"])
		}
		assert.True(t, foundDispatch, "expected POST /dispatch to be called")
		assert.Empty(t, recBus.getEvents())
	})

	t.Run("multi-commit sequence across synchronize produces distinct dispatch IDs", func(t *testing.T) {
		t.Parallel()
		srv, store, recBus, reqs := setupTestServer(t)
		build := mkBuild("b-ci-seq", "123456789012345678", "Build", "Obj", "running")
		chunk := mkChunk("c-ci-seq", "b-ci-seq", 0, "Chunk", "Instr", "medium", "pr_opened")
		chunk.PRNumber = 603
		chunk.Repo = "azylman/aerial"
		chunk.CIFailCount = 0
		require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

		// 1. CI fails on commit 1
		evFail1 := pubsub.PRLifecycleEvent{
			Event:     "ci_failed",
			Repo:      "azylman/aerial",
			PRNumber:  603,
			Branch:    "feat/test",
			HeadSHA:   "sha001",
			CheckName: "Go Unit Tests",
			Details:   "fail 1",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, evFail1))

		// 2. Synchronize event resets CI fail count
		evSync := pubsub.PRLifecycleEvent{
			Event:    "synchronize",
			Repo:     "azylman/aerial",
			PRNumber: 603,
			Branch:   "feat/test",
			HeadSHA:  "sha002",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, evSync))

		// 3. CI fails on commit 2
		evFail2 := pubsub.PRLifecycleEvent{
			Event:     "ci_failed",
			Repo:      "azylman/aerial",
			PRNumber:  603,
			Branch:    "feat/test",
			HeadSHA:   "sha002",
			CheckName: "Go Unit Tests",
			Details:   "fail 2",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, evFail2))

		var dispatchIDs []string
		for _, r := range *reqs {
			if r.Path != "/dispatch" {
				continue
			}
			id, ok := r.Body["id"].(string)
			require.Truef(t, ok, "dispatch id is %T, want string", r.Body["id"])
			dispatchIDs = append(dispatchIDs, id)
		}
		require.Len(t, dispatchIDs, 2)
		assert.Equal(t, "vanadis-c-ci-seq-cifail-sha001-1", dispatchIDs[0])
		assert.Equal(t, "vanadis-c-ci-seq-cifail-sha002-1", dispatchIDs[1])
		assert.Empty(t, recBus.getEvents())
	})
}

func TestHandlePRCIFailed_CapReached(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("attempt > 3 triggers remediation cap failure and alerts", func(t *testing.T) {
		t.Parallel()
		srv, store, recBus, reqs := setupTestServer(t)
		build := mkBuild("b-ci4", "123456789012345678", "Build", "Obj", "running")
		chunk := mkChunk("c-ci4", "b-ci4", 0, "Chunk", "Instr", "medium", "pr_opened")
		chunk.PRNumber = 604
		chunk.Repo = "azylman/aerial"
		chunk.CIFailCount = 3
		chunk.MergeAwakeableID = "awk-m-ci"
		require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

		ev := pubsub.PRLifecycleEvent{
			Event:     "ci_failed",
			Repo:      "azylman/aerial",
			PRNumber:  604,
			CheckName: "Go Unit Tests",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

		updatedChunk, updatedBuild, err := store.GetChunk(ctx, "c-ci4")
		require.NoError(t, err)
		assert.Equal(t, 4, updatedChunk.CIFailCount)
		assert.Equal(t, "failed", updatedChunk.Status)
		assert.Equal(t, "failed", updatedBuild.Status)
		assert.Contains(t, updatedChunk.Banner, "CI remediation cap (3/3) reached")

		var foundDirectMessage, foundReconcile bool
		for _, r := range *reqs {
			if r.Path == "/discord/message" {
				foundDirectMessage = true
				assert.Contains(t, r.Body["content"], "remediation cap 3/3 reached")
			}
			if r.Path == "/Build/b-ci4/Reconcile/send" {
				foundReconcile = true
			}
		}
		assert.True(t, foundDirectMessage, "expected POST /discord/message")
		assert.True(t, foundReconcile, "expected reconcile to be sent to restate")
		assert.NotEmpty(t, recBus.getEvents(), "must publish external events on failure cap")
	})

	t.Run("attempt > 3 triggers remediation cap failure for non-awakeable chunks and publishes external events", func(t *testing.T) {
		t.Parallel()
		srv, store, recBus, _ := setupTestServer(t)
		build := mkBuild("b-ci4-legacy", "123456789012345678", "Build", "Obj", "running")
		chunk := mkChunk("c-ci4-legacy", "b-ci4-legacy", 0, "Chunk", "Instr", "medium", "pr_opened")
		chunk.PRNumber = 605
		chunk.Repo = "azylman/aerial"
		chunk.CIFailCount = 3
		require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

		ev := pubsub.PRLifecycleEvent{
			Event:     "ci_failed",
			Repo:      "azylman/aerial",
			PRNumber:  605,
			CheckName: "Go Unit Tests",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

		updatedChunk, updatedBuild, err := store.GetChunk(ctx, "c-ci4-legacy")
		require.NoError(t, err)
		assert.Equal(t, 4, updatedChunk.CIFailCount)
		assert.Equal(t, "failed", updatedChunk.Status)
		assert.Equal(t, "failed", updatedBuild.Status)

		events := recBus.getEvents()
		var chunkFailed, buildFailed bool
		for _, ev := range events {
			if ev.Source == "vanadis_chunk" && ev.EventType == "failed" {
				chunkFailed = true
				assert.Equal(t, []string{"c-ci4-legacy"}, ev.Targets)
			}
			if ev.Source == "vanadis_build" && ev.EventType == "failed" {
				buildFailed = true
				assert.Equal(t, []string{"b-ci4-legacy"}, ev.Targets)
			}
		}
		assert.True(t, chunkFailed, "expected vanadis_chunk.failed external event")
		assert.True(t, buildFailed, "expected vanadis_build.failed external event")
	})
}

func TestHandlePRCIFailed_SkipConditions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name        string
		buildStatus string
		chunkStatus string
		checkName   string
		startCount  int
	}{
		{
			name:        "skips gate aggregator check",
			buildStatus: "running",
			chunkStatus: "pr_opened",
			checkName:   "CI Gate",
			startCount:  0,
		},
		{
			name:        "skips when chunk is already terminal",
			buildStatus: "failed",
			chunkStatus: "failed",
			checkName:   "Go Unit Tests",
			startCount:  4,
		},
		{
			name:        "skips when chunk is merged",
			buildStatus: "running",
			chunkStatus: "merged",
			checkName:   "Go Unit Tests",
			startCount:  0,
		},
		{
			name:        "skips when chunk is deploying",
			buildStatus: "running",
			chunkStatus: "deploying",
			checkName:   "Go Unit Tests",
			startCount:  0,
		},
		{
			name:        "skips when chunk is completed",
			buildStatus: "running",
			chunkStatus: "completed",
			checkName:   "Go Unit Tests",
			startCount:  0,
		},
		{
			name:        "skips when chunk is cancelled",
			buildStatus: "running",
			chunkStatus: "cancelled",
			checkName:   "Go Unit Tests",
			startCount:  0,
		},
		{
			name:        "skips when build is cancelled",
			buildStatus: "cancelled",
			chunkStatus: "pr_opened",
			checkName:   "Go Unit Tests",
			startCount:  0,
		},
		{
			name:        "skips when build is completed",
			buildStatus: "completed",
			chunkStatus: "pr_opened",
			checkName:   "Go Unit Tests",
			startCount:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, store, recBus, reqs := setupTestServer(t)
			build := mkBuild("b-skip-"+tc.name, "123456789012345678", "Build", "Obj", tc.buildStatus)
			chunk := mkChunk("c-skip-"+tc.name, build.ID, 0, "Chunk", "Instr", "medium", tc.chunkStatus)
			chunk.PRNumber = 699
			chunk.Repo = "azylman/aerial"
			chunk.CIFailCount = tc.startCount
			require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

			ev := pubsub.PRLifecycleEvent{
				Event:     "ci_failed",
				Repo:      "azylman/aerial",
				PRNumber:  699,
				CheckName: tc.checkName,
			}
			require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

			updatedChunk, _, err := store.GetChunk(ctx, chunk.ID)
			require.NoError(t, err)
			assert.Equal(t, tc.startCount, updatedChunk.CIFailCount)
			assert.Equal(t, tc.chunkStatus, updatedChunk.Status)
			assert.Empty(t, recBus.getEvents())
			assert.Empty(t, *reqs)
		})
	}
}

func TestHandlePRConflict_DeployFailed_SyncFailed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("conflict updates banner and dispatches brain remediation", func(t *testing.T) {
		t.Parallel()
		srv, store, _, reqs := setupTestServer(t)
		build := mkBuild("b-conf", "123456789012345678", "Build", "Obj", "running")
		chunk := mkChunk("c-conf", "b-conf", 0, "Chunk", "Instr", "medium", "pr_opened")
		chunk.PRNumber = 701
		chunk.Repo = "azylman/aerial"
		require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

		ev := pubsub.PRLifecycleEvent{
			Event:    "conflict",
			Repo:     "azylman/aerial",
			PRNumber: 701,
			Branch:   "feat/conf",
			HeadSHA:  "sha701",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

		updatedChunk, _, err := store.GetChunk(ctx, "c-conf")
		require.NoError(t, err)
		assert.Equal(t, "conflict", updatedChunk.Status)
		assert.Equal(t, "Merge conflict detected", updatedChunk.Banner)

		var foundDispatch bool
		for _, r := range *reqs {
			if r.Path != "/dispatch" {
				continue
			}
			foundDispatch = true
			assert.Equal(t, "vanadis-conflict-c-conf-sha701", r.Body["id"])
			assert.Equal(t, "vanadis-c-conf", r.Body["session_id"])
			assert.Equal(t, "123456789012345678", r.Body["target_id"])
			assert.Equal(t, "c-conf", r.Body["vanadis_chunk_id"])
			assert.Equal(t, true, r.Body["skip_if_active"])
			assert.Equal(t, "medium", r.Body["effort"])
			assert.Contains(t, r.Body["prompt"], "Merge conflict detected")
		}
		assert.True(t, foundDispatch, "expected POST /dispatch to be called")
	})

	t.Run("deploy_failed updates banner and wakes with prompt", func(t *testing.T) {
		t.Parallel()
		srv, store, _, reqs := setupTestServer(t)
		build := mkBuild("b-df", "123456789012345678", "Build", "Obj", "running")
		chunk := mkChunk("c-df", "b-df", 0, "Chunk", "Instr", "medium", "deploying")
		chunk.PRNumber = 702
		chunk.Repo = "azylman/aerial"
		require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

		ev := pubsub.PRLifecycleEvent{
			Event:    "deploy_failed",
			Repo:     "azylman/aerial",
			PRNumber: 702,
			JobName:  "aerial-eri",
			Details:  "pod CrashLoopBackOff",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

		updatedChunk, _, err := store.GetChunk(ctx, "c-df")
		require.NoError(t, err)
		assert.Equal(t, "deploy_failed", updatedChunk.Status)
		assert.Contains(t, updatedChunk.Banner, "Deployment failed for aerial-eri")

		var foundPrompt bool
		for _, r := range *reqs {
			if r.Path == "/prompt" {
				foundPrompt = true
				assert.Contains(t, r.Body["prompt"], "Deployment failed for job aerial-eri")
				assert.Contains(t, r.Body["prompt"], "pod CrashLoopBackOff")
			}
		}
		assert.True(t, foundPrompt)
	})

	t.Run("sync_failed updates banner and wakes with prompt", func(t *testing.T) {
		t.Parallel()
		srv, store, _, reqs := setupTestServer(t)
		build := mkBuild("b-sf", "123456789012345678", "Build", "Obj", "running")
		chunk := mkChunk("c-sf", "b-sf", 0, "Chunk", "Instr", "medium", "deploying")
		chunk.PRNumber = 703
		chunk.Repo = "azylman/aerial"
		require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

		ev := pubsub.PRLifecycleEvent{
			Event:    "sync_failed",
			Repo:     "azylman/aerial",
			PRNumber: 703,
			Details:  "git checkout conflict on k8s/deploy.yaml",
		}
		require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

		updatedChunk, _, err := store.GetChunk(ctx, "c-sf")
		require.NoError(t, err)
		assert.Equal(t, "deploy_failed", updatedChunk.Status)
		assert.Equal(t, "Git file sync failed", updatedChunk.Banner)

		var foundPrompt bool
		for _, r := range *reqs {
			if r.Path == "/prompt" {
				foundPrompt = true
				assert.Contains(t, r.Body["prompt"], "Git sync failed")
				assert.Contains(t, r.Body["prompt"], "git checkout conflict")
			}
		}
		assert.True(t, foundPrompt)
	})
}

func TestHandlePRMergedAndDeploySuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		event      string
		buildID    string
		chunkID    string
		prNum      int
		initStatus string
		wantStatus string
		wantEvent  string
	}{
		{
			name:       "merged event sets status to merged and reconciles",
			event:      "merged",
			buildID:    "b-m1",
			chunkID:    "c-m1",
			prNum:      801,
			initStatus: "pr_opened",
			wantStatus: "merged",
			wantEvent:  "merged",
		},
		{
			name:       "deploy_success event sets status to completed and reconciles",
			event:      "deploy_success",
			buildID:    "b-d1",
			chunkID:    "c-d1",
			prNum:      901,
			initStatus: "merged",
			wantStatus: "completed",
			wantEvent:  "completed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			srv, store, recBus, reqs := setupTestServer(t)
			build := mkBuild(tc.buildID, "123456789012345678", "Build", "Obj", "running")
			chunk := mkChunk(tc.chunkID, tc.buildID, 0, "Chunk", "Instr", "medium", tc.initStatus)
			chunk.PRNumber = tc.prNum
			chunk.Repo = "azylman/aerial"
			require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

			ev := pubsub.PRLifecycleEvent{
				Event:    tc.event,
				Repo:     "azylman/aerial",
				PRNumber: tc.prNum,
			}
			require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

			updatedChunk, _, err := store.GetChunk(ctx, tc.chunkID)
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, updatedChunk.Status)

			wantReconcilePath := fmt.Sprintf("/Build/%s/Reconcile/send", tc.buildID)
			var foundReconcile, foundCancel bool
			for _, r := range *reqs {
				if r.Path == wantReconcilePath {
					foundReconcile = true
				}
				if r.Path == "/dispatch/cancel" && r.Body["chunk_id"] == tc.chunkID {
					foundCancel = true
				}
			}
			assert.True(t, foundReconcile, "must call Build Virtual Object")
			assert.True(t, foundCancel, "must cancel chunk dispatches on merged/deploy_success")

			var foundEvent bool
			for _, published := range recBus.getEvents() {
				if published.Source == "vanadis_chunk" && published.EventType == tc.wantEvent {
					foundEvent = true
				}
			}
			assert.True(t, foundEvent, "must publish expected external event")
		})
	}
}

func TestHandlePRClosedUnmerged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv, store, _, _ := setupTestServer(t)

	build := mkBuild("b-closed", "123456789012345678", "Build", "Obj", "running")
	chunk := mkChunk("c-closed", "b-closed", 0, "Chunk", "Instr", "medium", "pr_opened")
	chunk.PRNumber = 999
	chunk.Repo = "azylman/aerial"
	require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

	ev := pubsub.PRLifecycleEvent{
		Event:    "closed_unmerged",
		Repo:     "azylman/aerial",
		PRNumber: 999,
	}
	require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, ev))

	updatedChunk, updatedBuild, err := store.GetChunk(ctx, "c-closed")
	require.NoError(t, err)
	assert.Equal(t, "failed", updatedChunk.Status)
	assert.Equal(t, "failed", updatedBuild.Status)
}

func TestPostgresVanadisStore_PREventMethods(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("nil receiver and db", func(t *testing.T) {
		t.Parallel()
		var nilStore *PostgresVanadisStore
		emptyStore := &PostgresVanadisStore{}

		for _, s := range []*PostgresVanadisStore{nilStore, emptyStore} {
			assert.Error(t, s.RecordPRCheckStatus(ctx, "c1", "lint", "passed"))
			assert.Error(t, s.RecordPRRolloutState(ctx, "c1", "deploying", &pubsub.PRRolloutState{}))
			assert.Error(t, s.ResetChunkForSynchronize(ctx, "c1", "b1"))
			_, err := s.IncrementChunkCIFailCount(ctx, "c1", "banner")
			assert.Error(t, err)
			assert.Error(t, s.UpdateChunkBannerAndStatus(ctx, "c1", "failed", "banner"))
		}
	})

	t.Run("validation empty arguments", func(t *testing.T) {
		t.Parallel()
		s := newMockStore(mockDB{})
		assert.Error(t, s.RecordPRCheckStatus(ctx, "", "lint", "passed"))
		assert.Error(t, s.RecordPRCheckStatus(ctx, "c1", "", "passed"))
		assert.Error(t, s.RecordPRRolloutState(ctx, "", "deploying", nil))
		assert.Error(t, s.ResetChunkForSynchronize(ctx, "", "b1"))
		assert.Error(t, s.ResetChunkForSynchronize(ctx, "c1", ""))
		_, err := s.IncrementChunkCIFailCount(ctx, "", "banner")
		assert.Error(t, err)
		assert.Error(t, s.UpdateChunkBannerAndStatus(ctx, "", "status", "banner"))
	})

	t.Run("exec and query errors propagate", func(t *testing.T) {
		t.Parallel()
		dbErr := errors.New("db failure")
		sErr := newMockStore(mockDB{
			execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				return pgconn.CommandTag{}, dbErr
			},
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return &mockRow{err: dbErr}
			},
		})

		assert.ErrorIs(t, sErr.RecordPRCheckStatus(ctx, "c1", "lint", "passed"), dbErr)
		assert.ErrorIs(t, sErr.RecordPRRolloutState(ctx, "c1", "deploying", nil), dbErr)
		assert.ErrorIs(t, sErr.ResetChunkForSynchronize(ctx, "c1", "b1"), dbErr)
		_, err := sErr.IncrementChunkCIFailCount(ctx, "c1", "banner")
		assert.ErrorIs(t, err, dbErr)
		assert.ErrorIs(t, sErr.UpdateChunkBannerAndStatus(ctx, "c1", "failed", "banner"), dbErr)
	})

	t.Run("not found returns ErrChunkNotFound", func(t *testing.T) {
		t.Parallel()
		sNotFound := newMockStore(mockDB{
			execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				return pgconn.NewCommandTag("UPDATE 0"), nil
			},
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return &mockRow{err: pgx.ErrNoRows}
			},
		})

		assert.ErrorIs(t, sNotFound.RecordPRCheckStatus(ctx, "c1", "lint", "passed"), ErrChunkNotFound)
		assert.ErrorIs(t, sNotFound.RecordPRRolloutState(ctx, "c1", "deploying", nil), ErrChunkNotFound)
		assert.ErrorIs(t, sNotFound.ResetChunkForSynchronize(ctx, "c1", "b1"), ErrChunkNotFound)
		_, err := sNotFound.IncrementChunkCIFailCount(ctx, "c1", "banner")
		assert.ErrorIs(t, err, ErrChunkNotFound)
		assert.ErrorIs(t, sNotFound.UpdateChunkBannerAndStatus(ctx, "c1", "failed", "banner"), ErrChunkNotFound)
	})

	t.Run("happy paths succeed", func(t *testing.T) {
		t.Parallel()
		var capturedSQL string
		var capturedArgs []any
		sOK := newMockStore(mockDB{
			execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				capturedSQL = sql
				capturedArgs = args
				return pgconn.NewCommandTag("UPDATE 1"), nil
			},
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return valuesRow(2)
			},
		})

		assert.NoError(t, sOK.RecordPRCheckStatus(ctx, "c1", "lint", "passed"))
		assert.Contains(t, capturedSQL, "jsonb_set")
		assert.Contains(t, capturedSQL, "jsonb_each_text")
		assert.Contains(t, capturedSQL, "status = 'ci_failed'")
		assert.Contains(t, capturedSQL, "'pr_opened'")
		assert.Equal(t, []any{"lint", "passed", "c1"}, capturedArgs)
		assert.NoError(t, sOK.RecordPRRolloutState(ctx, "c1", "deploying", &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}}))
		assert.NoError(t, sOK.ResetChunkForSynchronize(ctx, "c1", "b1"))
		count, err := sOK.IncrementChunkCIFailCount(ctx, "c1", "banner")
		require.NoError(t, err)
		assert.Equal(t, 2, count)
		assert.NoError(t, sOK.UpdateChunkBannerAndStatus(ctx, "c1", "failed", "banner"))
	})
}

func TestPREvents_EdgeBranches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("dispatchDirectMessage validation and nil handling", func(t *testing.T) {
		t.Parallel()
		var nilSrv *VanadisServer
		assert.NoError(t, nilSrv.dispatchDirectMessage(ctx, "123", "msg"))

		srv, _, _, _ := setupTestServer(t)
		assert.NoError(t, srv.dispatchDirectMessage(ctx, "", "msg"))
		assert.NoError(t, srv.dispatchDirectMessage(ctx, "123", ""))
	})

	t.Run("formatCIFailurePrompt robo-rev check", func(t *testing.T) {
		t.Parallel()
		srv, _, _, _ := setupTestServer(t)
		ev := pubsub.PRLifecycleEvent{
			Event:     "ci_failed",
			Repo:      "azylman/aerial",
			PRNumber:  456,
			CheckName: "roborev",
			Branch:    "feat/test",
			HeadSHA:   "sha456",
			CheckURL:  "https://github.com/azylman/aerial/actions/runs/1",
		}
		prompt := srv.formatCIFailurePrompt(ev, "Code analysis finding")
		assert.Contains(t, prompt, "RoboRev finding")
		assert.Contains(t, prompt, "override-roborev")
	})
}

func TestHandlePRDeploySuccess_ActionChunkWithPR(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv, store, _, reqs := setupTestServer(t)
	build := mkBuild("b-act-pr-cycle", "123456789012345678", "Build", "Action with PR", "running")
	chunk := mkChunk("c-act-pr-1", "b-act-pr-cycle", 0, "Action Chunk", "Fix code", "medium", "running")
	chunk.Mode = "action"
	chunk.Repo = ""
	require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

	// 1. Attach PR to action chunk
	_, _, _, err := store.AttachChunkPR(ctx, AttachChunkPRRequest{
		ChunkID:  "c-act-pr-1",
		PRNumber: 905,
		Repo:     "azylman/aerial",
	})
	require.NoError(t, err)

	// 2. PR merges: chunk transitions to merged but does NOT complete build
	require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:    "merged",
		Repo:     "azylman/aerial",
		PRNumber: 905,
	}))

	cAfterMerge, bAfterMerge, err := store.GetChunk(ctx, "c-act-pr-1")
	require.NoError(t, err)
	assert.Equal(t, "merged", cAfterMerge.Status)
	assert.Equal(t, "running", bAfterMerge.Status, "build must remain running while action chunk awaits deployment")

	// 3. Sync succeeds: progress only, must NOT reconcile on sync_success
	require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:    "sync_success",
		Repo:     "azylman/aerial",
		PRNumber: 905,
	}))

	// 4. PR deploy succeeds: Build Virtual Object is called and chunk becomes completed
	require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:    "deploy_success",
		Repo:     "azylman/aerial",
		PRNumber: 905,
	}))

	cAfterDeploy, _, err := store.GetChunk(ctx, "c-act-pr-1")
	require.NoError(t, err)
	assert.Equal(t, "completed", cAfterDeploy.Status)

	var foundReconcile bool
	for _, r := range *reqs {
		if r.Path == "/Build/b-act-pr-cycle/Reconcile/send" {
			foundReconcile = true
		}
	}
	assert.True(t, foundReconcile, "Build Virtual Object must be called on deploy_success")
}

func TestHandlePRDeploySuccess_WithoutAwakeable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv, store, bus, reqs := setupTestServer(t)
	build := mkBuild("b-no-awk", "123456789012345678", "Build", "Reconciler deploy_success", "running")
	chunk := mkChunk("c-no-awk-1", "b-no-awk", 0, "PR Chunk", "Fix code", "medium", "merged")
	chunk.Mode = "pr"
	chunk.Repo = "azylman/aerial"
	chunk.PRNumber = 906
	chunk.DeployAwakeableID = ""
	require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

	require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:    "deploy_success",
		Repo:     "azylman/aerial",
		PRNumber: 906,
	}))

	cAfter, _, err := store.GetChunk(ctx, "c-no-awk-1")
	require.NoError(t, err)
	assert.Equal(t, "completed", cAfter.Status)

	var foundChunkCompletedEvent bool
	for _, p := range bus.events {
		if p.Source == "vanadis_chunk" && p.EventType == "completed" {
			foundChunkCompletedEvent = true
		}
	}
	assert.True(t, foundChunkCompletedEvent, "vanadis_chunk.completed event must be published")

	var foundReconcileSend bool
	for _, r := range *reqs {
		if r.Path == "/Build/b-no-awk/Reconcile/send" {
			foundReconcileSend = true
		}
	}
	assert.True(t, foundReconcileSend, "reconcile request must be sent on deploy_success when deploy awakeable is empty")

	// Error returning when status update fails
	store.updateChunkErr = errors.New("cannot update chunk")
	chunk.Status = "merged"
	require.Error(t, srv.handlePRDeploySuccess(ctx, &chunk, &build))
	store.updateChunkErr = nil
}

func TestProcessPRLifecycleEvent_SyncSuccess_PRChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv, store, bus, reqs := setupTestServer(t)
	build := mkBuild("b-sync-pr", "123456789012345678", "Build", "PR sync_success", "running")
	chunk := mkChunk("c-sync-pr-1", "b-sync-pr", 0, "PR Chunk", "Fix code", "medium", "merged")
	chunk.Mode = "pr"
	chunk.Repo = "azylman/aerial"
	chunk.PRNumber = 907
	chunk.DeployAwakeableID = "awk-dep-907"
	require.NoError(t, store.CreateBuild(ctx, build, []VanadisChunk{chunk}))

	require.NoError(t, srv.ProcessPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:    "sync_success",
		Repo:     "azylman/aerial",
		PRNumber: 907,
	}))

	cAfter, bAfter, err := store.GetChunk(ctx, "c-sync-pr-1")
	require.NoError(t, err)
	assert.Equal(t, "merged", cAfter.Status, "chunk status must remain merged on sync_success")
	assert.Equal(t, "running", bAfter.Status, "build must remain running on sync_success")

	for _, p := range bus.events {
		assert.False(t, p.Source == "vanadis_chunk" && p.EventType == "completed", "must not publish completed event on sync_success")
	}
	for _, r := range *reqs {
		assert.NotEqual(t, "/restate/awakeables/awk-dep-907/resolve", r.Path, "must not resolve awakeable on sync_success")
		assert.NotEqual(t, "/Build/b-sync-pr/Reconcile/send", r.Path, "must not send reconcile on sync_success")
	}
}
