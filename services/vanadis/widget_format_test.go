package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestHandleCoalescedWidgetMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		srv        *VanadisServer
		payload    string
		wantTermed bool
		wantAcked  bool
		wantErr    bool
	}{
		{
			name:       "nil server or store terms message",
			srv:        nil,
			payload:    `{"build_id":"b1"}`,
			wantTermed: true,
		},
		{
			name: "invalid json terms message",
			srv: &VanadisServer{
				store: &memoryVanadisStore{},
			},
			payload:    `invalid json`,
			wantTermed: true,
		},
		{
			name: "empty build_id acks message",
			srv: &VanadisServer{
				store: &memoryVanadisStore{},
			},
			payload:   `{"build_id":"   "}`,
			wantAcked: true,
		},
		{
			name: "build not found acks message",
			srv: &VanadisServer{
				store: newMemoryVanadisStore(),
			},
			payload:   `{"build_id":"b-nonexistent"}`,
			wantAcked: true,
		},
		{
			name: "successful widget dispatch",
			srv: func() *VanadisServer {
				store := newMemoryVanadisStore()
				b := VanadisBuild{ID: "b-1", ThreadID: "123", Objective: "test"}
				c := []VanadisChunk{{ID: "c1", BuildID: "b-1", Title: "c1", Instructions: "do c1"}}
				_ = store.CreateBuild(context.Background(), b, c)

				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"message_id":"msg-1"}`))
				}))

				return &VanadisServer{
					cfg:        Config{BrainURL: ts.URL},
					store:      store,
					httpClient: ts.Client(),
				}
			}(),
			payload:   `{"build_id":"b-1","banner":"Banner"}`,
			wantAcked: true,
		},
		{
			name: "widget dispatch error returns error",
			srv: func() *VanadisServer {
				store := newMemoryVanadisStore()
				b := VanadisBuild{ID: "b-err", ThreadID: "123", Objective: "test"}
				c := []VanadisChunk{{ID: "c1", BuildID: "b-err", Title: "c1", Instructions: "do c1"}}
				_ = store.CreateBuild(context.Background(), b, c)

				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}))

				return &VanadisServer{
					cfg:        Config{BrainURL: ts.URL},
					store:      store,
					httpClient: ts.Client(),
				}
			}(),
			payload:    `{"build_id":"b-err","banner":"Banner"}`,
			wantErr:    true,
			wantTermed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			msg := &memoryMsg{data: []byte(tc.payload), numDelivered: 3}
			err := tc.srv.HandleCoalescedWidgetMessage(context.Background(), msg)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantTermed, msg.termed)
			assert.Equal(t, tc.wantAcked, msg.acked)
		})
	}
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

type errVanadisStore struct {
	VanadisStore

	getBuildErr     error
	getChunkByPRErr error
	completeRes     *VanadisAdvancementResult
	completeErr     error
}

func (s *errVanadisStore) GetBuild(ctx context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	if s.getBuildErr != nil {
		return nil, nil, s.getBuildErr
	}
	return nil, nil, errors.New("generic store error")
}

func (s *errVanadisStore) GetChunk(ctx context.Context, chunkID string) (*VanadisChunk, *VanadisBuild, error) {
	return nil, nil, errors.New("generic get chunk error")
}

func (s *errVanadisStore) GetChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error) {
	if s.getChunkByPRErr != nil {
		return nil, s.getChunkByPRErr
	}
	return &VanadisChunkPRLookup{Matched: true, BuildID: "b-matched"}, nil
}

func (s *errVanadisStore) CompleteActionChunk(ctx context.Context, chunkID string) (*VanadisAdvancementResult, error) {
	if s.completeErr != nil {
		return nil, s.completeErr
	}
	if s.completeRes != nil {
		return s.completeRes, nil
	}
	return nil, errors.New("simulated complete action chunk error")
}

func (s *errVanadisStore) FailChunk(ctx context.Context, chunkID, reason string) (*VanadisBuild, *VanadisChunk, bool, error) {
	return nil, nil, false, errors.New("simulated fail chunk error")
}

func (s *errVanadisStore) ClaimStalledChunk(ctx context.Context, chunkID string, dispatches ...string) (StalledChunkVerdict, *VanadisBuild, *VanadisChunk, error) {
	return StalledChunkVerdict(""), nil, nil, errors.New("simulated claim stalled chunk error")
}

func (s *errVanadisStore) UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error {
	return errors.New("simulated update status message id err")
}

type widgetMockMsgWithErrors struct {
	payload   []byte
	ackErr    error
	termErr   error
	delivered uint64
}

func (m *widgetMockMsgWithErrors) Data() []byte        { return m.payload }
func (m *widgetMockMsgWithErrors) Subject() string     { return "aerial.coalesced.vanadis_widget.test" }
func (m *widgetMockMsgWithErrors) Header() nats.Header { return nil }
func (m *widgetMockMsgWithErrors) NumDelivered() uint64 {
	if m.delivered > 0 {
		return m.delivered
	}
	return 1
}
func (m *widgetMockMsgWithErrors) Ack() error                       { return m.ackErr }
func (m *widgetMockMsgWithErrors) Nak() error                       { return nil }
func (m *widgetMockMsgWithErrors) NakWithDelay(time.Duration) error { return nil }
func (m *widgetMockMsgWithErrors) Term() error                      { return m.termErr }
func (m *widgetMockMsgWithErrors) Snooze(time.Duration) error       { return nil }
func (m *widgetMockMsgWithErrors) InProgress() error                { return nil }

func TestHandleCoalescedWidgetMessage_StoreError_And_AckErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Store error returns error
	srvStoreErr := &VanadisServer{
		store: &errVanadisStore{getBuildErr: errors.New("db disconnect")},
	}
	msg1 := &memoryMsg{data: []byte(`{"build_id":"b1"}`)}
	err := srvStoreErr.HandleCoalescedWidgetMessage(ctx, msg1)
	assert.Error(t, err)

	// mockMsgWithErrors to trigger term/ack warning branches
	msgAckErr := &widgetMockMsgWithErrors{
		payload: []byte(`{"build_id":""}`),
		ackErr:  errors.New("ack err"),
	}
	assert.NoError(t, srvStoreErr.HandleCoalescedWidgetMessage(ctx, msgAckErr))

	// Term error with nil srv
	msgTermErr := &widgetMockMsgWithErrors{
		payload: []byte(`{"build_id":"b1"}`),
		termErr: errors.New("term err"),
	}
	var nilSrv *VanadisServer
	assert.NoError(t, nilSrv.HandleCoalescedWidgetMessage(ctx, msgTermErr))

	// Bad json with termErr
	msgBadJSON := &widgetMockMsgWithErrors{
		payload: []byte(`bad-json`),
		termErr: errors.New("term err"),
	}
	assert.NoError(t, srvStoreErr.HandleCoalescedWidgetMessage(ctx, msgBadJSON))

	// Build not found with ackErr
	msgMissingAckErr := &widgetMockMsgWithErrors{
		payload: []byte(`{"build_id":"b-missing"}`),
		ackErr:  errors.New("ack err"),
	}
	storeMissing := newMemoryVanadisStore()
	srvMissing := &VanadisServer{store: storeMissing}
	assert.NoError(t, srvMissing.HandleCoalescedWidgetMessage(ctx, msgMissingAckErr))

	// Success with ackErr
	bSuccess := VanadisBuild{ID: "b-ack", ThreadID: "123", Objective: "test"}
	cSuccess := []VanadisChunk{{ID: "c1", BuildID: "b-ack", Title: "c1", Instructions: "do c1"}}
	_ = storeMissing.CreateBuild(ctx, bSuccess, cSuccess)
	tsSuccess := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"message_id":"msg-ack"}`))
	}))
	defer tsSuccess.Close()
	srvSuccess := &VanadisServer{
		cfg:        Config{BrainURL: tsSuccess.URL},
		store:      storeMissing,
		httpClient: tsSuccess.Client(),
	}
	msgSuccessAckErr := &widgetMockMsgWithErrors{
		payload: []byte(`{"build_id":"b-ack"}`),
		ackErr:  errors.New("ack err"),
	}
	assert.NoError(t, srvSuccess.HandleCoalescedWidgetMessage(ctx, msgSuccessAckErr))

	// Dispatch fail with term error when NumDelivered >= 3
	ts500Fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts500Fail.Close()
	srvFail := &VanadisServer{
		cfg:        Config{BrainURL: ts500Fail.URL},
		store:      storeMissing,
		httpClient: ts500Fail.Client(),
	}
	msgDispatchTermErr := &widgetMockMsgWithErrors{
		payload:   []byte(`{"build_id":"b-ack"}`),
		termErr:   errors.New("term error"),
		delivered: 3,
	}
	assert.Error(t, srvFail.HandleCoalescedWidgetMessage(ctx, msgDispatchTermErr))
}

func TestHandleSyncPRWidget_ErrorBranches(t *testing.T) {
	t.Parallel()

	// 1. BuildID specified, but dispatchAndPersistWidget fails
	store := newMemoryVanadisStore()
	b := VanadisBuild{ID: "b-fail", ThreadID: "123", Objective: "test"}
	c := []VanadisChunk{{ID: "c1", BuildID: "b-fail", Title: "c1"}}
	_ = store.CreateBuild(context.Background(), b, c)

	ts500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts500.Close()

	srv := &VanadisServer{
		cfg:        Config{BrainURL: ts500.URL},
		store:      store,
		httpClient: ts500.Client(),
	}

	mux := srv.Routes()

	req1 := httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(`{"build_id":"b-fail"}`))
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on dispatch fail, got %d", rec1.Code)
	}

	// 2. Repo + PRNumber, but GetChunkByPR fails
	sErr := &errVanadisStore{getChunkByPRErr: errors.New("lookup fail")}
	srvErr := &VanadisServer{store: sErr}
	muxErr := srvErr.Routes()

	req2 := httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(`{"repo":"aerial","pr_number":42}`))
	rec2 := httptest.NewRecorder()
	muxErr.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on GetChunkByPR fail, got %d", rec2.Code)
	}

	// 3. Repo + PRNumber, GetChunkByPR matches, but GetBuild fails
	sErr2 := &errVanadisStore{getBuildErr: errors.New("getbuild fail")}
	srvErr2 := &VanadisServer{store: sErr2}
	muxErr2 := srvErr2.Routes()

	req3 := httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(`{"repo":"aerial","pr_number":42}`))
	rec3 := httptest.NewRecorder()
	muxErr2.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on GetBuild fail, got %d", rec3.Code)
	}
}

func TestChunksEvents_And_PersistWidget_RemainingGaps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Case A: L46-47 in chunks_events.go (res.ChunkMode == "pr", ProcessStalledChunkCheck logs error)
	storePR := &errVanadisStore{
		completeRes: &VanadisAdvancementResult{
			ChunkMode: "pr",
			BuildID:   "b1",
		},
	}
	srvPR := &VanadisServer{store: storePR}
	assert.NoError(t, srvPR.handleCompletedDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c1",
		DispatchID:     "d1",
	}))

	// Case B: L59-61 in chunks_events.go (res.Advanced == true, NextChunkIndex >= 0, s.enqueueExecuteChunk returns error)
	storeAdvance := &errVanadisStore{
		completeRes: &VanadisAdvancementResult{
			Advanced:       true,
			BuildCompleted: false,
			NextChunkIndex: 1,
			BuildID:        "b1",
		},
	}
	srvAdvance := &VanadisServer{store: storeAdvance}
	errAdvance := srvAdvance.handleCompletedDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c1",
		DispatchID:     "d1",
	})
	assert.Error(t, errAdvance)

	// Case C: L1312-1313 in chunks.go (UpdateBuildStatusMessageID fails and logs warning)
	tsMsg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"message_id":"new-msg-id"}`))
	}))
	defer tsMsg.Close()

	srvPersist := &VanadisServer{
		cfg:        Config{BrainURL: tsMsg.URL},
		store:      &errVanadisStore{},
		httpClient: tsMsg.Client(),
	}
	errPersist := srvPersist.dispatchAndPersistWidget(ctx, "b1", "", StatusWidgetRequest{Title: "test"})
	assert.NoError(t, errPersist)

	// Case D: SkipSink sends widget with skip_sink: true
	var receivedBody []byte
	tsSkipSink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"message_id":"msg-skip"}`))
	}))
	defer tsSkipSink.Close()

	srvSkipSink := &VanadisServer{
		cfg:        Config{BrainURL: tsSkipSink.URL},
		store:      &errVanadisStore{},
		httpClient: tsSkipSink.Client(),
	}
	errSkipSink := srvSkipSink.dispatchAndPersistWidget(ctx, "b1", "", StatusWidgetRequest{Title: "quiet", SkipSink: true})
	assert.NoError(t, errSkipSink)
	assert.Contains(t, string(receivedBody), `"skip_sink":true`)
}
