package build

// Shared BuildObject test harness: the scripted SQL fake comes from
// internal/sqltest (dbScript/sqlRoute aliases plus the local open wrapper);
// mockEriClient/launch helpers and the unattached-PR spy stay here.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/eritest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/sqltest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type (
	dbScript = sqltest.Script
	sqlRoute = sqltest.Route
)

// nilDB is the sentinel script open turns into a Store without a database.
var nilDB = dbScript{ExecTag: "nil db"}

func open(s dbScript) (*store.Store, *[]string) {
	db, candidates := s.DB()
	if s.ExecTag == nilDB.ExecTag {
		return &store.Store{}, candidates
	}
	return store.New(db, "example-owner"), candidates
}

func attachRequest(chunkID, repo string) store.AttachChunkPRRequest {
	return store.AttachChunkPRRequest{ChunkID: chunkID, PRNumber: 101, Repo: repo}
}

// mockEriClient layers build's behavioural closures (attempt counters,
// prompt/banner capture) over the shared eritest.Client, which records every
// call; error-only cases should prefer the embedded Client's error fields.
type mockEriClient struct {
	eritest.Client

	dispatchFn func(ctx context.Context, req eriwire.BrainDispatchRequest) error
	cancelFn   func(ctx context.Context, chunkID string) error
	promptFn   func(ctx context.Context, threadID, prompt string) error
	widgetFn   func(ctx context.Context, req executor.StatusWidgetRequest) (string, error)
	messageFn  func(ctx context.Context, channelID, content string) error
}

func (m *mockEriClient) DispatchAccepted(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	return m.Dispatch(ctx, req)
}

func (m *mockEriClient) Dispatch(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	err := m.Client.Dispatch(ctx, req)
	if m.dispatchFn != nil {
		return m.dispatchFn(ctx, req)
	}
	return err
}

func (m *mockEriClient) CancelDispatch(ctx context.Context, chunkID string) error {
	err := m.Client.CancelDispatch(ctx, chunkID)
	if m.cancelFn != nil {
		return m.cancelFn(ctx, chunkID)
	}
	return err
}

func (m *mockEriClient) Prompt(ctx context.Context, threadID, prompt string) error {
	err := m.Client.Prompt(ctx, threadID, prompt)
	if m.promptFn != nil {
		return m.promptFn(ctx, threadID, prompt)
	}
	return err
}

func (m *mockEriClient) Widget(ctx context.Context, req executor.StatusWidgetRequest) (string, error) {
	id, err := m.Client.Widget(ctx, req)
	if m.widgetFn != nil {
		return m.widgetFn(ctx, req)
	}
	return id, err
}

func (m *mockEriClient) Message(ctx context.Context, channelID, content string) error {
	err := m.Client.Message(ctx, channelID, content)
	if m.messageFn != nil {
		return m.messageFn(ctx, channelID, content)
	}
	return err
}

type mockStoreForLaunch struct {
	store.VanadisStore

	reserveErr error
}

type mockBusExecutor struct {
	pubsub.Bus

	lastSubj string
}

func (m *mockStoreForLaunch) ReserveChunkForLaunch(ctx context.Context, chunkID string) error {
	return m.reserveErr
}

func (m *mockBusExecutor) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.lastSubj = subject
	return nil
}

func launchRecoveryServer(t *testing.T, id, first string, ready *atomic.Bool) (*httptest.Server, chan string) {
	t.Helper()
	ids := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dispatch" {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		var req eriwire.BrainDispatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		select {
		case ids <- req.ID:
		default:
			t.Error("unexpected repeated dispatch")
		}
		if req.ID == id && !ready.Load() {
			switch first {
			case "active":
				_, _ = w.Write([]byte(`{"status":"skipped_active"}`))
			case "lost":
				w.Header().Set("Content-Length", "100")
				_, _ = w.Write([]byte(`{"status":"accepted"`))
			default:
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		status := "accepted"
		if first == "lost" {
			status = "already_exists"
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "dispatch_id": req.ID})
	}))
	return srv, ids
}

type unattachedPREventSpyStore struct {
	store.VanadisStore

	chunks          []model.VanadisChunk
	getBuildErr     error
	attachErr       error
	attachedChunkID string
	attachedPR      int
	attachedRepo    string
	prStatusUpdated string
	cStatus         string
	banner          string
	failedChunkID   string
	syncWidgetCalls int
}

func (*unattachedPREventSpyStore) GetChunkByPR(_ context.Context, _ string, _ int) (*store.VanadisChunkPRLookup, error) {
	return &store.VanadisChunkPRLookup{Matched: false}, nil
}

func (s *unattachedPREventSpyStore) GetBuild(_ context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s.getBuildErr != nil {
		return nil, nil, s.getBuildErr
	}
	return &model.VanadisBuild{ID: buildID, Status: statusRunning, ThreadID: "th-1"}, s.chunks, nil
}

func (s *unattachedPREventSpyStore) AttachChunkPR(_ context.Context, req store.AttachChunkPRRequest) (string, string, string, error) {
	if s.attachErr != nil {
		return "", "", "", s.attachErr
	}
	s.attachedChunkID = req.ChunkID
	s.attachedPR = req.PRNumber
	s.attachedRepo = req.Repo
	return "b1", req.ChunkID, "th-1", nil
}

func (s *unattachedPREventSpyStore) UpdatePRLifecycleStatus(_ context.Context, chunkID, status string) error {
	s.prStatusUpdated = chunkID + ":" + status
	return nil
}

func (*unattachedPREventSpyStore) RecordPRCheckStatus(_ context.Context, _, _, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) ResetPRCheckState(_ context.Context, _, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) RecordPRRolloutState(_ context.Context, _, _ string, _ *pubsub.PRRolloutState) error {
	return nil
}

func (s *unattachedPREventSpyStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	s.cStatus = chunkID + ":" + status
	s.banner = banner
	return nil
}

func (s *unattachedPREventSpyStore) UpdateBuildStatusMessageID(_ context.Context, _, _ string) error {
	s.syncWidgetCalls++
	return nil
}

func (*unattachedPREventSpyStore) UpdateBuildStatus(_ context.Context, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) ReserveChunkForLaunch(_ context.Context, _ string) error {
	return nil
}

func (s *unattachedPREventSpyStore) IncrementChunkCIFailCount(_ context.Context, chunkID, banner string, _ ...string) (int, error) {
	s.cStatus = chunkID + ":ci_failed"
	s.banner = banner
	return 1, nil
}

func (s *unattachedPREventSpyStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	s.failedChunkID = chunkID + ":" + reason
	return &model.VanadisBuild{ID: "b1", Status: statusRunning, ThreadID: "th-1"}, &model.VanadisChunk{ID: chunkID, Status: statusFailed}, true, nil
}
