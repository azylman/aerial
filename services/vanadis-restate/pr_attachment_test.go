package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestFirstAttachmentRecovery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, parent, sibling string
		wantConflict          bool
	}{
		{"failed parent", "failed", "completed", false},
		{"running parent", "running", "pending", false},
		{"failed sibling", "failed", "failed", true},
		{"cancelled sibling", "failed", "cancelled", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := VanadisBuild{ID: "b1", Status: tc.parent}
			store, _ := dbScript{buildRow: &mockRow{b: &b}, chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: "failed"}, {ID: "other", Status: tc.sibling}}, routes: []sqlRoute{{match: "UPDATE vanadis_chunks", row: valuesRow("b1", "c1", "")}, {match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow("b1")}}, row: valuesRow("b1", "c1", "")}.open()
			_, _, _, err := store.AttachChunkPR(context.Background(), attachRequest("c1", "aerial"))
			if tc.wantConflict {
				assertStoreErr(t, err, ErrBuildConflict, false)
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func attachmentScript(chunk, build, thread, status, repo string) dbScript {
	return dbScript{routes: []sqlRoute{{match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow(build)}}, buildRow: &mockRow{b: &VanadisBuild{ID: build, ThreadID: thread, Status: status}}, chunks: []VanadisChunk{{ID: chunk, BuildID: build, Mode: "pr", Repo: repo, Status: "running"}}}
}

// This fixture exercises Go selection and error propagation, not SQL effects or row locking.
func TestAttachmentSelectionAndErrors(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("write failed")
	tests := []struct {
		name, parent, status, sibling, failure string
		previous, cap, index                   int
		commitErr, wantErr                     error
	}{
		{name: "first", parent: "failed", status: "failed"},
		{name: "middle", parent: "failed", status: "failed", index: 1},
		{name: "final", parent: "failed", status: "failed", index: 2},
		{name: "duplicate after failure", parent: "failed", status: "failed", previous: 101},
		{name: "cancelled chunk", parent: "running", status: "cancelled", wantErr: ErrBuildConflict},
		{name: "completed different PR", parent: "running", status: "completed", previous: 100, wantErr: ErrBuildConflict},
		{name: "duplicate at cap", parent: "failed", status: "failed", previous: 101, cap: 3},
		{name: "cancelled parent", parent: "cancelled", status: "failed", wantErr: ErrBuildConflict},
		{name: "completed parent", parent: "completed", status: "failed", wantErr: ErrBuildConflict},
		{name: "failed sibling", parent: "failed", status: "failed", sibling: "failed", wantErr: ErrBuildConflict},
		{name: "cancelled sibling", parent: "failed", status: "failed", sibling: "cancelled", wantErr: ErrBuildConflict},
		{name: "chunk write failure", parent: "failed", status: "failed", failure: "UPDATE vanadis_chunks", wantErr: writeErr},
		{name: "parent write failure", parent: "failed", status: "failed", failure: "UPDATE vanadis_builds", wantErr: writeErr},
		{name: "commit failure", parent: "failed", status: "failed", commitErr: writeErr, wantErr: writeErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunks := []VanadisChunk{
				{ID: "before", Status: "completed", Repo: "other", ChunkIndex: 0},
				{ID: "between", Status: "completed", Repo: "other", ChunkIndex: 1},
				{ID: "after", Status: "completed", Repo: "other", ChunkIndex: 2},
			}
			chunks[tc.index] = VanadisChunk{ID: "selected", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: tc.status, ChunkIndex: tc.index, PRNumber: tc.previous, CIFailCount: tc.cap}
			if tc.sibling != "" {
				chunks[(tc.index+1)%3].Status = tc.sibling
			}
			script := attachmentScript("selected", "b1", "thread-1", tc.parent, "aerial")
			script.chunks = chunks
			script.failExec, script.execErr, script.commitErr = tc.failure, writeErr, tc.commitErr
			// No failure route means writes succeed; this fake does not apply SQL state changes.
			if tc.failure == "" {
				script.execErr = nil
			}
			store, _ := script.open()
			b, c, th, err := store.AttachChunkPR(context.Background(), attachRequest("selected", "aerial"))
			if assertStoreErr(t, err, tc.wantErr, false) {
				return
			}
			if b != "b1" || c != "selected" || th != "thread-1" {
				t.Fatalf("attachment = %q %q %q", b, c, th)
			}
		})
	}
}

func TestFirstWorkerFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, mode, status string
		previous, failures int
		want               bool
	}{
		{"eligible", "pr", "failed", 0, 0, true},
		{"already attached", "pr", "failed", 101, 0, false},
		{"CI attempts", "pr", "failed", 0, 3, false},
		{"action", "action", "failed", 0, 0, false},
		{"running", "pr", "running", 0, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := firstWorkerFailure(VanadisChunk{Mode: tc.mode, Status: tc.status, PRNumber: tc.previous, CIFailCount: tc.failures})
			if got != tc.want {
				t.Fatalf("eligible = %v, want %v", got, tc.want)
			}
		})
	}
}

// The database supplies an already recovered chunk. This tests the real HTTP
// remediation wake, not persistence of the preceding attachment transaction.
func TestRecoveredChunkRemediationWake(t *testing.T) {
	t.Parallel()
	store, _ := dbScript{buildRow: &mockRow{b: &VanadisBuild{ID: "b1", ThreadID: "thread-1", Status: "running"}}, chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: "pr_opened", PRNumber: 101}}}.open()
	payloads := make(chan map[string]string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]string
		if r.URL.Path != "/prompt" || r.Method != http.MethodPost {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		payloads <- payload
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor(t.TempDir(), NewHTTPEriClient(server.URL, server.Client()), nil, "", nil))
	ctx := makeObjectMockContext(t, "b1")
	_, err := restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		return true, bo.handlePREventType(rc, "b1", "c1", "ci_failed", pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, CheckName: "unit", Details: "unit test failed"})
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-payloads:
		if payload["channel_id"] != "thread-1" || !strings.Contains(payload["prompt"], "unit test failed") || !strings.Contains(payload["prompt"], "101") {
			t.Fatalf("payload = %+v", payload)
		}
	default:
		t.Fatal("no remediation wake")
	}
}

// Inspect the callback error before the SDK can retry the journaled operation.
func TestExecutePRAttachment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		req         AttachChunkPRRequest
		db          dbScript
		wantCode    restate.Code
		wantMessage string
	}{
		{"missing action repo", attachRequest("c1", ""), dbScript{routes: attachRoutes("action", true, false)}, 400, "repo is required"},
		{"blank action repo", attachRequest("c1", "  "), dbScript{routes: attachRoutes("action", true, false)}, 400, "repo is required"},
		{"standalone repo", attachRequest("", ""), dbScript{}, 400, "repo is required"},
		{"invalid number", AttachChunkPRRequest{PRNumber: -1}, dbScript{}, 400, "valid pr number"},
		{"transient database error", attachRequest("c1", "aerial"), dbScript{row: errRow(errors.New("connection interrupted"))}, 0, "connection interrupted"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			bo := NewBuildObject("", nil).WithAuthority(store, nil)
			mc := mocks.NewMockContext(t)
			mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), _ any, _ ...restate.RunOption) restate.TerminalError {
				_, err := fn(mockRunContext{Context: context.Background()})
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantMessage)
				var terminal restate.TerminalError
				if tc.wantCode > 0 {
					require.True(t, errors.As(err, &terminal), "journal callback must stop retries: %v", err)
					assert.Equal(t, tc.wantCode, terminal.Code())
				} else {
					assert.False(t, errors.As(err, &terminal), "transient failures must remain retryable")
				}
				return restate.ToTerminalError(err)
			})
			recorded := false
			_, err := bo.executePRAttachment(restate.WithMockContext(mc), tc.req, &recorded)
			require.Error(t, err)
		})
	}
}

func sqlListContains(query, prefix, val string) bool {
	_, rest, ok := strings.Cut(query, prefix)
	if !ok {
		return false
	}
	list, _, ok := strings.Cut(rest, ")")
	return ok && strings.Contains(list, "'"+val+"'")
}

func (s dbScript) evalAttachSelectRow(query string, args []any) pgx.Row {
	if len(args) != 1 {
		return nil
	}
	isMode := strings.HasPrefix(query, "SELECT mode FROM vanadis_chunks")
	isBuild := strings.HasPrefix(query, "SELECT build_id FROM vanadis_chunks")
	if !isMode && !isBuild {
		return nil
	}
	for _, c := range s.chunks {
		if c.ID != args[0] {
			continue
		}
		if isMode {
			return valuesRow(c.Mode)
		}
		return valuesRow(c.BuildID)
	}
	return errRow(pgx.ErrNoRows)
}

func (s dbScript) evalAttachActionUpdate(query string, b *VanadisBuild, args []any) pgx.Row {
	repo, _ := args[0].(string)
	prNum, _ := args[1].(int)
	chunkID, _ := args[2].(string)
	cands, _ := args[3].([]string)
	for i := range s.chunks {
		c := &s.chunks[i]
		if c.ID != chunkID || c.BuildID != b.ID {
			continue
		}
		repoMatch := strings.Contains(query, "c.repo = ANY($4)") && slices.Contains(cands, c.Repo)
		samePR := repoMatch && c.PRNumber == prNum
		terminal := sqlListContains(query, "c.status NOT IN (", c.Status)
		if c.Status == statusCancelled || b.Status == statusCancelled || b.Status == statusCompleted || (c.Repo != "" && !repoMatch) || (terminal && !samePR) {
			return errRow(pgx.ErrNoRows)
		}
		prevStatus := c.Status
		if prevStatus != statusFailed && (!samePR || !sqlListContains(query, "c.pr_number = $2 AND c.status IN (", prevStatus)) {
			c.Status = statusPROpened
		}
		if !samePR && sqlListContains(query, "banner = CASE WHEN c.status IN (", prevStatus) {
			c.Banner = ""
		}
		c.Repo = repo
		c.PRNumber = prNum
		return valuesRow(b.ID, c.ID, b.ThreadID)
	}
	return errRow(pgx.ErrNoRows)
}

func (s dbScript) evalAttachExistsRow(query string, b *VanadisBuild, args []any) pgx.Row {
	chunkID, _ := args[0].(string)
	for _, c := range s.chunks {
		if c.ID != chunkID {
			continue
		}
		if strings.Contains(query, "c.status = 'cancelled'") {
			isTerm := c.Status == statusCancelled || sqlListContains(query, "c.status IN (", c.Status) || b.Status == statusCancelled || b.Status == statusCompleted
			return valuesRow(isTerm)
		}
		if strings.Contains(query, "repo != ''") && len(args) == 2 {
			cands, _ := args[1].([]string)
			return valuesRow(c.Repo != "" && !slices.Contains(cands, c.Repo))
		}
		return valuesRow(true)
	}
	return valuesRow(false)
}

func (s dbScript) evalAttachQueryRow(query string, args []any) pgx.Row {
	if row := s.evalAttachSelectRow(query, args); row != nil {
		return row
	}
	mr, ok := s.buildRow.(*mockRow)
	if !ok || mr == nil || mr.b == nil {
		return nil
	}
	if strings.HasPrefix(query, "UPDATE vanadis_chunks c SET repo = $1, pr_number = $2") && len(args) == 4 {
		return s.evalAttachActionUpdate(query, mr.b, args)
	}
	if strings.HasPrefix(query, "SELECT EXISTS") && len(args) >= 1 {
		return s.evalAttachExistsRow(query, mr.b, args)
	}
	return nil
}

func (s dbScript) evalAttachExec(query string, args []any) {
	if strings.HasPrefix(query, "UPDATE vanadis_chunks SET pr_number = $1") && len(args) == 3 {
		prNum, _ := args[0].(int)
		chunkID, _ := args[1].(string)
		recoverWorker, _ := args[2].(bool)
		for i := range s.chunks {
			c := &s.chunks[i]
			if c.ID != chunkID {
				continue
			}
			prevStatus := c.Status
			samePR := c.PRNumber == prNum
			if recoverWorker || (prevStatus != statusFailed && (!samePR || !sqlListContains(query, "COALESCE(pr_number, 0) = $1 AND status IN (", prevStatus))) {
				c.Status = statusPROpened
			}
			if recoverWorker || (!samePR && sqlListContains(query, "banner = CASE WHEN $3 OR (status IN (", prevStatus)) {
				c.Banner = ""
			}
			if recoverWorker && strings.Contains(query, "completed_at = CASE WHEN $3 THEN NULL") {
				c.CompletedAt = time.Time{}
			}
			c.PRNumber = prNum
		}
	}
	if strings.HasPrefix(query, "UPDATE vanadis_builds SET status = 'running'") {
		if mr, ok := s.buildRow.(*mockRow); ok && mr != nil && mr.b != nil {
			mr.b.Status = statusRunning
		}
	}
}

func TestAttachmentMatches_TerminalAndAdvancedStatuses(t *testing.T) {
	t.Parallel()

	completedTime := time.Unix(1700000000, 0).UTC()
	chunkRow := func(mode, status, repo string, pr int, banner string, completedAt time.Time) VanadisChunk {
		return VanadisChunk{ID: "c1", BuildID: "b1", Mode: mode, Status: status, Repo: repo, PRNumber: pr, Banner: banner, CompletedAt: completedAt}
	}
	reqRow := func(repo string, pr int) AttachChunkPRRequest {
		return AttachChunkPRRequest{ChunkID: "c1", Repo: repo, PRNumber: pr}
	}
	tests := []struct {
		name               string
		chunk              VanadisChunk
		build              VanadisBuild
		req                AttachChunkPRRequest
		wantConflict       bool
		wantStatus         string
		wantBanner         string
		wantBuildStatus    string
		wantRepo           string
		wantClearCompleted bool
	}{
		{"merged pr matching", chunkRow("pr", statusMerged, "aerial", 101, "merged banner", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), false, statusMerged, "merged banner", statusRunning, "aerial", false},
		{"merged pr mismatched", chunkRow("pr", statusMerged, "aerial", 101, "", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 102), true, "", "", "", "", false},
		{"synced pr matching", chunkRow("pr", statusSynced, "aerial", 101, "synced banner", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), false, statusSynced, "synced banner", statusRunning, "aerial", false},
		{"synced pr mismatched", chunkRow("pr", statusSynced, "aerial", 101, "", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 102), true, "", "", "", "", false},
		{"deploying pr matching", chunkRow("pr", statusDeploying, "aerial", 101, "", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), false, statusDeploying, "", statusRunning, "aerial", false},
		{"deploying pr mismatched", chunkRow("pr", statusDeploying, "aerial", 101, "", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 102), true, "", "", "", "", false},
		{"deployed pr matching", chunkRow("pr", statusDeployed, "aerial", 101, "", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), false, statusDeployed, "", statusRunning, "aerial", false},
		{"deployed pr mismatched", chunkRow("pr", statusDeployed, "aerial", 101, "", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 102), true, "", "", "", "", false},
		{"deploy_failed pr matching", chunkRow("pr", statusDeployFailed, "aerial", 101, "deploy failed", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), false, statusDeployFailed, "deploy failed", statusRunning, "aerial", false},
		{"deploy_failed pr mismatched", chunkRow("pr", statusDeployFailed, "aerial", 101, "", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 102), true, "", "", "", "", false},
		{"completed pr matching", chunkRow("pr", statusCompleted, "aerial", 101, "", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), false, statusCompleted, "", statusRunning, "aerial", false},
		{"ci_failed pr same PR preserves state", chunkRow("pr", statusCIFailed, "aerial", 101, "unit failed", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), false, statusCIFailed, "unit failed", statusRunning, "aerial", false},
		{"ci_failed pr replaced resets state", chunkRow("pr", statusCIFailed, "aerial", 100, "unit failed", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), false, statusPROpened, "", statusRunning, "aerial", false},
		{"conflict pr replaced resets state", chunkRow("pr", statusConflict, "aerial", 100, "merge conflict", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), false, statusPROpened, "", statusRunning, "aerial", false},
		{"worker failure recovery", chunkRow("pr", statusFailed, "aerial", 0, "worker crashed", completedTime), VanadisBuild{ID: "b1", Status: statusFailed}, reqRow("aerial", 101), false, statusPROpened, "", statusRunning, "aerial", true},
		{"action ci_failed repo alias preserves state", chunkRow("action", statusCIFailed, "azylman/aerial", 101, "action ci failed", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), false, statusCIFailed, "action ci failed", statusRunning, "aerial", false},
		{"action conflict repo alias replaced resets state", chunkRow("action", statusConflict, "azylman/aerial", 100, "action conflict", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), false, statusPROpened, "", statusRunning, "aerial", false},
		{"action merged repo alias rejects replacement", chunkRow("action", statusMerged, "azylman/aerial", 100, "", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), true, "", "", "", "", false},
		{"cancelled chunk rejected", chunkRow("pr", statusCancelled, "aerial", 101, "", time.Time{}), VanadisBuild{ID: "b1", Status: statusRunning}, reqRow("aerial", 101), true, "", "", "", "", false},
		{"completed build rejected", chunkRow("pr", statusRunning, "aerial", 0, "", time.Time{}), VanadisBuild{ID: "b1", Status: statusCompleted}, reqRow("aerial", 101), true, "", "", "", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			buildCopy := tc.build
			store, _ := dbScript{
				buildRow:           &mockRow{b: &buildCopy},
				chunks:             []VanadisChunk{tc.chunk},
				applyAttachUpdates: true,
			}.open()
			_, _, _, err := store.AttachChunkPR(context.Background(), tc.req)
			if tc.wantConflict {
				require.ErrorIs(t, err, ErrBuildConflict)
				return
			}
			require.NoError(t, err)
			gotBuild, gotChunks, gErr := store.GetBuild(context.Background(), tc.build.ID)
			require.NoError(t, gErr)
			require.Len(t, gotChunks, 1)
			assert.Equal(t, tc.wantBuildStatus, gotBuild.Status)
			assert.Equal(t, tc.wantStatus, gotChunks[0].Status)
			assert.Equal(t, tc.wantBanner, gotChunks[0].Banner)
			assert.Equal(t, tc.req.PRNumber, gotChunks[0].PRNumber)
			assert.Equal(t, tc.wantRepo, gotChunks[0].Repo)
			if tc.wantClearCompleted {
				assert.True(t, gotChunks[0].CompletedAt.IsZero())
			}
		})
	}
}
