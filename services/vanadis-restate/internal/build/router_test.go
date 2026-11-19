package build

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

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
	return &model.VanadisBuild{ID: "b1", Status: statusFailed, ThreadID: "th-1"}, &model.VanadisChunk{ID: chunkID, Status: statusFailed}, true, nil
}

func TestBuildObject_OnPREvent_UnattachedPRResolution_Matches(t *testing.T) {
	t.Parallel()

	startedAt := time.Unix(1700000000, 0).UTC()
	validGitHubPR := `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"Summary\n\n<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`
	prEv := func(ev, branch, sha, target string, pr int) pubsub.PRLifecycleEvent {
		return pubsub.PRLifecycleEvent{Event: ev, Repo: "external/aerial", PRNumber: pr, Branch: branch, HeadSHA: sha, TargetID: target}
	}
	ch := func(id, status string, pr int, started time.Time) []model.VanadisChunk {
		return []model.VanadisChunk{{ID: id, BuildID: "b1", Mode: "pr", Status: status, Repo: "external/aerial", PRNumber: pr, StartedAt: started}}
	}

	tests := []struct {
		name           string
		event          pubsub.PRLifecycleEvent
		chunks         []model.VanadisChunk
		ghResponse     string
		withoutExec    bool
		wantAttached   string
		wantAttachedPR int
		wantWidgetSync bool
		wantFailed     string
		widgetErr      error
	}{
		{"early opened attached via verified PR", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 101), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, false, "chunk-1", 101, true, "", nil},
		{"fork contributor PR rejected on close", prEv("closed_unmerged", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 102), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"closed","merged":false,"author_association":"FIRST_TIME_CONTRIBUTOR","user":{"login":"attacker"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-bad","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":true,"full_name":"attacker/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, "", nil},
		{"untrusted author association rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 103), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"NONE","user":{"login":"stranger"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, "", nil},
		{"missing dispatch trailer rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 104), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"manual PR","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, "", nil},
		{"unstarted chunk rejected before dispatch", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 105), ch("chunk-1", statusRunning, 0, time.Time{}), validGitHubPR, false, "", 0, false, "", nil},
		{"superseded pr rejected when chunk has different pr", prEv("closed_unmerged", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 101), ch("chunk-1", statusRunning, 200, startedAt), validGitHubPR, false, "", 0, false, "", nil},
		{"ineligible chunk status rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 101), ch("chunk-1", statusCompleted, 0, startedAt), validGitHubPR, false, "", 0, false, "", nil},
		{"unmatched pr event without branch encoding", prEv("opened", "main", "", "", 101), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, false, "", 0, false, "", nil},
		{"target chunk absent in chunks list", prEv("opened", "feat/aerial-pr-vanadis-chunk-99-abcdef", "", "", 101), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, false, "", 0, false, "", nil},
		{"collaborator author accepted with matching sha", prEv("check_updated", "feat/aerial-pr-vanadis-chunk-1-abcdef", "sha-1", "th-1", 101), ch("chunk-1", statusCoding, 0, startedAt), `{"state":"open","merged":false,"author_association":"COLLABORATOR","user":{"login":"aerial-bot"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "chunk-1", 101, true, "", nil},
		{"nonencoded branch resolved via unambiguous thread and verified trailer", prEv("ci_failed", "feat/custom-branch", "sha-1", "th-1", 101), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/custom-branch","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "chunk-1", 101, true, "", nil},
		{"absent event branch resolved via unambiguous thread and verified trailer", prEv("conflict", "", "sha-1", "th-1", 101), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/custom-branch","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "chunk-1", 101, true, "", nil},
		{"nonencoded branch rejected without trailer", prEv("opened", "feat/custom-branch", "sha-1", "th-1", 101), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"manual PR","head":{"sha":"sha-1","ref":"feat/custom-branch","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, "", nil},
		{"nonencoded branch rejected when parallel chunks ambiguous", prEv("opened", "feat/custom-branch", "sha-1", "th-1", 101), []model.VanadisChunk{{ID: "chunk-1", BuildID: "b1", Mode: "pr", Status: statusRunning, Repo: "external/aerial", StartedAt: startedAt}, {ID: "chunk-2", BuildID: "b1", Mode: "pr", Status: statusCoding, Repo: "external/aerial", StartedAt: startedAt}}, `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/custom-branch","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, "", nil},
		{"mismatched head repository rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 102), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"other/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, "", nil},
		{"mismatched head branch ref rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 102), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-2-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, "", nil},
		{"mismatched head sha rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "sha-other", "", 102), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, false, "", 0, false, "", nil},
		{"mismatched event target thread rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "th-wrong", 102), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, false, "", 0, false, "", nil},
		{"mismatched trailer chunk id rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 102), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-other\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, "", nil},
		{"nil executor refuses unverified attachment", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 101), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, true, "", 0, false, "", nil},
		{"widget sync failure still attaches", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 101), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, false, "chunk-1", 101, false, "", errors.New("widget down")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &unattachedPREventSpyStore{chunks: tc.chunks}
			var exec *executor.Executor
			if !tc.withoutExec {
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
					fmt.Fprint(w, tc.ghResponse)
				}))
				t.Cleanup(api.Close)
				eri := &mockEriClient{
					widgetFn: func(context.Context, executor.StatusWidgetRequest) (string, error) {
						return "msg-1", tc.widgetErr
					},
				}
				exec = executor.NewExecutor(t.TempDir(), eri, nil, "token", api.Client())
				exec.GitHubAPIURL = api.URL
			}
			bo := NewBuildObject("", nil).WithAuthority(spy, exec)
			ctx := makeObjectMockContext(t, "b1")

			assert.NoError(t, bo.OnPREvent(ctx, tc.event))
			assert.Equal(t, tc.wantAttached, spy.attachedChunkID)
			assert.Equal(t, tc.wantFailed, spy.failedChunkID)
			if tc.wantAttached != "" {
				assert.Equal(t, tc.wantAttachedPR, spy.attachedPR)
				assert.Equal(t, tc.event.Repo, spy.attachedRepo)
			}
			if tc.wantWidgetSync {
				assert.Positive(t, spy.syncWidgetCalls)
			}
		})
	}
}

func TestBuildObject_OnPREvent_UnattachedPRResolution_Errors(t *testing.T) {
	t.Parallel()

	errBuild := errors.New("build db error")
	errAttach := errors.New("attach generic error")
	startedAt := time.Unix(1700000000, 0).UTC()
	validGitHubPR := `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"azylman"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"azylman/aerial"}},"base":{"repo":{"full_name":"azylman/aerial"}}}`

	tests := []struct {
		name        string
		event       pubsub.PRLifecycleEvent
		getBuildErr error
		attachErr   error
		ghStatus    int
		wantErr     bool
	}{
		{
			name: "get build error propagates",
			event: pubsub.PRLifecycleEvent{
				Event:    "opened",
				Repo:     "aerial",
				PRNumber: 101,
				Branch:   "feat/aerial-pr-vanadis-chunk-1-abcdef",
			},
			getBuildErr: errBuild,
			ghStatus:    http.StatusOK,
			wantErr:     true,
		},
		{
			name: "get build not found returns nil",
			event: pubsub.PRLifecycleEvent{
				Event:    "opened",
				Repo:     "aerial",
				PRNumber: 101,
				Branch:   "feat/aerial-pr-vanadis-chunk-1-abcdef",
			},
			getBuildErr: store.ErrBuildNotFound,
			ghStatus:    http.StatusOK,
			wantErr:     false,
		},
		{
			name: "github verification failure propagates retryable error",
			event: pubsub.PRLifecycleEvent{
				Event:    "opened",
				Repo:     "aerial",
				PRNumber: 101,
				Branch:   "feat/aerial-pr-vanadis-chunk-1-abcdef",
			},
			ghStatus: http.StatusServiceUnavailable,
			wantErr:  true,
		},
		{
			name: "attach chunk conflict swallowed",
			event: pubsub.PRLifecycleEvent{
				Event:    "opened",
				Repo:     "aerial",
				PRNumber: 101,
				Branch:   "feat/aerial-pr-vanadis-chunk-1-abcdef",
			},
			attachErr: store.ErrBuildConflict,
			ghStatus:  http.StatusOK,
			wantErr:   false,
		},
		{
			name: "attach chunk generic error propagates",
			event: pubsub.PRLifecycleEvent{
				Event:    "opened",
				Repo:     "aerial",
				PRNumber: 101,
				Branch:   "feat/aerial-pr-vanadis-chunk-1-abcdef",
			},
			attachErr: errAttach,
			ghStatus:  http.StatusOK,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.ghStatus)
				fmt.Fprint(w, validGitHubPR)
			}))
			t.Cleanup(api.Close)
			exec := executor.NewExecutor(t.TempDir(), &mockEriClient{}, nil, "token", api.Client())
			exec.GitHubAPIURL = api.URL
			spy := &unattachedPREventSpyStore{
				chunks: []model.VanadisChunk{
					{ID: "chunk-1", BuildID: "b1", Mode: "pr", Status: statusRunning, Repo: "aerial", PRNumber: 0, StartedAt: startedAt},
				},
				getBuildErr: tc.getBuildErr,
				attachErr:   tc.attachErr,
			}
			bo := NewBuildObject("", nil).WithAuthority(spy, exec)
			ctx := makeObjectMockContext(t, "b1")

			err := bo.OnPREvent(ctx, tc.event)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNoPRTurnAction(t *testing.T) {
	t.Parallel()
	pr := func(status string, prNumber int) model.VanadisChunk {
		return model.VanadisChunk{ID: "c1", Mode: "pr", Status: status, PRNumber: prNumber}
	}
	tests := []struct {
		name        string
		chunk       model.VanadisChunk
		buildStatus string
		dispatchID  string
		want        noPRAction
	}{
		{"running pr chunk launch turn resumes", pr("running", 0), "running", "vanadis-c1-turn-0", noPRActionResume},
		{"coding pr chunk launch turn resumes", pr("coding", 0), "running", "vanadis-c1-turn-0", noPRActionResume},
		{"legacy empty mode launch turn resumes", model.VanadisChunk{ID: "c1", Status: "running"}, "running", "vanadis-c1-turn-0", noPRActionResume},
		{"resume turn without pr fails", pr("running", 0), "running", "vanadis-c1-nopr-resume-1", noPRActionFail},
		{"attached pr ignored", pr("running", 12), "running", "vanadis-c1-turn-0", noPRActionNone},
		{"pr opened status ignored", pr("pr_opened", 0), "running", "vanadis-c1-turn-0", noPRActionNone},
		{"failed chunk ignored", pr("failed", 0), "running", "vanadis-c1-nopr-resume-1", noPRActionNone},
		{"cancelled chunk ignored", pr("cancelled", 0), "running", "vanadis-c1-turn-0", noPRActionNone},
		{"failed build ignored", pr("running", 0), "failed", "vanadis-c1-turn-0", noPRActionNone},
		{"cancelled build ignored", pr("running", 0), "cancelled", "vanadis-c1-nopr-resume-1", noPRActionNone},
		{"ci remediation dispatch ignored", pr("running", 0), "running", "vanadis-c1-cifail-abc123-1", noPRActionNone},
		{"conflict remediation dispatch ignored", pr("running", 0), "running", "vanadis-conflict-c1-abc123", noPRActionNone},
		{"other chunk resume id ignored", pr("running", 0), "running", "vanadis-c2-nopr-resume-1", noPRActionNone},
		{"action chunk ignored", model.VanadisChunk{ID: "c1", Mode: "action", Status: "running"}, "running", "vanadis-c1-turn-0", noPRActionNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, noPRTurnAction(tc.chunk, tc.buildStatus, tc.dispatchID))
		})
	}
}

func TestFindChunkIndex(t *testing.T) {
	t.Parallel()
	present := []model.VanadisChunk{{ID: "c1"}, {ID: "c2"}}
	tests := []struct {
		name   string
		chunks []model.VanadisChunk
		id     string
		want   int
	}{
		{"first match", present, "c1", 0},
		{"later match", present, "c2", 1},
		{"missing chunk", present, "c9", -1},
		{"empty list", nil, "c1", -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, findChunkIndex(tc.chunks, tc.id))
		})
	}
}

func TestNoPRResumeDispatchID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, chunkID, want string
	}{
		{"plain chunk id", "chunk-1-fix", "vanadis-chunk-1-fix-nopr-resume-1"},
		{"whitespace trimmed", " c1 ", "vanadis-c1-nopr-resume-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, noPRResumeDispatchID(tc.chunkID))
		})
	}
}

func TestExtractChunkIDFromBranch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		repo   string
		branch string
		want   string
	}{
		{name: "empty repo", repo: "", branch: "feat/aerial-pr-vanadis-c1-012345", want: ""},
		{name: "empty branch", repo: "aerial", branch: "", want: ""},
		{name: "standalone branch without vanadis", repo: "aerial", branch: "feat/aerial-pr-20261116-123456", want: ""},
		{name: "valid with refs/heads", repo: "azylman/aerial", branch: "refs/heads/feat/aerial-pr-vanadis-chunk-1-router-a1b2c3", want: "chunk-1-router"},
		{name: "valid bare branch", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-012345", want: "c1"},
		{name: "valid uppercase hex", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-ABCDEF", want: "c1"},
		{name: "repo containing delimiter and chunk containing delimiter", repo: "owner/demo-pr-vanadis-tools", branch: "feat/demo-pr-vanadis-tools-pr-vanadis-c1-pr-vanadis-2-a1b2c3", want: "c1-pr-vanadis-2"},
		{name: "invalid suffix too short", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-12345", want: ""},
		{name: "invalid suffix too long", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-1234567", want: ""},
		{name: "invalid suffix non-hex", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-12345g", want: ""},
		{name: "no separator after vanadis", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1", want: ""},
		{name: "empty chunk id", repo: "aerial", branch: "feat/aerial-pr-vanadis--123456", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ExtractChunkIDFromBranch(tc.repo, tc.branch))
		})
	}
}
