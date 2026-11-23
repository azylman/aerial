package build

// Declarative store fakes for the build package. fakeStore is a pure
// in-memory store.VanadisStore for tests that do not need Postgres;
// faultStore wraps the real pgtest-backed store from openStore/seededStore and
// injects faults around it. Both record every call in calls as
// "Method\targ\targ..." (failed calls are prefixed with "!") and take canned
// errors from errs, keyed by "Method:chunkID" for a single chunk or by
// "Method" for every call.

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// fakeStore keeps builds and chunks snapshots and applies writes to them the
// way the PostgreSQL authority does. failFrom delays errs[method] until that
// 1-based call ordinal; completion, when set, is the canned
// CompleteActionChunk result; swept is what the cross-build sweep returns
// (alongside any errs entry).
type fakeStore struct {
	mu         sync.Mutex
	builds     []model.VanadisBuild
	chunks     []model.VanadisChunk
	errs       map[string]error
	failFrom   map[string]int
	completion *store.VanadisAdvancementResult
	swept      []store.SweptChunk
	calls      []string
	counts     map[string]int
}

var _ store.VanadisStore = (*fakeStore)(nil)

// callArgs returns the arguments of each successful call to method in order.
func callArgs(calls []string, method string) [][]string {
	var out [][]string
	for _, c := range calls {
		parts := strings.Split(c, "\t")
		if parts[0] == method {
			out = append(out, parts[1:])
		}
	}
	return out
}

// pickCalls joins the selected arguments of each successful call to method
// with sep; nil when there were none.
func pickCalls(calls []string, method, sep string, idx ...int) []string {
	matched := callArgs(calls, method)
	if len(matched) == 0 {
		return nil
	}
	out := make([]string, 0, len(matched))
	for _, args := range matched {
		sel := make([]string, len(idx))
		for i, n := range idx {
			if n < len(args) {
				sel[i] = args[n]
			}
		}
		out = append(out, strings.Join(sel, sep))
	}
	return out
}

// attemptedCalls returns every recorded call, successful or not.
func (s *fakeStore) attemptedCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.calls))
	for i, c := range s.calls {
		out[i] = strings.TrimPrefix(c, "!")
	}
	return out
}

// foreignReads lists every GetBuild of a build other than own.
func (s *fakeStore) foreignReads(own string) []string {
	var out []string
	for _, args := range callArgs(s.attemptedCalls(), "GetBuild") {
		if args[0] != own {
			out = append(out, args[0])
		}
	}
	return out
}

// fault returns the canned error for method on chunkID; callers hold mu.
func (s *fakeStore) fault(method, chunkID string) error {
	if s.counts == nil {
		s.counts = map[string]int{}
	}
	s.counts[method]++
	if err := s.errs[method+":"+chunkID]; err != nil {
		return err
	}
	if from := s.failFrom[method]; from > 0 && s.counts[method] < from {
		return nil
	}
	return s.errs[method]
}

// done records one call and returns err; callers hold mu.
func (s *fakeStore) done(err error, method string, args ...string) error {
	entry := strings.Join(append([]string{method}, args...), "\t")
	if err != nil {
		entry = "!" + entry
	}
	s.calls = append(s.calls, entry)
	return err
}

func (s *fakeStore) snapshotCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls)
}

func (s *fakeStore) build(id string) *model.VanadisBuild {
	for i := range s.builds {
		if s.builds[i].ID == id {
			return &s.builds[i]
		}
	}
	return nil
}

func (s *fakeStore) chunk(id string) *model.VanadisChunk {
	for i := range s.chunks {
		if s.chunks[i].ID == id {
			return &s.chunks[i]
		}
	}
	return nil
}

func (s *fakeStore) chunkSnapshot(id string) model.VanadisChunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.chunk(id); c != nil {
		return *c
	}
	return model.VanadisChunk{}
}

func (s *fakeStore) buildSnapshot(id string) model.VanadisBuild {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b := s.build(id); b != nil {
		return *b
	}
	return model.VanadisBuild{}
}

func errUnimplemented(method string) error {
	return fmt.Errorf("fakeStore: %s not implemented", method)
}

func (s *fakeStore) CreateBuild(_ context.Context, b model.VanadisBuild, chunks []model.VanadisChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("CreateBuild", ""); err != nil {
		return s.done(err, "CreateBuild", createArgs(b, chunks)...)
	}
	s.builds = slices.DeleteFunc(s.builds, func(x model.VanadisBuild) bool { return x.ID == b.ID })
	s.builds = append(s.builds, b)
	s.chunks = append(s.chunks, chunks...)
	return s.done(nil, "CreateBuild", createArgs(b, chunks)...)
}

// createArgs records a CreateBuild call as the build ID and creation time
// followed by each chunk's creation and update times (RFC 3339, nanoseconds).
func createArgs(b model.VanadisBuild, chunks []model.VanadisChunk) []string {
	args := make([]string, 0, 2+2*len(chunks))
	args = append(args, b.ID, b.CreatedAt.Format(time.RFC3339Nano))
	for _, c := range chunks {
		args = append(args, c.CreatedAt.Format(time.RFC3339Nano), c.UpdatedAt.Format(time.RFC3339Nano))
	}
	return args
}

func (s *fakeStore) GetBuild(_ context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("GetBuild", ""); err != nil {
		return nil, nil, s.done(err, "GetBuild", buildID)
	}
	b := s.build(buildID)
	if b == nil {
		return nil, nil, s.done(store.ErrBuildNotFound, "GetBuild", buildID)
	}
	bld := *b
	var chunks []model.VanadisChunk
	for _, c := range s.chunks {
		if c.BuildID == buildID {
			chunks = append(chunks, c)
		}
	}
	return &bld, chunks, s.done(nil, "GetBuild", buildID)
}

func (s *fakeStore) ReserveChunkForLaunch(_ context.Context, chunkID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done(s.fault("ReserveChunkForLaunch", chunkID), "ReserveChunkForLaunch", chunkID)
}

func (s *fakeStore) UpdateBuildStatus(_ context.Context, buildID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("UpdateBuildStatus", ""); err != nil {
		return s.done(err, "UpdateBuildStatus", buildID, status)
	}
	if b := s.build(buildID); b != nil {
		b.Status = status
	}
	return s.done(nil, "UpdateBuildStatus", buildID, status)
}

func (s *fakeStore) CompareAndSwapBuildStatusMessageID(_ context.Context, buildID, oldID, newID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("CompareAndSwapBuildStatusMessageID", ""); err != nil {
		return false, s.done(err, "CompareAndSwapBuildStatusMessageID", buildID, oldID, newID)
	}
	if b := s.build(buildID); b != nil {
		b.StatusMessageID = newID
	}
	return true, s.done(nil, "CompareAndSwapBuildStatusMessageID", buildID, oldID, newID)
}

func (s *fakeStore) GetChunkByPR(_ context.Context, repo string, prNumber int) (*store.VanadisChunkPRLookup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pr := strconv.Itoa(prNumber)
	if err := s.fault("GetChunkByPR", ""); err != nil {
		return nil, s.done(err, "GetChunkByPR", repo, pr)
	}
	for _, c := range s.chunks {
		if prNumber > 0 && c.PRNumber == prNumber && (c.Repo == "" || c.Repo == repo) {
			return &store.VanadisChunkPRLookup{Matched: true, ChunkID: c.ID, BuildID: c.BuildID, Effort: c.Effort}, s.done(nil, "GetChunkByPR", repo, pr)
		}
	}
	return &store.VanadisChunkPRLookup{}, s.done(nil, "GetChunkByPR", repo, pr)
}

func (s *fakeStore) AttachChunkPR(_ context.Context, req store.AttachChunkPRRequest) (string, string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	args := []string{req.ChunkID, strconv.Itoa(req.PRNumber), req.Repo, req.Agent}
	if err := s.fault("AttachChunkPR", req.ChunkID); err != nil {
		return "", "", "", s.done(err, "AttachChunkPR", args...)
	}
	c := s.chunk(req.ChunkID)
	if c == nil {
		return "", "", "", s.done(store.ErrChunkNotFound, "AttachChunkPR", args...)
	}
	c.PRNumber, c.Repo = req.PRNumber, req.Repo
	threadID := ""
	if b := s.build(c.BuildID); b != nil {
		threadID = b.ThreadID
	}
	return c.BuildID, c.ID, threadID, s.done(nil, "AttachChunkPR", args...)
}

// CompleteActionChunk accepts the current dispatch of a running action
// chunk in a running build, and acknowledges a repeat once it completed.
func (s *fakeStore) CompleteActionChunk(_ context.Context, chunkID, dispatchID string) (*store.VanadisAdvancementResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("CompleteActionChunk", chunkID); err != nil {
		return nil, s.done(err, "CompleteActionChunk", chunkID, dispatchID)
	}
	if s.completion != nil {
		res := *s.completion
		return &res, s.done(nil, "CompleteActionChunk", chunkID, dispatchID)
	}
	c := s.chunk(chunkID)
	if c == nil {
		return &store.VanadisAdvancementResult{}, s.done(nil, "CompleteActionChunk", chunkID, dispatchID)
	}
	res := &store.VanadisAdvancementResult{Matched: true, BuildID: c.BuildID}
	b := s.build(c.BuildID)
	if b == nil || dispatchID != model.ActionDispatchID(*c) || c.Mode != "action" {
		return res, s.done(nil, "CompleteActionChunk", chunkID, dispatchID)
	}
	switch {
	case c.Status == statusRunning && b.Status == statusRunning:
		c.Status = statusCompleted
		res.CompletionAccepted, res.Advanced = true, true
	case c.Status == statusCompleted && (b.Status == statusRunning || b.Status == statusCompleted):
		res.CompletionAccepted = true
	}
	return res, s.done(nil, "CompleteActionChunk", chunkID, dispatchID)
}

func (s *fakeStore) FailChunk(_ context.Context, chunkID, reason, dispatchID string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("FailChunk", chunkID); err != nil {
		return nil, nil, false, s.done(err, "FailChunk", chunkID, reason, dispatchID)
	}
	c := s.chunk(chunkID)
	if c == nil {
		return nil, nil, false, s.done(store.ErrChunkNotFound, "FailChunk", chunkID, reason, dispatchID)
	}
	c.Status = statusFailed
	var bld model.VanadisBuild
	if b := s.build(c.BuildID); b != nil {
		b.Status = statusFailed
		bld = *b
	}
	ck := *c
	return &bld, &ck, true, s.done(nil, "FailChunk", chunkID, reason, dispatchID)
}

func (s *fakeStore) RemoveChunk(_ context.Context, buildID, chunkRef string) (*model.VanadisChunk, []model.VanadisChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("RemoveChunk", chunkRef); err != nil {
		return nil, nil, s.done(err, "RemoveChunk", buildID, chunkRef)
	}
	c := s.chunk(chunkRef)
	if c == nil || c.BuildID != buildID {
		return nil, nil, s.done(store.ErrChunkNotFound, "RemoveChunk", buildID, chunkRef)
	}
	removed := *c
	s.chunks = slices.DeleteFunc(s.chunks, func(x model.VanadisChunk) bool { return x.ID == chunkRef })
	var rest []model.VanadisChunk
	for _, x := range s.chunks {
		if x.BuildID == buildID {
			rest = append(rest, x)
		}
	}
	return &removed, rest, s.done(nil, "RemoveChunk", buildID, chunkRef)
}

func (s *fakeStore) RecordPRRolloutState(_ context.Context, chunkID, status string, rolloutState *pubsub.PRRolloutState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("RecordPRRolloutState", chunkID); err != nil {
		return s.done(err, "RecordPRRolloutState", chunkID, status)
	}
	if c := s.chunk(chunkID); c != nil {
		if status != "" {
			c.Status = status
		}
		c.RolloutState = rolloutState
	}
	return s.done(nil, "RecordPRRolloutState", chunkID, status)
}

func (s *fakeStore) RecordPRCheckStatus(_ context.Context, chunkID, headSHA, checkName, checkState string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done(s.fault("RecordPRCheckStatus", chunkID), "RecordPRCheckStatus", chunkID, headSHA, checkName, checkState)
}

func (s *fakeStore) ResetPRCheckState(_ context.Context, chunkID, headSHA, mergeSHA string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("ResetPRCheckState", chunkID); err != nil {
		return s.done(err, "ResetPRCheckState", chunkID, headSHA, mergeSHA)
	}
	if c := s.chunk(chunkID); c != nil {
		if sha := strings.TrimSpace(headSHA); sha != "" {
			c.HeadSHA = sha
		}
	}
	return s.done(nil, "ResetPRCheckState", chunkID, headSHA, mergeSHA)
}

func (s *fakeStore) IncrementChunkCIFailCount(_ context.Context, chunkID, banner string, headSHA ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("IncrementChunkCIFailCount", chunkID); err != nil {
		return 0, s.done(err, "IncrementChunkCIFailCount", chunkID, banner)
	}
	c := s.chunk(chunkID)
	if c == nil {
		return 0, s.done(store.ErrChunkNotFound, "IncrementChunkCIFailCount", chunkID, banner)
	}
	c.CIFailCount++
	c.Status, c.Banner = "ci_failed", banner
	if len(headSHA) > 0 && strings.TrimSpace(headSHA[0]) != "" {
		c.RemediatedSHA = strings.TrimSpace(headSHA[0])
	}
	return c.CIFailCount, s.done(nil, "IncrementChunkCIFailCount", chunkID, banner)
}

// CompletePRDeployment completes a deploying chunk unless it or its build is
// cancelled, or it failed without an attached PR.
func (s *fakeStore) CompletePRDeployment(_ context.Context, chunkID string, rolloutState *pubsub.PRRolloutState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("CompletePRDeployment", chunkID); err != nil {
		return s.done(err, "CompletePRDeployment", chunkID)
	}
	c := s.chunk(chunkID)
	if c == nil {
		return s.done(store.ErrPRTransitionSkipped, "CompletePRDeployment", chunkID)
	}
	b := s.build(c.BuildID)
	if (c.Status == statusFailed && (c.Mode != "pr" || c.PRNumber <= 0)) || c.Status == statusCancelled || (b != nil && b.Status == statusCancelled) {
		return s.done(store.ErrPRTransitionSkipped, "CompletePRDeployment", chunkID)
	}
	c.Status = statusCompleted
	if rolloutState != nil {
		c.RolloutState = rolloutState
	}
	return s.done(nil, "CompletePRDeployment", chunkID)
}

// MarkActionRepairDeployed returns an action chunk to running once its
// repair PR deployed.
func (s *fakeStore) MarkActionRepairDeployed(_ context.Context, chunkID string, rolloutState *pubsub.PRRolloutState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("MarkActionRepairDeployed", chunkID); err != nil {
		return s.done(err, "MarkActionRepairDeployed", chunkID)
	}
	c := s.chunk(chunkID)
	if c == nil || c.Mode != "action" {
		return s.done(store.ErrPRTransitionSkipped, "MarkActionRepairDeployed", chunkID)
	}
	c.Status = statusRunning
	if rolloutState != nil {
		c.RolloutState = rolloutState
	}
	return s.done(nil, "MarkActionRepairDeployed", chunkID)
}

// SweepEarlierDeploymentsAcrossBuilds records the deploying chunk and its
// expected jobs and returns swept, with errs["SweepEarlierDeploymentsAcrossBuilds"].
func (s *fakeStore) SweepEarlierDeploymentsAcrossBuilds(_ context.Context, deploying model.VanadisChunk) ([]store.SweptChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	args := []string{deploying.ID}
	if deploying.RolloutState != nil {
		args = append(args, deploying.RolloutState.ExpectedJobs...)
	}
	err := s.fault("SweepEarlierDeploymentsAcrossBuilds", deploying.ID)
	return slices.Clone(s.swept), s.done(err, "SweepEarlierDeploymentsAcrossBuilds", args...)
}

func (s *fakeStore) UpdatePRLifecycleStatus(_ context.Context, chunkID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("UpdatePRLifecycleStatus", chunkID); err != nil {
		return s.done(err, "UpdatePRLifecycleStatus", chunkID, status)
	}
	if c := s.chunk(chunkID); c != nil {
		c.Status = status
	}
	return s.done(nil, "UpdatePRLifecycleStatus", chunkID, status)
}

func (s *fakeStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fault("UpdateChunkBannerAndStatus", chunkID); err != nil {
		return s.done(err, "UpdateChunkBannerAndStatus", chunkID, status, banner)
	}
	if c := s.chunk(chunkID); c != nil {
		c.Status, c.Banner = status, banner
	}
	return s.done(nil, "UpdateChunkBannerAndStatus", chunkID, status, banner)
}

func (s *fakeStore) unimplemented(method string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done(errUnimplemented(method), method)
}

func (s *fakeStore) RetryActionChunk(context.Context, store.RetryActionChunkRequest) (*store.RetryActionChunkResult, error) {
	return nil, s.unimplemented("RetryActionChunk")
}

func (s *fakeStore) LookupPRChunk(context.Context, string, int) (store.BuildRoute, string, error) {
	return store.BuildRoute{}, "", s.unimplemented("LookupPRChunk")
}

func (s *fakeStore) LookupChunkBuildID(context.Context, string) (store.BuildRoute, error) {
	return store.BuildRoute{}, s.unimplemented("LookupChunkBuildID")
}

func (s *fakeStore) LookupUnattachedChunkByThreadAndRepo(context.Context, string, string) (store.BuildRoute, string, error) {
	return store.BuildRoute{}, "", s.unimplemented("LookupUnattachedChunkByThreadAndRepo")
}

func (s *fakeStore) UpdateBuild(context.Context, string, store.UpdateBuildRequest) (*model.VanadisBuild, error) {
	return nil, s.unimplemented("UpdateBuild")
}

func (s *fakeStore) UpdateChunk(context.Context, string, string, store.UpdateChunkRequest) (*model.VanadisChunk, error) {
	return nil, s.unimplemented("UpdateChunk")
}

func (s *fakeStore) AppendChunks(context.Context, string, []store.CreateChunkInput) ([]model.VanadisChunk, error) {
	return nil, s.unimplemented("AppendChunks")
}

// faultStore wraps the real pgtest-backed store. errs[method] fails the call
// without reaching Postgres; lostAcks[method] lets the call commit and then
// reports that error once, the way a connection dropped after COMMIT does.
type faultStore struct {
	store.VanadisStore

	mu       sync.Mutex
	errs     map[string]error
	lostAcks map[string]error
	calls    []string
}

// before records the call and returns the injected pre-commit error.
func (s *faultStore) before(method string, args ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.errs[method]
	entry := strings.Join(append([]string{method}, args...), "\t")
	if err != nil {
		entry = "!" + entry
	}
	s.calls = append(s.calls, entry)
	return err
}

// after replaces a committed success with the one-shot lost-ack error.
func (s *faultStore) after(method string, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lost := s.lostAcks[method]; err == nil && lost != nil {
		delete(s.lostAcks, method)
		return lost
	}
	return err
}

func (s *faultStore) RetryActionChunk(ctx context.Context, req store.RetryActionChunkRequest) (*store.RetryActionChunkResult, error) {
	if err := s.before("RetryActionChunk", req.BuildID, req.ChunkID); err != nil {
		return nil, err
	}
	res, err := s.VanadisStore.RetryActionChunk(ctx, req)
	if err = s.after("RetryActionChunk", err); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *faultStore) CreateBuild(ctx context.Context, b model.VanadisBuild, chunks []model.VanadisChunk) error {
	if err := s.before("CreateBuild", createArgs(b, chunks)...); err != nil {
		return err
	}
	return s.after("CreateBuild", s.VanadisStore.CreateBuild(ctx, b, chunks))
}

func (s *faultStore) ReserveChunkForLaunch(ctx context.Context, chunkID string) error {
	if err := s.before("ReserveChunkForLaunch", chunkID); err != nil {
		return err
	}
	return s.after("ReserveChunkForLaunch", s.VanadisStore.ReserveChunkForLaunch(ctx, chunkID))
}

func (s *faultStore) FailChunk(ctx context.Context, chunkID, reason, dispatchID string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	if err := s.before("FailChunk", chunkID, reason, dispatchID); err != nil {
		return nil, nil, false, err
	}
	b, c, ok, err := s.VanadisStore.FailChunk(ctx, chunkID, reason, dispatchID)
	if err = s.after("FailChunk", err); err != nil {
		return nil, nil, false, err
	}
	return b, c, ok, nil
}

func (s *faultStore) snapshotCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls)
}
