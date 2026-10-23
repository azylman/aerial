package dbtest

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

var (
	_ db.Store           = (*FakeStore)(nil)
	_ db.MessageStore    = (*FakeStore)(nil)
	_ db.ScheduleStore   = (*FakeStore)(nil)
	_ db.SessionStore    = (*FakeStore)(nil)
	_ db.TranscriptStore = (*FakeStore)(nil)
	_ db.PRRegistryStore = (*FakeStore)(nil)
	_ db.VanadisStore    = (*FakeStore)(nil)
)

type threadSummaryRecord struct {
	summary, watermark string
}

func cloneSlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	out := make([]T, len(src))
	copy(out, src)
	return out
}

func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

func normalizeVanadisEffort(effort string) (string, error) {
	trimmed := strings.ToLower(strings.TrimSpace(effort))
	if trimmed == "" {
		return "medium", nil
	}
	switch trimmed {
	case "low", "medium", "high":
		return trimmed, nil
	default:
		return "", fmt.Errorf("invalid vanadis effort %q: must be 'low', 'medium', or 'high'", effort)
	}
}

func validateAndNormalizeVanadisBuild(build db.VanadisBuild, chunks []db.VanadisChunk) (db.VanadisBuild, []db.VanadisChunk, error) {
	buildID := strings.TrimSpace(build.ID)
	if buildID == "" {
		return db.VanadisBuild{}, nil, fmt.Errorf("build id cannot be empty")
	}
	threadID := strings.TrimSpace(build.ThreadID)
	if threadID == "" {
		return db.VanadisBuild{}, nil, fmt.Errorf("thread_id cannot be empty")
	}
	objective := strings.TrimSpace(build.Objective)
	if objective == "" {
		return db.VanadisBuild{}, nil, fmt.Errorf("objective cannot be empty")
	}
	buildTitle := strings.TrimSpace(build.Title)
	if buildTitle == "" {
		buildTitle = objective
	}
	tier := build.Tier
	if tier <= 0 {
		tier = 2
	}
	status := strings.TrimSpace(build.Status)
	if status == "" {
		status = "running"
	}

	normBuild := build
	normBuild.ID, normBuild.ThreadID, normBuild.Objective = buildID, threadID, objective
	normBuild.Title, normBuild.Tier, normBuild.Status = buildTitle, tier, status
	normBuild.SharedContext, normBuild.StatusMessageID = strings.TrimSpace(build.SharedContext), strings.TrimSpace(build.StatusMessageID)

	normChunks := make([]db.VanadisChunk, len(chunks))
	for i, c := range chunks {
		cid := strings.TrimSpace(c.ID)
		if cid == "" {
			return db.VanadisBuild{}, nil, fmt.Errorf("chunk[%d] id cannot be empty", i)
		}
		if cb := strings.TrimSpace(c.BuildID); cb != "" && cb != buildID {
			return db.VanadisBuild{}, nil, fmt.Errorf("chunk[%d] build_id %q does not match build id %q", i, cb, buildID)
		}
		title, instructions := strings.TrimSpace(c.Title), strings.TrimSpace(c.Instructions)
		if title == "" {
			return db.VanadisBuild{}, nil, fmt.Errorf("chunk[%d] title cannot be empty", i)
		}
		if instructions == "" {
			return db.VanadisBuild{}, nil, fmt.Errorf("chunk[%d] instructions cannot be empty", i)
		}
		effort, err := normalizeVanadisEffort(c.Effort)
		if err != nil {
			return db.VanadisBuild{}, nil, fmt.Errorf("chunk[%d] %w", i, err)
		}
		chunkRepo, cStatus := db.NormalizeRepo(c.Repo), strings.TrimSpace(c.Status)
		if chunkRepo == "" {
			chunkRepo = "azylman/aerial"
		}
		if cStatus == "" {
			cStatus = "pending"
		}
		files, symbols, criteria := c.TargetFiles, c.TargetSymbols, c.AcceptanceCriteria
		if files == nil {
			files = []string{}
		}
		if symbols == nil {
			symbols = []string{}
		}
		if criteria == nil {
			criteria = []string{}
		}

		normChunks[i] = db.VanadisChunk{
			ID: cid, BuildID: buildID, ChunkIndex: c.ChunkIndex, Repo: chunkRepo, Title: title, Instructions: instructions,
			TargetFiles: files, TargetSymbols: symbols, Effort: effort, AcceptanceCriteria: criteria, Status: cStatus, PRNumber: c.PRNumber, StartedAt: c.StartedAt,
		}
	}
	return normBuild, normChunks, nil
}

func setIf[T comparable](dst *T, src T) {
	var zero T
	if src != zero {
		*dst = src
	}
}

func defaultVal[T ~int | ~float64 | ~int64](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}

func mapValuesFiltered[K comparable, V any](m map[K]*V, filter func(*V) bool) []V {
	var res []V
	for _, v := range m {
		if v != nil && (filter == nil || filter(v)) {
			res = append(res, *v)
		}
	}
	return res
}

// FakeStore is a thread-safe, pure Go in-memory implementation of db.Store.
type FakeStore struct {
	mu sync.RWMutex

	closed   bool
	failNext map[string]error

	messages            map[string]*db.Message
	messageOrder        []string
	nextRowID, nextPRID int64

	sessions        map[string]*db.SessionInfo
	threadSummaries map[string]threadSummaryRecord

	oneShots map[string]*db.OneShotSchedule
	crons    map[string]*db.CronSchedule
	runs     map[string]*db.ScheduleRun

	sessionSummaries map[string]*db.SessionSummary
	transcriptSteps  map[string][]db.TranscriptStep

	prRecords     map[string]*db.PRRecord
	vanadisBuilds map[string]*db.VanadisBuild
	vanadisChunks map[string]*db.VanadisChunk
}

// NewFakeStore constructs an initialized, empty in-memory FakeStore.
func NewFakeStore() *FakeStore {
	f := &FakeStore{failNext: make(map[string]error), messages: make(map[string]*db.Message), messageOrder: make([]string, 0)}
	f.sessions, f.threadSummaries = make(map[string]*db.SessionInfo), make(map[string]threadSummaryRecord)
	f.oneShots, f.crons, f.runs = make(map[string]*db.OneShotSchedule), make(map[string]*db.CronSchedule), make(map[string]*db.ScheduleRun)
	f.sessionSummaries, f.transcriptSteps = make(map[string]*db.SessionSummary), make(map[string][]db.TranscriptStep)
	f.prRecords, f.vanadisBuilds, f.vanadisChunks = make(map[string]*db.PRRecord), make(map[string]*db.VanadisBuild), make(map[string]*db.VanadisChunk)
	return f
}

// Close marks the store as closed; subsequent operations return sql.ErrConnDone.
func (f *FakeStore) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// FailNext arms a simulated error for the specified method name on its immediate next call.
func (f *FakeStore) FailNext(method string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failNext[method] = err
}

func (f *FakeStore) checkClosedAndFail(method string) error {
	if f.closed {
		return sql.ErrConnDone
	}
	if err, ok := f.failNext[method]; ok {
		delete(f.failNext, method)
		return err
	}
	return nil
}

func (f *FakeStore) withLock(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func (f *FakeStore) withLockErr(method string, fn func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail(method); err != nil {
		return err
	}
	return fn()
}

func withFakeLock[T any](f *FakeStore, method string, fn func() (T, error)) (T, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var zero T
	if err := f.checkClosedAndFail(method); err != nil {
		return zero, err
	}
	return fn()
}

func withFakeLock2[T1, T2 any](f *FakeStore, method string, fn func() (T1, T2, error)) (T1, T2, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var zero1 T1
	var zero2 T2
	if err := f.checkClosedAndFail(method); err != nil {
		return zero1, zero2, err
	}
	return fn()
}

func clonePtr[T any](v *T) *T {
	if v == nil {
		return nil
	}
	cp := *v
	return &cp
}

func cloneMessage(m *db.Message) *db.Message {
	if m == nil {
		return nil
	}
	cp := *m
	cp.Metadata.Mentions, cp.Metadata.MentionUserIDs = cloneSlice(m.Metadata.Mentions), cloneSlice(m.Metadata.MentionUserIDs)
	cp.Metadata.MentionRoleIDs, cp.Metadata.Attachments = cloneSlice(m.Metadata.MentionRoleIDs), cloneSlice(m.Metadata.Attachments)
	return &cp
}

func cloneRun(r *db.ScheduleRun) *db.ScheduleRun {
	if r == nil {
		return nil
	}
	cp := *r
	cp.CompletedAt = clonePtr(r.CompletedAt)
	return &cp
}

func cloneSessionSummary(s *db.SessionSummary) *db.SessionSummary {
	if s == nil {
		return nil
	}
	cp := *s
	cp.Embedding = cloneSlice(s.Embedding)
	return &cp
}

func cloneSliceNonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return cloneSlice(s)
}

func cloneVanadisChunk(c *db.VanadisChunk) db.VanadisChunk {
	cp := *c
	cp.TargetFiles, cp.TargetSymbols = cloneSliceNonNil(c.TargetFiles), cloneSliceNonNil(c.TargetSymbols)
	cp.AcceptanceCriteria = cloneSliceNonNil(c.AcceptanceCriteria)
	if strings.TrimSpace(cp.Effort) == "" {
		cp.Effort = "medium"
	}
	return cp
}

func textMatchScore(text, qLower string, words []string) float64 {
	lower := strings.ToLower(text)
	if strings.Contains(lower, qLower) {
		return 1.0
	}
	var matched int
	for _, w := range words {
		if strings.Contains(lower, w) {
			matched++
		}
	}
	if len(words) > 0 {
		return float64(matched) / float64(len(words))
	}
	return 0
}

func textContainsAll(text, qLower string, words []string) bool {
	lower := strings.ToLower(text)
	if strings.Contains(lower, qLower) {
		return true
	}
	return len(words) > 0 && !slices.ContainsFunc(words, func(w string) bool { return !strings.Contains(lower, w) })
}

func (f *FakeStore) insertMessageLocked(msg db.Message) {
	if _, exists := f.messages[msg.ID]; exists {
		return
	}
	msg.RowID = atomic.AddInt64(&f.nextRowID, 1)
	now := time.Now().UTC()
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.UpdatedAt.IsZero() {
		msg.UpdatedAt = now
	}
	if msg.Status == "" {
		msg.Status = db.StatusPending
	}
	f.messages[msg.ID] = cloneMessage(&msg)
	f.messageOrder = append(f.messageOrder, msg.ID)
}

func (f *FakeStore) InsertMessage(ctx context.Context, msg db.Message) error {
	return f.withLockErr("InsertMessage", func() error {
		f.insertMessageLocked(msg)
		return nil
	})
}

func (f *FakeStore) mutateMessage(method, id string, fn func(*db.Message)) error {
	return f.withLockErr(method, func() error {
		if m, ok := f.messages[id]; ok {
			fn(m)
			m.UpdatedAt = time.Now().UTC()
		}
		return nil
	})
}

func (f *FakeStore) UpdateMessageStatus(ctx context.Context, id, status, errorMsg string) error {
	return f.mutateMessage("UpdateMessageStatus", id, func(m *db.Message) { m.Status, m.ErrorMessage = status, errorMsg })
}

func (f *FakeStore) UpdateMessageCompleted(ctx context.Context, id, responseText string) error {
	return f.mutateMessage("UpdateMessageCompleted", id, func(m *db.Message) { m.Status, m.ResponseText = db.StatusCompleted, responseText })
}

func (f *FakeStore) IncrementMessageRetry(ctx context.Context, id, errorMsg string) error {
	return f.mutateMessage("IncrementMessageRetry", id, func(m *db.Message) {
		m.RetryCount++
		m.ErrorMessage = errorMsg
	})
}

func (f *FakeStore) ResetMessageToPendingWithRestart(ctx context.Context, id, reason string) error {
	return f.mutateMessage("ResetMessageToPendingWithRestart", id, func(m *db.Message) {
		m.Status, m.RestartCount, m.ErrorMessage = db.StatusPending, m.RestartCount+1, reason
	})
}

func (f *FakeStore) GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]db.Message, error) {
	return withFakeLock(f, "GetPendingOrProcessingMessages", func() ([]db.Message, error) {
		var results []db.Message
		for _, id := range f.messageOrder {
			if m := f.messages[id]; m != nil && (m.Status == db.StatusPending || m.Status == db.StatusProcessing) {
				results = append(results, *cloneMessage(m))
				if limit > 0 && len(results) >= limit {
					break
				}
			}
		}
		return results, nil
	})
}

func (f *FakeStore) GetMessage(ctx context.Context, id string) (*db.Message, error) {
	return withFakeLock(f, "GetMessage", func() (*db.Message, error) {
		if m, ok := f.messages[id]; ok {
			return cloneMessage(m), nil
		}
		return nil, sql.ErrNoRows
	})
}

func (f *FakeStore) MessageExists(ctx context.Context, id string) (bool, error) {
	return withFakeLock(f, "MessageExists", func() (bool, error) {
		_, exists := f.messages[id]
		return exists, nil
	})
}

func (f *FakeStore) ClaimPendingMessage(ctx context.Context, id string) (bool, error) {
	return withFakeLock(f, "ClaimPendingMessage", func() (bool, error) {
		m, ok := f.messages[id]
		if !ok || m.Status != db.StatusPending {
			return false, nil
		}
		m.Status, m.UpdatedAt = db.StatusProcessing, time.Now().UTC()
		return true, nil
	})
}

func (f *FakeStore) GetActiveRecentThreadIDs(ctx context.Context, since time.Duration) ([]string, error) {
	return withFakeLock(f, "GetActiveRecentThreadIDs", func() ([]string, error) {
		cutoff, seen := time.Now().UTC().Add(-since), make(map[string]bool)
		var threadIDs []string
		for i := len(f.messageOrder) - 1; i >= 0; i-- {
			if m := f.messages[f.messageOrder[i]]; m != nil && m.ThreadID != "" && m.UpdatedAt.After(cutoff) && !seen[m.ThreadID] {
				seen[m.ThreadID] = true
				threadIDs = append(threadIDs, m.ThreadID)
			}
		}
		return threadIDs, nil
	})
}

func (f *FakeStore) GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]db.Message, error) {
	return withFakeLock(f, "GetRecentThreadMessages", func() ([]db.Message, error) {
		limit = defaultVal(limit, 10)
		var matches []db.Message
		for i := len(f.messageOrder) - 1; i >= 0; i-- {
			if m := f.messages[f.messageOrder[i]]; m != nil && m.ThreadID == threadID {
				matches = append(matches, *cloneMessage(m))
				if len(matches) >= limit {
					break
				}
			}
		}
		slices.Reverse(matches)
		return matches, nil
	})
}

func (f *FakeStore) SetSessionUpdatedAt(threadID string, t time.Time) {
	f.withLock(func() {
		if s := f.sessions[threadID]; s != nil {
			s.UpdatedAt = t
		}
	})
}

func (f *FakeStore) GetActiveTasks(ctx context.Context) ([]db.ActiveTask, error) {
	return withFakeLock(f, "GetActiveTasks", func() ([]db.ActiveTask, error) {
		var activeMsgs []*db.Message
		for _, id := range f.messageOrder {
			if m := f.messages[id]; m != nil && (m.Status == db.StatusPending || m.Status == db.StatusProcessing) {
				activeMsgs = append(activeMsgs, m)
			}
		}
		sort.Slice(activeMsgs, func(i, j int) bool {
			return activeMsgs[i].CreatedAt.Before(activeMsgs[j].CreatedAt) || (activeMsgs[i].CreatedAt.Equal(activeMsgs[j].CreatedAt) && activeMsgs[i].ID < activeMsgs[j].ID)
		})
		if len(activeMsgs) > 50 {
			activeMsgs = activeMsgs[:50]
		}
		tasks := make([]db.ActiveTask, len(activeMsgs))
		for i, m := range activeMsgs {
			summary := m.Summary
			if summary == "" {
				summary = db.CleanTaskSummary(m.Content)
			}
			tasks[i] = db.ActiveTask{ID: m.ID, RowID: m.RowID, ThreadID: m.ThreadID, SessionID: sessionInternalID(f.sessions[m.ThreadID]), AuthorName: m.AuthorName, AuthorID: m.AuthorID, Prompt: m.Content, Summary: summary, Status: m.Status, RetryCount: m.RetryCount, ScheduleRunID: m.ScheduleRunID, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt}
		}
		return tasks, nil
	})
}

func sessionInternalID(s *db.SessionInfo) string {
	if s == nil {
		return ""
	}
	return s.InternalSessionID
}

func sessionPrevID(s *db.SessionInfo) string {
	if s == nil {
		return ""
	}
	return s.PreviousSessionID
}

func sessionTurnCount(s *db.SessionInfo) int {
	if s == nil {
		return 0
	}
	return s.TurnCount
}

func (f *FakeStore) getOrCreateSession(key string) *db.SessionInfo {
	s := f.sessions[key]
	if s == nil {
		s = &db.SessionInfo{ThreadID: key, CreatedAt: time.Now().UTC()}
		f.sessions[key] = s
	}
	return s
}

func (f *FakeStore) GetSessionID(ctx context.Context, threadID string) (string, error) {
	return withFakeLock(f, "GetSessionID", func() (string, error) { return sessionInternalID(f.sessions[threadID]), nil })
}

func (f *FakeStore) GetPreviousSessionID(ctx context.Context, threadID string) (string, error) {
	return withFakeLock(f, "GetPreviousSessionID", func() (string, error) { return sessionPrevID(f.sessions[threadID]), nil })
}

func (f *FakeStore) SaveSessionID(ctx context.Context, threadID, sessionID string) error {
	return f.withLockErr("SaveSessionID", func() error {
		s := f.getOrCreateSession(threadID)
		s.InternalSessionID, s.UpdatedAt = sessionID, time.Now().UTC()
		return nil
	})
}

func (f *FakeStore) IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	return withFakeLock(f, "IncrementSessionTurnCount", func() (int, error) {
		s := f.getOrCreateSession(sessionKey)
		s.TurnCount++
		s.UpdatedAt = time.Now().UTC()
		return s.TurnCount, nil
	})
}

func (f *FakeStore) GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	return withFakeLock(f, "GetSessionTurnCount", func() (int, error) { return sessionTurnCount(f.sessions[sessionKey]), nil })
}

func (f *FakeStore) RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error {
	return f.withLockErr("RotateSessionID", func() error {
		now := time.Now().UTC()
		s := f.sessions[sessionKey]
		if s == nil {
			f.sessions[sessionKey] = &db.SessionInfo{ThreadID: sessionKey, InternalSessionID: newSessionID, CreatedAt: now, UpdatedAt: now}
			return nil
		}
		s.PreviousSessionID, s.InternalSessionID, s.TurnCount, s.UpdatedAt = s.InternalSessionID, newSessionID, 0, now
		return nil
	})
}

func (f *FakeStore) FindUnrotatedSessions(ctx context.Context, minTurns int, maxAge ...time.Duration) ([]db.SessionInfo, error) {
	return withFakeLock(f, "FindUnrotatedSessions", func() ([]db.SessionInfo, error) {
		minTurns = defaultVal(minTurns, session.DefaultMaxSessionTurns)
		limitAge := db.DefaultUnrotatedSessionMaxAge
		if len(maxAge) > 0 {
			limitAge = maxAge[0]
		}
		cutoff := time.Now().UTC().Add(-limitAge)
		var res []db.SessionInfo
		for _, s := range f.sessions {
			if s != nil && s.TurnCount > minTurns && (limitAge <= 0 || !s.UpdatedAt.Before(cutoff)) {
				res = append(res, *s)
			}
		}
		sort.Slice(res, func(i, j int) bool { return res[i].TurnCount > res[j].TurnCount })
		return res, nil
	})
}

func (f *FakeStore) GetThreadSummary(ctx context.Context, threadID string) (string, string, error) {
	return withFakeLock2(f, "GetThreadSummary", func() (string, string, error) {
		rec := f.threadSummaries[threadID]
		return rec.summary, rec.watermark, nil
	})
}

func (f *FakeStore) SaveThreadSummary(ctx context.Context, threadID, summary, lastSummarizedMsgID string) error {
	return f.withLockErr("SaveThreadSummary", func() error {
		f.threadSummaries[threadID] = threadSummaryRecord{summary: summary, watermark: lastSummarizedMsgID}
		return nil
	})
}

func maxTime(dst *time.Time, t time.Time) {
	if t.After(*dst) {
		*dst = t
	}
}

func (f *FakeStore) GetSessionActivityStats(ctx context.Context, threadID string) (*db.SessionActivityStats, error) {
	return withFakeLock(f, "GetSessionActivityStats", func() (*db.SessionActivityStats, error) {
		stats := &db.SessionActivityStats{}
		if s := f.sessions[threadID]; s != nil {
			stats.InternalSessionID, stats.TurnCount, stats.SessionUpdatedAt = s.InternalSessionID, s.TurnCount, s.UpdatedAt
		}
		for _, id := range f.messageOrder {
			m := f.messages[id]
			if m != nil && m.ThreadID == threadID && m.Status == db.StatusCompleted && !strings.HasPrefix(m.ResponseText, "[EXPIRED_STALE]") && !strings.HasPrefix(m.ResponseText, "[AMBIENT") && !strings.HasPrefix(m.ResponseText, "[IGNORED") {
				stats.CompletedTurns++
				maxTime(&stats.LastMessageAt, m.UpdatedAt)
				maxTime(&stats.LastCompletedMessageCreatedAt, m.CreatedAt)
			}
		}
		return stats, nil
	})
}

func (f *FakeStore) SetSessionInfo(s db.SessionInfo) {
	f.withLock(func() { f.sessions[s.ThreadID] = clonePtr(&s) })
}

func (f *FakeStore) GetExternalConversationID(ctx context.Context, internalID string) (string, error) {
	return withFakeLock(f, "GetExternalConversationID", func() (string, error) {
		if internalID != "" {
			for threadID, sess := range f.sessions {
				if sess != nil && sess.InternalSessionID == internalID {
					return threadID, nil
				}
			}
		}
		return "", nil
	})
}

func (f *FakeStore) CreateOneShotSchedule(ctx context.Context, s db.OneShotSchedule) error {
	return f.withLockErr("CreateOneShotSchedule", func() error {
		if s.CreatedAt.IsZero() {
			s.CreatedAt = time.Now().UTC()
		}
		f.oneShots[s.ID] = clonePtr(&s)
		return nil
	})
}

func (f *FakeStore) GetAllOneShotSchedules(ctx context.Context, threadID string) ([]db.OneShotSchedule, error) {
	return withFakeLock(f, "GetAllOneShotSchedules", func() ([]db.OneShotSchedule, error) {
		results := mapValuesFiltered(f.oneShots, func(s *db.OneShotSchedule) bool { return threadID == "" || s.ThreadID == threadID })
		sort.Slice(results, func(i, j int) bool { return results[i].RunAt.Before(results[j].RunAt) })
		return results, nil
	})
}

func (f *FakeStore) CreateCronSchedule(ctx context.Context, c db.CronSchedule) error {
	return f.withLockErr("CreateCronSchedule", func() error {
		if c.CreatedAt.IsZero() {
			c.CreatedAt = time.Now().UTC()
		}
		f.crons[c.ID] = clonePtr(&c)
		return nil
	})
}

func (f *FakeStore) GetAllCronSchedules(ctx context.Context, targetID string) ([]db.CronSchedule, error) {
	return withFakeLock(f, "GetAllCronSchedules", func() ([]db.CronSchedule, error) {
		results := mapValuesFiltered(f.crons, func(c *db.CronSchedule) bool { return targetID == "" || c.TargetID == targetID })
		sort.Slice(results, func(i, j int) bool { return results[i].NextRunAt.Before(results[j].NextRunAt) })
		return results, nil
	})
}

// SeedScheduleRun seeds a schedule run into the in-memory fake store for testing.
func (f *FakeStore) SeedScheduleRun(run db.ScheduleRun) {
	f.withLock(func() {
		if run.StartedAt.IsZero() {
			run.StartedAt = time.Now().UTC()
		}
		f.runs[run.ID] = cloneRun(&run)
	})
}

func (f *FakeStore) GetScheduleRunsPaginated(ctx context.Context, limit, offset int, scheduleID, status string) ([]db.ScheduleRun, int, error) {
	return withFakeLock2(f, "GetScheduleRunsPaginated", func() ([]db.ScheduleRun, int, error) {
		filtered := mapValuesFiltered(f.runs, func(r *db.ScheduleRun) bool {
			return (scheduleID == "" || r.ScheduleID == scheduleID) && (status == "" || r.Status == status)
		})
		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].StartedAt.After(filtered[j].StartedAt) || (filtered[i].StartedAt.Equal(filtered[j].StartedAt) && filtered[i].ID > filtered[j].ID)
		})
		total := len(filtered)
		if offset > total {
			return nil, total, nil
		}
		if limit <= 0 {
			limit = total
		}
		return filtered[offset:min(offset+limit, total)], total, nil
	})
}

func updateNextRun(next **time.Time, t time.Time) {
	if *next == nil || t.Before(**next) {
		cp := t
		*next = &cp
	}
}

func (f *FakeStore) GetScheduleSummaryMetrics(ctx context.Context) (db.ScheduleSummaryMetrics, error) {
	return withFakeLock(f, "GetScheduleSummaryMetrics", func() (db.ScheduleSummaryMetrics, error) {
		var m db.ScheduleSummaryMetrics
		for _, c := range f.crons {
			if c != nil && c.Enabled {
				m.CronCount++
				updateNextRun(&m.NextRunAt, c.NextRunAt)
			}
		}
		for _, s := range f.oneShots {
			if s != nil {
				updateNextRun(&m.NextRunAt, s.RunAt)
			}
		}
		m.OneShotCount, m.TotalActive = len(f.oneShots), m.CronCount+len(f.oneShots)

		cutoff24h := time.Now().UTC().Add(-24 * time.Hour)
		var completedRuns int
		for _, r := range f.runs {
			if r != nil && !r.StartedAt.Before(cutoff24h) {
				m.TotalRuns24h++
				if r.Status == "completed" {
					completedRuns++
				}
			}
		}
		m.SuccessRate24h = 100.0
		if m.TotalRuns24h > 0 {
			m.SuccessRate24h = math.Round((float64(completedRuns)/float64(m.TotalRuns24h))*1000.0) / 10.0
		}
		return m, nil
	})
}

func (f *FakeStore) GetSessionSyncStates(ctx context.Context) (map[string]db.SessionSyncState, error) {
	return withFakeLock(f, "GetSessionSyncStates", func() (map[string]db.SessionSyncState, error) {
		res := make(map[string]db.SessionSyncState, len(f.sessionSummaries))
		for id, s := range f.sessionSummaries {
			if s != nil {
				res[id] = db.SessionSyncState{SessionID: id, LastMtime: s.LastMtime, LastIndexedStep: s.LastIndexedStep, IsSettled: s.IsSettled}
			}
		}
		return res, nil
	})
}

func (f *FakeStore) UpsertSessionSummary(ctx context.Context, summary db.SessionSummary) error {
	return f.withLockErr("UpsertSessionSummary", func() error {
		if strings.TrimSpace(summary.SessionID) == "" {
			return fmt.Errorf("session ID cannot be empty")
		}
		now := time.Now().UTC()
		existing := f.sessionSummaries[summary.SessionID]
		if existing != nil {
			setIf(&existing.ThreadID, summary.ThreadID)
			setIf(&existing.Summary, summary.Summary)
			if len(summary.Embedding) > 0 {
				existing.Embedding = cloneSlice(summary.Embedding)
			}
			if summary.LastIndexedStep >= 0 {
				existing.LastIndexedStep = summary.LastIndexedStep
			} else if summary.LastIndexedStep == -2 {
				existing.LastIndexedStep = -1
			}
			if !summary.LastMtime.IsZero() {
				existing.LastMtime = summary.LastMtime
			}
			if summary.SummaryStepWatermark >= 0 {
				existing.SummaryStepWatermark = summary.SummaryStepWatermark
			}
			if summary.IsSettled {
				existing.IsSettled = true
			} else if summary.LastIndexedStep > existing.SummaryStepWatermark && existing.SummaryStepWatermark >= 0 {
				existing.IsSettled = false
			}
			existing.UpdatedAt = now
			return nil
		}
		cp := summary
		if cp.CreatedAt.IsZero() {
			cp.CreatedAt = now
		}
		cp.UpdatedAt = now
		f.sessionSummaries[summary.SessionID] = cloneSessionSummary(&cp)
		return nil
	})
}

func (f *FakeStore) BatchInsertTranscriptSteps(ctx context.Context, steps []db.TranscriptStep) error {
	return f.withLockErr("BatchInsertTranscriptSteps", func() error {
		now := time.Now().UTC()
		for _, s := range steps {
			if strings.TrimSpace(s.SessionID) == "" {
				continue
			}
			cp := s
			if cp.CreatedAt.IsZero() {
				cp.CreatedAt = now
			}
			if !slices.ContainsFunc(f.transcriptSteps[s.SessionID], func(e db.TranscriptStep) bool { return e.StepIndex == s.StepIndex }) {
				f.transcriptSteps[s.SessionID] = append(f.transcriptSteps[s.SessionID], cp)
			}
		}
		return nil
	})
}

func (f *FakeStore) SearchSessionSummaries(ctx context.Context, embedding []float32, queryText string, limit int, minScore float64) ([]db.SessionSummary, error) {
	return withFakeLock(f, "SearchSessionSummaries", func() ([]db.SessionSummary, error) {
		hasEmbedding, queryText := len(embedding) == db.ExpectedEmbeddingDim, strings.TrimSpace(queryText)
		hasText := queryText != ""
		if !hasEmbedding && !hasText {
			return nil, nil
		}
		limit = defaultVal(limit, 10)
		if minScore < 0 {
			minScore = 0.0
		}
		qLower, words := strings.ToLower(queryText), strings.Fields(strings.ToLower(queryText))
		var scored []db.SessionSummary
		for _, s := range f.sessionSummaries {
			if s == nil {
				continue
			}
			var sim, textScore float64
			if hasEmbedding && len(s.Embedding) == db.ExpectedEmbeddingDim {
				sim = cosineSimilarity(embedding, s.Embedding)
			}
			if hasText {
				textScore = textMatchScore(s.Summary, qLower, words)
			}
			score := textScore
			if hasEmbedding && hasText {
				score = 0.50*sim + 0.50*textScore
			} else if hasEmbedding {
				score = sim
			}
			if score >= minScore {
				cp := *cloneSessionSummary(s)
				cp.Score = score
				scored = append(scored, cp)
			}
		}
		sort.Slice(scored, func(i, j int) bool { return scored[i].Score > scored[j].Score })
		return scored[:min(limit, len(scored))], nil
	})
}

func (f *FakeStore) SearchTranscriptSteps(ctx context.Context, queryText, sessionFilter, toolFilter string, limit int) ([]db.TranscriptStep, error) {
	return withFakeLock(f, "SearchTranscriptSteps", func() ([]db.TranscriptStep, error) {
		queryText, sessionFilter, toolFilter = strings.TrimSpace(queryText), strings.TrimSpace(sessionFilter), strings.TrimSpace(toolFilter)
		if queryText == "" && sessionFilter == "" && toolFilter == "" {
			return nil, nil
		}
		limit = defaultVal(limit, 20)
		qLower, words := strings.ToLower(queryText), strings.Fields(strings.ToLower(queryText))
		var matched []db.TranscriptStep
		for sid, steps := range f.transcriptSteps {
			if sessionFilter != "" && sid != sessionFilter {
				continue
			}
			for _, s := range steps {
				if (toolFilter != "" && s.ToolName != toolFilter) || (queryText != "" && !textContainsAll(s.Content, qLower, words)) {
					continue
				}
				cp := s
				cp.RankScore = 1.0
				matched = append(matched, cp)
			}
		}
		sort.Slice(matched, func(i, j int) bool { return matched[i].CreatedAt.After(matched[j].CreatedAt) })
		return matched[:min(limit, len(matched))], nil
	})
}

func (f *FakeStore) GetTranscriptStepRange(ctx context.Context, sessionID string, startStep, endStep int) ([]db.TranscriptStep, error) {
	return withFakeLock(f, "GetTranscriptStepRange", func() ([]db.TranscriptStep, error) {
		sessionID = strings.TrimSpace(sessionID)
		if sessionID == "" {
			return nil, nil
		}
		if startStep < 0 {
			startStep = 0
		}
		if endStep < startStep {
			endStep = startStep
		}
		steps, exists := f.transcriptSteps[sessionID]
		if !exists {
			return nil, nil
		}
		var matched []db.TranscriptStep
		for _, s := range steps {
			if s.StepIndex >= startStep && s.StepIndex <= endStep {
				cp := s
				cp.RankScore = 1.0
				matched = append(matched, cp)
			}
		}
		sort.Slice(matched, func(i, j int) bool { return matched[i].StepIndex < matched[j].StepIndex })
		return matched, nil
	})
}

func prKey(repo string, prNumber int) string {
	return fmt.Sprintf("%s/%d", db.NormalizeRepo(repo), prNumber)
}

func (f *FakeStore) UpsertPR(ctx context.Context, record db.PRRecord) error {
	return f.withLockErr("UpsertPR", func() error {
		norm, err := db.ValidateAndNormalizeUpsertPR(record)
		if err != nil {
			return err
		}
		key, now := prKey(norm.Repo, norm.PRNumber), time.Now()
		existing, exists := f.prRecords[key]
		if exists {
			existing.Branch, existing.HeadSHA, existing.TargetID, existing.UpdatedAt = norm.Branch, norm.HeadSHA, norm.TargetID, now
			setIf(&existing.Title, norm.Title)
			setIf(&existing.StatusMessageID, norm.StatusMessageID)
			if norm.Status != "" && existing.Status != "merged" && existing.Status != "deploying" && existing.Status != "deployed" {
				existing.Status = norm.Status
			}
			if norm.Metadata != "" && norm.Metadata != "{}" {
				existing.Metadata = norm.Metadata
			}
			return nil
		}
		f.nextPRID++
		rec := norm
		rec.ID, rec.CreatedAt, rec.UpdatedAt = f.nextPRID, now, now
		f.prRecords[key] = &rec
		return nil
	})
}

func (f *FakeStore) getPRRecord(repo string, prNumber int) (*db.PRRecord, error) {
	if repo = db.NormalizeRepo(repo); repo == "" {
		return nil, fmt.Errorf("repo cannot be empty")
	}
	rec, exists := f.prRecords[prKey(repo, prNumber)]
	if !exists {
		return nil, db.ErrPRNotFound
	}
	return rec, nil
}

func (f *FakeStore) GetPRByNumber(ctx context.Context, repo string, prNumber int) (*db.PRRecord, error) {
	return withFakeLock(f, "GetPRByNumber", func() (*db.PRRecord, error) {
		rec, err := f.getPRRecord(repo, prNumber)
		return clonePtr(rec), err
	})
}

func (f *FakeStore) UpdatePRStatusMessageID(ctx context.Context, repo string, prNumber int, messageID string) error {
	return f.withLockErr("UpdatePRStatusMessageID", func() error {
		rec, err := f.getPRRecord(repo, prNumber)
		if err != nil {
			return err
		}
		rec.StatusMessageID, rec.UpdatedAt = strings.TrimSpace(messageID), time.Now()
		return nil
	})
}

func (f *FakeStore) CreateVanadisBuild(ctx context.Context, build db.VanadisBuild, chunks []db.VanadisChunk) error {
	return f.withLockErr("CreateVanadisBuild", func() error {
		normBuild, normChunks, err := validateAndNormalizeVanadisBuild(build, chunks)
		if err != nil {
			return err
		}
		if _, exists := f.vanadisBuilds[normBuild.ID]; exists {
			return fmt.Errorf("vanadis build %s already exists", normBuild.ID)
		}
		seenChunkIDs, seenIndices := make(map[string]bool, len(normChunks)), make(map[int]bool, len(normChunks))
		for _, c := range normChunks {
			if _, exists := f.vanadisChunks[c.ID]; exists || seenChunkIDs[c.ID] {
				return fmt.Errorf("vanadis chunk %s already exists", c.ID)
			}
			if seenIndices[c.ChunkIndex] {
				return fmt.Errorf("duplicate chunk_index %d in build %s", c.ChunkIndex, normBuild.ID)
			}
			seenChunkIDs[c.ID], seenIndices[c.ChunkIndex] = true, true
		}
		now := time.Now()
		b := normBuild
		b.CreatedAt, b.UpdatedAt = now, now
		f.vanadisBuilds[normBuild.ID] = &b
		for _, c := range normChunks {
			startedAt := c.StartedAt
			if startedAt.IsZero() && (c.Status == "running" || c.Status == "coding") {
				startedAt = now
			}
			cp := cloneVanadisChunk(&c)
			cp.StartedAt, cp.CreatedAt, cp.UpdatedAt = startedAt, now, now
			f.vanadisChunks[c.ID] = &cp
		}
		return nil
	})
}

func (f *FakeStore) GetVanadisBuild(ctx context.Context, buildID string) (*db.VanadisBuild, []db.VanadisChunk, error) {
	return withFakeLock2(f, "GetVanadisBuild", func() (*db.VanadisBuild, []db.VanadisChunk, error) {
		if buildID = strings.TrimSpace(buildID); buildID == "" {
			return nil, nil, fmt.Errorf("build id cannot be empty")
		}
		b := f.vanadisBuilds[buildID]
		if b == nil {
			return nil, nil, db.ErrVanadisBuildNotFound
		}
		chunks := mapValuesFiltered(f.vanadisChunks, func(c *db.VanadisChunk) bool { return c.BuildID == buildID })
		for i := range chunks {
			chunks[i] = cloneVanadisChunk(&chunks[i])
		}
		sort.Slice(chunks, func(i, j int) bool { return chunks[i].ChunkIndex < chunks[j].ChunkIndex })
		cp := *b
		return &cp, chunks, nil
	})
}

func (f *FakeStore) getVanadisChunkLocked(chunkID string) (*db.VanadisChunk, error) {
	if chunkID = strings.TrimSpace(chunkID); chunkID == "" {
		return nil, fmt.Errorf("chunk id cannot be empty")
	}
	c, exists := f.vanadisChunks[chunkID]
	if !exists {
		return nil, db.ErrVanadisChunkNotFound
	}
	return c, nil
}

func (f *FakeStore) GetVanadisChunk(ctx context.Context, chunkID string) (*db.VanadisChunk, error) {
	return withFakeLock(f, "GetVanadisChunk", func() (*db.VanadisChunk, error) {
		c, err := f.getVanadisChunkLocked(chunkID)
		if err != nil {
			return nil, err
		}
		cp := cloneVanadisChunk(c)
		return &cp, nil
	})
}

func (f *FakeStore) AttachVanadisChunkPR(ctx context.Context, chunkID string, prNumber int) error {
	return f.withLockErr("AttachVanadisChunkPR", func() error {
		if prNumber <= 0 {
			return fmt.Errorf("pr_number must be greater than 0")
		}
		c, err := f.getVanadisChunkLocked(chunkID)
		if err != nil {
			return err
		}
		now := time.Now()
		c.PRNumber, c.Status, c.UpdatedAt = prNumber, "pr_opened", now
		if c.StartedAt.IsZero() {
			c.StartedAt = now
			if !c.CreatedAt.IsZero() {
				c.StartedAt = c.CreatedAt
			}
		}
		return nil
	})
}

func (f *FakeStore) UpdateVanadisBuildStatusMessageID(ctx context.Context, buildID, messageID string) error {
	return f.withLockErr("UpdateVanadisBuildStatusMessageID", func() error {
		if buildID = strings.TrimSpace(buildID); buildID == "" {
			return fmt.Errorf("build id cannot be empty")
		}
		b, exists := f.vanadisBuilds[buildID]
		if !exists {
			return db.ErrVanadisBuildNotFound
		}
		b.StatusMessageID, b.UpdatedAt = strings.TrimSpace(messageID), time.Now()
		return nil
	})
}
