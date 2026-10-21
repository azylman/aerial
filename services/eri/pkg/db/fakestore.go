package db

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

	"github.com/azylman/aerial/services/eri/pkg/session"
)

var (
	_ Store           = (*FakeStore)(nil)
	_ MessageStore    = (*FakeStore)(nil)
	_ ScheduleStore   = (*FakeStore)(nil)
	_ SessionStore    = (*FakeStore)(nil)
	_ TranscriptStore = (*FakeStore)(nil)
	_ PRRegistryStore = (*FakeStore)(nil)
	_ VanadisStore    = (*FakeStore)(nil)
)

type threadSummaryRecord struct {
	summary, watermark string
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

	messages            map[string]*Message
	messageOrder        []string
	nextRowID, nextPRID int64

	sessions        map[string]*SessionInfo
	threadSummaries map[string]threadSummaryRecord

	oneShots map[string]*OneShotSchedule
	crons    map[string]*CronSchedule
	runs     map[string]*ScheduleRun

	sessionSummaries map[string]*SessionSummary
	transcriptSteps  map[string][]TranscriptStep

	prRecords     map[string]*PRRecord
	vanadisBuilds map[string]*VanadisBuild
	vanadisChunks map[string]*VanadisChunk
}

// NewFakeStore constructs an initialized, empty in-memory FakeStore.
func NewFakeStore() *FakeStore {
	f := &FakeStore{failNext: make(map[string]error), messages: make(map[string]*Message), messageOrder: make([]string, 0)}
	f.sessions, f.threadSummaries = make(map[string]*SessionInfo), make(map[string]threadSummaryRecord)
	f.oneShots, f.crons, f.runs = make(map[string]*OneShotSchedule), make(map[string]*CronSchedule), make(map[string]*ScheduleRun)
	f.sessionSummaries, f.transcriptSteps = make(map[string]*SessionSummary), make(map[string][]TranscriptStep)
	f.prRecords, f.vanadisBuilds, f.vanadisChunks = make(map[string]*PRRecord), make(map[string]*VanadisBuild), make(map[string]*VanadisChunk)
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

func cloneMessage(m *Message) *Message {
	if m == nil {
		return nil
	}
	cp := *m
	cp.Metadata.Mentions, cp.Metadata.MentionUserIDs = cloneSlice(m.Metadata.Mentions), cloneSlice(m.Metadata.MentionUserIDs)
	cp.Metadata.MentionRoleIDs, cp.Metadata.Attachments = cloneSlice(m.Metadata.MentionRoleIDs), cloneSlice(m.Metadata.Attachments)
	return &cp
}

func cloneRun(r *ScheduleRun) *ScheduleRun {
	if r == nil {
		return nil
	}
	cp := *r
	cp.CompletedAt = clonePtr(r.CompletedAt)
	return &cp
}

func cloneSessionSummary(s *SessionSummary) *SessionSummary {
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

func cloneVanadisChunk(c *VanadisChunk) VanadisChunk {
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

func (f *FakeStore) insertMessageLocked(msg Message) {
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
		msg.Status = StatusPending
	}
	f.messages[msg.ID] = cloneMessage(&msg)
	f.messageOrder = append(f.messageOrder, msg.ID)
}

func (f *FakeStore) InsertMessage(ctx context.Context, msg Message) error {
	return f.withLockErr("InsertMessage", func() error {
		f.insertMessageLocked(msg)
		return nil
	})
}

func (f *FakeStore) mutateMessage(method, id string, fn func(*Message)) error {
	return f.withLockErr(method, func() error {
		if m, ok := f.messages[id]; ok {
			fn(m)
			m.UpdatedAt = time.Now().UTC()
		}
		return nil
	})
}

func (f *FakeStore) UpdateMessageStatus(ctx context.Context, id, status, errorMsg string) error {
	return f.mutateMessage("UpdateMessageStatus", id, func(m *Message) { m.Status, m.ErrorMessage = status, errorMsg })
}

func (f *FakeStore) UpdateMessageCompleted(ctx context.Context, id, responseText string) error {
	return f.mutateMessage("UpdateMessageCompleted", id, func(m *Message) { m.Status, m.ResponseText = StatusCompleted, responseText })
}

func (f *FakeStore) IncrementMessageRetry(ctx context.Context, id, errorMsg string) error {
	return f.mutateMessage("IncrementMessageRetry", id, func(m *Message) {
		m.RetryCount++
		m.ErrorMessage = errorMsg
	})
}

func (f *FakeStore) ResetMessageToPendingWithRestart(ctx context.Context, id, reason string) error {
	return f.mutateMessage("ResetMessageToPendingWithRestart", id, func(m *Message) { m.Status, m.RestartCount, m.ErrorMessage = StatusPending, m.RestartCount+1, reason })
}

func (f *FakeStore) GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]Message, error) {
	return withFakeLock(f, "GetPendingOrProcessingMessages", func() ([]Message, error) {
		var results []Message
		for _, id := range f.messageOrder {
			if m := f.messages[id]; m != nil && (m.Status == StatusPending || m.Status == StatusProcessing) {
				results = append(results, *cloneMessage(m))
				if limit > 0 && len(results) >= limit {
					break
				}
			}
		}
		return results, nil
	})
}

func (f *FakeStore) GetMessage(ctx context.Context, id string) (*Message, error) {
	return withFakeLock(f, "GetMessage", func() (*Message, error) {
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
		if !ok || m.Status != StatusPending {
			return false, nil
		}
		m.Status, m.UpdatedAt = StatusProcessing, time.Now().UTC()
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

func (f *FakeStore) GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]Message, error) {
	return withFakeLock(f, "GetRecentThreadMessages", func() ([]Message, error) {
		limit = defaultVal(limit, 10)
		var matches []Message
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

func (f *FakeStore) GetActiveTasks(ctx context.Context) ([]ActiveTask, error) {
	return withFakeLock(f, "GetActiveTasks", func() ([]ActiveTask, error) {
		var activeMsgs []*Message
		for _, id := range f.messageOrder {
			if m := f.messages[id]; m != nil && (m.Status == StatusPending || m.Status == StatusProcessing) {
				activeMsgs = append(activeMsgs, m)
			}
		}
		sort.Slice(activeMsgs, func(i, j int) bool {
			return activeMsgs[i].CreatedAt.Before(activeMsgs[j].CreatedAt) || (activeMsgs[i].CreatedAt.Equal(activeMsgs[j].CreatedAt) && activeMsgs[i].ID < activeMsgs[j].ID)
		})
		if len(activeMsgs) > 50 {
			activeMsgs = activeMsgs[:50]
		}
		tasks := make([]ActiveTask, len(activeMsgs))
		for i, m := range activeMsgs {
			summary := m.Summary
			if summary == "" {
				summary = CleanTaskSummary(m.Content)
			}
			tasks[i] = ActiveTask{ID: m.ID, RowID: m.RowID, ThreadID: m.ThreadID, SessionID: f.sessions[m.ThreadID].internalID(), AuthorName: m.AuthorName, AuthorID: m.AuthorID, Prompt: m.Content, Summary: summary, Status: m.Status, RetryCount: m.RetryCount, ScheduleRunID: m.ScheduleRunID, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt}
		}
		return tasks, nil
	})
}

func (s *SessionInfo) internalID() string {
	if s == nil {
		return ""
	}
	return s.InternalSessionID
}

func (s *SessionInfo) prevID() string {
	if s == nil {
		return ""
	}
	return s.PreviousSessionID
}

func (s *SessionInfo) turnCount() int {
	if s == nil {
		return 0
	}
	return s.TurnCount
}

func (f *FakeStore) getOrCreateSession(key string) *SessionInfo {
	s := f.sessions[key]
	if s == nil {
		s = &SessionInfo{ThreadID: key, CreatedAt: time.Now().UTC()}
		f.sessions[key] = s
	}
	return s
}

func (f *FakeStore) GetSessionID(ctx context.Context, threadID string) (string, error) {
	return withFakeLock(f, "GetSessionID", func() (string, error) { return f.sessions[threadID].internalID(), nil })
}

func (f *FakeStore) GetPreviousSessionID(ctx context.Context, threadID string) (string, error) {
	return withFakeLock(f, "GetPreviousSessionID", func() (string, error) { return f.sessions[threadID].prevID(), nil })
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
	return withFakeLock(f, "GetSessionTurnCount", func() (int, error) { return f.sessions[sessionKey].turnCount(), nil })
}

func (f *FakeStore) RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error {
	return f.withLockErr("RotateSessionID", func() error {
		now := time.Now().UTC()
		s := f.sessions[sessionKey]
		if s == nil {
			f.sessions[sessionKey] = &SessionInfo{ThreadID: sessionKey, InternalSessionID: newSessionID, CreatedAt: now, UpdatedAt: now}
			return nil
		}
		s.PreviousSessionID, s.InternalSessionID, s.TurnCount, s.UpdatedAt = s.InternalSessionID, newSessionID, 0, now
		return nil
	})
}

func (f *FakeStore) FindUnrotatedSessions(ctx context.Context, minTurns int, maxAge ...time.Duration) ([]SessionInfo, error) {
	return withFakeLock(f, "FindUnrotatedSessions", func() ([]SessionInfo, error) {
		minTurns = defaultVal(minTurns, session.DefaultMaxSessionTurns)
		limitAge := DefaultUnrotatedSessionMaxAge
		if len(maxAge) > 0 {
			limitAge = maxAge[0]
		}
		cutoff := time.Now().UTC().Add(-limitAge)
		var res []SessionInfo
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

func (f *FakeStore) GetSessionActivityStats(ctx context.Context, threadID string) (*SessionActivityStats, error) {
	return withFakeLock(f, "GetSessionActivityStats", func() (*SessionActivityStats, error) {
		stats := &SessionActivityStats{}
		if s := f.sessions[threadID]; s != nil {
			stats.InternalSessionID, stats.TurnCount, stats.SessionUpdatedAt = s.InternalSessionID, s.TurnCount, s.UpdatedAt
		}
		for _, id := range f.messageOrder {
			m := f.messages[id]
			if m != nil && m.ThreadID == threadID && m.Status == StatusCompleted && !strings.HasPrefix(m.ResponseText, "[EXPIRED_STALE]") && !strings.HasPrefix(m.ResponseText, "[AMBIENT") && !strings.HasPrefix(m.ResponseText, "[IGNORED") {
				stats.CompletedTurns++
				maxTime(&stats.LastMessageAt, m.UpdatedAt)
				maxTime(&stats.LastCompletedMessageCreatedAt, m.CreatedAt)
			}
		}
		return stats, nil
	})
}

func (f *FakeStore) SetSessionInfo(s SessionInfo) {
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

func (f *FakeStore) CreateOneShotSchedule(ctx context.Context, s OneShotSchedule) error {
	return f.withLockErr("CreateOneShotSchedule", func() error {
		if s.CreatedAt.IsZero() {
			s.CreatedAt = time.Now().UTC()
		}
		f.oneShots[s.ID] = clonePtr(&s)
		return nil
	})
}

func (f *FakeStore) GetDueOneShotSchedules(ctx context.Context) ([]OneShotSchedule, error) {
	return withFakeLock(f, "GetDueOneShotSchedules", func() ([]OneShotSchedule, error) {
		now := time.Now().UTC()
		due := mapValuesFiltered(f.oneShots, func(s *OneShotSchedule) bool { return !s.RunAt.After(now) })
		sort.Slice(due, func(i, j int) bool { return due[i].RunAt.Before(due[j].RunAt) })
		return due, nil
	})
}

func (f *FakeStore) InsertMessageAndConsumeOneShot(ctx context.Context, scheduleID string, msg Message) error {
	return f.withLockErr("InsertMessageAndConsumeOneShot", func() error {
		if _, exists := f.oneShots[scheduleID]; !exists {
			return ErrScheduleAlreadyConsumed
		}
		delete(f.oneShots, scheduleID)
		f.insertMessageLocked(msg)
		return nil
	})
}

func (f *FakeStore) GetAllOneShotSchedules(ctx context.Context, threadID string) ([]OneShotSchedule, error) {
	return withFakeLock(f, "GetAllOneShotSchedules", func() ([]OneShotSchedule, error) {
		results := mapValuesFiltered(f.oneShots, func(s *OneShotSchedule) bool { return threadID == "" || s.ThreadID == threadID })
		sort.Slice(results, func(i, j int) bool { return results[i].RunAt.Before(results[j].RunAt) })
		return results, nil
	})
}

func (f *FakeStore) CreateCronSchedule(ctx context.Context, c CronSchedule) error {
	return f.withLockErr("CreateCronSchedule", func() error {
		if c.CreatedAt.IsZero() {
			c.CreatedAt = time.Now().UTC()
		}
		f.crons[c.ID] = clonePtr(&c)
		return nil
	})
}

func (f *FakeStore) GetDueCronSchedules(ctx context.Context) ([]CronSchedule, error) {
	return withFakeLock(f, "GetDueCronSchedules", func() ([]CronSchedule, error) {
		now := time.Now().UTC()
		due := mapValuesFiltered(f.crons, func(c *CronSchedule) bool { return c.Enabled && !c.NextRunAt.After(now) })
		sort.Slice(due, func(i, j int) bool { return due[i].NextRunAt.Before(due[j].NextRunAt) })
		return due, nil
	})
}

func (f *FakeStore) GetAllCronSchedules(ctx context.Context, targetID string) ([]CronSchedule, error) {
	return withFakeLock(f, "GetAllCronSchedules", func() ([]CronSchedule, error) {
		results := mapValuesFiltered(f.crons, func(c *CronSchedule) bool { return targetID == "" || c.TargetID == targetID })
		sort.Slice(results, func(i, j int) bool { return results[i].NextRunAt.Before(results[j].NextRunAt) })
		return results, nil
	})
}

func (f *FakeStore) UpdateCronNextRun(ctx context.Context, id string, nextRunAt time.Time) error {
	return f.withLockErr("UpdateCronNextRun", func() error {
		if c, ok := f.crons[id]; ok {
			c.NextRunAt = nextRunAt
		}
		return nil
	})
}

func (f *FakeStore) CreateScheduleRun(ctx context.Context, run ScheduleRun) error {
	return f.withLockErr("CreateScheduleRun", func() error {
		if run.StartedAt.IsZero() {
			run.StartedAt = time.Now().UTC()
		}
		f.runs[run.ID] = cloneRun(&run)
		return nil
	})
}

func (f *FakeStore) UpdateScheduleRunStatus(ctx context.Context, params UpdateRunParams) error {
	return f.withLockErr("UpdateScheduleRunStatus", func() error {
		r := f.runs[params.RunID]
		if r == nil {
			return nil
		}
		setIf(&r.Status, params.Status)
		setIf(&r.MessageID, params.MessageID)
		setIf(&r.Error, params.Error)
		setIf(&r.DurationMs, params.DurationMs)
		setIf(&r.Model, params.Model)
		if !params.CompletedAt.IsZero() {
			r.CompletedAt = clonePtr(&params.CompletedAt)
		}
		return nil
	})
}

func (f *FakeStore) GetScheduleRunsPaginated(ctx context.Context, limit, offset int, scheduleID, status string) ([]ScheduleRun, int, error) {
	return withFakeLock2(f, "GetScheduleRunsPaginated", func() ([]ScheduleRun, int, error) {
		filtered := mapValuesFiltered(f.runs, func(r *ScheduleRun) bool {
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

func (f *FakeStore) GetScheduleRun(id string) (*ScheduleRun, error) {
	return withFakeLock(f, "GetScheduleRun", func() (*ScheduleRun, error) {
		if r, ok := f.runs[id]; ok && r != nil {
			return cloneRun(r), nil
		}
		return nil, sql.ErrNoRows
	})
}

func updateNextRun(next **time.Time, t time.Time) {
	if *next == nil || t.Before(**next) {
		cp := t
		*next = &cp
	}
}

func (f *FakeStore) GetScheduleSummaryMetrics(ctx context.Context) (ScheduleSummaryMetrics, error) {
	return withFakeLock(f, "GetScheduleSummaryMetrics", func() (ScheduleSummaryMetrics, error) {
		var m ScheduleSummaryMetrics
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

func (f *FakeStore) ReconcileOrphanedScheduleRuns(ctx context.Context) (int64, error) {
	return withFakeLock(f, "ReconcileOrphanedScheduleRuns", func() (int64, error) {
		now := time.Now().UTC()
		var reconciled int64
		for _, r := range f.runs {
			if r != nil && (r.Status == "enqueued" || r.Status == "running") {
				r.Status, r.Error, r.CompletedAt = "failed", "Interrupted by server restart", &now
				reconciled++
			}
		}
		return reconciled, nil
	})
}

func (f *FakeStore) PruneScheduleRuns(ctx context.Context, maxCount int, maxAge time.Duration) (int64, error) {
	return withFakeLock(f, "PruneScheduleRuns", func() (int64, error) {
		maxCount, maxAge = defaultVal(maxCount, 1000), defaultVal(maxAge, 30*24*time.Hour)
		var pruned int64
		cutoff := time.Now().UTC().Add(-maxAge)
		for id, r := range f.runs {
			if r != nil && r.StartedAt.Before(cutoff) {
				delete(f.runs, id)
				pruned++
			}
		}
		if len(f.runs) > maxCount {
			var list []*ScheduleRun
			for _, r := range f.runs {
				list = append(list, r)
			}
			sort.Slice(list, func(i, j int) bool {
				return list[i].StartedAt.After(list[j].StartedAt) || (list[i].StartedAt.Equal(list[j].StartedAt) && list[i].ID > list[j].ID)
			})
			for _, r := range list[maxCount:] {
				delete(f.runs, r.ID)
				pruned++
			}
		}
		return pruned, nil
	})
}

func (f *FakeStore) GetSessionSyncStates(ctx context.Context) (map[string]SessionSyncState, error) {
	return withFakeLock(f, "GetSessionSyncStates", func() (map[string]SessionSyncState, error) {
		res := make(map[string]SessionSyncState, len(f.sessionSummaries))
		for id, s := range f.sessionSummaries {
			if s != nil {
				res[id] = SessionSyncState{SessionID: id, LastMtime: s.LastMtime, LastIndexedStep: s.LastIndexedStep, IsSettled: s.IsSettled}
			}
		}
		return res, nil
	})
}

func (f *FakeStore) UpsertSessionSummary(ctx context.Context, summary SessionSummary) error {
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

func (f *FakeStore) BatchInsertTranscriptSteps(ctx context.Context, steps []TranscriptStep) error {
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
			if !slices.ContainsFunc(f.transcriptSteps[s.SessionID], func(e TranscriptStep) bool { return e.StepIndex == s.StepIndex }) {
				f.transcriptSteps[s.SessionID] = append(f.transcriptSteps[s.SessionID], cp)
			}
		}
		return nil
	})
}

func (f *FakeStore) SearchSessionSummaries(ctx context.Context, embedding []float32, queryText string, limit int, minScore float64) ([]SessionSummary, error) {
	return withFakeLock(f, "SearchSessionSummaries", func() ([]SessionSummary, error) {
		hasEmbedding, queryText := len(embedding) == ExpectedEmbeddingDim, strings.TrimSpace(queryText)
		hasText := queryText != ""
		if !hasEmbedding && !hasText {
			return nil, nil
		}
		limit = defaultVal(limit, 10)
		if minScore < 0 {
			minScore = 0.0
		}
		qLower, words := strings.ToLower(queryText), strings.Fields(strings.ToLower(queryText))
		var scored []SessionSummary
		for _, s := range f.sessionSummaries {
			if s == nil {
				continue
			}
			var sim, textScore float64
			if hasEmbedding && len(s.Embedding) == ExpectedEmbeddingDim {
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

func (f *FakeStore) SearchTranscriptSteps(ctx context.Context, queryText, sessionFilter, toolFilter string, limit int) ([]TranscriptStep, error) {
	return withFakeLock(f, "SearchTranscriptSteps", func() ([]TranscriptStep, error) {
		queryText, sessionFilter, toolFilter = strings.TrimSpace(queryText), strings.TrimSpace(sessionFilter), strings.TrimSpace(toolFilter)
		if queryText == "" && sessionFilter == "" && toolFilter == "" {
			return nil, nil
		}
		limit = defaultVal(limit, 20)
		qLower, words := strings.ToLower(queryText), strings.Fields(strings.ToLower(queryText))
		var matched []TranscriptStep
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

func (f *FakeStore) GetTranscriptStepRange(ctx context.Context, sessionID string, startStep, endStep int) ([]TranscriptStep, error) {
	return withFakeLock(f, "GetTranscriptStepRange", func() ([]TranscriptStep, error) {
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
		var matched []TranscriptStep
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
	return fmt.Sprintf("%s/%d", normalizeRepo(repo), prNumber)
}

func (f *FakeStore) UpsertPR(ctx context.Context, record PRRecord) error {
	return f.withLockErr("UpsertPR", func() error {
		norm, err := validateAndNormalizeUpsertPR(record)
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

func (f *FakeStore) getPRRecord(repo string, prNumber int) (*PRRecord, error) {
	if repo = normalizeRepo(repo); repo == "" {
		return nil, fmt.Errorf("repo cannot be empty")
	}
	rec, exists := f.prRecords[prKey(repo, prNumber)]
	if !exists {
		return nil, ErrPRNotFound
	}
	return rec, nil
}

func (f *FakeStore) GetPRByNumber(ctx context.Context, repo string, prNumber int) (*PRRecord, error) {
	return withFakeLock(f, "GetPRByNumber", func() (*PRRecord, error) {
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

func (f *FakeStore) CreateVanadisBuild(ctx context.Context, build VanadisBuild, chunks []VanadisChunk) error {
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

func (f *FakeStore) GetVanadisBuild(ctx context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	return withFakeLock2(f, "GetVanadisBuild", func() (*VanadisBuild, []VanadisChunk, error) {
		if buildID = strings.TrimSpace(buildID); buildID == "" {
			return nil, nil, fmt.Errorf("build id cannot be empty")
		}
		b := f.vanadisBuilds[buildID]
		if b == nil {
			return nil, nil, ErrVanadisBuildNotFound
		}
		chunks := mapValuesFiltered(f.vanadisChunks, func(c *VanadisChunk) bool { return c.BuildID == buildID })
		for i := range chunks {
			chunks[i] = cloneVanadisChunk(&chunks[i])
		}
		sort.Slice(chunks, func(i, j int) bool { return chunks[i].ChunkIndex < chunks[j].ChunkIndex })
		cp := *b
		return &cp, chunks, nil
	})
}

func (f *FakeStore) getVanadisChunkLocked(chunkID string) (*VanadisChunk, error) {
	if chunkID = strings.TrimSpace(chunkID); chunkID == "" {
		return nil, fmt.Errorf("chunk id cannot be empty")
	}
	c, exists := f.vanadisChunks[chunkID]
	if !exists {
		return nil, ErrVanadisChunkNotFound
	}
	return c, nil
}

func (f *FakeStore) GetVanadisChunk(ctx context.Context, chunkID string) (*VanadisChunk, error) {
	return withFakeLock(f, "GetVanadisChunk", func() (*VanadisChunk, error) {
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
			return ErrVanadisBuildNotFound
		}
		b.StatusMessageID, b.UpdatedAt = strings.TrimSpace(messageID), time.Now()
		return nil
	})
}
