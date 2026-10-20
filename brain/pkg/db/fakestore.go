package db

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/session"
)

var (
	_ Store           = (*FakeStore)(nil)
	_ MessageStore    = (*FakeStore)(nil)
	_ ScheduleStore   = (*FakeStore)(nil)
	_ SessionStore    = (*FakeStore)(nil)
	_ FactStore       = (*FakeStore)(nil)
	_ TranscriptStore = (*FakeStore)(nil)
	_ PRRegistryStore = (*FakeStore)(nil)
	_ VanadisStore    = (*FakeStore)(nil)
)

type threadSummaryRecord struct {
	summary   string
	watermark string
}

// FakeStore is a thread-safe, pure Go in-memory implementation of db.Store.
type FakeStore struct {
	mu sync.RWMutex

	closed   bool
	failNext map[string]error

	// MessageStore state
	messages     map[string]*Message
	messageOrder []string
	nextRowID    int64

	// SessionStore state
	sessions        map[string]*SessionInfo
	threadSummaries map[string]threadSummaryRecord

	// ScheduleStore state
	oneShots map[string]*OneShotSchedule
	crons    map[string]*CronSchedule
	runs     map[string]*ScheduleRun

	// FactStore state
	facts       map[int64]*FactWithEmbedding
	nextFactID  int64
	watermarks  map[string]int64
	extractedAt map[string]time.Time

	// TranscriptStore state
	sessionSummaries map[string]*SessionSummary
	transcriptSteps  map[string][]TranscriptStep

	// PRRegistryStore state
	prRecords map[string]*PRRecord
	nextPRID  int64

	// VanadisStore state
	vanadisBuilds map[string]*VanadisBuild
	vanadisChunks map[string]*VanadisChunk
}

// NewFakeStore constructs an initialized, empty in-memory FakeStore.
func NewFakeStore() *FakeStore {
	return &FakeStore{
		failNext:         make(map[string]error),
		messages:         make(map[string]*Message),
		messageOrder:     make([]string, 0),
		sessions:         make(map[string]*SessionInfo),
		threadSummaries:  make(map[string]threadSummaryRecord),
		oneShots:         make(map[string]*OneShotSchedule),
		crons:            make(map[string]*CronSchedule),
		runs:             make(map[string]*ScheduleRun),
		facts:            make(map[int64]*FactWithEmbedding),
		watermarks:       make(map[string]int64),
		extractedAt:      make(map[string]time.Time),
		sessionSummaries: make(map[string]*SessionSummary),
		transcriptSteps:  make(map[string][]TranscriptStep),
		prRecords:        make(map[string]*PRRecord),
		vanadisBuilds:    make(map[string]*VanadisBuild),
		vanadisChunks:    make(map[string]*VanadisChunk),
	}
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
	cp.Metadata.Mentions = cloneSlice(m.Metadata.Mentions)
	cp.Metadata.MentionUserIDs = cloneSlice(m.Metadata.MentionUserIDs)
	cp.Metadata.MentionRoleIDs = cloneSlice(m.Metadata.MentionRoleIDs)
	cp.Metadata.Attachments = cloneSlice(m.Metadata.Attachments)
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

func cloneFactWithEmbedding(fe *FactWithEmbedding) *FactWithEmbedding {
	if fe == nil {
		return nil
	}
	cp := *fe
	cp.Embedding = cloneSlice(fe.Embedding)
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

func cloneTranscriptSteps(steps []TranscriptStep) []TranscriptStep {
	return cloneSlice(steps)
}

func cloneSliceNonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return cloneSlice(s)
}

func cloneVanadisChunk(c *VanadisChunk) VanadisChunk {
	cp := *c
	cp.TargetFiles = cloneSliceNonNil(c.TargetFiles)
	cp.TargetSymbols = cloneSliceNonNil(c.TargetSymbols)
	cp.AcceptanceCriteria = cloneSliceNonNil(c.AcceptanceCriteria)
	if strings.TrimSpace(cp.Effort) == "" {
		cp.Effort = "medium"
	}
	return cp
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("InsertMessage"); err != nil {
		return err
	}
	f.insertMessageLocked(msg)
	return nil
}

func (f *FakeStore) mutateMessage(method, id string, fn func(*Message)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail(method); err != nil {
		return err
	}
	if m, ok := f.messages[id]; ok {
		fn(m)
		m.UpdatedAt = time.Now().UTC()
	}
	return nil
}

func (f *FakeStore) UpdateMessageStatus(ctx context.Context, id, status, errorMsg string) error {
	return f.mutateMessage("UpdateMessageStatus", id, func(m *Message) {
		m.Status, m.ErrorMessage = status, errorMsg
	})
}

func (f *FakeStore) UpdateMessageCompleted(ctx context.Context, id, responseText string) error {
	return f.mutateMessage("UpdateMessageCompleted", id, func(m *Message) {
		m.Status, m.ResponseText = StatusCompleted, responseText
	})
}

func (f *FakeStore) IncrementMessageRetry(ctx context.Context, id, errorMsg string) error {
	return f.mutateMessage("IncrementMessageRetry", id, func(m *Message) {
		m.RetryCount++
		m.ErrorMessage = errorMsg
	})
}

func (f *FakeStore) ResetMessageToPendingWithRestart(ctx context.Context, id, reason string) error {
	return f.mutateMessage("ResetMessageToPendingWithRestart", id, func(m *Message) {
		m.Status, m.RestartCount, m.ErrorMessage = StatusPending, m.RestartCount+1, reason
	})
}

func (f *FakeStore) GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetPendingOrProcessingMessages"); err != nil {
		return nil, err
	}
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
}

func (f *FakeStore) GetMessage(ctx context.Context, id string) (*Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetMessage"); err != nil {
		return nil, err
	}
	if m, ok := f.messages[id]; ok {
		return cloneMessage(m), nil
	}
	return nil, sql.ErrNoRows
}

func (f *FakeStore) MessageExists(ctx context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("MessageExists"); err != nil {
		return false, err
	}
	_, exists := f.messages[id]
	return exists, nil
}

func (f *FakeStore) ClaimPendingMessage(ctx context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("ClaimPendingMessage"); err != nil {
		return false, err
	}
	m, ok := f.messages[id]
	if !ok || m.Status != StatusPending {
		return false, nil
	}
	m.Status, m.UpdatedAt = StatusProcessing, time.Now().UTC()
	return true, nil
}

func (f *FakeStore) GetActiveRecentThreadIDs(ctx context.Context, since time.Duration) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetActiveRecentThreadIDs"); err != nil {
		return nil, err
	}
	cutoff := time.Now().UTC().Add(-since)
	seen := make(map[string]bool)
	var threadIDs []string
	for i := len(f.messageOrder) - 1; i >= 0; i-- {
		id := f.messageOrder[i]
		if m := f.messages[id]; m != nil && m.ThreadID != "" && m.UpdatedAt.After(cutoff) && !seen[m.ThreadID] {
			seen[m.ThreadID] = true
			threadIDs = append(threadIDs, m.ThreadID)
		}
	}
	return threadIDs, nil
}

func (f *FakeStore) GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetRecentThreadMessages"); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 10
	}
	var matches []Message
	for i := len(f.messageOrder) - 1; i >= 0; i-- {
		if m := f.messages[f.messageOrder[i]]; m != nil && m.ThreadID == threadID {
			matches = append(matches, *cloneMessage(m))
			if len(matches) >= limit {
				break
			}
		}
	}
	for i, j := 0, len(matches)-1; i < j; i, j = i+1, j-1 {
		matches[i], matches[j] = matches[j], matches[i]
	}
	return matches, nil
}

func (f *FakeStore) GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetMaxMessageRowID"); err != nil {
		return 0, err
	}
	var maxID int64
	for _, id := range f.messageOrder {
		if m := f.messages[id]; m != nil && m.ThreadID == threadID && m.Status == StatusCompleted && m.RowID > maxID {
			maxID = m.RowID
		}
	}
	return maxID, nil
}

func (f *FakeStore) SetMessageUpdatedAt(id string, t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m, ok := f.messages[id]; ok && m != nil {
		m.UpdatedAt = t
	}
}

func (f *FakeStore) SetSessionUpdatedAt(threadID string, t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.sessions[threadID]; ok && s != nil {
		s.UpdatedAt = t
	}
}

func (f *FakeStore) GetActiveTasks(ctx context.Context) ([]ActiveTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetActiveTasks"); err != nil {
		return nil, err
	}
	var activeMsgs []*Message
	for _, id := range f.messageOrder {
		if m := f.messages[id]; m != nil && (m.Status == StatusPending || m.Status == StatusProcessing) {
			activeMsgs = append(activeMsgs, m)
		}
	}
	sort.Slice(activeMsgs, func(i, j int) bool {
		return activeMsgs[i].CreatedAt.Before(activeMsgs[j].CreatedAt)
	})
	if len(activeMsgs) > 50 {
		activeMsgs = activeMsgs[:50]
	}
	tasks := make([]ActiveTask, len(activeMsgs))
	for i, m := range activeMsgs {
		sessionID := ""
		if s := f.sessions[m.ThreadID]; s != nil {
			sessionID = s.InternalSessionID
		}
		summary := m.Summary
		if summary == "" {
			summary = CleanTaskSummary(m.Content)
		}
		tasks[i] = ActiveTask{
			ID:            m.ID,
			RowID:         m.RowID,
			ThreadID:      m.ThreadID,
			SessionID:     sessionID,
			AuthorName:    m.AuthorName,
			AuthorID:      m.AuthorID,
			Prompt:        m.Content,
			Summary:       summary,
			Status:        m.Status,
			RetryCount:    m.RetryCount,
			ScheduleRunID: m.ScheduleRunID,
			CreatedAt:     m.CreatedAt,
			UpdatedAt:     m.UpdatedAt,
		}
	}
	return tasks, nil
}

func (f *FakeStore) GetSessionID(ctx context.Context, threadID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetSessionID"); err != nil {
		return "", err
	}
	if s := f.sessions[threadID]; s != nil {
		return s.InternalSessionID, nil
	}
	return "", nil
}

func (f *FakeStore) GetPreviousSessionID(ctx context.Context, threadID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetPreviousSessionID"); err != nil {
		return "", err
	}
	if s := f.sessions[threadID]; s != nil {
		return s.PreviousSessionID, nil
	}
	return "", nil
}

func (f *FakeStore) SaveSessionID(ctx context.Context, threadID, sessionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("SaveSessionID"); err != nil {
		return err
	}
	now := time.Now().UTC()
	if s, ok := f.sessions[threadID]; ok && s != nil {
		s.InternalSessionID, s.UpdatedAt = sessionID, now
		return nil
	}
	f.sessions[threadID] = &SessionInfo{ThreadID: threadID, InternalSessionID: sessionID, CreatedAt: now, UpdatedAt: now}
	return nil
}

func (f *FakeStore) IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("IncrementSessionTurnCount"); err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	s, ok := f.sessions[sessionKey]
	if !ok || s == nil {
		f.sessions[sessionKey] = &SessionInfo{ThreadID: sessionKey, TurnCount: 1, CreatedAt: now, UpdatedAt: now}
		return 1, nil
	}
	s.TurnCount++
	s.UpdatedAt = now
	return s.TurnCount, nil
}

func (f *FakeStore) GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetSessionTurnCount"); err != nil {
		return 0, err
	}
	if s := f.sessions[sessionKey]; s != nil {
		return s.TurnCount, nil
	}
	return 0, nil
}

func (f *FakeStore) RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("RotateSessionID"); err != nil {
		return err
	}
	now := time.Now().UTC()
	s, ok := f.sessions[sessionKey]
	if !ok || s == nil {
		f.sessions[sessionKey] = &SessionInfo{ThreadID: sessionKey, InternalSessionID: newSessionID, CreatedAt: now, UpdatedAt: now}
		return nil
	}
	s.PreviousSessionID, s.InternalSessionID, s.TurnCount, s.UpdatedAt = s.InternalSessionID, newSessionID, 0, now
	return nil
}

func (f *FakeStore) FindUnrotatedSessions(ctx context.Context, minTurns int, maxAge ...time.Duration) ([]SessionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("FindUnrotatedSessions"); err != nil {
		return nil, err
	}
	if minTurns <= 0 {
		minTurns = session.DefaultMaxSessionTurns
	}
	limitAge := DefaultUnrotatedSessionMaxAge
	if len(maxAge) > 0 {
		limitAge = maxAge[0]
	}
	var cutoff time.Time
	if limitAge > 0 {
		cutoff = time.Now().UTC().Add(-limitAge)
	}
	var res []SessionInfo
	for _, s := range f.sessions {
		if s != nil && s.TurnCount > minTurns {
			if limitAge <= 0 || s.UpdatedAt.After(cutoff) || s.UpdatedAt.Equal(cutoff) {
				res = append(res, *clonePtr(s))
			}
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].TurnCount > res[j].TurnCount
	})
	return res, nil
}

func (f *FakeStore) GetThreadSummary(ctx context.Context, threadID string) (string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetThreadSummary"); err != nil {
		return "", "", err
	}
	rec, ok := f.threadSummaries[threadID]
	if !ok {
		return "", "", nil
	}
	return rec.summary, rec.watermark, nil
}

func (f *FakeStore) SaveThreadSummary(ctx context.Context, threadID, summary, lastSummarizedMsgID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("SaveThreadSummary"); err != nil {
		return err
	}
	f.threadSummaries[threadID] = threadSummaryRecord{summary: summary, watermark: lastSummarizedMsgID}
	return nil
}

func (f *FakeStore) GetSessionActivityStats(ctx context.Context, threadID string) (*SessionActivityStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetSessionActivityStats"); err != nil {
		return nil, err
	}
	stats := &SessionActivityStats{}
	if s := f.sessions[threadID]; s != nil {
		stats.InternalSessionID = s.InternalSessionID
		stats.TurnCount = s.TurnCount
		stats.SessionUpdatedAt = s.UpdatedAt
	}
	for _, id := range f.messageOrder {
		m := f.messages[id]
		if m != nil && m.ThreadID == threadID && m.Status == StatusCompleted {
			if !strings.HasPrefix(m.ResponseText, "[EXPIRED_STALE]") &&
				!strings.HasPrefix(m.ResponseText, "[AMBIENT") &&
				!strings.HasPrefix(m.ResponseText, "[IGNORED") {
				stats.CompletedTurns++
				if m.UpdatedAt.After(stats.LastMessageAt) {
					stats.LastMessageAt = m.UpdatedAt
				}
				if m.CreatedAt.After(stats.LastCompletedMessageCreatedAt) {
					stats.LastCompletedMessageCreatedAt = m.CreatedAt
				}
			}
		}
	}
	return stats, nil
}

func (f *FakeStore) SetSessionInfo(s SessionInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions[s.ThreadID] = clonePtr(&s)
}

func (f *FakeStore) GetExternalConversationID(ctx context.Context, internalID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetExternalConversationID"); err != nil {
		return "", err
	}
	if internalID != "" {
		for threadID, sess := range f.sessions {
			if sess != nil && sess.InternalSessionID == internalID {
				return threadID, nil
			}
		}
	}
	return "", nil
}

func (f *FakeStore) CreateOneShotSchedule(ctx context.Context, s OneShotSchedule) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("CreateOneShotSchedule"); err != nil {
		return err
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	f.oneShots[s.ID] = clonePtr(&s)
	return nil
}

func (f *FakeStore) GetDueOneShotSchedules(ctx context.Context) ([]OneShotSchedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetDueOneShotSchedules"); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	var due []OneShotSchedule
	for _, s := range f.oneShots {
		if s != nil && !s.RunAt.After(now) {
			due = append(due, *clonePtr(s))
		}
	}
	sort.Slice(due, func(i, j int) bool {
		return due[i].RunAt.Before(due[j].RunAt)
	})
	return due, nil
}

func (f *FakeStore) InsertMessageAndConsumeOneShot(ctx context.Context, scheduleID string, msg Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("InsertMessageAndConsumeOneShot"); err != nil {
		return err
	}
	if _, exists := f.oneShots[scheduleID]; !exists {
		return ErrScheduleAlreadyConsumed
	}
	delete(f.oneShots, scheduleID)
	f.insertMessageLocked(msg)
	return nil
}

func (f *FakeStore) GetAllOneShotSchedules(ctx context.Context, threadID string) ([]OneShotSchedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetAllOneShotSchedules"); err != nil {
		return nil, err
	}
	var results []OneShotSchedule
	for _, s := range f.oneShots {
		if s != nil && (threadID == "" || s.ThreadID == threadID) {
			results = append(results, *clonePtr(s))
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].RunAt.Before(results[j].RunAt)
	})
	return results, nil
}

func (f *FakeStore) CreateCronSchedule(ctx context.Context, c CronSchedule) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("CreateCronSchedule"); err != nil {
		return err
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	f.crons[c.ID] = clonePtr(&c)
	return nil
}

func (f *FakeStore) GetDueCronSchedules(ctx context.Context) ([]CronSchedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetDueCronSchedules"); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	var due []CronSchedule
	for _, c := range f.crons {
		if c != nil && c.Enabled && !c.NextRunAt.After(now) {
			due = append(due, *clonePtr(c))
		}
	}
	sort.Slice(due, func(i, j int) bool {
		return due[i].NextRunAt.Before(due[j].NextRunAt)
	})
	return due, nil
}

func (f *FakeStore) GetAllCronSchedules(ctx context.Context, targetID string) ([]CronSchedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetAllCronSchedules"); err != nil {
		return nil, err
	}
	var results []CronSchedule
	for _, c := range f.crons {
		if c != nil && (targetID == "" || c.TargetID == targetID) {
			results = append(results, *clonePtr(c))
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].NextRunAt.Before(results[j].NextRunAt)
	})
	return results, nil
}

func (f *FakeStore) UpdateCronNextRun(ctx context.Context, id string, nextRunAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpdateCronNextRun"); err != nil {
		return err
	}
	if c, ok := f.crons[id]; ok {
		c.NextRunAt = nextRunAt
	}
	return nil
}

func (f *FakeStore) CreateScheduleRun(ctx context.Context, run ScheduleRun) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("CreateScheduleRun"); err != nil {
		return err
	}
	if run.StartedAt.IsZero() {
		run.StartedAt = time.Now().UTC()
	}
	f.runs[run.ID] = cloneRun(&run)
	return nil
}

func (f *FakeStore) UpdateScheduleRunStatus(ctx context.Context, params UpdateRunParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpdateScheduleRunStatus"); err != nil {
		return err
	}
	r, ok := f.runs[params.RunID]
	if !ok {
		return nil
	}
	if params.Status != "" {
		r.Status = params.Status
	}
	if params.MessageID != "" {
		r.MessageID = params.MessageID
	}
	if params.Error != "" {
		r.Error = params.Error
	}
	if !params.CompletedAt.IsZero() {
		t := params.CompletedAt
		r.CompletedAt = &t
	}
	if params.DurationMs > 0 {
		r.DurationMs = params.DurationMs
	}
	if params.Model != "" {
		r.Model = params.Model
	}
	return nil
}

func (f *FakeStore) GetScheduleRunsPaginated(ctx context.Context, limit, offset int, scheduleID, status string) ([]ScheduleRun, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetScheduleRunsPaginated"); err != nil {
		return nil, 0, err
	}
	var filtered []ScheduleRun
	for _, r := range f.runs {
		if r == nil {
			continue
		}
		if scheduleID != "" && r.ScheduleID != scheduleID {
			continue
		}
		if status != "" && r.Status != status {
			continue
		}
		filtered = append(filtered, *cloneRun(r))
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].StartedAt.Equal(filtered[j].StartedAt) {
			return filtered[i].ID > filtered[j].ID
		}
		return filtered[i].StartedAt.After(filtered[j].StartedAt)
	})
	total := len(filtered)
	if offset > total {
		return nil, total, nil
	}
	end := offset + limit
	if limit <= 0 || end > total {
		end = total
	}
	return filtered[offset:end], total, nil
}

func (f *FakeStore) GetScheduleRun(id string) (*ScheduleRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetScheduleRun"); err != nil {
		return nil, err
	}
	if r, ok := f.runs[id]; ok && r != nil {
		return cloneRun(r), nil
	}
	return nil, sql.ErrNoRows
}

func (f *FakeStore) GetScheduleSummaryMetrics(ctx context.Context) (ScheduleSummaryMetrics, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetScheduleSummaryMetrics"); err != nil {
		return ScheduleSummaryMetrics{}, err
	}
	var m ScheduleSummaryMetrics
	for _, c := range f.crons {
		if c != nil && c.Enabled {
			m.CronCount++
		}
	}
	m.OneShotCount = len(f.oneShots)
	m.TotalActive = m.CronCount + m.OneShotCount

	now := time.Now().UTC()
	cutoff24h := now.Add(-24 * time.Hour)
	var totalRuns, completedRuns int
	for _, r := range f.runs {
		if r != nil && !r.StartedAt.Before(cutoff24h) {
			totalRuns++
			if r.Status == "completed" {
				completedRuns++
			}
		}
	}
	m.TotalRuns24h = totalRuns
	if totalRuns == 0 {
		m.SuccessRate24h = 100.0
	} else {
		m.SuccessRate24h = math.Round((float64(completedRuns)/float64(totalRuns))*1000.0) / 10.0
	}

	var earliest *time.Time
	for _, c := range f.crons {
		if c != nil && c.Enabled && (earliest == nil || c.NextRunAt.Before(*earliest)) {
			t := c.NextRunAt
			earliest = &t
		}
	}
	for _, s := range f.oneShots {
		if s != nil && (earliest == nil || s.RunAt.Before(*earliest)) {
			t := s.RunAt
			earliest = &t
		}
	}
	m.NextRunAt = earliest
	return m, nil
}

func (f *FakeStore) ReconcileOrphanedScheduleRuns(ctx context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("ReconcileOrphanedScheduleRuns"); err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	var reconciled int64
	for _, r := range f.runs {
		if r != nil && (r.Status == "enqueued" || r.Status == "running") {
			r.Status, r.Error, r.CompletedAt = "failed", "Interrupted by server restart", &now
			reconciled++
		}
	}
	return reconciled, nil
}

func (f *FakeStore) PruneScheduleRuns(ctx context.Context, maxCount int, maxAge time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("PruneScheduleRuns"); err != nil {
		return 0, err
	}
	if maxCount <= 0 {
		maxCount = 1000
	}
	if maxAge <= 0 {
		maxAge = 30 * 24 * time.Hour
	}
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
			if list[i].StartedAt.Equal(list[j].StartedAt) {
				return list[i].ID > list[j].ID
			}
			return list[i].StartedAt.After(list[j].StartedAt)
		})
		for i := maxCount; i < len(list); i++ {
			delete(f.runs, list[i].ID)
			pruned++
		}
	}
	return pruned, nil
}

func (f *FakeStore) InsertFact(ctx context.Context, category, factText string, importance float64, embedding []float32) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("InsertFact"); err != nil {
		return 0, err
	}
	if importance <= 0 {
		importance = 0.5
	}
	id := atomic.AddInt64(&f.nextFactID, 1)
	now := time.Now().UTC()
	fe := &FactWithEmbedding{
		Fact: Fact{
			ID:               id,
			Category:         category,
			FactText:         factText,
			Importance:       importance,
			CreatedAt:        now,
			LastReinforcedAt: now,
			LastDecayedAt:    now,
			ReinforceCount:   1,
		},
		Embedding: embedding,
	}
	f.facts[id] = cloneFactWithEmbedding(fe)
	return id, nil
}

func (f *FakeStore) SearchSimilarFacts(ctx context.Context, embedding []float32, queryText string, limit int, minScore float64) ([]Fact, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("SearchSimilarFacts"); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 10
	}
	if minScore <= 0 {
		minScore = 0.20
	}
	type scored struct {
		fact  Fact
		score float64
	}
	var matches []scored
	qLower := strings.ToLower(strings.TrimSpace(queryText))
	hasVec, hasText := len(embedding) > 0, qLower != ""
	if !hasVec && !hasText {
		return nil, nil
	}
	for _, fe := range f.facts {
		if fe == nil {
			continue
		}
		var sim, textScore float64
		if hasVec && len(embedding) == len(fe.Embedding) {
			sim = cosineSimilarity(embedding, fe.Embedding)
		}
		if hasText && strings.Contains(strings.ToLower(fe.Fact.FactText), qLower) {
			textScore = 1.0
		}
		imp := fe.Fact.Importance
		if imp <= 0 {
			imp = 1.0
		}
		var score float64
		if hasVec && hasText {
			score = (0.70*sim + 0.30*textScore) * imp
		} else if hasVec {
			score = sim * imp
		} else {
			score = textScore * imp
		}
		if score >= minScore {
			matches = append(matches, scored{fact: fe.Fact, score: score})
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].score > matches[j].score
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	result := make([]Fact, len(matches))
	for i, m := range matches {
		result[i] = m.fact
	}
	return result, nil
}

func (f *FakeStore) GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetFactsPaginated"); err != nil {
		return nil, err
	}
	var list []Fact
	for _, fe := range f.facts {
		if fe == nil {
			continue
		}
		if filter.Category != "" && fe.Fact.Category != filter.Category {
			continue
		}
		if filter.Query != "" && !strings.Contains(strings.ToLower(fe.Fact.FactText), strings.ToLower(filter.Query)) {
			continue
		}
		list = append(list, fe.Fact)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].CreatedAt.After(list[j].CreatedAt)
	})
	total := len(list)
	if filter.Offset > total {
		return &FactsResult{Facts: nil, Total: total, Limit: filter.Limit, Offset: filter.Offset}, nil
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	end := filter.Offset + limit
	if end > total {
		end = total
	}
	return &FactsResult{Facts: list[filter.Offset:end], Total: total, Limit: filter.Limit, Offset: filter.Offset}, nil
}

func (f *FakeStore) GetFactsByThreadWithEmbeddings(ctx context.Context, threadID string) ([]FactWithEmbedding, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetFactsByThreadWithEmbeddings"); err != nil {
		return nil, err
	}
	var results []FactWithEmbedding
	for _, fe := range f.facts {
		if fe != nil {
			results = append(results, *cloneFactWithEmbedding(fe))
		}
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Fact.CreatedAt.Equal(results[j].Fact.CreatedAt) {
			return results[i].Fact.ID < results[j].Fact.ID
		}
		return results[i].Fact.CreatedAt.Before(results[j].Fact.CreatedAt)
	})
	return results, nil
}

func (f *FakeStore) GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetActiveConversationsForExtraction"); err != nil {
		return nil, err
	}
	if activeHours <= 0 {
		activeHours = 24
	}
	cutoff := time.Now().UTC().Add(-time.Duration(activeHours) * time.Hour)
	seen := make(map[string]bool)
	var threads []string
	for _, id := range f.messageOrder {
		m := f.messages[id]
		if m != nil && m.ThreadID != "" && (m.CreatedAt.After(cutoff) || m.CreatedAt.Equal(cutoff)) && IsExtractableAdminTurn(*m) {
			watermark := f.watermarks[m.ThreadID]
			if (watermark == 0 || m.RowID > watermark) && !seen[m.ThreadID] {
				seen[m.ThreadID] = true
				threads = append(threads, m.ThreadID)
				if len(threads) >= 20 {
					break
				}
			}
		}
	}
	sort.Strings(threads)
	return threads, nil
}

func (f *FakeStore) GetUnextractedAdminTurns(ctx context.Context, threadID string) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetUnextractedAdminTurns"); err != nil {
		return nil, err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return nil, nil
	}
	watermark := f.watermarks[threadID]
	var turns []Message
	for _, id := range f.messageOrder {
		m := f.messages[id]
		if m != nil && m.ThreadID == threadID && (watermark == 0 || m.RowID > watermark) && IsExtractableAdminTurn(*m) {
			turns = append(turns, *cloneMessage(m))
		}
	}
	return turns, nil
}

func (f *FakeStore) UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpdateConversationFactWatermark"); err != nil {
		return err
	}
	f.watermarks[threadID] = maxRowID
	return nil
}

func (f *FakeStore) UpdateConversationFactExtractedAt(ctx context.Context, threadID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpdateConversationFactExtractedAt"); err != nil {
		return err
	}
	f.extractedAt[threadID] = time.Now().UTC()
	return nil
}

func (f *FakeStore) FindDuplicateFact(ctx context.Context, embedding []float32, minSim float64) (*Fact, float64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("FindDuplicateFact"); err != nil {
		return nil, 0, err
	}
	if minSim <= 0 {
		minSim = 0.85
	}
	var bestFact *Fact
	var bestSim float64
	for _, fe := range f.facts {
		if fe == nil || len(fe.Embedding) == 0 {
			continue
		}
		if sim := cosineSimilarity(embedding, fe.Embedding); sim >= minSim && sim > bestSim {
			bestSim = sim
			cp := fe.Fact
			bestFact = &cp
		}
	}
	return bestFact, bestSim, nil
}

func (f *FakeStore) ReinforceFact(ctx context.Context, id int64, newText string, newEmbedding []float32, boost float64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("ReinforceFact"); err != nil {
		return err
	}
	fe, ok := f.facts[id]
	if !ok || fe == nil {
		return ErrFactNotFound
	}
	if boost <= 0 {
		boost = 0.15
	}
	fe.Fact.Importance = math.Min(1.0, math.Max(0.70, math.Round((fe.Fact.Importance+boost)*100)/100))
	fe.Fact.LastReinforcedAt = time.Now().UTC()
	fe.Fact.ReinforceCount++
	return nil
}

func (f *FakeStore) DecayAndPruneFacts(ctx context.Context, decayStep float64, pruneFloor float64, pruneAgeDays int) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("DecayAndPruneFacts"); err != nil {
		return 0, 0, err
	}
	if decayStep <= 0 {
		decayStep = 0.05
	}
	if pruneFloor <= 0 {
		pruneFloor = 0.15
	}
	now := time.Now().UTC()
	ageCutoff := now.Add(-time.Duration(pruneAgeDays) * 24 * time.Hour)
	var decayed, pruned int64
	for id, fe := range f.facts {
		if fe == nil {
			continue
		}
		fe.Fact.Importance = math.Max(0.0, math.Round((fe.Fact.Importance-decayStep)*100)/100)
		fe.Fact.LastDecayedAt = now
		decayed++
		if fe.Fact.Importance <= pruneFloor && (pruneAgeDays <= 0 || fe.Fact.CreatedAt.Before(ageCutoff)) {
			delete(f.facts, id)
			pruned++
		}
	}
	return decayed, pruned, nil
}

func (f *FakeStore) GetFactsMissingEmbeddings(ctx context.Context, limit int) ([]Fact, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetFactsMissingEmbeddings"); err != nil {
		return nil, err
	}
	var results []Fact
	for _, fe := range f.facts {
		if fe != nil && len(fe.Embedding) == 0 {
			results = append(results, fe.Fact)
			if limit > 0 && len(results) >= limit {
				break
			}
		}
	}
	return results, nil
}

func (f *FakeStore) UpdateFactEmbedding(ctx context.Context, id int64, embedding []float32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpdateFactEmbedding"); err != nil {
		return err
	}
	fe, ok := f.facts[id]
	if !ok || fe == nil {
		return ErrFactNotFound
	}
	fe.Embedding = cloneSlice(embedding)
	return nil
}

func (f *FakeStore) GetSessionSyncStates(ctx context.Context) (map[string]SessionSyncState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetSessionSyncStates"); err != nil {
		return nil, err
	}
	res := make(map[string]SessionSyncState, len(f.sessionSummaries))
	for id, s := range f.sessionSummaries {
		if s != nil {
			res[id] = SessionSyncState{
				SessionID:       id,
				LastMtime:       s.LastMtime,
				LastIndexedStep: s.LastIndexedStep,
				IsSettled:       s.IsSettled,
			}
		}
	}
	return res, nil
}

func (f *FakeStore) UpsertSessionSummary(ctx context.Context, summary SessionSummary) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpsertSessionSummary"); err != nil {
		return err
	}
	if strings.TrimSpace(summary.SessionID) == "" {
		return fmt.Errorf("session ID cannot be empty")
	}
	now := time.Now().UTC()
	if existing, ok := f.sessionSummaries[summary.SessionID]; ok && existing != nil {
		if summary.ThreadID != "" {
			existing.ThreadID = summary.ThreadID
		}
		if summary.Summary != "" {
			existing.Summary = summary.Summary
		}
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
}

func (f *FakeStore) BatchInsertTranscriptSteps(ctx context.Context, steps []TranscriptStep) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("BatchInsertTranscriptSteps"); err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, s := range steps {
		if strings.TrimSpace(s.SessionID) == "" {
			continue
		}
		cp := s
		if cp.CreatedAt.IsZero() {
			cp.CreatedAt = now
		}
		existingSteps := f.transcriptSteps[s.SessionID]
		found := false
		for _, existing := range existingSteps {
			if existing.StepIndex == s.StepIndex {
				found = true
				break
			}
		}
		if !found {
			f.transcriptSteps[s.SessionID] = append(f.transcriptSteps[s.SessionID], cp)
		}
	}
	return nil
}

func (f *FakeStore) SearchSessionSummaries(ctx context.Context, embedding []float32, queryText string, limit int, minScore float64) ([]SessionSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("SearchSessionSummaries"); err != nil {
		return nil, err
	}
	hasEmbedding := len(embedding) == ExpectedEmbeddingDim
	queryText = strings.TrimSpace(queryText)
	hasText := queryText != ""
	if !hasEmbedding && !hasText {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	if minScore < 0 {
		minScore = 0.0
	}
	qLower := strings.ToLower(queryText)
	var scored []SessionSummary
	for _, s := range f.sessionSummaries {
		if s == nil {
			continue
		}
		var sim float64
		if hasEmbedding && len(s.Embedding) == ExpectedEmbeddingDim {
			sim = cosineSimilarity(embedding, s.Embedding)
		}
		var textScore float64
		if hasText {
			if strings.Contains(strings.ToLower(s.Summary), qLower) {
				textScore = 1.0
			} else {
				words := strings.Fields(qLower)
				matchedWords := 0
				for _, w := range words {
					if strings.Contains(strings.ToLower(s.Summary), w) {
						matchedWords++
					}
				}
				if len(words) > 0 && matchedWords > 0 {
					textScore = float64(matchedWords) / float64(len(words))
				}
			}
		}
		var score float64
		if hasEmbedding && hasText {
			score = 0.50*sim + 0.50*textScore
		} else if hasEmbedding {
			score = sim
		} else {
			score = textScore
		}
		if score >= minScore {
			cp := *cloneSessionSummary(s)
			cp.Score = score
			scored = append(scored, cp)
		}
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})
	if len(scored) > limit {
		scored = scored[:limit]
	}
	return scored, nil
}

func (f *FakeStore) SearchTranscriptSteps(ctx context.Context, queryText, sessionFilter, toolFilter string, limit int) ([]TranscriptStep, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("SearchTranscriptSteps"); err != nil {
		return nil, err
	}
	queryText, sessionFilter, toolFilter = strings.TrimSpace(queryText), strings.TrimSpace(sessionFilter), strings.TrimSpace(toolFilter)
	if queryText == "" && sessionFilter == "" && toolFilter == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	qLower := strings.ToLower(queryText)
	var matched []TranscriptStep
	for sid, steps := range f.transcriptSteps {
		if sessionFilter != "" && sid != sessionFilter {
			continue
		}
		for _, s := range steps {
			if toolFilter != "" && s.ToolName != toolFilter {
				continue
			}
			isMatch := queryText == ""
			if !isMatch {
				if strings.Contains(strings.ToLower(s.Content), qLower) {
					isMatch = true
				} else {
					words := strings.Fields(qLower)
					allMatch := len(words) > 0
					for _, w := range words {
						if !strings.Contains(strings.ToLower(s.Content), w) {
							allMatch = false
							break
						}
					}
					isMatch = allMatch
				}
			}
			if isMatch {
				cp := s
				cp.RankScore = 1.0
				matched = append(matched, cp)
			}
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})
	if len(matched) > limit {
		matched = matched[:limit]
	}
	return matched, nil
}

func prKey(repo string, prNumber int) string {
	return fmt.Sprintf("%s/%d", normalizeRepo(repo), prNumber)
}

func (f *FakeStore) UpsertPR(ctx context.Context, record PRRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpsertPR"); err != nil {
		return err
	}
	norm, err := validateAndNormalizeUpsertPR(record)
	if err != nil {
		return err
	}
	key := prKey(norm.Repo, norm.PRNumber)
	now := time.Now()
	existing, exists := f.prRecords[key]
	if exists {
		existing.Branch = norm.Branch
		existing.HeadSHA = norm.HeadSHA
		existing.TargetID = norm.TargetID
		if norm.Title != "" {
			existing.Title = norm.Title
		}
		if norm.Status != "" && existing.Status != "merged" && existing.Status != "deploying" && existing.Status != "deployed" {
			existing.Status = norm.Status
		}
		if norm.StatusMessageID != "" {
			existing.StatusMessageID = norm.StatusMessageID
		}
		if norm.Metadata != "" && norm.Metadata != "{}" {
			existing.Metadata = norm.Metadata
		}
		existing.UpdatedAt = now
		return nil
	}
	f.nextPRID++
	f.prRecords[key] = &PRRecord{
		ID:              f.nextPRID,
		Repo:            norm.Repo,
		PRNumber:        norm.PRNumber,
		Branch:          norm.Branch,
		HeadSHA:         norm.HeadSHA,
		MergeSHA:        norm.MergeSHA,
		TargetID:        norm.TargetID,
		Status:          norm.Status,
		Title:           norm.Title,
		StatusMessageID: norm.StatusMessageID,
		Metadata:        norm.Metadata,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	return nil
}

func (f *FakeStore) GetPRByNumber(ctx context.Context, repo string, prNumber int) (*PRRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetPRByNumber"); err != nil {
		return nil, err
	}
	if repo = normalizeRepo(repo); repo == "" {
		return nil, fmt.Errorf("repo cannot be empty")
	}
	rec, exists := f.prRecords[prKey(repo, prNumber)]
	if !exists {
		return nil, ErrPRNotFound
	}
	cp := *rec
	return &cp, nil
}

func (f *FakeStore) UpdatePRStatusMessageID(ctx context.Context, repo string, prNumber int, messageID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpdatePRStatusMessageID"); err != nil {
		return err
	}
	if repo = normalizeRepo(repo); repo == "" {
		return fmt.Errorf("repo cannot be empty")
	}
	rec, exists := f.prRecords[prKey(repo, prNumber)]
	if !exists {
		return ErrPRNotFound
	}
	rec.StatusMessageID, rec.UpdatedAt = strings.TrimSpace(messageID), time.Now()
	return nil
}

func (f *FakeStore) CreateVanadisBuild(ctx context.Context, build VanadisBuild, chunks []VanadisChunk) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("CreateVanadisBuild"); err != nil {
		return err
	}
	normBuild, normChunks, err := validateAndNormalizeVanadisBuild(build, chunks)
	if err != nil {
		return err
	}
	if _, exists := f.vanadisBuilds[normBuild.ID]; exists {
		return fmt.Errorf("vanadis build %s already exists", normBuild.ID)
	}
	seenChunkIDs := make(map[string]bool, len(normChunks))
	seenIndices := make(map[int]bool, len(normChunks))
	for _, c := range normChunks {
		if _, exists := f.vanadisChunks[c.ID]; exists || seenChunkIDs[c.ID] {
			return fmt.Errorf("vanadis chunk %s already exists", c.ID)
		}
		if seenIndices[c.ChunkIndex] {
			return fmt.Errorf("duplicate chunk_index %d in build %s", c.ChunkIndex, normBuild.ID)
		}
		seenChunkIDs[c.ID] = true
		seenIndices[c.ChunkIndex] = true
	}
	now := time.Now()
	f.vanadisBuilds[normBuild.ID] = &VanadisBuild{
		ID:              normBuild.ID,
		ThreadID:        normBuild.ThreadID,
		Tier:            normBuild.Tier,
		Title:           normBuild.Title,
		Objective:       normBuild.Objective,
		SharedContext:   normBuild.SharedContext,
		Status:          normBuild.Status,
		StatusMessageID: normBuild.StatusMessageID,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	for _, c := range normChunks {
		startedAt := c.StartedAt
		if startedAt.IsZero() && (c.Status == "running" || c.Status == "coding") {
			startedAt = now
		}
		f.vanadisChunks[c.ID] = &VanadisChunk{
			ID:                 c.ID,
			BuildID:            c.BuildID,
			ChunkIndex:         c.ChunkIndex,
			Repo:               c.Repo,
			Title:              c.Title,
			Instructions:       c.Instructions,
			TargetFiles:        cloneSlice(c.TargetFiles),
			TargetSymbols:      cloneSlice(c.TargetSymbols),
			Effort:             c.Effort,
			AcceptanceCriteria: cloneSlice(c.AcceptanceCriteria),
			Status:             c.Status,
			PRNumber:           c.PRNumber,
			StartedAt:          startedAt,
			CreatedAt:          now,
			UpdatedAt:          now,
		}
	}
	return nil
}

func (f *FakeStore) GetVanadisBuild(ctx context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetVanadisBuild"); err != nil {
		return nil, nil, err
	}
	if buildID = strings.TrimSpace(buildID); buildID == "" {
		return nil, nil, fmt.Errorf("build id cannot be empty")
	}
	b, exists := f.vanadisBuilds[buildID]
	if !exists {
		return nil, nil, ErrVanadisBuildNotFound
	}
	var chunks []VanadisChunk
	for _, c := range f.vanadisChunks {
		if c.BuildID == buildID {
			chunks = append(chunks, cloneVanadisChunk(c))
		}
	}
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].ChunkIndex < chunks[j].ChunkIndex
	})
	cp := *b
	return &cp, chunks, nil
}

func (f *FakeStore) GetVanadisChunk(ctx context.Context, chunkID string) (*VanadisChunk, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("GetVanadisChunk"); err != nil {
		return nil, err
	}
	if chunkID = strings.TrimSpace(chunkID); chunkID == "" {
		return nil, fmt.Errorf("chunk id cannot be empty")
	}
	c, exists := f.vanadisChunks[chunkID]
	if !exists {
		return nil, ErrVanadisChunkNotFound
	}
	cp := cloneVanadisChunk(c)
	return &cp, nil
}

func (f *FakeStore) AttachVanadisChunkPR(ctx context.Context, chunkID string, prNumber int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("AttachVanadisChunkPR"); err != nil {
		return err
	}
	if chunkID = strings.TrimSpace(chunkID); chunkID == "" {
		return fmt.Errorf("chunk id cannot be empty")
	}
	if prNumber <= 0 {
		return fmt.Errorf("pr_number must be greater than 0")
	}
	c, exists := f.vanadisChunks[chunkID]
	if !exists {
		return ErrVanadisChunkNotFound
	}
	c.PRNumber = prNumber
	c.Status = "pr_opened"
	if c.StartedAt.IsZero() {
		if !c.CreatedAt.IsZero() {
			c.StartedAt = c.CreatedAt
		} else {
			c.StartedAt = time.Now()
		}
	}
	c.UpdatedAt = time.Now()
	return nil
}

func (f *FakeStore) UpdateVanadisBuildStatusMessageID(ctx context.Context, buildID, messageID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpdateVanadisBuildStatusMessageID"); err != nil {
		return err
	}
	if buildID = strings.TrimSpace(buildID); buildID == "" {
		return fmt.Errorf("build id cannot be empty")
	}
	b, exists := f.vanadisBuilds[buildID]
	if !exists {
		return ErrVanadisBuildNotFound
	}
	b.StatusMessageID, b.UpdatedAt = strings.TrimSpace(messageID), time.Now()
	return nil
}
