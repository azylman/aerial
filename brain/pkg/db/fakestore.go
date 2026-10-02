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
)

var (
	_ Store           = (*FakeStore)(nil)
	_ MessageStore    = (*FakeStore)(nil)
	_ ScheduleStore   = (*FakeStore)(nil)
	_ SessionStore    = (*FakeStore)(nil)
	_ FactStore       = (*FakeStore)(nil)
	_ TranscriptStore = (*FakeStore)(nil)
	_ PRRegistryStore = (*FakeStore)(nil)
)

type threadSummaryRecord struct {
	summary   string
	watermark string
}

// FakeStore is a thread-safe, pure Go in-memory implementation of db.Store.
// It stores state in memory without any external SQL driver, filesystem, or CGo dependencies.
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
	facts        map[int64]*FactWithEmbedding
	nextFactID   int64
	watermarks   map[string]int64
	extractedAt  map[string]time.Time

	// TranscriptStore state
	sessionSummaries map[string]*SessionSummary
	transcriptSteps  map[string][]TranscriptStep

	// PRRegistryStore state
	prRecords map[string]*PRRecord
	nextPRID  int64
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

// Deep clone helpers to prevent pointer leaks across -race test goroutines
func cloneMessage(m *Message) *Message {
	if m == nil {
		return nil
	}
	cp := *m
	if m.Metadata.Mentions != nil {
		cp.Metadata.Mentions = append([]string(nil), m.Metadata.Mentions...)
	}
	if m.Metadata.MentionUserIDs != nil {
		cp.Metadata.MentionUserIDs = append([]string(nil), m.Metadata.MentionUserIDs...)
	}
	if m.Metadata.MentionRoleIDs != nil {
		cp.Metadata.MentionRoleIDs = append([]string(nil), m.Metadata.MentionRoleIDs...)
	}
	if m.Metadata.Attachments != nil {
		cp.Metadata.Attachments = append([]string(nil), m.Metadata.Attachments...)
	}
	return &cp
}

func cloneSessionInfo(s *SessionInfo) *SessionInfo {
	if s == nil {
		return nil
	}
	cp := *s
	return &cp
}

func cloneOneShot(s *OneShotSchedule) *OneShotSchedule {
	if s == nil {
		return nil
	}
	cp := *s
	return &cp
}

func cloneCron(c *CronSchedule) *CronSchedule {
	if c == nil {
		return nil
	}
	cp := *c
	return &cp
}

func cloneRun(r *ScheduleRun) *ScheduleRun {
	if r == nil {
		return nil
	}
	cp := *r
	if r.CompletedAt != nil {
		t := *r.CompletedAt
		cp.CompletedAt = &t
	}
	return &cp
}

func cloneFactWithEmbedding(fe *FactWithEmbedding) *FactWithEmbedding {
	if fe == nil {
		return nil
	}
	cp := *fe
	if fe.Embedding != nil {
		cp.Embedding = make([]float32, len(fe.Embedding))
		copy(cp.Embedding, fe.Embedding)
	}
	return &cp
}

func cloneSessionSummary(s *SessionSummary) *SessionSummary {
	if s == nil {
		return nil
	}
	cp := *s
	if s.Embedding != nil {
		cp.Embedding = append([]float32(nil), s.Embedding...)
	}
	return &cp
}

func cloneTranscriptSteps(steps []TranscriptStep) []TranscriptStep {
	if steps == nil {
		return nil
	}
	cp := make([]TranscriptStep, len(steps))
	copy(cp, steps)
	return cp
}

// =========================================================================
// MessageStore implementation
// =========================================================================

func (f *FakeStore) InsertMessage(ctx context.Context, msg Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("InsertMessage"); err != nil {
		return err
	}

	if _, exists := f.messages[msg.ID]; exists {
		// Idempotent insert: do not overwrite or reassign row_id
		return nil
	}

	rowID := atomic.AddInt64(&f.nextRowID, 1)
	msg.RowID = rowID
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
	return nil
}

func (f *FakeStore) UpdateMessageStatus(ctx context.Context, id, status, errorMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpdateMessageStatus"); err != nil {
		return err
	}

	m, ok := f.messages[id]
	if !ok {
		return nil
	}
	m.Status = status
	m.ErrorMessage = errorMsg
	m.UpdatedAt = time.Now().UTC()
	return nil
}

func (f *FakeStore) UpdateMessageCompleted(ctx context.Context, id, responseText string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpdateMessageCompleted"); err != nil {
		return err
	}

	m, ok := f.messages[id]
	if !ok {
		return nil
	}
	m.Status = StatusCompleted
	m.ResponseText = responseText
	m.UpdatedAt = time.Now().UTC()
	return nil
}

func (f *FakeStore) IncrementMessageRetry(ctx context.Context, id, errorMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("IncrementMessageRetry"); err != nil {
		return err
	}

	m, ok := f.messages[id]
	if !ok {
		return nil
	}
	m.RetryCount++
	m.ErrorMessage = errorMsg
	m.UpdatedAt = time.Now().UTC()
	return nil
}

func (f *FakeStore) IncrementMessageRestart(ctx context.Context, id, errorMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("IncrementMessageRestart"); err != nil {
		return err
	}

	m, ok := f.messages[id]
	if !ok {
		return nil
	}
	m.RestartCount++
	m.ErrorMessage = errorMsg
	m.UpdatedAt = time.Now().UTC()
	return nil
}

func (f *FakeStore) ResetMessageToPendingWithRestart(ctx context.Context, id, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("ResetMessageToPendingWithRestart"); err != nil {
		return err
	}

	m, ok := f.messages[id]
	if !ok {
		return nil
	}
	m.Status = StatusPending
	m.RestartCount++
	m.ErrorMessage = reason
	m.UpdatedAt = time.Now().UTC()
	return nil
}

func (f *FakeStore) GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]Message, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetPendingOrProcessingMessages"); err != nil {
		return nil, err
	}

	var results []Message
	for _, id := range f.messageOrder {
		m := f.messages[id]
		if m != nil && (m.Status == StatusPending || m.Status == StatusProcessing) {
			results = append(results, *cloneMessage(m))
			if limit > 0 && len(results) >= limit {
				break
			}
		}
	}
	return results, nil
}

func (f *FakeStore) GetMessage(ctx context.Context, id string) (*Message, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetMessage"); err != nil {
		return nil, err
	}

	m, ok := f.messages[id]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return cloneMessage(m), nil
}

func (f *FakeStore) MessageExists(ctx context.Context, id string) (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
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
	m.Status = StatusProcessing
	m.UpdatedAt = time.Now().UTC()
	return true, nil
}

func (f *FakeStore) ClaimNextPendingMessage(ctx context.Context, workerID string) (*Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("ClaimNextPendingMessage"); err != nil {
		return nil, err
	}

	// Deterministic FIFO scan
	for _, id := range f.messageOrder {
		m := f.messages[id]
		if m != nil && m.Status == StatusPending {
			m.Status = StatusProcessing
			m.UpdatedAt = time.Now().UTC()
			return cloneMessage(m), nil
		}
	}
	return nil, nil
}

func (f *FakeStore) GetActiveRecentThreadIDs(ctx context.Context, since time.Duration) ([]string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetActiveRecentThreadIDs"); err != nil {
		return nil, err
	}

	cutoff := time.Now().UTC().Add(-since)
	seen := make(map[string]bool)
	var threadIDs []string
	for i := len(f.messageOrder) - 1; i >= 0; i-- {
		id := f.messageOrder[i]
		m := f.messages[id]
		if m != nil && m.ThreadID != "" && m.UpdatedAt.After(cutoff) {
			if !seen[m.ThreadID] {
				seen[m.ThreadID] = true
				threadIDs = append(threadIDs, m.ThreadID)
			}
		}
	}
	return threadIDs, nil
}

func (f *FakeStore) GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]Message, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetRecentThreadMessages"); err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 10
	}
	var matches []Message
	for i := len(f.messageOrder) - 1; i >= 0; i-- {
		id := f.messageOrder[i]
		m := f.messages[id]
		if m != nil && m.ThreadID == threadID {
			matches = append(matches, *cloneMessage(m))
			if len(matches) >= limit {
				break
			}
		}
	}
	// Return ascending order (oldest to newest)
	for i, j := 0, len(matches)-1; i < j; i, j = i+1, j-1 {
		matches[i], matches[j] = matches[j], matches[i]
	}
	return matches, nil
}

func (f *FakeStore) GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetMaxMessageRowID"); err != nil {
		return 0, err
	}

	var maxID int64
	for _, id := range f.messageOrder {
		m := f.messages[id]
		if m != nil && m.ThreadID == threadID && m.Status == StatusCompleted {
			if m.RowID > maxID {
				maxID = m.RowID
			}
		}
	}
	return maxID, nil
}

// SetMessageUpdatedAt updates the UpdatedAt timestamp of an existing message (useful in tests).
func (f *FakeStore) SetMessageUpdatedAt(id string, t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m, ok := f.messages[id]; ok && m != nil {
		m.UpdatedAt = t
	}
}

func (f *FakeStore) GetActiveTasks(ctx context.Context) ([]ActiveTask, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetActiveTasks"); err != nil {
		return nil, err
	}

	var activeMsgs []*Message
	for _, id := range f.messageOrder {
		if m, ok := f.messages[id]; ok && m != nil {
			if m.Status == StatusPending || m.Status == StatusProcessing {
				activeMsgs = append(activeMsgs, m)
			}
		}
	}

	sort.Slice(activeMsgs, func(i, j int) bool {
		return activeMsgs[i].CreatedAt.Before(activeMsgs[j].CreatedAt)
	})

	if len(activeMsgs) > 50 {
		activeMsgs = activeMsgs[:50]
	}

	tasks := make([]ActiveTask, 0, len(activeMsgs))
	for _, m := range activeMsgs {
		sessionID := ""
		if s, ok := f.sessions[m.ThreadID]; ok && s != nil {
			sessionID = s.InternalSessionID
		}
		summary := m.Summary
		if summary == "" {
			summary = CleanTaskSummary(m.Content)
		}
		tasks = append(tasks, ActiveTask{
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
		})
	}
	return tasks, nil
}

// =========================================================================
// SessionStore implementation
// =========================================================================

func (f *FakeStore) GetSessionID(ctx context.Context, threadID string) (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetSessionID"); err != nil {
		return "", err
	}

	s, ok := f.sessions[threadID]
	if !ok || s == nil {
		return "", nil
	}
	return s.InternalSessionID, nil
}

func (f *FakeStore) GetPreviousSessionID(ctx context.Context, threadID string) (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetPreviousSessionID"); err != nil {
		return "", err
	}

	s, ok := f.sessions[threadID]
	if !ok || s == nil {
		return "", nil
	}
	return s.PreviousSessionID, nil
}

func (f *FakeStore) SaveSessionID(ctx context.Context, threadID, sessionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("SaveSessionID"); err != nil {
		return err
	}

	now := time.Now().UTC()
	s, ok := f.sessions[threadID]
	if !ok || s == nil {
		f.sessions[threadID] = &SessionInfo{
			ThreadID:          threadID,
			InternalSessionID: sessionID,
			CreatedAt:         now,
			UpdatedAt:         now,
		}
		return nil
	}
	s.InternalSessionID = sessionID
	s.UpdatedAt = now
	return nil
}

func (f *FakeStore) DeleteSessionID(ctx context.Context, threadID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("DeleteSessionID"); err != nil {
		return err
	}

	delete(f.sessions, threadID)
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
		s = &SessionInfo{
			ThreadID:  sessionKey,
			TurnCount: 1,
			CreatedAt: now,
			UpdatedAt: now,
		}
		f.sessions[sessionKey] = s
		return 1, nil
	}
	s.TurnCount++
	s.UpdatedAt = now
	return s.TurnCount, nil
}

func (f *FakeStore) GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetSessionTurnCount"); err != nil {
		return 0, err
	}

	s, ok := f.sessions[sessionKey]
	if !ok || s == nil {
		return 0, nil
	}
	return s.TurnCount, nil
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
		f.sessions[sessionKey] = &SessionInfo{
			ThreadID:          sessionKey,
			InternalSessionID: newSessionID,
			TurnCount:         0,
			CreatedAt:         now,
			UpdatedAt:         now,
		}
		return nil
	}
	s.PreviousSessionID = s.InternalSessionID
	s.InternalSessionID = newSessionID
	s.TurnCount = 0
	s.UpdatedAt = now
	return nil
}

func (f *FakeStore) GetSessionInfo(ctx context.Context, threadID string) (*SessionInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetSessionInfo"); err != nil {
		return nil, err
	}

	s, ok := f.sessions[threadID]
	if !ok || s == nil {
		return nil, nil
	}
	return cloneSessionInfo(s), nil
}

func (f *FakeStore) GetThreadSummary(ctx context.Context, threadID string) (string, string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
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

	f.threadSummaries[threadID] = threadSummaryRecord{
		summary:   summary,
		watermark: lastSummarizedMsgID,
	}
	return nil
}

func (f *FakeStore) GetSessionActivityStats(ctx context.Context, threadID string) (*SessionActivityStats, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetSessionActivityStats"); err != nil {
		return nil, err
	}

	stats := &SessionActivityStats{}
	if s, ok := f.sessions[threadID]; ok && s != nil {
		stats.InternalSessionID = s.InternalSessionID
		stats.TurnCount = s.TurnCount
		stats.SessionUpdatedAt = s.UpdatedAt
	}

	var completedCount int64
	var maxUpdated time.Time
	var maxCreated time.Time
	for _, id := range f.messageOrder {
		m := f.messages[id]
		if m != nil && m.ThreadID == threadID && m.Status == StatusCompleted {
			if !strings.HasPrefix(m.ResponseText, "[EXPIRED_STALE]") &&
				!strings.HasPrefix(m.ResponseText, "[AMBIENT") &&
				!strings.HasPrefix(m.ResponseText, "[IGNORED") {
				completedCount++
				if m.UpdatedAt.After(maxUpdated) {
					maxUpdated = m.UpdatedAt
				}
				if m.CreatedAt.After(maxCreated) {
					maxCreated = m.CreatedAt
				}
			}
		}
	}
	stats.CompletedTurns = completedCount
	stats.LastMessageAt = maxUpdated
	stats.LastCompletedMessageCreatedAt = maxCreated
	return stats, nil
}

// SetSessionInfo sets or overwrites session info for a thread (useful in tests).
func (f *FakeStore) SetSessionInfo(s SessionInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions[s.ThreadID] = cloneSessionInfo(&s)
}

func (f *FakeStore) GetExternalConversationID(ctx context.Context, internalID string) (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetExternalConversationID"); err != nil {
		return "", err
	}
	if internalID == "" {
		return "", nil
	}
	for threadID, sess := range f.sessions {
		if sess != nil && sess.InternalSessionID == internalID {
			return threadID, nil
		}
	}
	return "", nil
}

func (f *FakeStore) SaveConversationMapping(ctx context.Context, externalID, internalID string) error {
	return f.SaveSessionID(ctx, externalID, internalID)
}

// =========================================================================
// ScheduleStore implementation
// =========================================================================

func (f *FakeStore) CreateOneShotSchedule(ctx context.Context, s OneShotSchedule) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("CreateOneShotSchedule"); err != nil {
		return err
	}

	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	f.oneShots[s.ID] = cloneOneShot(&s)
	return nil
}

func (f *FakeStore) GetDueOneShotSchedules(ctx context.Context) ([]OneShotSchedule, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetDueOneShotSchedules"); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	var due []OneShotSchedule
	for _, s := range f.oneShots {
		if s != nil && !s.RunAt.After(now) {
			due = append(due, *cloneOneShot(s))
		}
	}
	sort.Slice(due, func(i, j int) bool {
		return due[i].RunAt.Before(due[j].RunAt)
	})
	return due, nil
}

func (f *FakeStore) DeleteOneShotSchedule(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("DeleteOneShotSchedule"); err != nil {
		return err
	}

	delete(f.oneShots, id)
	return nil
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

	rowID := atomic.AddInt64(&f.nextRowID, 1)
	msg.RowID = rowID
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now().UTC()
	}
	if msg.UpdatedAt.IsZero() {
		msg.UpdatedAt = msg.CreatedAt
	}
	f.messages[msg.ID] = cloneMessage(&msg)
	f.messageOrder = append(f.messageOrder, msg.ID)
	return nil
}

func (f *FakeStore) GetAllOneShotSchedules(ctx context.Context, threadID string) ([]OneShotSchedule, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetAllOneShotSchedules"); err != nil {
		return nil, err
	}

	var results []OneShotSchedule
	for _, s := range f.oneShots {
		if s != nil && (threadID == "" || s.ThreadID == threadID) {
			results = append(results, *cloneOneShot(s))
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
	f.crons[c.ID] = cloneCron(&c)
	return nil
}

func (f *FakeStore) GetDueCronSchedules(ctx context.Context) ([]CronSchedule, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetDueCronSchedules"); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	var due []CronSchedule
	for _, c := range f.crons {
		if c != nil && c.Enabled && !c.NextRunAt.After(now) {
			due = append(due, *cloneCron(c))
		}
	}
	sort.Slice(due, func(i, j int) bool {
		return due[i].NextRunAt.Before(due[j].NextRunAt)
	})
	return due, nil
}

func (f *FakeStore) GetAllCronSchedules(ctx context.Context, targetID string) ([]CronSchedule, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetAllCronSchedules"); err != nil {
		return nil, err
	}

	var results []CronSchedule
	for _, c := range f.crons {
		if c != nil && (targetID == "" || c.TargetID == targetID) {
			results = append(results, *cloneCron(c))
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].NextRunAt.Before(results[j].NextRunAt)
	})
	return results, nil
}

func (f *FakeStore) DeleteCronSchedule(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("DeleteCronSchedule"); err != nil {
		return err
	}

	delete(f.crons, id)
	return nil
}

func (f *FakeStore) UpdateCronNextRun(ctx context.Context, id string, nextRunAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpdateCronNextRun"); err != nil {
		return err
	}

	c, ok := f.crons[id]
	if !ok {
		return nil
	}
	c.NextRunAt = nextRunAt
	return nil
}

func (f *FakeStore) UpdateCronScheduleEffort(ctx context.Context, id, effort string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("UpdateCronScheduleEffort"); err != nil {
		return err
	}

	c, ok := f.crons[id]
	if !ok {
		return nil
	}
	c.Effort = effort
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
	f.mu.RLock()
	defer f.mu.RUnlock()
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

// GetScheduleRun retrieves a schedule run by its ID (useful in tests).
func (f *FakeStore) GetScheduleRun(id string) (*ScheduleRun, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetScheduleRun"); err != nil {
		return nil, err
	}
	r, ok := f.runs[id]
	if !ok || r == nil {
		return nil, sql.ErrNoRows
	}
	return cloneRun(r), nil
}

func (f *FakeStore) GetScheduleSummaryMetrics(ctx context.Context) (ScheduleSummaryMetrics, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
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
		if r == nil {
			continue
		}
		if !r.StartedAt.Before(cutoff24h) {
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
		if c != nil && c.Enabled {
			if earliest == nil || c.NextRunAt.Before(*earliest) {
				t := c.NextRunAt
				earliest = &t
			}
		}
	}
	for _, s := range f.oneShots {
		if s != nil {
			if earliest == nil || s.RunAt.Before(*earliest) {
				t := s.RunAt
				earliest = &t
			}
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
			r.Status = "failed"
			r.Error = "Interrupted by server restart"
			r.CompletedAt = &now
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

// =========================================================================
// FactStore implementation
// =========================================================================

func (f *FakeStore) InsertFact(ctx context.Context, category, factText string, importance float64, embedding []float32) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("InsertFact"); err != nil {
		return 0, err
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
	f.mu.RLock()
	defer f.mu.RUnlock()
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
	hasVec := len(embedding) > 0
	hasText := qLower != ""

	if !hasVec && !hasText {
		return nil, nil
	}

	for _, fe := range f.facts {
		if fe == nil {
			continue
		}
		var sim float64
		if hasVec && len(embedding) == len(fe.Embedding) {
			sim = cosineSimilarity(embedding, fe.Embedding)
		}
		var textScore float64
		if hasText {
			if strings.Contains(strings.ToLower(fe.Fact.FactText), qLower) {
				textScore = 1.0
			}
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
		} else if hasText {
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
	f.mu.RLock()
	defer f.mu.RUnlock()
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
	offset := filter.Offset
	if offset > total {
		return &FactsResult{Facts: nil, Total: total, Limit: filter.Limit, Offset: filter.Offset}, nil
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return &FactsResult{Facts: list[offset:end], Total: total, Limit: filter.Limit, Offset: filter.Offset}, nil
}

func (f *FakeStore) GetAllFactsWithEmbeddings(ctx context.Context) ([]FactWithEmbedding, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetAllFactsWithEmbeddings"); err != nil {
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

func (f *FakeStore) GetFactsByThreadWithEmbeddings(ctx context.Context, threadID string) ([]FactWithEmbedding, error) {
	return f.GetAllFactsWithEmbeddings(ctx)
}

func (f *FakeStore) GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
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
		if m != nil && m.ThreadID != "" && (m.CreatedAt.After(cutoff) || m.CreatedAt.Equal(cutoff)) {
			watermark := f.watermarks[m.ThreadID]
			if watermark == 0 || m.RowID > watermark {
				if !seen[m.ThreadID] {
					seen[m.ThreadID] = true
					threads = append(threads, m.ThreadID)
					if len(threads) >= 20 {
						break
					}
				}
			}
		}
	}
	sort.Strings(threads)
	return threads, nil
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
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("FindDuplicateFact"); err != nil {
		return nil, 0, err
	}

	var bestFact *Fact
	var bestSim float64
	for _, fe := range f.facts {
		if fe == nil || len(fe.Embedding) == 0 {
			continue
		}
		sim := cosineSimilarity(embedding, fe.Embedding)
		if sim >= minSim && sim > bestSim {
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
	if newText != "" {
		fe.Fact.FactText = newText
	}
	if len(newEmbedding) > 0 {
		fe.Embedding = make([]float32, len(newEmbedding))
		copy(fe.Embedding, newEmbedding)
	}
	return nil
}

func (f *FakeStore) DecayAndPruneFacts(ctx context.Context, decayStep float64, pruneFloor float64, pruneAgeDays int) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkClosedAndFail("DecayAndPruneFacts"); err != nil {
		return 0, 0, err
	}

	if decayStep <= 0 {
		decayStep = 0.02
	}
	if pruneFloor <= 0 {
		pruneFloor = 0.10
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
	f.mu.RLock()
	defer f.mu.RUnlock()
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
	fe.Embedding = make([]float32, len(embedding))
	copy(fe.Embedding, embedding)
	return nil
}

// =========================================================================
// Transaction implementation (Copy-on-write staging delta with rollback)
// =========================================================================

func (f *FakeStore) WithTx(ctx context.Context, fn func(txStore Store) error) error {
	f.mu.Lock()
	if err := f.checkClosedAndFail("WithTx"); err != nil {
		f.mu.Unlock()
		return err
	}

	// Create an isolated snapshot copy of current state
	txCopy := &FakeStore{
		failNext:         make(map[string]error),
		messages:         make(map[string]*Message, len(f.messages)),
		messageOrder:     make([]string, len(f.messageOrder)),
		nextRowID:        f.nextRowID,
		sessions:         make(map[string]*SessionInfo, len(f.sessions)),
		threadSummaries:  make(map[string]threadSummaryRecord, len(f.threadSummaries)),
		oneShots:         make(map[string]*OneShotSchedule, len(f.oneShots)),
		crons:            make(map[string]*CronSchedule, len(f.crons)),
		runs:             make(map[string]*ScheduleRun, len(f.runs)),
		facts:            make(map[int64]*FactWithEmbedding, len(f.facts)),
		nextFactID:       f.nextFactID,
		watermarks:       make(map[string]int64, len(f.watermarks)),
		extractedAt:      make(map[string]time.Time, len(f.extractedAt)),
		sessionSummaries: make(map[string]*SessionSummary, len(f.sessionSummaries)),
		transcriptSteps:  make(map[string][]TranscriptStep, len(f.transcriptSteps)),
	}

	copy(txCopy.messageOrder, f.messageOrder)
	for k, v := range f.messages {
		txCopy.messages[k] = cloneMessage(v)
	}
	for k, v := range f.sessions {
		txCopy.sessions[k] = cloneSessionInfo(v)
	}
	for k, v := range f.threadSummaries {
		txCopy.threadSummaries[k] = v
	}
	for k, v := range f.oneShots {
		txCopy.oneShots[k] = cloneOneShot(v)
	}
	for k, v := range f.crons {
		txCopy.crons[k] = cloneCron(v)
	}
	for k, v := range f.runs {
		txCopy.runs[k] = cloneRun(v)
	}
	for k, v := range f.facts {
		txCopy.facts[k] = cloneFactWithEmbedding(v)
	}
	for k, v := range f.watermarks {
		txCopy.watermarks[k] = v
	}
	for k, v := range f.extractedAt {
		txCopy.extractedAt[k] = v
	}
	for k, v := range f.sessionSummaries {
		txCopy.sessionSummaries[k] = cloneSessionSummary(v)
	}
	for k, v := range f.transcriptSteps {
		txCopy.transcriptSteps[k] = cloneTranscriptSteps(v)
	}
	f.mu.Unlock()

	// Execute transaction function on isolated copy (no outer locks held = zero deadlocks)
	if err := fn(txCopy); err != nil {
		// Rollback: simply discard txCopy!
		return err
	}

	// Commit: merge mutated snapshot back into root store under lock
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return sql.ErrConnDone
	}

	f.messages = txCopy.messages
	f.messageOrder = txCopy.messageOrder
	f.nextRowID = txCopy.nextRowID
	f.sessions = txCopy.sessions
	f.threadSummaries = txCopy.threadSummaries
	f.oneShots = txCopy.oneShots
	f.crons = txCopy.crons
	f.runs = txCopy.runs
	f.facts = txCopy.facts
	f.nextFactID = txCopy.nextFactID
	f.watermarks = txCopy.watermarks
	f.extractedAt = txCopy.extractedAt
	f.sessionSummaries = txCopy.sessionSummaries
	f.transcriptSteps = txCopy.transcriptSteps
	return nil
}

// =========================================================================
// TranscriptStore implementation
// =========================================================================

func (f *FakeStore) GetSessionSyncStates(ctx context.Context) (map[string]SessionSyncState, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("GetSessionSyncStates"); err != nil {
		return nil, err
	}

	res := make(map[string]SessionSyncState, len(f.sessionSummaries))
	for id, s := range f.sessionSummaries {
		if s == nil {
			continue
		}
		res[id] = SessionSyncState{
			SessionID:       id,
			LastMtime:       s.LastMtime,
			LastIndexedStep: s.LastIndexedStep,
			IsSettled:       s.IsSettled,
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
			existing.Embedding = append([]float32(nil), summary.Embedding...)
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
	f.mu.RLock()
	defer f.mu.RUnlock()
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
		} else if hasText {
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
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.checkClosedAndFail("SearchTranscriptSteps"); err != nil {
		return nil, err
	}

	queryText = strings.TrimSpace(queryText)
	sessionFilter = strings.TrimSpace(sessionFilter)
	toolFilter = strings.TrimSpace(toolFilter)

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

// DeleteSessionSummary removes a session summary and cascades deletion of its transcript steps.
func (f *FakeStore) DeleteSessionSummary(sessionID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.sessionSummaries, sessionID)
	delete(f.transcriptSteps, sessionID)
}

func prKey(repo string, prNumber int) string {
	return fmt.Sprintf("%s/%d", normalizeRepo(repo), prNumber)
}

// UpsertPR inserts or updates a Pull Request tracking record in FakeStore.
func (f *FakeStore) UpsertPR(ctx context.Context, record PRRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return sql.ErrConnDone
	}
	if err, ok := f.failNext["UpsertPR"]; ok {
		delete(f.failNext, "UpsertPR")
		return err
	}

	repo := normalizeRepo(record.Repo)
	if repo == "" {
		return fmt.Errorf("repo cannot be empty")
	}
	if record.PRNumber <= 0 {
		return fmt.Errorf("pr_number must be greater than 0")
	}
	branch := strings.TrimSpace(record.Branch)
	if branch == "" {
		return fmt.Errorf("branch cannot be empty")
	}
	headSHA := strings.TrimSpace(record.HeadSHA)
	if headSHA == "" {
		return fmt.Errorf("head_sha cannot be empty")
	}
	targetID := strings.TrimSpace(record.TargetID)
	if targetID == "" {
		return fmt.Errorf("target_id cannot be empty")
	}

	key := prKey(repo, record.PRNumber)
	now := time.Now()

	existing, exists := f.prRecords[key]
	if exists {
		existing.Branch = branch
		existing.HeadSHA = headSHA
		existing.TargetID = targetID
		if record.Title != "" {
			existing.Title = record.Title
		}
		if record.Status != "" {
			existing.Status = record.Status
		}
		if record.Metadata != "" && record.Metadata != "{}" {
			existing.Metadata = record.Metadata
		}
		existing.UpdatedAt = now
		return nil
	}

	f.nextPRID++
	status := strings.TrimSpace(record.Status)
	if status == "" {
		status = "open"
	}
	meta := strings.TrimSpace(record.Metadata)
	if meta == "" {
		meta = "{}"
	}

	rec := &PRRecord{
		ID:        f.nextPRID,
		Repo:      repo,
		PRNumber:  record.PRNumber,
		Branch:    branch,
		HeadSHA:   headSHA,
		MergeSHA:  record.MergeSHA,
		TargetID:  targetID,
		Status:    status,
		Title:     record.Title,
		Metadata:  meta,
		CreatedAt: now,
		UpdatedAt: now,
	}
	f.prRecords[key] = rec
	return nil
}

// GetPRByNumber retrieves a PR record by repository name and PR number.
func (f *FakeStore) GetPRByNumber(ctx context.Context, repo string, prNumber int) (*PRRecord, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return nil, sql.ErrConnDone
	}
	if err, ok := f.failNext["GetPRByNumber"]; ok {
		delete(f.failNext, "GetPRByNumber")
		return nil, err
	}

	key := prKey(repo, prNumber)
	rec, exists := f.prRecords[key]
	if !exists {
		return nil, ErrPRNotFound
	}
	cp := *rec
	return &cp, nil
}

// GetPRByHeadSHA retrieves the most recent PR record matching repo and head SHA.
func (f *FakeStore) GetPRByHeadSHA(ctx context.Context, repo string, headSHA string) (*PRRecord, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return nil, sql.ErrConnDone
	}
	if err, ok := f.failNext["GetPRByHeadSHA"]; ok {
		delete(f.failNext, "GetPRByHeadSHA")
		return nil, err
	}

	normRepo := normalizeRepo(repo)
	var latest *PRRecord
	for _, rec := range f.prRecords {
		if rec.Repo == normRepo && rec.HeadSHA == headSHA {
			if latest == nil || rec.UpdatedAt.After(latest.UpdatedAt) {
				latest = rec
			}
		}
	}
	if latest == nil {
		return nil, ErrPRNotFound
	}
	cp := *latest
	return &cp, nil
}

// GetPRByMergeSHA retrieves the PR record matching repo and squash merge commit SHA.
func (f *FakeStore) GetPRByMergeSHA(ctx context.Context, repo string, mergeSHA string) (*PRRecord, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return nil, sql.ErrConnDone
	}
	if err, ok := f.failNext["GetPRByMergeSHA"]; ok {
		delete(f.failNext, "GetPRByMergeSHA")
		return nil, err
	}

	normRepo := normalizeRepo(repo)
	var latest *PRRecord
	for _, rec := range f.prRecords {
		if rec.Repo == normRepo && rec.MergeSHA == mergeSHA {
			if latest == nil || rec.UpdatedAt.After(latest.UpdatedAt) {
				latest = rec
			}
		}
	}
	if latest == nil {
		return nil, ErrPRNotFound
	}
	cp := *latest
	return &cp, nil
}

// UpdatePRMergeSHA records the squash merge commit SHA on main and marks the PR merged.
func (f *FakeStore) UpdatePRMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return sql.ErrConnDone
	}
	if err, ok := f.failNext["UpdatePRMergeSHA"]; ok {
		delete(f.failNext, "UpdatePRMergeSHA")
		return err
	}

	key := prKey(repo, prNumber)
	rec, exists := f.prRecords[key]
	if !exists {
		return ErrPRNotFound
	}
	rec.MergeSHA = mergeSHA
	rec.Status = "merged"
	rec.UpdatedAt = time.Now()
	return nil
}

// UpdatePRStatus updates the lifecycle status of a PR.
func (f *FakeStore) UpdatePRStatus(ctx context.Context, repo string, prNumber int, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return sql.ErrConnDone
	}
	if err, ok := f.failNext["UpdatePRStatus"]; ok {
		delete(f.failNext, "UpdatePRStatus")
		return err
	}

	key := prKey(repo, prNumber)
	rec, exists := f.prRecords[key]
	if !exists {
		return ErrPRNotFound
	}
	rec.Status = status
	rec.UpdatedAt = time.Now()
	return nil
}

// AtomicTransitionPRStatus conditionally transitions PR status if it is not already in notStatus (CAS gate).
func (f *FakeStore) AtomicTransitionPRStatus(ctx context.Context, repo string, prNumber int, toStatus, notStatus string) (*PRRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil, sql.ErrConnDone
	}
	if err, ok := f.failNext["AtomicTransitionPRStatus"]; ok {
		delete(f.failNext, "AtomicTransitionPRStatus")
		return nil, err
	}

	key := prKey(repo, prNumber)
	rec, exists := f.prRecords[key]
	if !exists {
		return nil, nil
	}
	if rec.Status == notStatus {
		return nil, nil // No transition
	}
	rec.Status = toStatus
	rec.UpdatedAt = time.Now()
	cp := *rec
	return &cp, nil
}

// AtomicTransitionPRStatusByMergeSHA conditionally transitions PR status by merge SHA if not already in notStatus (CAS gate).
func (f *FakeStore) AtomicTransitionPRStatusByMergeSHA(ctx context.Context, repo string, mergeSHA string, toStatus, notStatus string) (*PRRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil, sql.ErrConnDone
	}
	if err, ok := f.failNext["AtomicTransitionPRStatusByMergeSHA"]; ok {
		delete(f.failNext, "AtomicTransitionPRStatusByMergeSHA")
		return nil, err
	}

	normRepo := normalizeRepo(repo)
	var target *PRRecord
	for _, rec := range f.prRecords {
		if rec.Repo == normRepo && rec.MergeSHA == mergeSHA {
			target = rec
			break
		}
	}
	if target == nil {
		return nil, nil
	}
	if target.Status == notStatus {
		return nil, nil // No transition
	}
	target.Status = toStatus
	target.UpdatedAt = time.Now()
	cp := *target
	return &cp, nil
}
