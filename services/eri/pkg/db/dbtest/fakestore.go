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
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

var (
	_ db.Store           = (*FakeStore)(nil)
	_ db.MessageStore    = (*FakeStore)(nil)
	_ db.DispatchStore   = (*FakeStore)(nil)
	_ db.SessionStore    = (*FakeStore)(nil)
	_ db.TranscriptStore = (*FakeStore)(nil)
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

// FakeStore is a thread-safe, pure Go in-memory implementation of db.Store.
type FakeStore struct {
	mu sync.RWMutex

	closed   bool
	failNext map[string]error

	messages     map[string]*db.Message
	messageOrder []string
	nextRowID    int64

	dispatches        map[string]*db.Dispatch
	dispatchOrder     []string
	nextDispatchRowID int64

	sessions        map[string]*db.SessionInfo
	threadSummaries map[string]threadSummaryRecord

	sessionSummaries map[string]*db.SessionSummary
	transcriptSteps  map[string][]db.TranscriptStep
}

// NewFakeStore constructs an initialized, empty in-memory FakeStore.
func NewFakeStore() *FakeStore {
	f := &FakeStore{
		failNext:         make(map[string]error),
		messages:         make(map[string]*db.Message),
		messageOrder:     make([]string, 0),
		dispatches:       make(map[string]*db.Dispatch),
		dispatchOrder:    make([]string, 0),
		sessions:         make(map[string]*db.SessionInfo),
		threadSummaries:  make(map[string]threadSummaryRecord),
		sessionSummaries: make(map[string]*db.SessionSummary),
		transcriptSteps:  make(map[string][]db.TranscriptStep),
	}
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
	cp.Mentions, cp.MentionUserIDs = cloneSlice(m.Mentions), cloneSlice(m.MentionUserIDs)
	cp.MentionRoleIDs, cp.Attachments = cloneSlice(m.MentionRoleIDs), cloneSlice(m.Attachments)
	return &cp
}

func cloneDispatch(d *db.Dispatch) *db.Dispatch {
	if d == nil {
		return nil
	}
	cp := *d
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
	if len(words) == 0 {
		return 0
	}
	return float64(matched) / float64(len(words))
}

func textContainsAll(content, qLower string, words []string) bool {
	lower := strings.ToLower(content)
	if strings.Contains(lower, qLower) {
		return true
	}
	for _, w := range words {
		if !strings.Contains(lower, w) {
			return false
		}
	}
	return len(words) > 0
}

func (f *FakeStore) insertMessageLocked(msg db.Message) {
	if strings.TrimSpace(msg.ID) == "" {
		return
	}
	if _, exists := f.messages[msg.ID]; exists {
		return
	}
	now := time.Now().UTC()
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.UpdatedAt.IsZero() {
		msg.UpdatedAt = now
	}
	f.nextRowID++
	if msg.RowID == 0 {
		msg.RowID = f.nextRowID
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

func (f *FakeStore) UpdateMessageCompleted(ctx context.Context, id, responseText string, conversationID ...string) error {
	return f.mutateMessage("UpdateMessageCompleted", id, func(m *db.Message) {
		m.Status, m.ResponseText, m.ErrorMessage = db.StatusCompleted, responseText, ""
		if len(conversationID) > 0 && strings.TrimSpace(conversationID[0]) != "" {
			m.ConversationID = strings.TrimSpace(conversationID[0])
		}
	})
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
		m, ok := f.messages[id]
		if !ok {
			return nil, sql.ErrNoRows
		}
		return cloneMessage(m), nil
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
		for key, s := range f.sessions {
			if s != nil && strings.HasPrefix(key, "discord-") && s.UpdatedAt.After(cutoff) {
				tid := strings.TrimPrefix(key, "discord-")
				if tid != "" && !seen[tid] {
					seen[tid] = true
					threadIDs = append(threadIDs, tid)
				}
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

func (f *FakeStore) HasActiveThreadTurn(ctx context.Context, threadID string) (bool, error) {
	return withFakeLock(f, "HasActiveThreadTurn", func() (bool, error) {
		trimmed := strings.TrimSpace(threadID)
		if trimmed == "" {
			return false, nil
		}
		for _, m := range f.messages {
			if m != nil && m.ThreadID == trimmed && (m.Status == db.StatusPending || m.Status == db.StatusProcessing) {
				return true, nil
			}
		}
		return false, nil
	})
}

func (f *FakeStore) SetSessionUpdatedAt(threadID string, t time.Time) {
	key := db.DiscordSessionID(threadID)
	f.withLock(func() {
		if s := f.sessions[key]; s != nil {
			s.UpdatedAt = t
		}
	})
}

func matchesTaskFilter(m *db.Message, flt db.TasksFilter) bool {
	if len(flt.Statuses) > 0 {
		statusMatch := false
		for _, st := range flt.Statuses {
			if strings.EqualFold(m.Status, strings.TrimSpace(st)) {
				statusMatch = true
				break
			}
		}
		if !statusMatch {
			return false
		}
	}
	if flt.ThreadID != "" && m.ThreadID != flt.ThreadID {
		return false
	}
	if flt.IsAdmin != nil && m.IsAdmin != *flt.IsAdmin {
		return false
	}
	if !flt.Since.IsZero() && m.UpdatedAt.Before(flt.Since) {
		return false
	}
	if flt.AfterRowID > 0 && m.RowID <= flt.AfterRowID {
		return false
	}
	if flt.BeforeRowID > 0 && m.RowID > flt.BeforeRowID {
		return false
	}
	return true
}

func buildActiveTaskRecord(m *db.Message, sess *db.SessionInfo, threadSummary string) db.ActiveTask {
	summary := m.Summary
	if summary == "" {
		summary = db.CleanTaskSummary(m.Content)
	}
	var sessID, prevSessID string
	sessID = m.ConversationID
	if sessID == "" && sess != nil {
		sessID = sess.InternalSessionID
	}
	if sess != nil {
		prevSessID = sess.PreviousSessionID
	}
	return db.ActiveTask{
		ID:                m.ID,
		RowID:             m.RowID,
		ThreadID:          m.ThreadID,
		SessionID:         sessID,
		PreviousSessionID: prevSessID,
		ThreadSummary:     threadSummary,
		AuthorName:        m.AuthorName,
		AuthorID:          m.AuthorID,
		Prompt:            m.Content,
		Summary:           summary,
		ResponseText:      m.ResponseText,
		IsAdmin:           m.IsAdmin,
		Status:            m.Status,
		RetryCount:        m.RetryCount,
		TriggerType:       db.InferTriggerType(m.AuthorID),
		CreatedAt:         m.CreatedAt,
		UpdatedAt:         m.UpdatedAt,
	}
}

func buildActiveTaskFromDispatch(d *db.Dispatch, sess *db.SessionInfo, threadSummary string) db.ActiveTask {
	summary := d.Summary
	if summary == "" {
		summary = db.CleanTaskSummary(d.Prompt)
	}
	threadID := d.SessionID
	if threadID == "" {
		threadID = "dispatch-" + d.ID
	}
	var sessID, prevSessID string
	if sess != nil {
		prevSessID = sess.PreviousSessionID
	}
	if d.SessionID != "" {
		sessID = d.ConversationID
		if sessID == "" && sess != nil {
			sessID = sess.InternalSessionID
		}
	}
	return db.ActiveTask{
		ID:                d.ID,
		RowID:             d.RowID,
		ThreadID:          threadID,
		SessionID:         sessID,
		PreviousSessionID: prevSessID,
		ThreadSummary:     threadSummary,
		AuthorName:        d.Source,
		AuthorID:          "dispatch:" + d.Source,
		Prompt:            d.Prompt,
		Summary:           summary,
		ResponseText:      d.ResponseText,
		IsAdmin:           true,
		Status:            d.Status,
		RetryCount:        d.RetryCount,
		TriggerType:       d.Source,
		CreatedAt:         d.CreatedAt,
		UpdatedAt:         d.UpdatedAt,
	}
}

func (f *FakeStore) getActiveTasksUnfiltered() []db.ActiveTask {
	var matchedMsgs []*db.Message
	for _, id := range f.messageOrder {
		m := f.messages[id]
		if m != nil && (m.Status == db.StatusPending || m.Status == db.StatusProcessing) {
			matchedMsgs = append(matchedMsgs, m)
		}
	}

	var activeDispatches []*db.Dispatch
	for _, id := range f.dispatchOrder {
		d := f.dispatches[id]
		if d != nil && (d.Status == db.StatusPending || d.Status == db.StatusProcessing) {
			activeDispatches = append(activeDispatches, d)
		}
	}

	tasks := make([]db.ActiveTask, 0, len(matchedMsgs)+len(activeDispatches))
	for _, m := range matchedMsgs {
		sessKey := db.DiscordSessionID(m.ThreadID)
		tasks = append(tasks, buildActiveTaskRecord(m, f.sessions[sessKey], f.threadSummaries[sessKey].summary))
	}
	for _, d := range activeDispatches {
		var sess *db.SessionInfo
		var threadSum string
		if d.SessionID != "" {
			sess = f.sessions[d.SessionID]
			threadSum = f.threadSummaries[d.SessionID].summary
		}
		tasks = append(tasks, buildActiveTaskFromDispatch(d, sess, threadSum))
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].CreatedAt.Before(tasks[j].CreatedAt) || (tasks[i].CreatedAt.Equal(tasks[j].CreatedAt) && tasks[i].ID < tasks[j].ID)
	})
	if len(tasks) > 50 {
		tasks = tasks[:50]
	}
	return tasks
}

func (f *FakeStore) getActiveTasksFiltered(flt db.TasksFilter) []db.ActiveTask {
	var matchedMsgs []*db.Message
	for _, id := range f.messageOrder {
		m := f.messages[id]
		if m != nil && matchesTaskFilter(m, flt) {
			matchedMsgs = append(matchedMsgs, m)
		}
	}

	limit := 50
	if flt.AfterRowID > 0 {
		sort.Slice(matchedMsgs, func(i, j int) bool {
			return matchedMsgs[i].RowID < matchedMsgs[j].RowID
		})
	} else {
		sort.Slice(matchedMsgs, func(i, j int) bool {
			return matchedMsgs[i].RowID > matchedMsgs[j].RowID
		})
	}
	if flt.Limit > 0 {
		limit = flt.Limit
		if limit > 200 {
			limit = 200
		}
	}

	if len(matchedMsgs) > limit {
		matchedMsgs = matchedMsgs[:limit]
	}
	tasks := make([]db.ActiveTask, len(matchedMsgs))
	for i, m := range matchedMsgs {
		sessKey := db.DiscordSessionID(m.ThreadID)
		tasks[i] = buildActiveTaskRecord(m, f.sessions[sessKey], f.threadSummaries[sessKey].summary)
	}
	return tasks
}

func (f *FakeStore) GetActiveTasks(ctx context.Context, filter ...db.TasksFilter) ([]db.ActiveTask, error) {
	return withFakeLock(f, "GetActiveTasks", func() ([]db.ActiveTask, error) {
		if len(filter) == 0 || filter[0].IsZero() {
			return f.getActiveTasksUnfiltered(), nil
		}
		return f.getActiveTasksFiltered(filter[0]), nil
	})
}

// DispatchStore implementation for FakeStore

func (f *FakeStore) InsertDispatch(ctx context.Context, d db.Dispatch) error {
	return f.withLockErr("InsertDispatch", func() error {
		if strings.TrimSpace(d.ID) == "" {
			return fmt.Errorf("dispatch id cannot be empty")
		}
		if _, exists := f.dispatches[d.ID]; exists {
			return nil
		}
		if d.Status == "" {
			d.Status = db.StatusPending
		}
		if strings.TrimSpace(d.Summary) == "" {
			d.Summary = db.CleanTaskSummary(d.Prompt)
		}
		now := time.Now().UTC()
		if d.CreatedAt.IsZero() {
			d.CreatedAt = now
		}
		if d.UpdatedAt.IsZero() {
			d.UpdatedAt = now
		}
		f.nextDispatchRowID++
		if d.RowID == 0 {
			d.RowID = f.nextDispatchRowID
		}
		f.dispatches[d.ID] = cloneDispatch(&d)
		f.dispatchOrder = append(f.dispatchOrder, d.ID)
		return nil
	})
}

func (f *FakeStore) GetDispatch(ctx context.Context, id string) (*db.Dispatch, error) {
	return withFakeLock(f, "GetDispatch", func() (*db.Dispatch, error) {
		d, ok := f.dispatches[id]
		if !ok {
			return nil, nil
		}
		return cloneDispatch(d), nil
	})
}

func (f *FakeStore) ClaimPendingDispatch(ctx context.Context, id string) (bool, error) {
	return withFakeLock(f, "ClaimPendingDispatch", func() (bool, error) {
		d, ok := f.dispatches[id]
		if !ok || d.Status != db.StatusPending {
			return false, nil
		}
		d.Status, d.UpdatedAt = db.StatusProcessing, time.Now().UTC()
		return true, nil
	})
}

func (f *FakeStore) UpdateDispatchStatus(ctx context.Context, id, status, errorMsg string) error {
	return f.withLockErr("UpdateDispatchStatus", func() error {
		if d, ok := f.dispatches[id]; ok {
			d.Status, d.ErrorMessage, d.UpdatedAt = status, errorMsg, time.Now().UTC()
		}
		return nil
	})
}

func (f *FakeStore) UpdateDispatchCompleted(ctx context.Context, id, responseText string, conversationID ...string) error {
	return f.withLockErr("UpdateDispatchCompleted", func() error {
		if d, ok := f.dispatches[id]; ok {
			d.Status, d.ResponseText, d.ErrorMessage, d.UpdatedAt = db.StatusCompleted, responseText, "", time.Now().UTC()
			if len(conversationID) > 0 && strings.TrimSpace(conversationID[0]) != "" {
				d.ConversationID = strings.TrimSpace(conversationID[0])
			}
		}
		return nil
	})
}

func (f *FakeStore) IncrementDispatchRetry(ctx context.Context, id, errorMsg string) error {
	return f.withLockErr("IncrementDispatchRetry", func() error {
		if d, ok := f.dispatches[id]; ok {
			d.RetryCount++
			d.ErrorMessage, d.UpdatedAt = errorMsg, time.Now().UTC()
		}
		return nil
	})
}

func (f *FakeStore) ResetDispatchToPendingWithRestart(ctx context.Context, id, reason string) error {
	return f.withLockErr("ResetDispatchToPendingWithRestart", func() error {
		if d, ok := f.dispatches[id]; ok && d.Status == db.StatusProcessing {
			d.RestartCount++
			d.Status, d.ErrorMessage, d.UpdatedAt = db.StatusPending, reason, time.Now().UTC()
		}
		return nil
	})
}

func (f *FakeStore) GetPendingOrProcessingDispatches(ctx context.Context, limit int) ([]db.Dispatch, error) {
	return withFakeLock(f, "GetPendingOrProcessingDispatches", func() ([]db.Dispatch, error) {
		var results []db.Dispatch
		for _, id := range f.dispatchOrder {
			if d := f.dispatches[id]; d != nil && (d.Status == db.StatusPending || d.Status == db.StatusProcessing) {
				results = append(results, *cloneDispatch(d))
			}
		}
		sort.Slice(results, func(i, j int) bool {
			return results[i].CreatedAt.Before(results[j].CreatedAt)
		})
		if limit > 0 && len(results) > limit {
			results = results[:limit]
		}
		return results, nil
	})
}

func (f *FakeStore) GetRecentSessionDispatches(ctx context.Context, sessionID string, limit int) ([]db.Dispatch, error) {
	return withFakeLock(f, "GetRecentSessionDispatches", func() ([]db.Dispatch, error) {
		if strings.TrimSpace(sessionID) == "" {
			return nil, nil
		}
		limit = defaultVal(limit, 10)
		var matches []db.Dispatch
		for i := len(f.dispatchOrder) - 1; i >= 0; i-- {
			if d := f.dispatches[f.dispatchOrder[i]]; d != nil && d.SessionID == sessionID && d.Status == db.StatusCompleted {
				matches = append(matches, *cloneDispatch(d))
				if len(matches) >= limit {
					break
				}
			}
		}
		slices.Reverse(matches)
		return matches, nil
	})
}

func (f *FakeStore) HasActiveDispatchTurn(ctx context.Context, sessionID, id string) (bool, error) {
	return withFakeLock(f, "HasActiveDispatchTurn", func() (bool, error) {
		sID := strings.TrimSpace(sessionID)
		tID := strings.TrimSpace(id)
		if sID == "" && tID == "" {
			return false, nil
		}
		for _, d := range f.dispatches {
			if d != nil && (d.Status == db.StatusPending || d.Status == db.StatusProcessing) {
				if (sID != "" && d.SessionID == sID) || (tID != "" && d.ID == tID) {
					return true, nil
				}
			}
		}
		return false, nil
	})
}

func (f *FakeStore) CancelPendingDispatches(ctx context.Context, dispatchID, buildID, reason string) ([]string, error) {
	return withFakeLock(f, "CancelPendingDispatches", func() ([]string, error) {
		dID := strings.TrimSpace(dispatchID)
		bID := strings.TrimSpace(buildID)
		if dID == "" && bID == "" {
			return nil, nil
		}
		var cancelled []string
		now := time.Now().UTC()
		for _, id := range f.dispatchOrder {
			d := f.dispatches[id]
			if d == nil || d.Status != db.StatusPending {
				continue
			}
			match := (dID != "" && d.ID == dID) || (bID != "" && strings.Contains(d.Prompt, "Build ID: "+bID+"\n"))
			if match {
				d.Status = db.StatusFailed
				d.ErrorMessage = reason
				d.UpdatedAt = now
				cancelled = append(cancelled, d.ID)
			}
		}
		return cancelled, nil
	})
}

// SessionStore helpers

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
	normKey := db.DiscordSessionID(key)
	s := f.sessions[normKey]
	if s == nil {
		s = &db.SessionInfo{ThreadID: normKey, CreatedAt: time.Now().UTC()}
		f.sessions[normKey] = s
	}
	return s
}

func (f *FakeStore) GetSessionID(ctx context.Context, threadID string) (string, error) {
	return withFakeLock(f, "GetSessionID", func() (string, error) { return sessionInternalID(f.sessions[db.DiscordSessionID(threadID)]), nil })
}

func (f *FakeStore) GetPreviousSessionID(ctx context.Context, threadID string) (string, error) {
	return withFakeLock(f, "GetPreviousSessionID", func() (string, error) { return sessionPrevID(f.sessions[db.DiscordSessionID(threadID)]), nil })
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
	return withFakeLock(f, "GetSessionTurnCount", func() (int, error) { return sessionTurnCount(f.sessions[db.DiscordSessionID(sessionKey)]), nil })
}

func (f *FakeStore) RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error {
	return f.withLockErr("RotateSessionID", func() error {
		normKey := db.DiscordSessionID(sessionKey)
		now := time.Now().UTC()
		s := f.sessions[normKey]
		if s == nil {
			f.sessions[normKey] = &db.SessionInfo{ThreadID: normKey, InternalSessionID: newSessionID, CreatedAt: now, UpdatedAt: now}
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
		rec := f.threadSummaries[db.DiscordSessionID(threadID)]
		return rec.summary, rec.watermark, nil
	})
}

func (f *FakeStore) SaveThreadSummary(ctx context.Context, threadID, summary, lastSummarizedMsgID string) error {
	return f.withLockErr("SaveThreadSummary", func() error {
		f.threadSummaries[db.DiscordSessionID(threadID)] = threadSummaryRecord{summary: summary, watermark: lastSummarizedMsgID}
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
		sessKey := db.DiscordSessionID(threadID)
		rawMsgThreadID := strings.TrimPrefix(strings.TrimSpace(threadID), "discord-")
		if s := f.sessions[sessKey]; s != nil {
			stats.InternalSessionID, stats.TurnCount, stats.SessionUpdatedAt = s.InternalSessionID, s.TurnCount, s.UpdatedAt
		}
		for _, id := range f.messageOrder {
			m := f.messages[id]
			if m != nil && m.ThreadID == rawMsgThreadID && m.Status == db.StatusCompleted && !strings.HasPrefix(m.ResponseText, "[EXPIRED_STALE]") && !strings.HasPrefix(m.ResponseText, "[AMBIENT") && !strings.HasPrefix(m.ResponseText, "[IGNORED") {
				stats.CompletedTurns++
				maxTime(&stats.LastMessageAt, m.UpdatedAt)
				maxTime(&stats.LastCompletedMessageCreatedAt, m.CreatedAt)
			}
		}
		return stats, nil
	})
}

func (f *FakeStore) SetSessionInfo(s db.SessionInfo) {
	s.ThreadID = db.DiscordSessionID(s.ThreadID)
	f.withLock(func() { f.sessions[s.ThreadID] = clonePtr(&s) })
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

func parseAllowedSessions(filter string) map[string]bool {
	if filter == "" {
		return nil
	}
	allowed := make(map[string]bool)
	for _, sid := range strings.Split(filter, ",") {
		if trimmed := strings.TrimSpace(sid); trimmed != "" {
			allowed[trimmed] = true
		}
	}
	return allowed
}

func matchesTranscriptStep(s db.TranscriptStep, toolFilter string, sinceTime time.Time, queryText, qLower string, words []string) bool {
	if toolFilter != "" && s.ToolName != toolFilter {
		return false
	}
	if !sinceTime.IsZero() && !s.CreatedAt.After(sinceTime) {
		return false
	}
	if queryText != "" && !textContainsAll(s.Content, qLower, words) {
		return false
	}
	return true
}

func (f *FakeStore) SearchTranscriptSteps(ctx context.Context, queryText, sessionFilter, toolFilter string, limit int, since ...time.Time) ([]db.TranscriptStep, error) {
	return withFakeLock(f, "SearchTranscriptSteps", func() ([]db.TranscriptStep, error) {
		queryText, sessionFilter, toolFilter = strings.TrimSpace(queryText), strings.TrimSpace(sessionFilter), strings.TrimSpace(toolFilter)
		var sinceTime time.Time
		if len(since) > 0 {
			sinceTime = since[0]
		}
		if queryText == "" && sessionFilter == "" && toolFilter == "" && sinceTime.IsZero() {
			return nil, nil
		}
		limit = defaultVal(limit, 20)
		qLower, words := strings.ToLower(queryText), strings.Fields(strings.ToLower(queryText))
		allowedSessions := parseAllowedSessions(sessionFilter)

		var matched []db.TranscriptStep
		for sid, steps := range f.transcriptSteps {
			if allowedSessions != nil && !allowedSessions[sid] {
				continue
			}
			for _, s := range steps {
				if !matchesTranscriptStep(s, toolFilter, sinceTime, queryText, qLower, words) {
					continue
				}
				cp := s
				cp.RankScore = 1.0
				matched = append(matched, cp)
			}
		}

		if !sinceTime.IsZero() && queryText == "" {
			sort.Slice(matched, func(i, j int) bool {
				if matched[i].CreatedAt.Equal(matched[j].CreatedAt) {
					return matched[i].StepIndex < matched[j].StepIndex
				}
				return matched[i].CreatedAt.Before(matched[j].CreatedAt)
			})
		} else {
			sort.Slice(matched, func(i, j int) bool { return matched[i].CreatedAt.After(matched[j].CreatedAt) })
		}
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
