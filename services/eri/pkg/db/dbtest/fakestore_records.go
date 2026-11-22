package dbtest

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
)

// By-ID queue record helpers

func fakeStatusIn(status string, statuses []string) bool {
	return slices.Contains(statuses, status)
}

func fakeUpdateByID[T any](ids []string, rows map[string]*T, status func(*T) string, statuses []string, apply func(*T)) []string {
	var changed []string
	for _, id := range db.NormalizeRecordIDs(ids) {
		if r, ok := rows[id]; ok && r != nil && fakeStatusIn(status(r), statuses) {
			apply(r)
			changed = append(changed, id)
		}
	}
	return changed
}

func fakeBeginAttempt(ordinal int, retry, restart *int, updatedAt *time.Time) (db.AttemptStart, error) {
	if ordinal < 1 {
		return db.AttemptStart{}, fmt.Errorf("attempt ordinal must be >= 1, got %d", ordinal)
	}
	start := db.AttemptStart{}
	if *retry >= ordinal {
		*restart++
		start.Reentry = true
	} else {
		*retry = ordinal
	}
	*updatedAt = time.Now().UTC()
	start.RetryCount, start.RestartCount = *retry, *restart
	return start, nil
}

var (
	fakeActiveStatuses  = []string{db.StatusPending, db.StatusProcessing}
	fakePendingStatuses = []string{db.StatusPending}
)

func messageStatus(m *db.Message) string   { return m.Status }
func dispatchStatus(d *db.Dispatch) string { return d.Status }

func (f *FakeStore) pendingMessagesSorted(match func(*db.Message) bool) []string {
	var rows []*db.Message
	for _, id := range f.messageOrder {
		if m := f.messages[id]; m != nil && m.Status == db.StatusPending && match(m) {
			rows = append(rows, m)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].CreatedAt.Before(rows[j].CreatedAt) })
	out := make([]string, 0, len(rows))
	for _, m := range rows {
		out = append(out, m.ID)
	}
	return out
}

func (f *FakeStore) failMessages(method string, ids []string, reason string, statuses []string) ([]string, error) {
	return withFakeLock(f, method, func() ([]string, error) {
		now := time.Now().UTC()
		return fakeUpdateByID(ids, f.messages, messageStatus, statuses, func(m *db.Message) {
			m.Status, m.ErrorMessage, m.UpdatedAt = db.StatusFailed, reason, now
		}), nil
	})
}

func (f *FakeStore) failDispatches(method string, ids []string, reason string, statuses []string) ([]string, error) {
	return withFakeLock(f, method, func() ([]string, error) {
		now := time.Now().UTC()
		return fakeUpdateByID(ids, f.dispatches, dispatchStatus, statuses, func(d *db.Dispatch) {
			d.Status, d.ErrorMessage, d.UpdatedAt = db.StatusFailed, reason, now
		}), nil
	})
}

func (f *FakeStore) ListPendingMessageIDsForThread(ctx context.Context, threadID string) ([]string, error) {
	return withFakeLock(f, "ListPendingMessageIDsForThread", func() ([]string, error) {
		threadID = strings.TrimSpace(threadID)
		if threadID == "" {
			return nil, nil
		}
		return f.pendingMessagesSorted(func(m *db.Message) bool { return m.ThreadID == threadID }), nil
	})
}

func (f *FakeStore) GetMessagesByID(ctx context.Context, ids []string) ([]db.Message, error) {
	return withFakeLock(f, "GetMessagesByID", func() ([]db.Message, error) {
		var out []db.Message
		for _, id := range db.NormalizeRecordIDs(ids) {
			if m := f.messages[id]; m != nil {
				out = append(out, *cloneMessage(m))
			}
		}
		return out, nil
	})
}

func (f *FakeStore) FailMessagesIfActive(ctx context.Context, ids []string, reason string) ([]string, error) {
	return f.failMessages("FailMessagesIfActive", ids, reason, fakeActiveStatuses)
}

func (f *FakeStore) FailMessagesIfPending(ctx context.Context, ids []string, reason string) ([]string, error) {
	return f.failMessages("FailMessagesIfPending", ids, reason, fakePendingStatuses)
}

func (f *FakeStore) RequeueProcessingMessages(ctx context.Context, ids []string, reason string) ([]string, error) {
	return withFakeLock(f, "RequeueProcessingMessages", func() ([]string, error) {
		now := time.Now().UTC()
		return fakeUpdateByID(ids, f.messages, messageStatus, []string{db.StatusProcessing}, func(m *db.Message) {
			m.Status, m.RetryCount, m.ErrorMessage, m.UpdatedAt = db.StatusPending, m.RetryCount+1, reason, now
		}), nil
	})
}

func (f *FakeStore) BeginMessageAttempt(ctx context.Context, id string, ordinal int) (db.AttemptStart, error) {
	return withFakeLock(f, "BeginMessageAttempt", func() (db.AttemptStart, error) {
		m := f.messages[strings.TrimSpace(id)]
		if m == nil {
			return db.AttemptStart{}, fmt.Errorf("message %s: %w", id, db.ErrRecordNotFound)
		}
		return fakeBeginAttempt(ordinal, &m.RetryCount, &m.RestartCount, &m.UpdatedAt)
	})
}

func (f *FakeStore) ListPendingDispatchIDsForSession(ctx context.Context, sessionKey string) ([]string, error) {
	return withFakeLock(f, "ListPendingDispatchIDsForSession", func() ([]string, error) {
		key := strings.TrimSpace(sessionKey)
		if key == "" {
			return nil, nil
		}
		var rows []*db.Dispatch
		for _, id := range f.dispatchOrder {
			d := f.dispatches[id]
			if d == nil || d.Status != db.StatusPending {
				continue
			}
			if d.SessionID == key || (d.VanadisChunkID != "" && "vanadis-"+d.VanadisChunkID == key) || "dispatch-"+d.ID == key {
				rows = append(rows, d)
			}
		}
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].CreatedAt.Before(rows[j].CreatedAt) })
		out := make([]string, 0, len(rows))
		for _, d := range rows {
			out = append(out, d.ID)
		}
		return out, nil
	})
}

func (f *FakeStore) FailDispatchesIfActive(ctx context.Context, ids []string, reason string) ([]string, error) {
	return f.failDispatches("FailDispatchesIfActive", ids, reason, fakeActiveStatuses)
}

func (f *FakeStore) FailDispatchesIfPending(ctx context.Context, ids []string, reason string) ([]string, error) {
	return f.failDispatches("FailDispatchesIfPending", ids, reason, fakePendingStatuses)
}

func (f *FakeStore) BeginDispatchAttempt(ctx context.Context, id string, ordinal int) (db.AttemptStart, error) {
	return withFakeLock(f, "BeginDispatchAttempt", func() (db.AttemptStart, error) {
		d := f.dispatches[strings.TrimSpace(id)]
		if d == nil {
			return db.AttemptStart{}, fmt.Errorf("dispatch %s: %w", id, db.ErrRecordNotFound)
		}
		return fakeBeginAttempt(ordinal, &d.RetryCount, &d.RestartCount, &d.UpdatedAt)
	})
}

func (f *FakeStore) ListActiveDispatchSessionKeys(ctx context.Context, dispatchID, buildID string) ([]string, error) {
	return withFakeLock(f, "ListActiveDispatchSessionKeys", func() ([]string, error) {
		dID, bID := strings.TrimSpace(dispatchID), strings.TrimSpace(buildID)
		if dID == "" && bID == "" {
			return nil, nil
		}
		var keys []string
		for _, d := range f.dispatches {
			if d == nil || d.SessionID == "" || !fakeStatusIn(d.Status, fakeActiveStatuses) {
				continue
			}
			chunk := strings.TrimSpace(d.VanadisChunkID)
			match := (dID != "" && d.ID == dID) || (bID != "" && (chunk == bID ||
				strings.HasPrefix(chunk, bID+"-chunk-") || d.SessionID == "vanadis-"+bID ||
				strings.HasPrefix(d.SessionID, "vanadis-"+bID+"-chunk-")))
			if match && !slices.Contains(keys, d.SessionID) {
				keys = append(keys, d.SessionID)
			}
		}
		sort.Strings(keys)
		return keys, nil
	})
}
