package queue

import (
	"context"
	"testing"

	"github.com/azylman/aerial/brain/pkg/db"
)

// setupTestStore returns a clean FakeStore for testing.
func setupTestStore(t testing.TB) *db.FakeStore {
	return db.NewFakeStore()
}

func insertMessage(store db.Store, msg db.Message) error {
	if store == nil {
		return nil
	}
	return store.InsertMessage(context.Background(), msg)
}

func getMessage(store db.Store, id string) (*db.Message, error) {
	if store == nil {
		return nil, nil
	}
	return store.GetMessage(context.Background(), id)
}

func saveSessionID(store db.Store, threadID, sessionID string) error {
	if store == nil {
		return nil
	}
	return store.SaveSessionID(context.Background(), threadID, sessionID)
}

func getSessionID(store db.Store, threadID string) (string, error) {
	if store == nil {
		return "", nil
	}
	return store.GetSessionID(context.Background(), threadID)
}

func getPreviousSessionID(store db.Store, threadID string) (string, error) {
	if store == nil {
		return "", nil
	}
	return store.GetPreviousSessionID(context.Background(), threadID)
}

func createScheduleRun(store db.Store, run db.ScheduleRun) error {
	if store == nil {
		return nil
	}
	return store.CreateScheduleRun(context.Background(), run)
}

func getScheduleRun(store db.Store, id string) (*db.ScheduleRun, error) {
	if store == nil {
		return nil, nil
	}
	if fake, ok := store.(*db.FakeStore); ok {
		return fake.GetScheduleRun(id)
	}
	runs, _, err := store.GetScheduleRunsPaginated(context.Background(), 100, 0, "", "")
	if err != nil {
		return nil, err
	}
	for _, r := range runs {
		if r.ID == id {
			return &r, nil
		}
	}
	return nil, nil
}

func getSessionTurnCount(store db.Store, sessionKey string) (int, error) {
	if store == nil {
		return 0, nil
	}
	return store.GetSessionTurnCount(context.Background(), sessionKey)
}

func incrementSessionTurnCount(store db.Store, sessionKey string) (int, error) {
	if store == nil {
		return 0, nil
	}
	return store.IncrementSessionTurnCount(context.Background(), sessionKey)
}

func getScheduleRunsPaginated(store db.Store, limit, offset int, scheduleID, status string) ([]db.ScheduleRun, int, error) {
	if store == nil {
		return nil, 0, nil
	}
	return store.GetScheduleRunsPaginated(context.Background(), limit, offset, scheduleID, status)
}

func getThreadSummary(store db.Store, threadID string) (string, string, error) {
	if store == nil {
		return "", "", nil
	}
	return store.GetThreadSummary(context.Background(), threadID)
}

func saveThreadSummary(store db.Store, threadID, summary, lastMsgID string) error {
	if store == nil {
		return nil
	}
	return store.SaveThreadSummary(context.Background(), threadID, summary, lastMsgID)
}

func rotateSessionID(store db.Store, sessionKey, newSessionID string) error {
	if store == nil {
		return nil
	}
	return store.RotateSessionID(context.Background(), sessionKey, newSessionID)
}
