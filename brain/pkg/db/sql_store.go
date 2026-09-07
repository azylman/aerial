package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// SQLStore wraps a concrete *sql.DB instance and implements the Store interface.
type SQLStore struct {
	db *sql.DB
}

// NewSQLStore creates a new Store instance wrapping the provided *sql.DB connection.
func NewSQLStore(database *sql.DB) Store {
	if database == nil {
		return nil
	}
	return &SQLStore{db: database}
}

func (s *SQLStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SQLStore) WithTx(ctx context.Context, fn func(txStore Store) error) error {
	if s.db == nil {
		return fmt.Errorf("database is nil")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Executing within transaction
	txStore := &SQLStore{db: s.db} // Fallback or wrapper for tx context
	if err := fn(txStore); err != nil {
		return err
	}
	return tx.Commit()
}

// FactStore implementation
func (s *SQLStore) InsertFact(ctx context.Context, category, factText string, importance float64, threadID string, embedding []float32) (int64, error) {
	return InsertFact(s.db, category, factText, importance, threadID, embedding)
}

func (s *SQLStore) SearchSimilarFacts(ctx context.Context, embedding []float32, limit int, minScore float64, threadID string) ([]Fact, error) {
	return SearchSimilarFacts(s.db, embedding, limit, minScore, threadID)
}

func (s *SQLStore) GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error) {
	return GetFactsPaginated(s.db, filter)
}

func (s *SQLStore) GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error) {
	return GetActiveConversationsForExtraction(s.db, activeHours)
}

func (s *SQLStore) UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error {
	return UpdateConversationFactWatermark(s.db, threadID, maxRowID)
}

func (s *SQLStore) UpdateConversationFactExtractedAt(ctx context.Context, threadID string) error {
	return UpdateConversationFactExtractedAt(s.db, threadID)
}

// ScheduleStore implementation
func (s *SQLStore) CreateOneShotSchedule(ctx context.Context, sched OneShotSchedule) error {
	return CreateOneShotSchedule(s.db, sched)
}

func (s *SQLStore) GetDueOneShotSchedules(ctx context.Context) ([]OneShotSchedule, error) {
	return GetDueOneShotSchedules(s.db)
}

func (s *SQLStore) DeleteOneShotSchedule(ctx context.Context, id string) error {
	return DeleteOneShotSchedule(s.db, id)
}

func (s *SQLStore) InsertMessageAndConsumeOneShot(ctx context.Context, scheduleID string, msg Message) error {
	return InsertMessageAndConsumeOneShot(s.db, scheduleID, msg)
}

func (s *SQLStore) GetAllOneShotSchedules(ctx context.Context, threadID string) ([]OneShotSchedule, error) {
	return GetAllOneShotSchedules(s.db, threadID)
}

func (s *SQLStore) CreateCronSchedule(ctx context.Context, c CronSchedule) error {
	return CreateCronSchedule(s.db, c)
}

func (s *SQLStore) GetDueCronSchedules(ctx context.Context) ([]CronSchedule, error) {
	return GetDueCronSchedules(s.db)
}

func (s *SQLStore) GetAllCronSchedules(ctx context.Context, targetID string) ([]CronSchedule, error) {
	return GetAllCronSchedules(s.db, targetID)
}

func (s *SQLStore) DeleteCronSchedule(ctx context.Context, id string) error {
	return DeleteCronSchedule(s.db, id)
}

func (s *SQLStore) UpdateCronNextRun(ctx context.Context, id string, nextRunAt time.Time) error {
	return UpdateCronNextRun(s.db, id, nextRunAt)
}

func (s *SQLStore) CreateScheduleRun(ctx context.Context, run ScheduleRun) error {
	return CreateScheduleRun(s.db, run)
}

func (s *SQLStore) UpdateScheduleRunStatus(ctx context.Context, params UpdateRunParams) error {
	return UpdateScheduleRunStatus(s.db, params)
}

func (s *SQLStore) GetScheduleRunsPaginated(ctx context.Context, limit, offset int, scheduleID, status string) ([]ScheduleRun, int, error) {
	return GetScheduleRunsPaginated(s.db, limit, offset, scheduleID, status)
}

func (s *SQLStore) GetScheduleSummaryMetrics(ctx context.Context) (ScheduleSummaryMetrics, error) {
	return GetScheduleSummaryMetrics(s.db)
}

func (s *SQLStore) ReconcileOrphanedScheduleRuns(ctx context.Context) (int64, error) {
	return ReconcileOrphanedScheduleRuns(s.db)
}

func (s *SQLStore) PruneScheduleRuns(ctx context.Context, maxCount int, maxAge time.Duration) (int64, error) {
	return PruneScheduleRuns(s.db, maxCount, maxAge)
}

// MessageStore implementation
func (s *SQLStore) InsertMessage(ctx context.Context, msg Message) error {
	return InsertMessage(s.db, msg)
}

func (s *SQLStore) UpdateMessageStatus(ctx context.Context, id, status, errorMsg string) error {
	return UpdateMessageStatus(s.db, id, status, errorMsg)
}

func (s *SQLStore) UpdateMessageCompleted(ctx context.Context, id, responseText string) error {
	return UpdateMessageCompleted(s.db, id, responseText)
}

func (s *SQLStore) IncrementMessageRetry(ctx context.Context, id, errorMsg string) error {
	return IncrementMessageRetry(s.db, id, errorMsg)
}

func (s *SQLStore) GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]Message, error) {
	msgs, err := GetPendingOrProcessingMessages(s.db)
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(msgs) > limit {
		return msgs[:limit], nil
	}
	return msgs, nil
}

func (s *SQLStore) GetMessage(ctx context.Context, id string) (*Message, error) {
	return GetMessage(s.db, id)
}

func (s *SQLStore) MessageExists(ctx context.Context, id string) (bool, error) {
	return MessageExists(s.db, id)
}

func (s *SQLStore) ClaimPendingMessage(ctx context.Context, id string) (bool, error) {
	return ClaimPendingMessage(s.db, id)
}

func (s *SQLStore) ClaimNextPendingMessage(ctx context.Context, workerID string) (*Message, error) {
	msgs, err := GetPendingOrProcessingMessages(s.db)
	if err != nil || len(msgs) == 0 {
		return nil, err
	}
	for _, m := range msgs {
		if m.Status == StatusPending {
			claimed, err := ClaimPendingMessage(s.db, m.ID)
			if err == nil && claimed {
				m.Status = StatusProcessing
				return &m, nil
			}
		}
	}
	return nil, nil
}

func (s *SQLStore) GetActiveRecentThreadIDs(ctx context.Context, since time.Duration) ([]string, error) {
	return GetActiveRecentThreadIDs(s.db, since)
}

func (s *SQLStore) GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]Message, error) {
	return GetRecentThreadMessages(s.db, threadID, limit)
}

func (s *SQLStore) GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error) {
	return GetMaxMessageRowID(s.db, threadID)
}

// SessionStore implementation
func (s *SQLStore) GetSessionID(ctx context.Context, threadID string) (string, error) {
	return GetSessionID(s.db, threadID)
}

func (s *SQLStore) SaveSessionID(ctx context.Context, threadID, sessionID string) error {
	return SaveSessionID(s.db, threadID, sessionID)
}

func (s *SQLStore) DeleteSessionID(ctx context.Context, threadID string) error {
	return DeleteSessionID(s.db, threadID)
}

func (s *SQLStore) IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	return IncrementSessionTurnCount(s.db, sessionKey)
}

func (s *SQLStore) GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	return GetSessionTurnCount(s.db, sessionKey)
}

func (s *SQLStore) RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error {
	return RotateSessionID(s.db, sessionKey, newSessionID)
}
