package db

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

// SQLStore wraps a DBTX instance (either *sql.DB connection pool or *sql.Tx) and implements the Store interface.
type SQLStore struct {
	db         DBTX
	isPostgres bool
}

func isDBTXNil(d DBTX) bool {
	if d == nil {
		return true
	}
	v := reflect.ValueOf(d)
	return v.Kind() == reflect.Ptr && v.IsNil()
}

// NewSQLStore creates a new Store instance wrapping the provided *sql.DB connection.
func NewSQLStore(database *sql.DB) Store {
	if database == nil {
		return nil
	}
	return &SQLStore{
		db:         database,
		isPostgres: isPostgres(database),
	}
}

// NewTxStore creates a Store instance bound to an active transaction.
func NewTxStore(tx DBTX, isPg bool) Store {
	if isDBTXNil(tx) {
		return nil
	}
	return &SQLStore{
		db:         tx,
		isPostgres: isPg,
	}
}

func (s *SQLStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	if closer, ok := s.db.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func (s *SQLStore) WithTx(ctx context.Context, fn func(txStore Store) error) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}

	type beginner interface {
		BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	}

	b, ok := s.db.(beginner)
	if !ok {
		// Already inside a transaction or DBTX does not support BeginTx; execute fn within current context.
		return fn(s)
	}

	tx, err := b.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	txStore := NewTxStore(tx, s.isPostgres)
	if err := fn(txStore); err != nil {
		return err
	}
	return tx.Commit()
}

// FactStore implementation
func (s *SQLStore) InsertFact(ctx context.Context, category, factText string, importance float64, threadID string, embedding []float32) (int64, error) {
	if s == nil || isDBTXNil(s.db) {
		return 0, fmt.Errorf("database is nil")
	}
	return InsertFactWithContext(ctx, s.db, s.isPostgres, category, factText, importance, threadID, embedding)
}

func (s *SQLStore) SearchSimilarFacts(ctx context.Context, embedding []float32, limit int, minScore float64, threadID string) ([]Fact, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
	return SearchSimilarFactsWithContext(ctx, s.db, s.isPostgres, embedding, limit, minScore, threadID)
}

func (s *SQLStore) GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
	return GetFactsPaginatedWithContext(ctx, s.db, s.isPostgres, filter)
}

func (s *SQLStore) GetFactsByThreadWithEmbeddings(ctx context.Context, threadID string) ([]FactWithEmbedding, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
	return GetFactsByThreadWithEmbeddings(s.db, threadID)
}

func (s *SQLStore) GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
	return GetActiveConversationsForExtraction(s.db, activeHours)
}

func (s *SQLStore) UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return UpdateConversationFactWatermark(s.db, threadID, maxRowID)
}

func (s *SQLStore) UpdateConversationFactExtractedAt(ctx context.Context, threadID string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return UpdateConversationFactExtractedAt(s.db, threadID)
}

// ScheduleStore implementation
func (s *SQLStore) CreateOneShotSchedule(ctx context.Context, sched OneShotSchedule) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return CreateOneShotSchedule(s.db, sched)
}

func (s *SQLStore) GetDueOneShotSchedules(ctx context.Context) ([]OneShotSchedule, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
	return GetDueOneShotSchedules(s.db)
}

func (s *SQLStore) DeleteOneShotSchedule(ctx context.Context, id string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return DeleteOneShotSchedule(s.db, id)
}

func (s *SQLStore) InsertMessageAndConsumeOneShot(ctx context.Context, scheduleID string, msg Message) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return InsertMessageAndConsumeOneShot(s.db, scheduleID, msg)
}

func (s *SQLStore) GetAllOneShotSchedules(ctx context.Context, threadID string) ([]OneShotSchedule, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
	return GetAllOneShotSchedules(s.db, threadID)
}

func (s *SQLStore) CreateCronSchedule(ctx context.Context, c CronSchedule) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return CreateCronSchedule(s.db, c)
}

func (s *SQLStore) GetDueCronSchedules(ctx context.Context) ([]CronSchedule, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
	return GetDueCronSchedules(s.db)
}

func (s *SQLStore) GetAllCronSchedules(ctx context.Context, targetID string) ([]CronSchedule, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
	return GetAllCronSchedules(s.db, targetID)
}

func (s *SQLStore) DeleteCronSchedule(ctx context.Context, id string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return DeleteCronSchedule(s.db, id)
}

func (s *SQLStore) UpdateCronNextRun(ctx context.Context, id string, nextRunAt time.Time) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return UpdateCronNextRun(s.db, id, nextRunAt)
}

func (s *SQLStore) CreateScheduleRun(ctx context.Context, run ScheduleRun) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return CreateScheduleRun(s.db, run)
}

func (s *SQLStore) UpdateScheduleRunStatus(ctx context.Context, params UpdateRunParams) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return UpdateScheduleRunStatus(s.db, params)
}

func (s *SQLStore) GetScheduleRunsPaginated(ctx context.Context, limit, offset int, scheduleID, status string) ([]ScheduleRun, int, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, 0, fmt.Errorf("database is nil")
	}
	return GetScheduleRunsPaginated(s.db, limit, offset, scheduleID, status)
}

func (s *SQLStore) GetScheduleSummaryMetrics(ctx context.Context) (ScheduleSummaryMetrics, error) {
	if s == nil || isDBTXNil(s.db) {
		return ScheduleSummaryMetrics{}, fmt.Errorf("database is nil")
	}
	return GetScheduleSummaryMetrics(s.db)
}

func (s *SQLStore) ReconcileOrphanedScheduleRuns(ctx context.Context) (int64, error) {
	if s == nil || isDBTXNil(s.db) {
		return 0, fmt.Errorf("database is nil")
	}
	return ReconcileOrphanedScheduleRuns(s.db)
}

func (s *SQLStore) PruneScheduleRuns(ctx context.Context, maxCount int, maxAge time.Duration) (int64, error) {
	if s == nil || isDBTXNil(s.db) {
		return 0, fmt.Errorf("database is nil")
	}
	return PruneScheduleRuns(s.db, maxCount, maxAge)
}

// MessageStore implementation
func (s *SQLStore) InsertMessage(ctx context.Context, msg Message) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return InsertMessage(s.db, msg)
}

func (s *SQLStore) UpdateMessageStatus(ctx context.Context, id, status, errorMsg string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return UpdateMessageStatus(s.db, id, status, errorMsg)
}

func (s *SQLStore) UpdateMessageCompleted(ctx context.Context, id, responseText string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return UpdateMessageCompleted(s.db, id, responseText)
}

func (s *SQLStore) IncrementMessageRetry(ctx context.Context, id, errorMsg string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return IncrementMessageRetry(s.db, id, errorMsg)
}

func (s *SQLStore) GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]Message, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
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
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
	return GetMessage(s.db, id)
}

func (s *SQLStore) MessageExists(ctx context.Context, id string) (bool, error) {
	if s == nil || isDBTXNil(s.db) {
		return false, fmt.Errorf("database is nil")
	}
	return MessageExists(s.db, id)
}

func (s *SQLStore) ClaimPendingMessage(ctx context.Context, id string) (bool, error) {
	if s == nil || isDBTXNil(s.db) {
		return false, fmt.Errorf("database is nil")
	}
	return ClaimPendingMessage(s.db, id)
}

func (s *SQLStore) ClaimNextPendingMessage(ctx context.Context, workerID string) (*Message, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
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
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
	return GetActiveRecentThreadIDs(s.db, since)
}

func (s *SQLStore) GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]Message, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database is nil")
	}
	return GetRecentThreadMessages(s.db, threadID, limit)
}

func (s *SQLStore) GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error) {
	if s == nil || isDBTXNil(s.db) {
		return 0, fmt.Errorf("database is nil")
	}
	return GetMaxMessageRowID(s.db, threadID)
}

// SessionStore implementation
func (s *SQLStore) GetSessionID(ctx context.Context, threadID string) (string, error) {
	if s == nil || isDBTXNil(s.db) {
		return "", fmt.Errorf("database is nil")
	}
	return GetSessionID(s.db, threadID)
}

func (s *SQLStore) SaveSessionID(ctx context.Context, threadID, sessionID string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return SaveSessionID(s.db, threadID, sessionID)
}

func (s *SQLStore) DeleteSessionID(ctx context.Context, threadID string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return DeleteSessionID(s.db, threadID)
}

func (s *SQLStore) IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	if s == nil || isDBTXNil(s.db) {
		return 0, fmt.Errorf("database is nil")
	}
	return IncrementSessionTurnCount(s.db, sessionKey)
}

func (s *SQLStore) GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	if s == nil || isDBTXNil(s.db) {
		return 0, fmt.Errorf("database is nil")
	}
	return GetSessionTurnCount(s.db, sessionKey)
}

func (s *SQLStore) RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database is nil")
	}
	return RotateSessionID(s.db, sessionKey, newSessionID)
}
