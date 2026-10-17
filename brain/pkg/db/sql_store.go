package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// SQLStore wraps a DBTX instance (either *sql.DB connection pool or *sql.Tx) and implements the Store interface.
type SQLStore struct {
	db         DBTX
	isPostgres bool
}

var ErrNilDatabase = errors.New("database is nil")

func (s *SQLStore) checkDB() error {
	if s == nil || isDBTXNil(s.db) {
		return ErrNilDatabase
	}
	return nil
}

func execDB(s *SQLStore, fn func(DBTX) error) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	return fn(s.db)
}

func queryDB[T any](s *SQLStore, fn func(DBTX) (T, error)) (T, error) {
	if err := s.checkDB(); err != nil {
		var zero T
		return zero, err
	}
	return fn(s.db)
}

func isDBTXNil(d DBTX) bool {
	if d == nil {
		return true
	}
	v := reflect.ValueOf(d)
	return v.Kind() == reflect.Pointer && v.IsNil()
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
	if err := s.checkDB(); err != nil {
		return err
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
	defer rollbackWarn(tx, "tx")

	txStore := NewTxStore(tx, s.isPostgres)
	if err := fn(txStore); err != nil {
		return err
	}
	return tx.Commit()
}

// FactStore implementation
func (s *SQLStore) InsertFact(ctx context.Context, category, factText string, importance float64, embedding []float32) (int64, error) {
	return queryDB(s, func(db DBTX) (int64, error) {
		return InsertFactWithContext(ctx, db, s.isPostgres, category, factText, importance, embedding)
	})
}

func (s *SQLStore) SearchSimilarFacts(ctx context.Context, embedding []float32, queryText string, limit int, minScore float64) ([]Fact, error) {
	return queryDB(s, func(db DBTX) ([]Fact, error) {
		return SearchSimilarFactsWithContext(ctx, db, s.isPostgres, embedding, queryText, limit, minScore)
	})
}

func (s *SQLStore) GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error) {
	return queryDB(s, func(db DBTX) (*FactsResult, error) {
		return GetFactsPaginatedWithContext(ctx, db, s.isPostgres, filter)
	})
}

func (s *SQLStore) GetFactsByThreadWithEmbeddings(ctx context.Context, threadID string) ([]FactWithEmbedding, error) {
	return queryDB(s, func(db DBTX) ([]FactWithEmbedding, error) {
		return GetFactsByThreadWithEmbeddings(db, threadID)
	})
}

func (s *SQLStore) GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error) {
	return queryDB(s, func(db DBTX) ([]string, error) {
		return GetActiveConversationsForExtraction(db, activeHours)
	})
}

func (s *SQLStore) UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error {
	return execDB(s, func(db DBTX) error {
		return UpdateConversationFactWatermark(db, threadID, maxRowID)
	})
}

func (s *SQLStore) UpdateConversationFactExtractedAt(ctx context.Context, threadID string) error {
	return execDB(s, func(db DBTX) error {
		return UpdateConversationFactExtractedAt(db, threadID)
	})
}

func (s *SQLStore) FindDuplicateFact(ctx context.Context, embedding []float32, minSim float64) (*Fact, float64, error) {
	if err := s.checkDB(); err != nil {
		return nil, 0, err
	}
	return FindDuplicateFactWithContext(ctx, s.db, s.isPostgres, embedding, minSim)
}

func (s *SQLStore) ReinforceFact(ctx context.Context, id int64, newText string, newEmbedding []float32, boost float64) error {
	return execDB(s, func(db DBTX) error {
		return ReinforceFactWithContext(ctx, db, s.isPostgres, id, newText, newEmbedding, boost)
	})
}

func (s *SQLStore) DecayAndPruneFacts(ctx context.Context, decayStep float64, pruneFloor float64, pruneAgeDays int) (int64, int64, error) {
	if err := s.checkDB(); err != nil {
		return 0, 0, err
	}
	return DecayAndPruneFactsWithContext(ctx, s.db, s.isPostgres, decayStep, pruneFloor, pruneAgeDays)
}

// ScheduleStore implementation
func (s *SQLStore) CreateOneShotSchedule(ctx context.Context, sched OneShotSchedule) error {
	return execDB(s, func(db DBTX) error {
		return CreateOneShotSchedule(db, sched)
	})
}

func (s *SQLStore) GetDueOneShotSchedules(ctx context.Context) ([]OneShotSchedule, error) {
	return queryDB(s, func(db DBTX) ([]OneShotSchedule, error) {
		return GetDueOneShotSchedules(db)
	})
}

func (s *SQLStore) InsertMessageAndConsumeOneShot(ctx context.Context, scheduleID string, msg Message) error {
	return execDB(s, func(db DBTX) error {
		return InsertMessageAndConsumeOneShot(db, scheduleID, msg)
	})
}

func (s *SQLStore) GetAllOneShotSchedules(ctx context.Context, threadID string) ([]OneShotSchedule, error) {
	return queryDB(s, func(db DBTX) ([]OneShotSchedule, error) {
		return GetAllOneShotSchedules(db, threadID)
	})
}

func (s *SQLStore) CreateCronSchedule(ctx context.Context, c CronSchedule) error {
	return execDB(s, func(db DBTX) error {
		return CreateCronSchedule(db, c)
	})
}

func (s *SQLStore) GetDueCronSchedules(ctx context.Context) ([]CronSchedule, error) {
	return queryDB(s, func(db DBTX) ([]CronSchedule, error) {
		return GetDueCronSchedules(db)
	})
}

func (s *SQLStore) GetAllCronSchedules(ctx context.Context, targetID string) ([]CronSchedule, error) {
	return queryDB(s, func(db DBTX) ([]CronSchedule, error) {
		return GetAllCronSchedules(db, targetID)
	})
}

func (s *SQLStore) UpdateCronNextRun(ctx context.Context, id string, nextRunAt time.Time) error {
	return execDB(s, func(db DBTX) error {
		return UpdateCronNextRun(db, id, nextRunAt)
	})
}

func (s *SQLStore) CreateScheduleRun(ctx context.Context, run ScheduleRun) error {
	return execDB(s, func(db DBTX) error {
		return CreateScheduleRun(db, run)
	})
}

func (s *SQLStore) UpdateScheduleRunStatus(ctx context.Context, params UpdateRunParams) error {
	return execDB(s, func(db DBTX) error {
		return UpdateScheduleRunStatus(db, params)
	})
}

func (s *SQLStore) GetScheduleRunsPaginated(ctx context.Context, limit, offset int, scheduleID, status string) ([]ScheduleRun, int, error) {
	if err := s.checkDB(); err != nil {
		return nil, 0, err
	}
	return GetScheduleRunsPaginated(s.db, limit, offset, scheduleID, status)
}

func (s *SQLStore) GetScheduleSummaryMetrics(ctx context.Context) (ScheduleSummaryMetrics, error) {
	return queryDB(s, func(db DBTX) (ScheduleSummaryMetrics, error) {
		return GetScheduleSummaryMetrics(db)
	})
}

func (s *SQLStore) ReconcileOrphanedScheduleRuns(ctx context.Context) (int64, error) {
	return queryDB(s, func(db DBTX) (int64, error) {
		return ReconcileOrphanedScheduleRuns(db)
	})
}

func (s *SQLStore) PruneScheduleRuns(ctx context.Context, maxCount int, maxAge time.Duration) (int64, error) {
	return queryDB(s, func(db DBTX) (int64, error) {
		return PruneScheduleRuns(db, maxCount, maxAge)
	})
}

// MessageStore implementation
func (s *SQLStore) InsertMessage(ctx context.Context, msg Message) error {
	return execDB(s, func(db DBTX) error {
		return InsertMessage(db, msg)
	})
}

func (s *SQLStore) UpdateMessageStatus(ctx context.Context, id, status, errorMsg string) error {
	return execDB(s, func(db DBTX) error {
		return UpdateMessageStatus(db, id, status, errorMsg)
	})
}

func (s *SQLStore) UpdateMessageCompleted(ctx context.Context, id, responseText string) error {
	return execDB(s, func(db DBTX) error {
		return UpdateMessageCompleted(db, id, responseText)
	})
}

func (s *SQLStore) IncrementMessageRetry(ctx context.Context, id, errorMsg string) error {
	return execDB(s, func(db DBTX) error {
		return IncrementMessageRetry(db, id, errorMsg)
	})
}

func (s *SQLStore) ResetMessageToPendingWithRestart(ctx context.Context, id, reason string) error {
	return execDB(s, func(db DBTX) error {
		return ResetMessageToPendingWithRestart(db, id, reason)
	})
}

func (s *SQLStore) GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]Message, error) {
	return queryDB(s, func(db DBTX) ([]Message, error) {
		msgs, err := GetPendingOrProcessingMessages(db)
		if err != nil {
			return nil, err
		}
		if limit > 0 && len(msgs) > limit {
			return msgs[:limit], nil
		}
		return msgs, nil
	})
}

func (s *SQLStore) GetMessage(ctx context.Context, id string) (*Message, error) {
	return queryDB(s, func(db DBTX) (*Message, error) {
		return GetMessage(db, id)
	})
}

func (s *SQLStore) MessageExists(ctx context.Context, id string) (bool, error) {
	return queryDB(s, func(db DBTX) (bool, error) {
		return MessageExists(db, id)
	})
}

func (s *SQLStore) ClaimPendingMessage(ctx context.Context, id string) (bool, error) {
	return queryDB(s, func(db DBTX) (bool, error) {
		return ClaimPendingMessage(db, id)
	})
}

func (s *SQLStore) GetActiveRecentThreadIDs(ctx context.Context, since time.Duration) ([]string, error) {
	return queryDB(s, func(db DBTX) ([]string, error) {
		return GetActiveRecentThreadIDs(db, since)
	})
}

func (s *SQLStore) GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]Message, error) {
	return queryDB(s, func(db DBTX) ([]Message, error) {
		return GetRecentThreadMessages(db, threadID, limit)
	})
}

func (s *SQLStore) GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error) {
	return queryDB(s, func(db DBTX) (int64, error) {
		return GetMaxMessageRowID(db, threadID)
	})
}

// SessionStore implementation
func (s *SQLStore) GetSessionID(ctx context.Context, threadID string) (string, error) {
	return queryDB(s, func(db DBTX) (string, error) {
		return GetSessionID(db, threadID)
	})
}

func (s *SQLStore) SaveSessionID(ctx context.Context, threadID, sessionID string) error {
	return execDB(s, func(db DBTX) error {
		return SaveSessionID(db, threadID, sessionID)
	})
}

func (s *SQLStore) IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	return queryDB(s, func(db DBTX) (int, error) {
		return IncrementSessionTurnCount(db, sessionKey)
	})
}

func (s *SQLStore) GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	return queryDB(s, func(db DBTX) (int, error) {
		return GetSessionTurnCount(db, sessionKey)
	})
}

func (s *SQLStore) RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error {
	return execDB(s, func(db DBTX) error {
		return RotateSessionID(db, sessionKey, newSessionID)
	})
}

func (s *SQLStore) FindUnrotatedSessions(ctx context.Context, minTurns int, maxAge ...time.Duration) ([]SessionInfo, error) {
	return queryDB(s, func(db DBTX) ([]SessionInfo, error) {
		return FindUnrotatedSessions(ctx, db, minTurns, maxAge...)
	})
}

func (s *SQLStore) GetPreviousSessionID(ctx context.Context, threadID string) (string, error) {
	return queryDB(s, func(db DBTX) (string, error) {
		return GetPreviousSessionID(db, threadID)
	})
}

func (s *SQLStore) GetActiveTasks(ctx context.Context) ([]ActiveTask, error) {
	return queryDB(s, func(db DBTX) ([]ActiveTask, error) {
		return GetActiveTasks(db)
	})
}

func (s *SQLStore) GetExternalConversationID(ctx context.Context, internalID string) (string, error) {
	return queryDB(s, func(db DBTX) (string, error) {
		return GetExternalConversationID(db, internalID)
	})
}

func (s *SQLStore) SaveConversationMapping(ctx context.Context, externalID, internalID string) error {
	return execDB(s, func(db DBTX) error {
		return SaveConversationMapping(db, externalID, internalID)
	})
}
