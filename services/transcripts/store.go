package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/transcriptwire"
)

// BatchDecision names the ingest path decideBatch selects for one step batch.
type BatchDecision string

const (
	// DecisionInsert handles the first batch seen for a session.
	DecisionInsert BatchDecision = "insert"
	// DecisionAppend adds new steps to an already indexed session.
	DecisionAppend BatchDecision = "append"
	// DecisionReset deletes then re-inserts after a file rewrite (same or newer epoch).
	DecisionReset BatchDecision = "reset"
	// DecisionStale acks without indexing: the batch predates the indexed epoch.
	DecisionStale BatchDecision = "stale"
	// DecisionDuplicateFinal re-publishes the indexed event for an already indexed Final batch.
	DecisionDuplicateFinal BatchDecision = "duplicate_final"
)

// BatchOutcome reports what ApplyBatch did with one step batch.
type BatchOutcome struct {
	Decision     BatchDecision
	StepsIndexed int
	Stale        bool
}

// sessionCursor is the indexed state decideBatch compares a batch against.
type sessionCursor struct {
	found        bool
	lastMtime    time.Time
	lastStep     int
	indexedFinal bool
}

// decideBatch is the pure ingest policy: one tx per batch starts from this
// decision. A batch whose FileMtime predates the indexed epoch is stale
// (gap batches ack without indexing). A Reset batch at or past the epoch
// deletes then re-inserts. An already indexed Final batch re-publishes.
func decideBatch(cur sessionCursor, batch transcriptwire.TranscriptStepBatch) BatchDecision {
	if !cur.found {
		return DecisionInsert
	}
	if batch.FileMtime.Before(cur.lastMtime) {
		return DecisionStale
	}
	if batch.Reset {
		// A redelivered Reset batch shares the indexed epoch. When the cursor
		// already covers its step range the steps are present, so treat it as
		// a duplicate re-publish instead of deleting then re-inserting: an
		// at-least-once replay after newer appends landed must not wipe them.
		// A strictly newer epoch, or a same-epoch rewrite that extends past
		// the cursor, still performs the delete-then-insert.
		if batch.FileMtime.After(cur.lastMtime) || batch.LastStep > cur.lastStep {
			return DecisionReset
		}
		return DecisionDuplicateFinal
	}
	if batch.Final && batch.LastStep <= cur.lastStep && cur.indexedFinal {
		return DecisionDuplicateFinal
	}
	return DecisionAppend
}

// Store persists transcript step batches into the shared stack-labelled index.
// Writes are always scoped to the store's own stack; reads take an explicit
// stack filter where empty means all stacks.
type Store struct {
	db    *sql.DB
	stack string
}

// NewStore binds a transcript store to a database, writing rows labelled with stack.
// An empty stack yields a read-only store.
func NewStore(database *sql.DB, stack string) *Store {
	return &Store{db: database, stack: strings.TrimSpace(stack)}
}

func (s *Store) checkDB() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("transcripts store database is nil")
	}
	return nil
}

func (s *Store) checkWriter() error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if s.stack == "" {
		return fmt.Errorf("transcript store stack cannot be empty for stack-scoped writes")
	}
	return nil
}

// forUpdateClause keeps row locking on PostgreSQL while staying runnable on
// SQLite hermetic fixtures, which reject FOR UPDATE.
func forUpdateClause(db *sql.DB) string {
	if isPostgresDriver(db.Driver()) {
		return " FOR UPDATE"
	}
	return ""
}

// isPostgresDriver reports whether the database/sql driver is the pgx stdlib
// bridge, mirroring the eri store check so hermetic SQLite fixtures take the
// no-lock path while production PostgreSQL takes row and advisory locks.
func isPostgresDriver(d driver.Driver) bool {
	if d == nil {
		return false
	}
	return strings.Contains(reflect.TypeOf(d).String(), "stdlib.Driver")
}

// ApplyBatch ingests one transcript step batch in a single transaction: it
// locks the session summary row, applies decideBatch, and reports the outcome.
// Stale batches commit nothing. Callers publish TranscriptsIndexedEvent after
// a non-stale outcome and must return publish errors so the batch naks.
func (s *Store) ApplyBatch(ctx context.Context, batch transcriptwire.TranscriptStepBatch) (outcome BatchOutcome, err error) {
	sessionID, checkErr := s.validatedBatchSession(batch)
	if checkErr != nil {
		return BatchOutcome{}, checkErr
	}
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	tx, beginErr := s.db.BeginTx(queryCtx, nil)
	if beginErr != nil {
		return BatchOutcome{}, fmt.Errorf("begin transcript batch tx: %w", beginErr)
	}
	committed := false
	defer func() {
		if !committed {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	cur, lockErr := lockSessionCursor(queryCtx, tx, s.db, s.stack, sessionID)
	if lockErr != nil {
		return BatchOutcome{}, lockErr
	}

	decision := decideBatch(cur, batch)
	if decision == DecisionStale {
		return BatchOutcome{Decision: DecisionStale, Stale: true}, nil
	}
	indexed, applyErr := applyBatchDecision(queryCtx, tx, s.stack, sessionID, cur, batch, decision)
	if applyErr != nil {
		return BatchOutcome{}, applyErr
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return BatchOutcome{}, fmt.Errorf("commit transcript batch: %w", commitErr)
	}
	committed = true
	return BatchOutcome{Decision: decision, StepsIndexed: indexed}, nil
}

// validatedBatchSession enforces writer and stack/session scoping for a batch.
func (s *Store) validatedBatchSession(batch transcriptwire.TranscriptStepBatch) (string, error) {
	if checkErr := s.checkWriter(); checkErr != nil {
		return "", checkErr
	}
	stack := strings.TrimSpace(batch.Stack)
	if stack == "" {
		stack = s.stack
	}
	if stack != s.stack {
		return "", fmt.Errorf("transcript batch stack %q does not match store stack %q", stack, s.stack)
	}
	sessionID := strings.TrimSpace(batch.SessionID)
	if sessionID == "" {
		return "", fmt.Errorf("transcript batch session id cannot be empty")
	}
	return sessionID, nil
}

// lockSessionCursor reads the indexed cursor for a session under a row lock.
func lockSessionCursor(ctx context.Context, tx *sql.Tx, db *sql.DB, stack, sessionID string) (sessionCursor, error) {
	var cur sessionCursor
	var rawMtime any
	var lastStep sql.NullInt64
	var isSettled sql.NullBool
	row := tx.QueryRowContext(ctx, `SELECT last_mtime, last_indexed_step, is_settled FROM session_summaries WHERE stack = $1 AND session_id = $2`+forUpdateClause(db), stack, sessionID)
	switch scanErr := row.Scan(&rawMtime, &lastStep, &isSettled); {
	case scanErr == nil:
		cur.found = true
		// any-typed scan plus parseDBTime keeps NULL and driver-specific
		// time encodings portable across PostgreSQL and SQLite fixtures.
		if t, ok := parseDBTime(rawMtime); ok {
			cur.lastMtime = t
		}
		if lastStep.Valid {
			cur.lastStep = int(lastStep.Int64)
		}
		cur.indexedFinal = isSettled.Valid && isSettled.Bool
		return cur, nil
	case errors.Is(scanErr, sql.ErrNoRows):
		return sessionCursor{}, nil
	default:
		return sessionCursor{}, fmt.Errorf("lock session summary row: %w", scanErr)
	}
}

// applyBatchDecision writes one non-stale decision inside the caller's tx.
func applyBatchDecision(ctx context.Context, tx *sql.Tx, stack, sessionID string, cur sessionCursor, batch transcriptwire.TranscriptStepBatch, decision BatchDecision) (int, error) {
	switch decision {
	case DecisionReset:
		return applyResetBatch(ctx, tx, stack, sessionID, batch)
	case DecisionInsert:
		return applyInsertBatch(ctx, tx, stack, sessionID, batch)
	case DecisionDuplicateFinal:
		return 0, nil
	default:
		return applyAppendBatch(ctx, tx, stack, sessionID, cur, batch)
	}
}

func applyResetBatch(ctx context.Context, tx *sql.Tx, stack, sessionID string, batch transcriptwire.TranscriptStepBatch) (int, error) {
	if _, err := tx.ExecContext(ctx, `DELETE FROM transcript_steps WHERE stack = $1 AND session_id = $2`, stack, sessionID); err != nil {
		return 0, fmt.Errorf("reset transcript steps: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO session_summaries (stack, session_id, engine, thread_id, last_indexed_step, last_mtime, is_settled, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
		 ON CONFLICT (stack, session_id) DO UPDATE SET engine = EXCLUDED.engine, thread_id = EXCLUDED.thread_id,
		 last_indexed_step = EXCLUDED.last_indexed_step, last_mtime = EXCLUDED.last_mtime, is_settled = EXCLUDED.is_settled,
		 updated_at = CURRENT_TIMESTAMP`,
		stack, sessionID, strings.TrimSpace(batch.Engine), strings.TrimSpace(batch.ThreadID),
		batch.LastStep, batch.FileMtime.UTC(), batch.Final); err != nil {
		return 0, fmt.Errorf("upsert reset session summary: %w", err)
	}
	return insertBatchSteps(ctx, tx, stack, sessionID, batch.Steps)
}

func applyInsertBatch(ctx context.Context, tx *sql.Tx, stack, sessionID string, batch transcriptwire.TranscriptStepBatch) (int, error) {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO session_summaries (stack, session_id, engine, thread_id, last_indexed_step, last_mtime, is_settled, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
		 ON CONFLICT (stack, session_id) DO NOTHING`,
		stack, sessionID, strings.TrimSpace(batch.Engine), strings.TrimSpace(batch.ThreadID),
		batch.LastStep, batch.FileMtime.UTC(), batch.Final); err != nil {
		return 0, fmt.Errorf("insert session summary: %w", err)
	}
	indexed, err := insertBatchSteps(ctx, tx, stack, sessionID, batch.Steps)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE session_summaries SET last_indexed_step = CASE WHEN last_indexed_step > $3 THEN last_indexed_step ELSE $3 END, is_settled = is_settled OR $4, updated_at = CURRENT_TIMESTAMP WHERE stack = $1 AND session_id = $2`,
		stack, sessionID, batch.LastStep, batch.Final); err != nil {
		return 0, fmt.Errorf("advance session summary: %w", err)
	}
	return indexed, nil
}

func applyAppendBatch(ctx context.Context, tx *sql.Tx, stack, sessionID string, cur sessionCursor, batch transcriptwire.TranscriptStepBatch) (int, error) {
	indexed, err := insertBatchSteps(ctx, tx, stack, sessionID, batch.Steps)
	if err != nil {
		return 0, err
	}
	mtime := batch.FileMtime.UTC()
	if !mtime.After(cur.lastMtime) {
		mtime = cur.lastMtime
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE session_summaries SET last_indexed_step = CASE WHEN last_indexed_step > $3 THEN last_indexed_step ELSE $3 END, last_mtime = $4, is_settled = is_settled OR $5, engine = CASE WHEN $6 <> '' THEN $6 ELSE engine END, thread_id = CASE WHEN $7 <> '' THEN $7 ELSE thread_id END, updated_at = CURRENT_TIMESTAMP WHERE stack = $1 AND session_id = $2`,
		stack, sessionID, batch.LastStep, mtime, batch.Final, strings.TrimSpace(batch.Engine), strings.TrimSpace(batch.ThreadID)); err != nil {
		return 0, fmt.Errorf("advance session summary: %w", err)
	}
	return indexed, nil
}

// insertBatchSteps writes batch steps idempotently: replays of a concurrent
// duplicate first batch collide on the primary key and are ignored, so at-least-once
// redelivery never double counts.
func insertBatchSteps(ctx context.Context, tx *sql.Tx, stack, sessionID string, steps []transcriptwire.TranscriptStep) (int, error) {
	if len(steps) == 0 {
		return 0, nil
	}
	indexed := 0
	for _, st := range steps {
		toolCallID := ""
		if len(st.ToolCalls) > 0 {
			toolCallID = strings.TrimSpace(st.ToolCalls[0].ID)
		}
		res, err := tx.ExecContext(ctx,
			`INSERT INTO transcript_steps (stack, session_id, step_index, step_type, tool_name, tool_kind, tool_call_id, content, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
			 ON CONFLICT (stack, session_id, step_index) DO NOTHING`,
			stack, sessionID, st.StepIndex, strings.TrimSpace(st.Type), strings.TrimSpace(st.ToolName),
			"", toolCallID, sanitizeTranscriptText(st.Content))
		if err != nil {
			return indexed, fmt.Errorf("insert transcript step %d: %w", st.StepIndex, err)
		}
		affected, rowsErr := res.RowsAffected()
		if rowsErr != nil {
			return indexed, fmt.Errorf("count inserted transcript step %d: %w", st.StepIndex, rowsErr)
		}
		if affected > 0 {
			indexed++
		}
	}
	return indexed, nil
}

func sanitizeTranscriptText(s string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "\uFFFD")
}

// summaryLockKey scopes the per-thread summarizer advisory lock.
func summaryLockKey(threadID string) string {
	return "transcripts-summary:" + strings.TrimSpace(threadID)
}

// AcquireThreadSummaryLock takes a session-level PostgreSQL advisory lock for
// one thread's idle summarization and returns an idempotent release closure.
// The release unlocks on a detached context with a short timeout so shutdown
// cancellation cannot strand the lock; when the unlock itself fails the
// connection is destroyed instead of returning a lock-holding session to the pool.
// Non-PostgreSQL drivers (SQLite hermetic fixtures) yield a no-op release.
func (s *Store) AcquireThreadSummaryLock(ctx context.Context, threadID string) (func(), error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(threadID) == "" {
		return nil, fmt.Errorf("thread id cannot be empty for summary lock")
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire summary lock conn: %w", err)
	}
	if !isPostgresDriver(s.db.Driver()) {
		return func() { httpkit.CloseWarn(conn, "summary lock fixture conn") }, nil
	}
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, summaryLockKey(threadID)); err != nil {
		httpkit.CloseWarn(conn, "summary lock conn after failed lock")
		return nil, fmt.Errorf("acquire summary lock: %w", err)
	}
	unlockQuery := `SELECT pg_advisory_unlock(hashtext($1))`
	lockKey := summaryLockKey(threadID)
	var once sync.Once
	return func() {
		once.Do(func() {
			// Release usually runs after the caller ctx is done; the unlock
			// gets its own detached timeout so it is never skipped.
			unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if _, unlockErr := conn.ExecContext(unlockCtx, unlockQuery, lockKey); unlockErr != nil {
				destroyConn(conn)
				return
			}
			httpkit.CloseWarn(conn, "summary lock conn")
		})
	}, nil
}

// destroyConn permanently destroys a pooled connection (for example one whose
// advisory unlock failed and may still hold the lock) instead of returning it
// for reuse. It hijacks the underlying pgx connection when available so the
// pool cannot hand the tainted session to another borrower.
func destroyConn(conn *sql.Conn) {
	if rawErr := conn.Raw(func(dc any) error {
		if pc, ok := dc.(*pgx.Conn); ok {
			if closeErr := pc.Close(context.Background()); closeErr != nil {
				log.Printf("[Transcripts] warning: failed to destroy summary lock conn: %v", closeErr)
			}
		}
		return nil
	}); rawErr != nil {
		log.Printf("[Transcripts] warning: failed to access summary lock conn for destroy: %v", rawErr)
	}
	httpkit.CloseWarn(conn, "summary lock conn after failed unlock")
}
