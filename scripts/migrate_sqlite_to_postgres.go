package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pgvector/pgvector-go"
	_ "modernc.org/sqlite"
)

const ExpectedEmbeddingDim = 384

func bytesToFloat32(b []byte) []float32 {
	if len(b) == 0 || len(b)%4 != 0 {
		return nil
	}
	count := len(b) / 4
	slice := make([]float32, count)
	for i := 0; i < count; i++ {
		bits := binary.LittleEndian.Uint32(b[i*4 : (i+1)*4])
		slice[i] = math.Float32frombits(bits)
	}
	return slice
}

func parseTime(val any) (time.Time, bool) {
	if val == nil {
		return time.Time{}, false
	}
	switch v := val.(type) {
	case time.Time:
		return v.UTC(), true
	case string:
		v = strings.TrimSpace(v)
		if v == "" {
			return time.Time{}, false
		}
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999-07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, v); err == nil {
				return t.UTC(), true
			}
		}
	}
	return time.Time{}, false
}

func tableExistsInSQLite(db *sql.DB, tableName string) bool {
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", tableName).Scan(&count)
	return count > 0
}

func main() {
	sqlitePath := flag.String("sqlite", "/data/aerial.db", "Path to source SQLite database file")
	postgresDSN := flag.String("postgres", "", "Target PostgreSQL DSN (defaults to DATABASE_URL env var)")
	batchSize := flag.Int("batch-size", 200, "Batch insert chunk size")
	flag.Parse()

	targetDSN := *postgresDSN
	if targetDSN == "" {
		targetDSN = os.Getenv("DATABASE_URL")
	}
	if targetDSN == "" {
		targetDSN = os.Getenv("TEST_DATABASE_URL")
	}
	if targetDSN == "" {
		log.Fatal("ERROR: Target PostgreSQL DSN must be provided via -postgres or DATABASE_URL env var")
	}

	if _, err := os.Stat(*sqlitePath); os.IsNotExist(err) {
		log.Fatalf("ERROR: SQLite database file not found at %s", *sqlitePath)
	}

	log.Printf("===============================================================")
	log.Printf("   AERIAL SQLITE -> POSTGRESQL + PGVECTOR DATA MIGRATION")
	log.Printf("===============================================================")
	log.Printf("Source SQLite:     %s", *sqlitePath)
	log.Printf("Target PostgreSQL: %s", targetDSN)
	log.Printf("Batch Size:        %d", *batchSize)
	log.Printf("---------------------------------------------------------------")

	// 1. Open SQLite (Read-Only WAL mode)
	sqliteDSN := *sqlitePath
	if !strings.Contains(sqliteDSN, "?") {
		sqliteDSN += "?_pragma=busy_timeout(5000)&_pragma=query_only(true)&_pragma=journal_mode(WAL)"
	} else {
		sqliteDSN += "&_pragma=busy_timeout(5000)&_pragma=query_only(true)&_pragma=journal_mode(WAL)"
	}

	sqliteDB, err := sql.Open("sqlite", sqliteDSN)
	if err != nil {
		log.Fatalf("Failed to open SQLite database: %v", err)
	}
	defer sqliteDB.Close()

	if err := sqliteDB.Ping(); err != nil {
		log.Fatalf("Failed to ping SQLite database: %v", err)
	}

	// 2. Open PostgreSQL
	pgDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		log.Fatalf("Failed to open PostgreSQL database: %v", err)
	}
	defer pgDB.Close()

	if err := pgDB.Ping(); err != nil {
		log.Fatalf("Failed to ping PostgreSQL database: %v", err)
	}

	// 3. Acquire migration advisory lock
	const migrationLockID = 849201948201
	ctx := context.Background()
	log.Println("[Migration] Acquiring PostgreSQL advisory lock...")
	if _, err := pgDB.ExecContext(ctx, "SELECT pg_advisory_lock($1);", migrationLockID); err != nil {
		log.Fatalf("Failed to acquire advisory lock: %v", err)
	}
	defer func() {
		_, _ = pgDB.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1);", migrationLockID)
		log.Println("[Migration] Released PostgreSQL advisory lock.")
	}()

	// 4. Ensure schema exists
	log.Println("[Migration] Ensuring PostgreSQL schema and pgvector extension exist...")
	schema := `
	CREATE EXTENSION IF NOT EXISTS vector;

	CREATE TABLE IF NOT EXISTS messages (
		id TEXT PRIMARY KEY,
		row_id BIGSERIAL UNIQUE,
		thread_id TEXT NOT NULL DEFAULT '',
		guild_id TEXT NOT NULL DEFAULT '',
		author_id TEXT NOT NULL DEFAULT '',
		author_name TEXT NOT NULL DEFAULT '',
		content TEXT NOT NULL DEFAULT '',
		summary TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'PENDING',
		retry_count INTEGER NOT NULL DEFAULT 0,
		error_message TEXT,
		response_text TEXT,
		schedule_run_id TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_messages_thread_status_created ON messages(thread_id, status, created_at);
	CREATE INDEX IF NOT EXISTS idx_messages_status_created ON messages(status, created_at);
	CREATE INDEX IF NOT EXISTS idx_messages_row_id ON messages(row_id);

	CREATE TABLE IF NOT EXISTS sessions (
		thread_id TEXT PRIMARY KEY,
		internal_session_id TEXT NOT NULL DEFAULT '',
		turn_count INTEGER NOT NULL DEFAULT 0,
		last_extracted_rowid BIGINT NOT NULL DEFAULT 0,
		fact_extracted_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS one_shot_schedules (
		id TEXT PRIMARY KEY,
		thread_id TEXT NOT NULL,
		prompt TEXT NOT NULL,
		run_at TIMESTAMPTZ NOT NULL,
		created_at TIMESTAMPTZ NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);

	CREATE TABLE IF NOT EXISTS cron_schedules (
		id TEXT PRIMARY KEY,
		target_id TEXT NOT NULL,
		title_prefix TEXT NOT NULL DEFAULT '',
		cron_expr TEXT NOT NULL,
		prompt TEXT NOT NULL,
		timezone TEXT NOT NULL DEFAULT 'UTC',
		next_run_at TIMESTAMPTZ NOT NULL,
		enabled BOOLEAN NOT NULL DEFAULT TRUE,
		created_at TIMESTAMPTZ NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);

	CREATE TABLE IF NOT EXISTS schedule_runs (
		id TEXT PRIMARY KEY,
		schedule_id TEXT NOT NULL,
		schedule_type TEXT NOT NULL,
		message_id TEXT NOT NULL DEFAULT '',
		target_id TEXT NOT NULL,
		thread_id TEXT NOT NULL,
		title TEXT NOT NULL DEFAULT '',
		prompt TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'enqueued',
		started_at TIMESTAMPTZ NOT NULL,
		completed_at TIMESTAMPTZ,
		duration_ms BIGINT DEFAULT 0,
		error TEXT NOT NULL DEFAULT ''
	);
	CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_id ON schedule_runs(schedule_id);

	CREATE TABLE IF NOT EXISTS facts (
		id BIGSERIAL PRIMARY KEY,
		category TEXT NOT NULL DEFAULT 'general',
		fact_text TEXT NOT NULL,
		importance DOUBLE PRECISION NOT NULL DEFAULT 1.0,
		thread_id TEXT NOT NULL DEFAULT '',
		embedding vector(384),
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_facts_category ON facts(category);
	CREATE INDEX IF NOT EXISTS idx_facts_created_at ON facts(created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_facts_embedding_hnsw ON facts USING hnsw (embedding vector_cosine_ops);
	`
	if _, err := pgDB.ExecContext(ctx, schema); err != nil {
		log.Fatalf("Failed to initialize target PostgreSQL schema: %v", err)
	}

	// 5. Migrate Messages
	if tableExistsInSQLite(sqliteDB, "messages") {
		log.Println("[Migration] Migrating `messages` table...")
		rows, err := sqliteDB.Query(`
			SELECT id, thread_id, guild_id, author_id, author_name, content, 
			       COALESCE(summary, ''), status, retry_count, error_message, response_text, 
			       COALESCE(schedule_run_id, ''), created_at, updated_at 
			FROM messages ORDER BY created_at ASC
		`)
		if err != nil {
			log.Fatalf("Failed to query SQLite messages: %v", err)
		}
		defer rows.Close()

		msgCount := 0
		stmt, err := pgDB.PrepareContext(ctx, `
			INSERT INTO messages (id, thread_id, guild_id, author_id, author_name, content, summary, status, retry_count, error_message, response_text, schedule_run_id, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
			ON CONFLICT (id) DO UPDATE SET
				thread_id = EXCLUDED.thread_id,
				guild_id = EXCLUDED.guild_id,
				author_id = EXCLUDED.author_id,
				author_name = EXCLUDED.author_name,
				content = EXCLUDED.content,
				summary = EXCLUDED.summary,
				status = EXCLUDED.status,
				retry_count = EXCLUDED.retry_count,
				error_message = EXCLUDED.error_message,
				response_text = EXCLUDED.response_text,
				schedule_run_id = EXCLUDED.schedule_run_id,
				updated_at = EXCLUDED.updated_at
		`)
		if err != nil {
			log.Fatalf("Failed to prepare PG insert message: %v", err)
		}
		defer stmt.Close()

		for rows.Next() {
			var id, threadID, guildID, authorID, authorName, content, summary, status, schedRunID string
			var retryCount int
			var errMsg, respText sql.NullString
			var rawCreatedAt, rawUpdatedAt any

			if err := rows.Scan(&id, &threadID, &guildID, &authorID, &authorName, &content, &summary, &status, &retryCount, &errMsg, &respText, &schedRunID, &rawCreatedAt, &rawUpdatedAt); err != nil {
				log.Fatalf("Failed to scan message row: %v", err)
			}

			createdAt, ok := parseTime(rawCreatedAt)
			if !ok {
				createdAt = time.Now().UTC()
			}
			updatedAt, ok := parseTime(rawUpdatedAt)
			if !ok {
				updatedAt = createdAt
			}

			_, err = stmt.ExecContext(ctx, id, threadID, guildID, authorID, authorName, content, summary, status, retryCount, errMsg, respText, schedRunID, createdAt, updatedAt)
			if err != nil {
				log.Fatalf("Failed to insert message %s: %v", id, err)
			}
			msgCount++
		}
		log.Printf("[Migration] Migrated %d message(s) successfully.", msgCount)
	}

	// 6. Migrate Sessions
	if tableExistsInSQLite(sqliteDB, "sessions") {
		log.Println("[Migration] Migrating `sessions` table...")
		rows, err := sqliteDB.Query(`
			SELECT thread_id, internal_session_id, turn_count, 
			       COALESCE(last_extracted_rowid, 0), fact_extracted_at, created_at, updated_at 
			FROM sessions ORDER BY created_at ASC
		`)
		if err != nil {
			log.Fatalf("Failed to query SQLite sessions: %v", err)
		}
		defer rows.Close()

		sessCount := 0
		stmt, err := pgDB.PrepareContext(ctx, `
			INSERT INTO sessions (thread_id, internal_session_id, turn_count, last_extracted_rowid, fact_extracted_at, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (thread_id) DO UPDATE SET
				internal_session_id = EXCLUDED.internal_session_id,
				turn_count = EXCLUDED.turn_count,
				last_extracted_rowid = EXCLUDED.last_extracted_rowid,
				fact_extracted_at = EXCLUDED.fact_extracted_at,
				updated_at = EXCLUDED.updated_at
		`)
		if err != nil {
			log.Fatalf("Failed to prepare PG insert session: %v", err)
		}
		defer stmt.Close()

		for rows.Next() {
			var threadID, sessionID string
			var turnCount int
			var lastRowID int64
			var rawFactExtractedAt, rawCreatedAt, rawUpdatedAt any

			if err := rows.Scan(&threadID, &sessionID, &turnCount, &lastRowID, &rawFactExtractedAt, &rawCreatedAt, &rawUpdatedAt); err != nil {
				log.Fatalf("Failed to scan session row: %v", err)
			}

			createdAt, ok := parseTime(rawCreatedAt)
			if !ok {
				createdAt = time.Now().UTC()
			}
			updatedAt, ok := parseTime(rawUpdatedAt)
			if !ok {
				updatedAt = createdAt
			}
			var factExtractedAt sql.NullTime
			if t, ok := parseTime(rawFactExtractedAt); ok {
				factExtractedAt = sql.NullTime{Time: t, Valid: true}
			}

			_, err = stmt.ExecContext(ctx, threadID, sessionID, turnCount, lastRowID, factExtractedAt, createdAt, updatedAt)
			if err != nil {
				log.Fatalf("Failed to insert session %s: %v", threadID, err)
			}
			sessCount++
		}
		log.Printf("[Migration] Migrated %d session(s) successfully.", sessCount)
	}

	// 7. Migrate Cron Schedules
	if tableExistsInSQLite(sqliteDB, "cron_schedules") {
		log.Println("[Migration] Migrating `cron_schedules` table...")
		rows, err := sqliteDB.Query(`
			SELECT id, target_id, COALESCE(title_prefix, ''), cron_expr, prompt, 
			       COALESCE(timezone, 'UTC'), next_run_at, enabled, created_at 
			FROM cron_schedules ORDER BY created_at ASC
		`)
		if err != nil {
			log.Fatalf("Failed to query SQLite cron_schedules: %v", err)
		}
		defer rows.Close()

		cronCount := 0
		stmt, err := pgDB.PrepareContext(ctx, `
			INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (id) DO UPDATE SET
				target_id = EXCLUDED.target_id,
				title_prefix = EXCLUDED.title_prefix,
				cron_expr = EXCLUDED.cron_expr,
				prompt = EXCLUDED.prompt,
				timezone = EXCLUDED.timezone,
				next_run_at = EXCLUDED.next_run_at,
				enabled = EXCLUDED.enabled
		`)
		if err != nil {
			log.Fatalf("Failed to prepare PG insert cron_schedule: %v", err)
		}
		defer stmt.Close()

		for rows.Next() {
			var id, targetID, titlePrefix, cronExpr, prompt, timezone string
			var enabled bool
			var rawNextRunAt, rawCreatedAt any

			if err := rows.Scan(&id, &targetID, &titlePrefix, &cronExpr, &prompt, &timezone, &rawNextRunAt, &enabled, &rawCreatedAt); err != nil {
				log.Fatalf("Failed to scan cron schedule row: %v", err)
			}

			nextRunAt, ok := parseTime(rawNextRunAt)
			if !ok {
				nextRunAt = time.Now().UTC()
			}
			createdAt, ok := parseTime(rawCreatedAt)
			if !ok {
				createdAt = time.Now().UTC()
			}

			_, err = stmt.ExecContext(ctx, id, targetID, titlePrefix, cronExpr, prompt, timezone, nextRunAt, enabled, createdAt)
			if err != nil {
				log.Fatalf("Failed to insert cron schedule %s: %v", id, err)
			}
			cronCount++
		}
		log.Printf("[Migration] Migrated %d cron schedule(s) successfully.", cronCount)
	}

	// 8. Migrate One-Shot Schedules
	if tableExistsInSQLite(sqliteDB, "one_shot_schedules") {
		log.Println("[Migration] Migrating `one_shot_schedules` table...")
		rows, err := sqliteDB.Query(`
			SELECT id, thread_id, prompt, run_at, created_at 
			FROM one_shot_schedules ORDER BY created_at ASC
		`)
		if err != nil {
			log.Fatalf("Failed to query SQLite one_shot_schedules: %v", err)
		}
		defer rows.Close()

		oneShotCount := 0
		stmt, err := pgDB.PrepareContext(ctx, `
			INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (id) DO UPDATE SET
				thread_id = EXCLUDED.thread_id,
				prompt = EXCLUDED.prompt,
				run_at = EXCLUDED.run_at
		`)
		if err != nil {
			log.Fatalf("Failed to prepare PG insert one_shot_schedule: %v", err)
		}
		defer stmt.Close()

		for rows.Next() {
			var id, threadID, prompt string
			var rawRunAt, rawCreatedAt any

			if err := rows.Scan(&id, &threadID, &prompt, &rawRunAt, &rawCreatedAt); err != nil {
				log.Fatalf("Failed to scan one-shot schedule row: %v", err)
			}

			runAt, ok := parseTime(rawRunAt)
			if !ok {
				runAt = time.Now().UTC()
			}
			createdAt, ok := parseTime(rawCreatedAt)
			if !ok {
				createdAt = time.Now().UTC()
			}

			_, err = stmt.ExecContext(ctx, id, threadID, prompt, runAt, createdAt)
			if err != nil {
				log.Fatalf("Failed to insert one-shot schedule %s: %v", id, err)
			}
			oneShotCount++
		}
		log.Printf("[Migration] Migrated %d one-shot schedule(s) successfully.", oneShotCount)
	}

	// 9. Migrate Schedule Runs
	if tableExistsInSQLite(sqliteDB, "schedule_runs") {
		log.Println("[Migration] Migrating `schedule_runs` table...")
		rows, err := sqliteDB.Query(`
			SELECT id, schedule_id, schedule_type, message_id, target_id, thread_id, 
			       title, prompt, status, started_at, completed_at, duration_ms, error 
			FROM schedule_runs ORDER BY started_at ASC
		`)
		if err != nil {
			log.Fatalf("Failed to query SQLite schedule_runs: %v", err)
		}
		defer rows.Close()

		runCount := 0
		stmt, err := pgDB.PrepareContext(ctx, `
			INSERT INTO schedule_runs (id, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, completed_at, duration_ms, error)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (id) DO UPDATE SET
				status = EXCLUDED.status,
				completed_at = EXCLUDED.completed_at,
				duration_ms = EXCLUDED.duration_ms,
				error = EXCLUDED.error
		`)
		if err != nil {
			log.Fatalf("Failed to prepare PG insert schedule_run: %v", err)
		}
		defer stmt.Close()

		for rows.Next() {
			var id, schedID, schedType, msgID, targetID, threadID, title, prompt, status, errStr string
			var durationMs int64
			var rawStartedAt, rawCompletedAt any

			if err := rows.Scan(&id, &schedID, &schedType, &msgID, &targetID, &threadID, &title, &prompt, &status, &rawStartedAt, &rawCompletedAt, &durationMs, &errStr); err != nil {
				log.Fatalf("Failed to scan schedule run row: %v", err)
			}

			startedAt, ok := parseTime(rawStartedAt)
			if !ok {
				startedAt = time.Now().UTC()
			}
			var completedAt sql.NullTime
			if t, ok := parseTime(rawCompletedAt); ok {
				completedAt = sql.NullTime{Time: t, Valid: true}
			}

			_, err = stmt.ExecContext(ctx, id, schedID, schedType, msgID, targetID, threadID, title, prompt, status, startedAt, completedAt, durationMs, errStr)
			if err != nil {
				log.Fatalf("Failed to insert schedule run %s: %v", id, err)
			}
			runCount++
		}
		log.Printf("[Migration] Migrated %d schedule run(s) successfully.", runCount)
	}

	// 10. Migrate Facts with Vector BLOB to pgvector translation
	if tableExistsInSQLite(sqliteDB, "facts") {
		log.Println("[Migration] Migrating `facts` table with vector translation...")
		rows, err := sqliteDB.Query(`
			SELECT id, category, fact_text, importance, COALESCE(thread_id, ''), embedding, created_at 
			FROM facts ORDER BY id ASC
		`)
		if err != nil {
			log.Fatalf("Failed to query SQLite facts: %v", err)
		}
		defer rows.Close()

		factCount := 0
		vecCount := 0
		stmt, err := pgDB.PrepareContext(ctx, `
			INSERT INTO facts (id, category, fact_text, importance, thread_id, embedding, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (id) DO UPDATE SET
				category = EXCLUDED.category,
				fact_text = EXCLUDED.fact_text,
				importance = EXCLUDED.importance,
				thread_id = EXCLUDED.thread_id,
				embedding = EXCLUDED.embedding
		`)
		if err != nil {
			log.Fatalf("Failed to prepare PG insert fact: %v", err)
		}
		defer stmt.Close()

		for rows.Next() {
			var id int64
			var category, factText, threadID string
			var importance float64
			var embBytes []byte
			var rawCreatedAt any

			if err := rows.Scan(&id, &category, &factText, &importance, &threadID, &embBytes, &rawCreatedAt); err != nil {
				log.Fatalf("Failed to scan fact row: %v", err)
			}

			createdAt, ok := parseTime(rawCreatedAt)
			if !ok {
				createdAt = time.Now().UTC()
			}

			var vecVal any = nil
			if len(embBytes) > 0 {
				floats := bytesToFloat32(embBytes)
				if len(floats) == ExpectedEmbeddingDim {
					vecVal = pgvector.NewVector(floats)
					vecCount++
				}
			}

			_, err = stmt.ExecContext(ctx, id, category, factText, importance, threadID, vecVal, createdAt)
			if err != nil {
				log.Fatalf("Failed to insert fact ID %d: %v", id, err)
			}
			factCount++
		}
		log.Printf("[Migration] Migrated %d fact(s) successfully (%d with pgvector embeddings).", factCount, vecCount)
	}

	// 11. Resynchronize sequences
	log.Println("[Migration] Resynchronizing PostgreSQL serial sequences...")
	_, err = pgDB.ExecContext(ctx, `
		SELECT setval(pg_get_serial_sequence('facts', 'id'), COALESCE((SELECT MAX(id) FROM facts), 1), (SELECT COUNT(*) > 0 FROM facts));
		SELECT setval(pg_get_serial_sequence('messages', 'row_id'), COALESCE((SELECT MAX(row_id) FROM messages), 1), (SELECT COUNT(*) > 0 FROM messages));
	`)
	if err != nil {
		log.Fatalf("Failed to resynchronize sequences: %v", err)
	}

	// 12. Verification and Row Count Reconciliation
	log.Printf("\n===============================================================")
	log.Printf("   RECONCILIATION AUDIT & ROW COUNT VERIFICATION")
	log.Printf("===============================================================")
	tables := []string{"messages", "sessions", "cron_schedules", "one_shot_schedules", "schedule_runs", "facts"}
	allMatch := true

	for _, table := range tables {
		sqliteCount := 0
		if tableExistsInSQLite(sqliteDB, table) {
			_ = sqliteDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&sqliteCount)
		}
		pgCount := 0
		_ = pgDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&pgCount)

		status := "MATCH"
		if sqliteCount != pgCount {
			status = "MISMATCH"
			allMatch = false
		}
		log.Printf("Table: %-20s | SQLite: %6d | PostgreSQL: %6d | Status: %s", table, sqliteCount, pgCount, status)
	}
	log.Printf("---------------------------------------------------------------")

	if allMatch {
		log.Printf("🎉 MIGRATION VERIFICATION SUCCESSFUL! All row counts match 100%%.")
	} else {
		log.Printf("⚠️ MIGRATION COMPLETED WITH WARNINGS: Some table counts mismatched.")
	}
}
