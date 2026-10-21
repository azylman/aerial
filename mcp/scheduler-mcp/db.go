package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var sqliteTestInitHook func(string) (*sql.DB, error)

func RegisterSQLiteTestHook(fn func(string) (*sql.DB, error)) { sqliteTestInitHook = fn }

type CronSchedule struct {
	ID          string    `json:"id"`
	TargetID    string    `json:"channel_id"`
	TitlePrefix string    `json:"title_prefix"`
	CronExpr    string    `json:"cron_expression"`
	Prompt      string    `json:"prompt"`
	Timezone    string    `json:"timezone"`
	NextRunAt   time.Time `json:"next_run_at"`
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"created_at"`
	Effort      string    `json:"effort,omitempty"`
}

type OneShotSchedule struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	Prompt    string    `json:"prompt"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
	Effort    string    `json:"effort,omitempty"`
}

const migrationLockID = 849201948201

func NewDB(cfg *Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	return initDB(cfg.DatabaseURL)
}

func InitDB(cfg *Config) (*sql.DB, error) { return NewDB(cfg) }

func closeWarn(c io.Closer, resource string) {
	if c != nil {
		if err := c.Close(); err != nil {
			log.Printf("[Scheduler DB] Warning closing %s: %v", resource, err)
		}
	}
}

func isPostgres(database *sql.DB) bool {
	if database == nil {
		return false
	}
	drv := fmt.Sprintf("%T", database.Driver())
	return strings.Contains(drv, "stdlib") || strings.Contains(drv, "pgx")
}

func rebindQuery(query string, isPg bool) string {
	if !isPg {
		return query
	}
	var b strings.Builder
	paramIdx := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteString(fmt.Sprintf("$%d", paramIdx))
			paramIdx++
		} else {
			b.WriteByte(query[i])
		}
	}
	return b.String()
}

var (
	postgresMaxAttempts = 10
	postgresRetryBase   = 500 * time.Millisecond
)

func initDB(dsn string) (*sql.DB, error) {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return nil, fmt.Errorf("db: connection string cannot be empty")
	}
	trimmed = strings.TrimPrefix(trimmed, "sqlite://")
	if sqliteTestInitHook != nil && (trimmed == ":memory:" || strings.Contains(trimmed, ".db") || strings.Contains(trimmed, "sqlite") || strings.HasPrefix(trimmed, "file:")) {
		return sqliteTestInitHook(trimmed)
	}
	if !strings.HasPrefix(trimmed, "postgres://") && !strings.HasPrefix(trimmed, "postgresql://") {
		return nil, fmt.Errorf("db: unsupported database scheme in %q: only postgres:// or postgresql:// supported", trimmed)
	}

	var database *sql.DB
	var err error
	maxAttempts, backoff := postgresMaxAttempts, postgresRetryBase
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		database, err = sql.Open("pgx", trimmed)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = database.PingContext(ctx)
			cancel()
			if err == nil {
				break
			}
			closeWarn(database, "database on ping failure")
		}
		log.Printf("[Scheduler DB] Waiting for PostgreSQL (attempt %d/%d): %v", attempt, maxAttempts, err)
		time.Sleep(backoff)
		backoff = time.Duration(float64(backoff) * 1.5)
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL after %d attempts: %w", maxAttempts, err)
	}

	database.SetMaxOpenConns(10)
	database.SetMaxIdleConns(5)
	database.SetConnMaxLifetime(30 * time.Minute)
	database.SetConnMaxIdleTime(5 * time.Minute)

	var success bool
	defer func() {
		if !success {
			closeWarn(database, "database on initialization failure")
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := database.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection for migrations: %w", err)
	}
	defer closeWarn(conn, "migration conn")

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1);", migrationLockID); err != nil {
		return nil, fmt.Errorf("failed to acquire migration advisory lock: %w", err)
	}
	defer func() {
		if _, unlockErr := conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1);", migrationLockID); unlockErr != nil {
			log.Printf("[Scheduler DB] Warning releasing migration advisory lock: %v", unlockErr)
		}
	}()

	schema := `CREATE TABLE IF NOT EXISTS cron_schedules (
		id TEXT PRIMARY KEY, target_id TEXT NOT NULL, title_prefix TEXT NOT NULL DEFAULT '',
		cron_expr TEXT NOT NULL, prompt TEXT NOT NULL, timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
		next_run_at TIMESTAMPTZ NOT NULL, enabled BOOLEAN NOT NULL DEFAULT TRUE, effort TEXT NOT NULL DEFAULT 'low',
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);
	CREATE TABLE IF NOT EXISTS one_shot_schedules (
		id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, prompt TEXT NOT NULL,
		run_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, effort TEXT NOT NULL DEFAULT 'low'
	);
	CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);`
	if _, err := conn.ExecContext(ctx, schema); err != nil {
		return nil, fmt.Errorf("failed to execute postgres schema: %w", err)
	}

	log.Printf("[Scheduler DB] PostgreSQL initialized successfully at %s", trimmed)
	success = true
	return database, nil
}

func normCtxAndDB(ctx context.Context, database *sql.DB) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	return ctx, nil
}

func normalizeScheduleEffort(effort string) string {
	switch eff := strings.ToLower(strings.TrimSpace(effort)); eff {
	case "low", "medium", "high":
		return eff
	default:
		return "medium"
	}
}

func InsertCronSchedule(ctx context.Context, database *sql.DB, c CronSchedule) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	if c.Timezone == "" {
		c.Timezone = "America/Los_Angeles"
	}
	query := rebindQuery(`INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, isPostgres(database))
	_, err = database.ExecContext(ctx, query, c.ID, c.TargetID, c.TitlePrefix, c.CronExpr, c.Prompt, c.Timezone, c.NextRunAt, c.Enabled, c.CreatedAt, normalizeScheduleEffort(c.Effort))
	return err
}

func InsertOneShotSchedule(ctx context.Context, database *sql.DB, s OneShotSchedule) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	query := rebindQuery(`INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES (?, ?, ?, ?, ?, ?)`, isPostgres(database))
	_, err = database.ExecContext(ctx, query, s.ID, s.ThreadID, s.Prompt, s.RunAt, s.CreatedAt, normalizeScheduleEffort(s.Effort))
	return err
}

func querySchedules[T any](ctx context.Context, database *sql.DB, baseQuery, filterCol, targetID, orderCol string, scanFn func(*sql.Rows) (T, error)) ([]T, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	query := baseQuery
	var rows *sql.Rows
	if targetID != "" {
		sep := " WHERE "
		if strings.Contains(query, "WHERE") {
			sep = " AND "
		}
		query = fmt.Sprintf("%s%s%s = ? ORDER BY %s ASC", query, sep, filterCol, orderCol)
		rows, err = database.QueryContext(ctx, rebindQuery(query, isPostgres(database)), targetID)
	} else {
		query = fmt.Sprintf("%s ORDER BY %s ASC", query, orderCol)
		rows, err = database.QueryContext(ctx, rebindQuery(query, isPostgres(database)))
	}
	if err != nil {
		return nil, err
	}
	defer closeWarn(rows, "rows")

	var results []T
	for rows.Next() {
		item, err := scanFn(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, item)
	}
	return results, rows.Err()
}

func ListCronSchedules(ctx context.Context, database *sql.DB, targetID string) ([]CronSchedule, error) {
	baseQuery := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM cron_schedules WHERE enabled = TRUE`
	return querySchedules(ctx, database, baseQuery, "target_id", targetID, "created_at", func(rows *sql.Rows) (CronSchedule, error) {
		var c CronSchedule
		err := rows.Scan(&c.ID, &c.TargetID, &c.TitlePrefix, &c.CronExpr, &c.Prompt, &c.Timezone, &c.NextRunAt, &c.Enabled, &c.CreatedAt, &c.Effort)
		return c, err
	})
}

func ListOneShotSchedules(ctx context.Context, database *sql.DB, targetID string) ([]OneShotSchedule, error) {
	baseQuery := `SELECT id, thread_id, prompt, run_at, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM one_shot_schedules`
	return querySchedules(ctx, database, baseQuery, "thread_id", targetID, "run_at", func(rows *sql.Rows) (OneShotSchedule, error) {
		var s OneShotSchedule
		err := rows.Scan(&s.ID, &s.ThreadID, &s.Prompt, &s.RunAt, &s.CreatedAt, &s.Effort)
		return s, err
	})
}

func UpdateCronSchedule(ctx context.Context, database *sql.DB, id string, effort, cronExpr, prompt, titlePrefix, timezone *string, nextRunAt *time.Time) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	if id = strings.TrimSpace(id); id == "" {
		return fmt.Errorf("schedule_id cannot be empty")
	}

	var sets []string
	var args []interface{}
	addSet := func(col string, val *string) {
		if val != nil && strings.TrimSpace(*val) != "" {
			sets = append(sets, col+" = ?")
			args = append(args, strings.TrimSpace(*val))
		}
	}
	if effort != nil {
		sets = append(sets, "effort = ?")
		args = append(args, normalizeScheduleEffort(*effort))
	}
	addSet("cron_expr", cronExpr)
	addSet("prompt", prompt)
	if titlePrefix != nil {
		sets = append(sets, "title_prefix = ?")
		args = append(args, strings.TrimSpace(*titlePrefix))
	}
	addSet("timezone", timezone)
	if nextRunAt != nil && !nextRunAt.IsZero() {
		sets = append(sets, "next_run_at = ?")
		args = append(args, *nextRunAt)
	}

	if len(sets) == 0 {
		return ctx.Err()
	}

	query := fmt.Sprintf("UPDATE cron_schedules SET %s WHERE id = ?", strings.Join(sets, ", "))
	args = append(args, id)
	res, err := database.ExecContext(ctx, rebindQuery(query, isPostgres(database)), args...)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to determine rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("cron schedule %q not found", id)
	}
	return nil
}

func DeleteSchedule(ctx context.Context, database *sql.DB, scheduleID string) (bool, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return false, err
	}
	var total int64
	for _, tbl := range []string{"cron_schedules", "one_shot_schedules"} {
		res, err := database.ExecContext(ctx, rebindQuery("DELETE FROM "+tbl+" WHERE id = ?", isPostgres(database)), scheduleID)
		if err != nil {
			return false, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			log.Printf("[Scheduler DB] Warning getting %s rows affected: %v", tbl, err)
		}
		total += rows
	}
	return total > 0, nil
}
