package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var sqliteTestInitHook func(string) (*sql.DB, error)

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

type HeartbeatSchedule struct {
	ID          string    `json:"id"`
	TargetID    string    `json:"channel_id"`
	TitlePrefix string    `json:"title_prefix"`
	CronExpr    string    `json:"cron_expression"`
	Prompt      string    `json:"prompt"`
	Timezone    string    `json:"timezone"`
	Effort      string    `json:"effort,omitempty"`
	NextRunAt   time.Time `json:"next_run_at"`
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"created_at"`
}

type ScheduleRun struct {
	ID           string     `json:"id"`
	ScheduleID   string     `json:"schedule_id"`
	ScheduleType string     `json:"schedule_type"`
	MessageID    string     `json:"message_id"`
	TargetID     string     `json:"target_id"`
	ThreadID     string     `json:"thread_id"`
	Title        string     `json:"title"`
	Prompt       string     `json:"prompt"`
	Status       string     `json:"status"`
	StartedAt    time.Time  `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	DurationMs   int64      `json:"duration_ms"`
	Error        string     `json:"error,omitempty"`
	Effort       string     `json:"effort,omitempty"`
	Model        string     `json:"model,omitempty"`
}

func OpenDB(cfg *Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	return openDB(cfg.DatabaseURL)
}

func NewDB(cfg *Config) (*sql.DB, error) {
	return OpenDB(cfg)
}

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

func openDB(dsn string) (*sql.DB, error) {
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

	log.Printf("[Scheduler DB] PostgreSQL initialized successfully at %s", trimmed)
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

func InsertHeartbeatSchedule(ctx context.Context, database *sql.DB, h HeartbeatSchedule) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	if h.CreatedAt.IsZero() {
		h.CreatedAt = time.Now().UTC()
	}
	if h.Timezone == "" {
		h.Timezone = "UTC"
	}
	effort, err := NormalizeEffort(h.Effort)
	if err != nil {
		effort = "low"
	}
	query := rebindQuery(`INSERT INTO heartbeat_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, effort, next_run_at, enabled, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, isPostgres(database))
	_, err = database.ExecContext(ctx, query, h.ID, h.TargetID, h.TitlePrefix, h.CronExpr, h.Prompt, h.Timezone, effort, h.NextRunAt, h.Enabled, h.CreatedAt)
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

func ListHeartbeatSchedules(ctx context.Context, database *sql.DB, targetID string) ([]HeartbeatSchedule, error) {
	baseQuery := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'low') FROM heartbeat_schedules WHERE enabled = TRUE`
	return querySchedules(ctx, database, baseQuery, "target_id", targetID, "created_at", func(rows *sql.Rows) (HeartbeatSchedule, error) {
		var h HeartbeatSchedule
		err := rows.Scan(&h.ID, &h.TargetID, &h.TitlePrefix, &h.CronExpr, &h.Prompt, &h.Timezone, &h.NextRunAt, &h.Enabled, &h.CreatedAt, &h.Effort)
		return h, err
	})
}

func GetHeartbeatSchedule(ctx context.Context, database *sql.DB, id string) (*HeartbeatSchedule, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, fmt.Errorf("schedule_id cannot be empty")
	}
	query := rebindQuery(`SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'low') FROM heartbeat_schedules WHERE id = ?`, isPostgres(database))
	var h HeartbeatSchedule
	row := database.QueryRowContext(ctx, query, id)
	if err := row.Scan(&h.ID, &h.TargetID, &h.TitlePrefix, &h.CronExpr, &h.Prompt, &h.Timezone, &h.NextRunAt, &h.Enabled, &h.CreatedAt, &h.Effort); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("heartbeat schedule %q not found", id)
		}
		return nil, err
	}
	return &h, nil
}

func UpdateHeartbeatSchedule(ctx context.Context, database *sql.DB, h HeartbeatSchedule) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	h.ID = strings.TrimSpace(h.ID)
	if h.ID == "" {
		return fmt.Errorf("schedule_id cannot be empty")
	}
	effort, err := NormalizeEffort(h.Effort)
	if err != nil {
		return err
	}
	query := rebindQuery(`UPDATE heartbeat_schedules SET target_id = ?, title_prefix = ?, cron_expr = ?, prompt = ?, timezone = ?, effort = ?, next_run_at = ?, enabled = ? WHERE id = ?`, isPostgres(database))
	res, err := database.ExecContext(ctx, query, h.TargetID, h.TitlePrefix, h.CronExpr, h.Prompt, h.Timezone, effort, h.NextRunAt, h.Enabled, h.ID)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to determine rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("heartbeat schedule %q not found", h.ID)
	}
	return nil
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
	for _, tbl := range []string{"cron_schedules", "one_shot_schedules", "heartbeat_schedules"} {
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

func GetDueCronSchedules(ctx context.Context, database *sql.DB, now time.Time) ([]CronSchedule, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	baseQuery := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM cron_schedules WHERE enabled = TRUE AND next_run_at <= ? ORDER BY next_run_at ASC`
	rows, err := database.QueryContext(ctx, rebindQuery(baseQuery, isPostgres(database)), now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []CronSchedule
	for rows.Next() {
		var c CronSchedule
		if err := rows.Scan(&c.ID, &c.TargetID, &c.TitlePrefix, &c.CronExpr, &c.Prompt, &c.Timezone, &c.NextRunAt, &c.Enabled, &c.CreatedAt, &c.Effort); err != nil {
			return nil, err
		}
		results = append(results, c)
	}
	return results, rows.Err()
}

func AdvanceCronNextRun(ctx context.Context, database *sql.DB, id string, nextRun time.Time) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("schedule_id cannot be empty")
	}
	query := rebindQuery(`UPDATE cron_schedules SET next_run_at = ? WHERE id = ?`, isPostgres(database))
	res, err := database.ExecContext(ctx, query, nextRun, id)
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

func GetDueHeartbeatSchedules(ctx context.Context, database *sql.DB, now time.Time) ([]HeartbeatSchedule, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	baseQuery := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'low') FROM heartbeat_schedules WHERE enabled = TRUE AND next_run_at <= ? ORDER BY next_run_at ASC`
	rows, err := database.QueryContext(ctx, rebindQuery(baseQuery, isPostgres(database)), now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []HeartbeatSchedule
	for rows.Next() {
		var h HeartbeatSchedule
		if err := rows.Scan(&h.ID, &h.TargetID, &h.TitlePrefix, &h.CronExpr, &h.Prompt, &h.Timezone, &h.NextRunAt, &h.Enabled, &h.CreatedAt, &h.Effort); err != nil {
			return nil, err
		}
		results = append(results, h)
	}
	return results, rows.Err()
}

func AdvanceHeartbeatNextRun(ctx context.Context, database *sql.DB, id string, nextRun time.Time) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("schedule_id cannot be empty")
	}
	query := rebindQuery(`UPDATE heartbeat_schedules SET next_run_at = ? WHERE id = ?`, isPostgres(database))
	res, err := database.ExecContext(ctx, query, nextRun, id)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to determine rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("heartbeat schedule %q not found", id)
	}
	return nil
}

func GetDueOneShotSchedules(ctx context.Context, database *sql.DB, now time.Time) ([]OneShotSchedule, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	baseQuery := `SELECT id, thread_id, prompt, run_at, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM one_shot_schedules WHERE run_at <= ? ORDER BY run_at ASC`
	rows, err := database.QueryContext(ctx, rebindQuery(baseQuery, isPostgres(database)), now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []OneShotSchedule
	for rows.Next() {
		var s OneShotSchedule
		if err := rows.Scan(&s.ID, &s.ThreadID, &s.Prompt, &s.RunAt, &s.CreatedAt, &s.Effort); err != nil {
			return nil, err
		}
		results = append(results, s)
	}
	return results, rows.Err()
}

func DeleteOneShotSchedule(ctx context.Context, database *sql.DB, id string) (bool, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return false, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return false, fmt.Errorf("schedule_id cannot be empty")
	}
	query := rebindQuery(`DELETE FROM one_shot_schedules WHERE id = ?`, isPostgres(database))
	res, err := database.ExecContext(ctx, query, id)
	if err != nil {
		return false, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to determine rows affected: %w", err)
	}
	return rowsAffected > 0, nil
}

func HasActiveThreadTurn(ctx context.Context, database *sql.DB, threadID string) (bool, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return false, err
	}
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return false, nil
	}
	query := rebindQuery(`SELECT EXISTS(SELECT 1 FROM messages WHERE thread_id = ? AND (status IN ('pending', 'processing') OR status IN ('PENDING', 'PROCESSING')))`, isPostgres(database))
	var exists bool
	if err := database.QueryRowContext(ctx, query, threadID).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func CreateScheduleRun(ctx context.Context, database *sql.DB, r ScheduleRun) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	if strings.TrimSpace(r.ID) == "" {
		return fmt.Errorf("run id cannot be empty")
	}
	if r.StartedAt.IsZero() {
		r.StartedAt = time.Now().UTC()
	}
	query := rebindQuery(`INSERT INTO schedule_runs (id, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, completed_at, duration_ms, error, effort, model) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, isPostgres(database))
	_, err = database.ExecContext(ctx, query,
		r.ID,
		r.ScheduleID,
		r.ScheduleType,
		r.MessageID,
		r.TargetID,
		r.ThreadID,
		r.Title,
		r.Prompt,
		r.Status,
		r.StartedAt,
		r.CompletedAt,
		r.DurationMs,
		r.Error,
		normalizeScheduleEffort(r.Effort),
		r.Model,
	)
	return err
}

func PruneScheduleRuns(ctx context.Context, database *sql.DB, maxCount int, maxAge time.Duration, now time.Time) (int64, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return 0, err
	}
	cutoff := now.Add(-maxAge)
	query := rebindQuery(`DELETE FROM schedule_runs WHERE started_at < ? OR id NOT IN (SELECT id FROM schedule_runs ORDER BY started_at DESC, id DESC LIMIT ?)`, isPostgres(database))
	res, err := database.ExecContext(ctx, query, cutoff, maxCount)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
