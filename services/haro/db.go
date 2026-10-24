package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var sqliteTestInitHook func(string) (*sql.DB, error)

type ScheduleSummaryMetrics struct {
	TotalActive    int        `json:"total_active"`
	CronCount      int        `json:"cron_count"`
	HeartbeatCount int        `json:"heartbeat_count"`
	OneShotCount   int        `json:"one_shot_count"`
	TotalRuns24h   int        `json:"total_runs_24h"`
	NextRunAt      *time.Time `json:"next_run_at"`
	SuccessRate24h float64    `json:"success_rate_24h"`
}

type ScheduleRunJSON struct {
	ID           string     `json:"id"`
	ScheduleID   string     `json:"schedule_id"`
	ScheduleType string     `json:"schedule_type"`
	MessageID    string     `json:"message_id,omitempty"`
	TargetID     string     `json:"target_id"`
	ThreadID     string     `json:"thread_id,omitempty"`
	Title        string     `json:"title"`
	Prompt       string     `json:"prompt"`
	StartedAt    time.Time  `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	DurationMs   int64      `json:"duration_ms"`
	Status       string     `json:"status"`
	Error        string     `json:"error,omitempty"`
}

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

type EventTrigger struct {
	ID        string    `json:"id"`
	ChannelID string    `json:"channel_id"`
	Source    string    `json:"source"`
	Target    string    `json:"target"`
	EventType string    `json:"event_type"`
	Prompt    string    `json:"prompt"`
	Effort    string    `json:"effort"`
	CreatedAt time.Time `json:"created_at"`
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
			httpkit.CloseWarn(database, "database on ping failure")
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
	query := `INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
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
	query := `INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6)`
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
	query := `INSERT INTO heartbeat_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, effort, next_run_at, enabled, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
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
		query = fmt.Sprintf("%s%s%s = $1 ORDER BY %s ASC", query, sep, filterCol, orderCol)
		rows, err = database.QueryContext(ctx, query, targetID)
	} else {
		query = fmt.Sprintf("%s ORDER BY %s ASC", query, orderCol)
		rows, err = database.QueryContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	defer httpkit.CloseWarn(rows, "rows")

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
	query := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'low') FROM heartbeat_schedules WHERE id = $1`
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
	query := `UPDATE heartbeat_schedules SET target_id = $1, title_prefix = $2, cron_expr = $3, prompt = $4, timezone = $5, effort = $6, next_run_at = $7, enabled = $8 WHERE id = $9`
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
	var args []any
	addSet := func(col string, val *string) {
		if val != nil && strings.TrimSpace(*val) != "" {
			args = append(args, strings.TrimSpace(*val))
			sets = append(sets, fmt.Sprintf("%s = $%d", col, len(args)))
		}
	}
	if effort != nil {
		args = append(args, normalizeScheduleEffort(*effort))
		sets = append(sets, fmt.Sprintf("effort = $%d", len(args)))
	}
	addSet("cron_expr", cronExpr)
	addSet("prompt", prompt)
	if titlePrefix != nil {
		args = append(args, strings.TrimSpace(*titlePrefix))
		sets = append(sets, fmt.Sprintf("title_prefix = $%d", len(args)))
	}
	addSet("timezone", timezone)
	if nextRunAt != nil && !nextRunAt.IsZero() {
		args = append(args, *nextRunAt)
		sets = append(sets, fmt.Sprintf("next_run_at = $%d", len(args)))
	}

	if len(sets) == 0 {
		return ctx.Err()
	}

	args = append(args, id)
	query := fmt.Sprintf("UPDATE cron_schedules SET %s WHERE id = $%d", strings.Join(sets, ", "), len(args))
	res, err := database.ExecContext(ctx, query, args...)
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
	for _, tbl := range []string{"cron_schedules", "one_shot_schedules", "heartbeat_schedules", "event_triggers"} {
		res, err := database.ExecContext(ctx, "DELETE FROM "+tbl+" WHERE id = $1", scheduleID)
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
	baseQuery := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM cron_schedules WHERE enabled = TRUE AND next_run_at <= $1 ORDER BY next_run_at ASC`
	rows, err := database.QueryContext(ctx, baseQuery, now)
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
	query := `UPDATE cron_schedules SET next_run_at = $1 WHERE id = $2`
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
	baseQuery := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'low') FROM heartbeat_schedules WHERE enabled = TRUE AND next_run_at <= $1 ORDER BY next_run_at ASC`
	rows, err := database.QueryContext(ctx, baseQuery, now)
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
	query := `UPDATE heartbeat_schedules SET next_run_at = $1 WHERE id = $2`
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
	baseQuery := `SELECT id, thread_id, prompt, run_at, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM one_shot_schedules WHERE run_at <= $1 ORDER BY run_at ASC`
	rows, err := database.QueryContext(ctx, baseQuery, now)
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
	query := `DELETE FROM one_shot_schedules WHERE id = $1`
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
	query := `INSERT INTO schedule_runs (id, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, completed_at, duration_ms, error, effort, model) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`
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
	query := `DELETE FROM schedule_runs WHERE started_at < $1 OR id NOT IN (SELECT id FROM schedule_runs ORDER BY started_at DESC, id DESC LIMIT $2)`
	res, err := database.ExecContext(ctx, query, cutoff, maxCount)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

var (
	vanadisMu     sync.RWMutex
	vanadisURL    string
	vanadisClient = &http.Client{Timeout: 5 * time.Second}
)

// SetVanadisURL sets the base URL for Vanadis HTTP API calls.
func SetVanadisURL(u string) {
	vanadisMu.Lock()
	defer vanadisMu.Unlock()
	vanadisURL = strings.TrimRight(u, "/")
}

// GetVanadisURL gets the base URL for Vanadis HTTP API calls.
// Explicitly configured URLs via SetVanadisURL take precedence over environment defaults.
func GetVanadisURL() string {
	vanadisMu.RLock()
	defer vanadisMu.RUnlock()
	if vanadisURL != "" {
		return vanadisURL
	}
	if env := strings.TrimSpace(os.Getenv("VANADIS_URL")); env != "" {
		return strings.TrimRight(env, "/")
	}
	return "http://vanadis.aerial"
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// ValidateLiveEventTarget checks that the referenced target is known and not in a terminal state.
func ValidateLiveEventTarget(ctx context.Context, database *sql.DB, source, target string) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return fmt.Errorf("target cannot be empty")
	}
	baseURL := GetVanadisURL()
	switch source {
	case "vanadis_build":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/builds/%s", baseURL, url.PathEscape(target)), nil)
		if err != nil {
			return fmt.Errorf("create vanadis build request: %w", err)
		}
		resp, err := vanadisClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to query vanadis build %q: %w", target, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("vanadis build %q not found", target)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("failed to query vanadis build %q: HTTP %d", target, resp.StatusCode)
		}
		var bResp struct {
			Build struct {
				Status string `json:"status"`
			} `json:"build"`
			Status string `json:"status"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&bResp); err != nil {
			return fmt.Errorf("failed to decode vanadis build %q response: %w", target, err)
		}
		status := firstNonEmpty(bResp.Build.Status, bResp.Status)
		switch strings.ToLower(strings.TrimSpace(status)) {
		case "completed", "merged", "failed", "cancelled", "canceled":
			return fmt.Errorf("vanadis build %q is already in terminal state %q", target, status)
		}
	case "vanadis_chunk":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/chunks/%s", baseURL, url.PathEscape(target)), nil)
		if err != nil {
			return fmt.Errorf("create vanadis chunk request: %w", err)
		}
		resp, err := vanadisClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to query vanadis chunk %q: %w", target, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("vanadis chunk %q not found", target)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("failed to query vanadis chunk %q: HTTP %d", target, resp.StatusCode)
		}
		var cResp struct {
			Chunk struct {
				Status string `json:"status"`
			} `json:"chunk"`
			Status string `json:"status"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&cResp); err != nil {
			return fmt.Errorf("failed to decode vanadis chunk %q response: %w", target, err)
		}
		status := firstNonEmpty(cResp.Chunk.Status, cResp.Status)
		switch strings.ToLower(strings.TrimSpace(status)) {
		case "merged", "completed", "failed":
			return fmt.Errorf("vanadis chunk %q is already in terminal state %q", target, status)
		}
	}
	return nil
}

// InsertEventTrigger persists a new event trigger in the database.
func InsertEventTrigger(ctx context.Context, database *sql.DB, t EventTrigger) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now().UTC()
	}
	effort, err := NormalizeEffort(t.Effort)
	if err != nil {
		effort = "medium"
	}
	query := `INSERT INTO event_triggers (id, channel_id, source, target, event_type, prompt, effort, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	_, err = database.ExecContext(ctx, query, t.ID, t.ChannelID, t.Source, t.Target, t.EventType, t.Prompt, effort, t.CreatedAt)
	return err
}

// ListEventTriggers retrieves event triggers, optionally filtered by channel_id.
func ListEventTriggers(ctx context.Context, database *sql.DB, channelID string) ([]EventTrigger, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	channelID = strings.TrimSpace(channelID)
	var query string
	var rows *sql.Rows
	if channelID != "" {
		query = `SELECT id, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium'), created_at FROM event_triggers WHERE channel_id = $1 ORDER BY created_at ASC`
		rows, err = database.QueryContext(ctx, query, channelID)
	} else {
		query = `SELECT id, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium'), created_at FROM event_triggers ORDER BY created_at ASC`
		rows, err = database.QueryContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	return scanEventTriggerRows(rows)
}

func scanEventTriggerRows(rows *sql.Rows) ([]EventTrigger, error) {
	defer rows.Close()
	results := []EventTrigger{}
	for rows.Next() {
		var t EventTrigger
		if err := rows.Scan(&t.ID, &t.ChannelID, &t.Source, &t.Target, &t.EventType, &t.Prompt, &t.Effort, &t.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, t)
	}
	return results, rows.Err()
}

// ClaimEventTriggers atomically deletes and returns matching event triggers from event_triggers.
func ClaimEventTriggers(ctx context.Context, database *sql.DB, source string, targets []string, eventType string) ([]EventTrigger, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	source, eventType = strings.TrimSpace(source), strings.TrimSpace(eventType)
	var cleanTargets []string
	seen := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		if clean := strings.TrimSpace(t); clean != "" {
			if _, ok := seen[clean]; !ok {
				seen[clean] = struct{}{}
				cleanTargets = append(cleanTargets, clean)
			}
		}
	}
	if source == "" || eventType == "" || len(cleanTargets) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(cleanTargets))
	args := make([]any, 0, 2+len(cleanTargets))
	args = append(args, source, eventType)
	for i, t := range cleanTargets {
		placeholders[i] = fmt.Sprintf("$%d", i+3)
		args = append(args, t)
	}
	rawQuery := fmt.Sprintf(`DELETE FROM event_triggers WHERE source = $1 AND event_type = $2 AND target IN (%s) RETURNING id, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium'), created_at`, strings.Join(placeholders, ", "))
	rows, err := database.QueryContext(ctx, rawQuery, args...)
	if err != nil {
		return nil, err
	}
	return scanEventTriggerRows(rows)
}

// DeleteEventTrigger removes an event trigger by id.
func DeleteEventTrigger(ctx context.Context, database *sql.DB, id string) (bool, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return false, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return false, fmt.Errorf("id cannot be empty")
	}
	query := `DELETE FROM event_triggers WHERE id = $1`
	res, err := database.ExecContext(ctx, query, id)
	if err != nil {
		return false, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rowsAffected > 0, nil
}

func findEarliestNextRun(crons []CronSchedule, heartbeats []HeartbeatSchedule, oneshots []OneShotSchedule) *time.Time {
	var earliest *time.Time
	for _, c := range crons {
		if !c.NextRunAt.IsZero() && (earliest == nil || c.NextRunAt.Before(*earliest)) {
			t := c.NextRunAt
			earliest = &t
		}
	}
	for _, h := range heartbeats {
		if !h.NextRunAt.IsZero() && (earliest == nil || h.NextRunAt.Before(*earliest)) {
			t := h.NextRunAt
			earliest = &t
		}
	}
	for _, o := range oneshots {
		if !o.RunAt.IsZero() && (earliest == nil || o.RunAt.Before(*earliest)) {
			t := o.RunAt
			earliest = &t
		}
	}
	return earliest
}

func compute24hRunMetrics(ctx context.Context, database *sql.DB) (int, float64, error) {
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	query := `SELECT status FROM schedule_runs WHERE started_at >= $1`
	rows, err := database.QueryContext(ctx, query, cutoff)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query schedule runs for 24h metrics: %w", err)
	}
	defer rows.Close()

	totalRuns := 0
	successCount := 0
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			return 0, 0, fmt.Errorf("failed to scan schedule run status: %w", err)
		}
		totalRuns++
		normStatus := strings.ToLower(strings.TrimSpace(status))
		if normStatus == "completed" || normStatus == "enqueued" || normStatus == "success" {
			successCount++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("error iterating schedule runs: %w", err)
	}

	rate := 0.0
	if totalRuns > 0 {
		rate = math.Round((float64(successCount)/float64(totalRuns))*1000) / 10
	}
	return totalRuns, rate, nil
}

// GetScheduleSummaryMetrics computes summary metrics across all schedules and runs in the last 24 hours.
func GetScheduleSummaryMetrics(ctx context.Context, database *sql.DB) (ScheduleSummaryMetrics, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return ScheduleSummaryMetrics{}, err
	}

	crons, err := ListCronSchedules(ctx, database, "")
	if err != nil {
		return ScheduleSummaryMetrics{}, fmt.Errorf("failed to list cron schedules: %w", err)
	}
	heartbeats, err := ListHeartbeatSchedules(ctx, database, "")
	if err != nil {
		return ScheduleSummaryMetrics{}, fmt.Errorf("failed to list heartbeat schedules: %w", err)
	}
	oneshots, err := ListOneShotSchedules(ctx, database, "")
	if err != nil {
		return ScheduleSummaryMetrics{}, fmt.Errorf("failed to list one-shot schedules: %w", err)
	}

	totalRuns, successRate, err := compute24hRunMetrics(ctx, database)
	if err != nil {
		return ScheduleSummaryMetrics{}, err
	}

	return ScheduleSummaryMetrics{
		TotalActive:    len(crons) + len(heartbeats) + len(oneshots),
		CronCount:      len(crons),
		HeartbeatCount: len(heartbeats),
		OneShotCount:   len(oneshots),
		TotalRuns24h:   totalRuns,
		NextRunAt:      findEarliestNextRun(crons, heartbeats, oneshots),
		SuccessRate24h: successRate,
	}, nil
}

// GetScheduleRunsPaginated fetches a paginated, filtered slice of schedule runs and the total count.
func GetScheduleRunsPaginated(ctx context.Context, database *sql.DB, limit, offset int, scheduleID, status string) ([]ScheduleRunJSON, int, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, 0, err
	}

	if limit <= 0 {
		limit = 50
	} else if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	whereClause := " WHERE 1=1"
	var filterArgs []any
	if trimmedID := strings.TrimSpace(scheduleID); trimmedID != "" {
		filterArgs = append(filterArgs, trimmedID)
		whereClause += fmt.Sprintf(" AND schedule_id = $%d", len(filterArgs))
	}
	if trimmedStatus := strings.TrimSpace(status); trimmedStatus != "" {
		filterArgs = append(filterArgs, trimmedStatus)
		whereClause += fmt.Sprintf(" AND LOWER(status) = LOWER($%d)", len(filterArgs))
	}

	countQuery := "SELECT COUNT(*) FROM schedule_runs" + whereClause
	var total int
	if err := database.QueryRowContext(ctx, countQuery, filterArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count schedule runs: %w", err)
	}

	selectArgs := make([]any, len(filterArgs), len(filterArgs)+2)
	copy(selectArgs, filterArgs)
	selectArgs = append(selectArgs, limit, offset)
	limitParam := len(selectArgs) - 1
	offsetParam := len(selectArgs)

	selectQuery := fmt.Sprintf("SELECT id, schedule_id, schedule_type, COALESCE(message_id, ''), COALESCE(target_id, ''), COALESCE(thread_id, ''), COALESCE(title, ''), COALESCE(prompt, ''), started_at, completed_at, COALESCE(duration_ms, 0), status, COALESCE(error, '') FROM schedule_runs%s ORDER BY started_at DESC LIMIT $%d OFFSET $%d", whereClause, limitParam, offsetParam)

	rows, err := database.QueryContext(ctx, selectQuery, selectArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query schedule runs: %w", err)
	}
	defer rows.Close()

	runs := make([]ScheduleRunJSON, 0)
	for rows.Next() {
		var r ScheduleRunJSON
		var completedAt sql.NullTime
		if err := rows.Scan(
			&r.ID,
			&r.ScheduleID,
			&r.ScheduleType,
			&r.MessageID,
			&r.TargetID,
			&r.ThreadID,
			&r.Title,
			&r.Prompt,
			&r.StartedAt,
			&completedAt,
			&r.DurationMs,
			&r.Status,
			&r.Error,
		); err != nil {
			return nil, 0, fmt.Errorf("failed to scan schedule run: %w", err)
		}
		if completedAt.Valid {
			t := completedAt.Time
			r.CompletedAt = &t
		}
		runs = append(runs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error iterating schedule runs: %w", err)
	}

	return runs, total, nil
}
