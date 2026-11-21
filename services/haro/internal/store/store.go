// Package store is the Layer-2 persistence package for haro schedule and
// run records. It owns the schedule/run core of the former root db.go:
// connection setup, cron/one-shot/heartbeat schedule CRUD, due-scan and
// next-run advancement, schedule-run records, and summary/run-list queries.
// SQL text, scan order, and error strings are byte-identical to the root
// implementation this package was extracted from. It imports Layer-0
// packages internal/model (row types, constants) and internal/config
// (Config, DefaultTimezone) only. Event-trigger persistence (Vanadis URL
// accessors, live-target validation, event_triggers CRUD/claim) lives in
// events.go; root db.go keeps zero-logic wrappers until the haro-07b purge.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/model"
)

// OpenDB opens the scheduler database from cfg.
func OpenDB(ctx context.Context, cfg *config.Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	return pgutil.OpenSQL(ctx, cfg.DatabaseURL, pgutil.SQLOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
	})
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

// NormalizeScheduleEffort normalizes a schedule effort tier for storage:
// valid low/medium/high values pass through (lowercased and trimmed);
// empty or invalid values fall back to medium. This is explicitly DISTINCT
// from model.NormalizeEffort, which defaults empty to low and errors on
// invalid input - never substitute one for the other.
func NormalizeScheduleEffort(effort string) string {
	switch eff := strings.ToLower(strings.TrimSpace(effort)); eff {
	case model.EffortLow, model.EffortMedium, model.EffortHigh:
		return eff
	default:
		return model.EffortMedium
	}
}

// InsertCronSchedule persists a new cron schedule row.
func InsertCronSchedule(ctx context.Context, database *sql.DB, c model.CronSchedule) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	if c.Timezone == "" {
		c.Timezone = config.DefaultTimezone
	}
	c.Agent = model.NormalizeAgent(c.Agent)
	query := `INSERT INTO cron_schedules (id, agent, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	_, err = database.ExecContext(ctx, query, c.ID, c.Agent, c.TargetID, c.TitlePrefix, c.CronExpr, c.Prompt, c.Timezone, c.NextRunAt, c.Enabled, c.CreatedAt, NormalizeScheduleEffort(c.Effort))
	return err
}

// InsertOneShotSchedule persists a new one-shot schedule row.
func InsertOneShotSchedule(ctx context.Context, database *sql.DB, s model.OneShotSchedule) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	s.Agent = model.NormalizeAgent(s.Agent)
	query := `INSERT INTO one_shot_schedules (id, agent, thread_id, prompt, run_at, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO NOTHING`
	_, err = database.ExecContext(ctx, query, s.ID, s.Agent, s.ThreadID, s.Prompt, s.RunAt, s.CreatedAt, NormalizeScheduleEffort(s.Effort))
	return err
}

// InsertHeartbeatSchedule persists a new heartbeat schedule row.
func InsertHeartbeatSchedule(ctx context.Context, database *sql.DB, h model.HeartbeatSchedule) error {
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
	effort, err := model.NormalizeEffort(h.Effort)
	if err != nil {
		effort = model.EffortLow
	}
	h.Agent = model.NormalizeAgent(h.Agent)
	query := `INSERT INTO heartbeat_schedules (id, agent, target_id, title_prefix, cron_expr, prompt, timezone, effort, next_run_at, enabled, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	_, err = database.ExecContext(ctx, query, h.ID, h.Agent, h.TargetID, h.TitlePrefix, h.CronExpr, h.Prompt, h.Timezone, effort, h.NextRunAt, h.Enabled, h.CreatedAt)
	return err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanCronSchedule(r rowScanner) (model.CronSchedule, error) {
	var c model.CronSchedule
	err := r.Scan(&c.ID, &c.Agent, &c.TargetID, &c.TitlePrefix, &c.CronExpr, &c.Prompt, &c.Timezone, &c.NextRunAt, &c.Enabled, &c.CreatedAt, &c.Effort)
	c.Agent = model.NormalizeAgent(c.Agent)
	return c, err
}

func scanOneShotSchedule(r rowScanner) (model.OneShotSchedule, error) {
	var s model.OneShotSchedule
	err := r.Scan(&s.ID, &s.Agent, &s.ThreadID, &s.Prompt, &s.RunAt, &s.CreatedAt, &s.Effort)
	s.Agent = model.NormalizeAgent(s.Agent)
	return s, err
}

func scanHeartbeatSchedule(r rowScanner) (model.HeartbeatSchedule, error) {
	var h model.HeartbeatSchedule
	var lastCompletedAt sql.NullTime
	err := r.Scan(&h.ID, &h.Agent, &h.TargetID, &h.TitlePrefix, &h.CronExpr, &h.Prompt, &h.Timezone, &h.NextRunAt, &h.Enabled, &h.CreatedAt, &h.Effort, &lastCompletedAt)
	if err != nil {
		return h, err
	}
	h.Agent = model.NormalizeAgent(h.Agent)
	if lastCompletedAt.Valid {
		t := lastCompletedAt.Time
		h.LastCompletedAt = &t
	}
	return h, nil
}

func querySchedules[T any](ctx context.Context, database *sql.DB, baseQuery, filterCol, targetID, agentFilter, orderCol string, scanFn func(rowScanner) (T, error)) ([]T, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	query := baseQuery
	var args []any
	sep := " WHERE "
	if strings.Contains(query, "WHERE") {
		sep = " AND "
	}
	if targetID = strings.TrimSpace(targetID); targetID != "" {
		args = append(args, targetID)
		query = fmt.Sprintf("%s%s%s = $%d", query, sep, filterCol, len(args))
		sep = " AND "
	}
	if agentFilter = strings.TrimSpace(agentFilter); agentFilter != "" && agentFilter != "*" {
		args = append(args, agentFilter)
		query = fmt.Sprintf("%s%sagent = $%d", query, sep, len(args))
	}
	query = fmt.Sprintf("%s ORDER BY %s ASC", query, orderCol)
	rows, err := database.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

// ListCronSchedules lists enabled cron schedules, optionally filtered by target and agent.
func ListCronSchedules(ctx context.Context, database *sql.DB, targetID string, agent ...string) ([]model.CronSchedule, error) {
	agentFilter := ""
	if len(agent) > 0 {
		agentFilter = agent[0]
	}
	baseQuery := `SELECT id, agent, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM cron_schedules WHERE enabled = TRUE`
	return querySchedules(ctx, database, baseQuery, "target_id", targetID, agentFilter, "created_at", scanCronSchedule)
}

// ListOneShotSchedules lists one-shot schedules, optionally filtered by thread and agent.
func ListOneShotSchedules(ctx context.Context, database *sql.DB, targetID string, agent ...string) ([]model.OneShotSchedule, error) {
	agentFilter := ""
	if len(agent) > 0 {
		agentFilter = agent[0]
	}
	baseQuery := `SELECT id, agent, thread_id, prompt, run_at, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM one_shot_schedules`
	return querySchedules(ctx, database, baseQuery, "thread_id", targetID, agentFilter, "run_at", scanOneShotSchedule)
}

// ListHeartbeatSchedules lists enabled heartbeat schedules, optionally filtered by target and agent.
func ListHeartbeatSchedules(ctx context.Context, database *sql.DB, targetID string, agent ...string) ([]model.HeartbeatSchedule, error) {
	agentFilter := ""
	if len(agent) > 0 {
		agentFilter = agent[0]
	}
	baseQuery := `SELECT id, agent, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'low'), last_completed_at FROM heartbeat_schedules WHERE enabled = TRUE`
	return querySchedules(ctx, database, baseQuery, "target_id", targetID, agentFilter, "created_at", scanHeartbeatSchedule)
}

func getScheduleByID[T any](ctx context.Context, database *sql.DB, id, label, query string, scanFn func(rowScanner) (T, error)) (*T, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, fmt.Errorf("schedule_id cannot be empty")
	}
	row := database.QueryRowContext(ctx, query, id)
	item, err := scanFn(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%s schedule %q not found: %w", label, id, sql.ErrNoRows)
		}
		return nil, err
	}
	return &item, nil
}

// GetCronSchedule fetches a cron schedule by id.
func GetCronSchedule(ctx context.Context, database *sql.DB, id string) (*model.CronSchedule, error) {
	query := `SELECT id, agent, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM cron_schedules WHERE id = $1`
	return getScheduleByID(ctx, database, id, model.ScheduleTypeCron, query, scanCronSchedule)
}

// GetOneShotSchedule fetches a one-shot schedule by id.
func GetOneShotSchedule(ctx context.Context, database *sql.DB, id string) (*model.OneShotSchedule, error) {
	query := `SELECT id, agent, thread_id, prompt, run_at, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM one_shot_schedules WHERE id = $1`
	return getScheduleByID(ctx, database, id, "one-shot", query, scanOneShotSchedule)
}

// GetHeartbeatSchedule fetches a heartbeat schedule by id.
func GetHeartbeatSchedule(ctx context.Context, database *sql.DB, id string) (*model.HeartbeatSchedule, error) {
	query := `SELECT id, agent, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'low'), last_completed_at FROM heartbeat_schedules WHERE id = $1`
	return getScheduleByID(ctx, database, id, model.ScheduleTypeHeartbeat, query, scanHeartbeatSchedule)
}

func execScheduleUpdate(ctx context.Context, database *sql.DB, label, id, query string, args ...any) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("schedule_id cannot be empty")
	}
	res, err := database.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to determine rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%s schedule %q not found", label, id)
	}
	return nil
}

// UpdateHeartbeatSchedule replaces a heartbeat schedule row.
func UpdateHeartbeatSchedule(ctx context.Context, database *sql.DB, h model.HeartbeatSchedule) error {
	effort, err := model.NormalizeEffort(h.Effort)
	if err != nil {
		return err
	}
	id := strings.TrimSpace(h.ID)
	query := `UPDATE heartbeat_schedules SET target_id = $1, title_prefix = $2, cron_expr = $3, prompt = $4, timezone = $5, effort = $6, next_run_at = $7, enabled = $8 WHERE id = $9`
	return execScheduleUpdate(ctx, database, model.ScheduleTypeHeartbeat, id, query, h.TargetID, h.TitlePrefix, h.CronExpr, h.Prompt, h.Timezone, effort, h.NextRunAt, h.Enabled, id)
}

// UpdateCronSchedule patches selected cron schedule columns.
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
		args = append(args, NormalizeScheduleEffort(*effort))
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

// DeleteSchedule deletes a schedule id from every schedule table.
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

func queryDueSchedules[T any](ctx context.Context, database *sql.DB, query string, now time.Time, scanFn func(rowScanner) (T, error)) ([]T, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	rows, err := database.QueryContext(ctx, query, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

func advanceScheduleNextRun(ctx context.Context, database *sql.DB, table, label, id string, nextRun time.Time) error {
	query := fmt.Sprintf("UPDATE %s SET next_run_at = $1 WHERE id = $2", table)
	return execScheduleUpdate(ctx, database, label, id, query, nextRun, strings.TrimSpace(id))
}

// GetDueCronSchedules lists enabled cron schedules whose next run is due.
func GetDueCronSchedules(ctx context.Context, database *sql.DB, now time.Time) ([]model.CronSchedule, error) {
	baseQuery := `SELECT id, agent, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM cron_schedules WHERE enabled = TRUE AND next_run_at <= $1 ORDER BY next_run_at ASC`
	return queryDueSchedules(ctx, database, baseQuery, now, scanCronSchedule)
}

// AdvanceCronNextRun moves a cron schedule's next run timestamp.
func AdvanceCronNextRun(ctx context.Context, database *sql.DB, id string, nextRun time.Time) error {
	return advanceScheduleNextRun(ctx, database, "cron_schedules", model.ScheduleTypeCron, id, nextRun)
}

// GetDueHeartbeatSchedules lists enabled heartbeat schedules whose next run is due.
func GetDueHeartbeatSchedules(ctx context.Context, database *sql.DB, now time.Time) ([]model.HeartbeatSchedule, error) {
	baseQuery := `SELECT id, agent, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'low'), last_completed_at FROM heartbeat_schedules WHERE enabled = TRUE AND next_run_at <= $1 ORDER BY next_run_at ASC`
	return queryDueSchedules(ctx, database, baseQuery, now, scanHeartbeatSchedule)
}

// AdvanceHeartbeatNextRun moves a heartbeat schedule's next run timestamp.
func AdvanceHeartbeatNextRun(ctx context.Context, database *sql.DB, id string, nextRun time.Time) error {
	return advanceScheduleNextRun(ctx, database, "heartbeat_schedules", model.ScheduleTypeHeartbeat, id, nextRun)
}

// AdvanceHeartbeatLastCompletedAt advances a heartbeat's watermark monotonically.
func AdvanceHeartbeatLastCompletedAt(ctx context.Context, database *sql.DB, id string, completedAt time.Time) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("schedule_id cannot be empty")
	}
	query := `UPDATE heartbeat_schedules SET last_completed_at = GREATEST(COALESCE(last_completed_at, $1), $1) WHERE id = $2`
	if isSQLiteDBFn(database) {
		query = `UPDATE heartbeat_schedules SET last_completed_at = MAX(COALESCE(last_completed_at, $1), $1) WHERE id = $2`
	}
	return execScheduleUpdate(ctx, database, model.ScheduleTypeHeartbeat, id, query, completedAt, id)
}

// GetDueOneShotSchedules lists one-shot schedules whose run time is due.
func GetDueOneShotSchedules(ctx context.Context, database *sql.DB, now time.Time) ([]model.OneShotSchedule, error) {
	baseQuery := `SELECT id, agent, thread_id, prompt, run_at, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM one_shot_schedules WHERE run_at <= $1 ORDER BY run_at ASC`
	return queryDueSchedules(ctx, database, baseQuery, now, scanOneShotSchedule)
}

func deleteByID(ctx context.Context, database *sql.DB, query, emptyErrMsg, errPrefix, id string) (bool, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return false, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return false, fmt.Errorf("%s", emptyErrMsg)
	}
	res, err := database.ExecContext(ctx, query, id)
	if err != nil {
		return false, fmt.Errorf("%s: %w", errPrefix, err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%s: failed to determine rows affected: %w", errPrefix, err)
	}
	return rowsAffected > 0, nil
}

// DeleteOneShotSchedule removes a one-shot schedule by id.
func DeleteOneShotSchedule(ctx context.Context, database *sql.DB, id string) (bool, error) {
	return deleteByID(ctx, database, "DELETE FROM one_shot_schedules WHERE id = $1", "schedule_id cannot be empty", "failed to delete one-shot schedule", id)
}

// CreateScheduleRun records a new schedule execution attempt.
func CreateScheduleRun(ctx context.Context, database *sql.DB, r model.ScheduleRun) error {
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
	query := `INSERT INTO schedule_runs (id, agent, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, completed_at, duration_ms, error, effort, model) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`
	_, err = database.ExecContext(ctx, query,
		r.ID,
		model.NormalizeAgent(r.Agent),
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
		NormalizeScheduleEffort(r.Effort),
		r.Model,
	)
	return err
}

// HasRunningHeartbeatRun reports whether a heartbeat has a running run since a cutoff.
func HasRunningHeartbeatRun(ctx context.Context, database *sql.DB, scheduleID string, since time.Time) (bool, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return false, err
	}
	scheduleID = strings.TrimSpace(scheduleID)
	if scheduleID == "" {
		return false, nil
	}
	query := `SELECT COUNT(*) FROM schedule_runs WHERE schedule_id = $1 AND schedule_type = 'heartbeat' AND status = 'running' AND started_at >= $2`
	var count int
	err = database.QueryRowContext(ctx, query, scheduleID, since).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetScheduleRun fetches a schedule run by id.
func GetScheduleRun(ctx context.Context, database *sql.DB, id string) (*model.ScheduleRun, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, fmt.Errorf("run id cannot be empty")
	}
	query := `SELECT id, agent, schedule_id, schedule_type, COALESCE(message_id, ''), COALESCE(target_id, ''), COALESCE(thread_id, ''), COALESCE(title, ''), COALESCE(prompt, ''), status, started_at, completed_at, COALESCE(duration_ms, 0), COALESCE(error, ''), COALESCE(effort, 'medium'), COALESCE(model, '') FROM schedule_runs WHERE id = $1`
	var r model.ScheduleRun
	var completedAt sql.NullTime
	row := database.QueryRowContext(ctx, query, id)
	err = row.Scan(&r.ID, &r.Agent, &r.ScheduleID, &r.ScheduleType, &r.MessageID, &r.TargetID, &r.ThreadID, &r.Title, &r.Prompt, &r.Status, &r.StartedAt, &completedAt, &r.DurationMs, &r.Error, &r.Effort, &r.Model)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("schedule run %q not found: %w", id, sql.ErrNoRows)
		}
		return nil, err
	}
	r.Agent = model.NormalizeAgent(r.Agent)
	if completedAt.Valid {
		t := completedAt.Time
		r.CompletedAt = &t
	}
	return &r, nil
}

// GetScheduleAgent finds the owning agent of a schedule across cron, one-shot, heartbeat, and event trigger tables.
func GetScheduleAgent(ctx context.Context, database *sql.DB, id string) (string, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return "", err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return "", fmt.Errorf("schedule_id cannot be empty")
	}
	for _, tbl := range []string{"cron_schedules", "one_shot_schedules", "heartbeat_schedules", "event_triggers"} {
		query := fmt.Sprintf("SELECT agent FROM %s WHERE id = $1", tbl)
		var agent string
		err := database.QueryRowContext(ctx, query, id).Scan(&agent)
		if err == nil {
			return model.NormalizeAgent(agent), nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
	}
	return "", fmt.Errorf("schedule %q not found: %w", id, sql.ErrNoRows)
}

// UpdateScheduleRunOutcome records the terminal outcome of a schedule run.
func UpdateScheduleRunOutcome(ctx context.Context, database *sql.DB, id, status, errStr string, completedAt time.Time, durationMs int64) error {
	query := `UPDATE schedule_runs SET status = $1, error = $2, completed_at = $3, duration_ms = $4 WHERE id = $5`
	return execScheduleUpdate(ctx, database, "schedule run", id, query, status, errStr, completedAt, durationMs, strings.TrimSpace(id))
}

// PruneScheduleRuns deletes runs older than maxAge or beyond maxCount newest.
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

var isSQLiteDBFn = isSQLiteDB

func isSQLiteDB(db *sql.DB) bool {
	if db == nil {
		return false
	}
	return strings.Contains(strings.ToLower(fmt.Sprintf("%T", db.Driver())), "sqlite")
}

func findEarliestNextRun(crons []model.CronSchedule, heartbeats []model.HeartbeatSchedule, oneshots []model.OneShotSchedule) *time.Time {
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

func compute24hRunMetrics(ctx context.Context, database *sql.DB, agent ...string) (int, float64, error) {
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	agentFilter := ""
	if len(agent) > 0 {
		agentFilter = strings.TrimSpace(agent[0])
	}

	query := `SELECT status FROM schedule_runs WHERE started_at >= $1`
	var rows *sql.Rows
	var err error
	if agentFilter != "" && agentFilter != "*" {
		query += ` AND agent = $2`
		rows, err = database.QueryContext(ctx, query, cutoff, agentFilter)
	} else {
		rows, err = database.QueryContext(ctx, query, cutoff)
	}
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
		if normStatus == model.StatusCompleted || normStatus == "enqueued" || normStatus == model.StatusSuccess {
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
func GetScheduleSummaryMetrics(ctx context.Context, database *sql.DB, agent ...string) (model.ScheduleSummaryMetrics, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return model.ScheduleSummaryMetrics{}, err
	}

	agentFilter := ""
	if len(agent) > 0 {
		agentFilter = strings.TrimSpace(agent[0])
	}

	crons, err := ListCronSchedules(ctx, database, "", agentFilter)
	if err != nil {
		return model.ScheduleSummaryMetrics{}, fmt.Errorf("failed to list cron schedules: %w", err)
	}
	heartbeats, err := ListHeartbeatSchedules(ctx, database, "", agentFilter)
	if err != nil {
		return model.ScheduleSummaryMetrics{}, fmt.Errorf("failed to list heartbeat schedules: %w", err)
	}
	oneshots, err := ListOneShotSchedules(ctx, database, "", agentFilter)
	if err != nil {
		return model.ScheduleSummaryMetrics{}, fmt.Errorf("failed to list one-shot schedules: %w", err)
	}

	totalRuns, successRate, err := compute24hRunMetrics(ctx, database, agentFilter)
	if err != nil {
		return model.ScheduleSummaryMetrics{}, err
	}

	return model.ScheduleSummaryMetrics{
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
func GetScheduleRunsPaginated(ctx context.Context, database *sql.DB, limit, offset int, scheduleID, status string, agent ...string) ([]model.ScheduleRunJSON, int, error) {
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

	agentFilter := ""
	if len(agent) > 0 {
		agentFilter = strings.TrimSpace(agent[0])
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
	if agentFilter != "" && agentFilter != "*" {
		filterArgs = append(filterArgs, agentFilter)
		whereClause += fmt.Sprintf(" AND agent = $%d", len(filterArgs))
	}

	countQuery := "SELECT COUNT(*) FROM schedule_runs" + whereClause
	var total int
	if err = database.QueryRowContext(ctx, countQuery, filterArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count schedule runs: %w", err)
	}

	selectArgs := make([]any, len(filterArgs), len(filterArgs)+2)
	copy(selectArgs, filterArgs)
	selectArgs = append(selectArgs, limit, offset)
	limitParam := len(selectArgs) - 1
	offsetParam := len(selectArgs)

	selectQuery := fmt.Sprintf("SELECT id, agent, schedule_id, schedule_type, COALESCE(message_id, ''), COALESCE(target_id, ''), COALESCE(thread_id, ''), COALESCE(title, ''), COALESCE(prompt, ''), started_at, completed_at, COALESCE(duration_ms, 0), status, COALESCE(error, '') FROM schedule_runs%s ORDER BY started_at DESC LIMIT $%d OFFSET $%d", whereClause, limitParam, offsetParam)

	rows, err := database.QueryContext(ctx, selectQuery, selectArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query schedule runs: %w", err)
	}
	defer rows.Close()

	runs := make([]model.ScheduleRunJSON, 0)
	for rows.Next() {
		var r model.ScheduleRunJSON
		var completedAt sql.NullTime
		if err := rows.Scan(
			&r.ID,
			&r.Agent,
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
		r.Agent = model.NormalizeAgent(r.Agent)
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
