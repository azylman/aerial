package db

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"
)

type OneShotSchedule struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	Prompt    string    `json:"prompt"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
	Effort    string    `json:"effort,omitempty"`
}

type CronSchedule struct {
	ID          string    `json:"id"`
	TargetID    string    `json:"target_id"`
	TitlePrefix string    `json:"title_prefix"`
	CronExpr    string    `json:"cron_expr"`
	Prompt      string    `json:"prompt"`
	Timezone    string    `json:"timezone"`
	NextRunAt   time.Time `json:"next_run_at"`
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"created_at"`
	Effort      string    `json:"effort,omitempty"`
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

type UpdateRunParams struct {
	RunID       string    `json:"run_id"`
	MessageID   string    `json:"message_id"`
	Status      string    `json:"status"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMs  int64     `json:"duration_ms"`
	Error       string    `json:"error"`
	Model       string    `json:"model,omitempty"`
}

type ScheduleSummaryMetrics struct {
	TotalActive    int        `json:"total_active"`
	CronCount      int        `json:"cron_count"`
	OneShotCount   int        `json:"one_shot_count"`
	TotalRuns24h   int        `json:"total_runs_24h"`
	NextRunAt      *time.Time `json:"next_run_at"`
	SuccessRate24h float64    `json:"success_rate_24h"`
}

func normalizeScheduleEffort(effort string) string {
	switch eff := strings.ToLower(strings.TrimSpace(effort)); eff {
	case "low", "medium", "high":
		return eff
	default:
		return "medium"
	}
}

func (s *SQLStore) execRowsAffected(ctx context.Context, d time.Duration, query string, args ...any) (int64, error) {
	res, err := s.execWithTimeout(ctx, d, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func scanOneShotRows(rows *sql.Rows) ([]OneShotSchedule, error) {
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

func (s *SQLStore) queryOneShotSchedules(ctx context.Context, suffixSQL string, args ...any) ([]OneShotSchedule, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()
	query := "SELECT id, thread_id, prompt, run_at, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM one_shot_schedules"
	if suffixSQL != "" {
		query += " " + suffixSQL
	}
	rows, err := s.db.QueryContext(queryCtx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOneShotRows(rows)
}

func (s *SQLStore) CreateOneShotSchedule(ctx context.Context, sched OneShotSchedule) error {
	if sched.CreatedAt.IsZero() {
		sched.CreatedAt = time.Now().UTC()
	}
	sched.Effort = normalizeScheduleEffort(sched.Effort)
	query := `INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := s.execWithTimeout(ctx, 5*time.Second, query, sched.ID, sched.ThreadID, sched.Prompt, sched.RunAt, sched.CreatedAt, sched.Effort)
	return err
}

func (s *SQLStore) GetDueOneShotSchedules(ctx context.Context) ([]OneShotSchedule, error) {
	return s.queryOneShotSchedules(ctx, "WHERE run_at <= $1", time.Now().UTC())
}

func (s *SQLStore) GetAllOneShotSchedules(ctx context.Context, threadID string) ([]OneShotSchedule, error) {
	if threadID != "" {
		return s.queryOneShotSchedules(ctx, "WHERE thread_id = $1 ORDER BY run_at ASC", threadID)
	}
	return s.queryOneShotSchedules(ctx, "ORDER BY run_at ASC")
}

func (s *SQLStore) InsertMessageAndConsumeOneShot(ctx context.Context, scheduleID string, msg Message) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	exec := s.db
	type beginner interface {
		BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	}
	var tx *sql.Tx
	if b, ok := s.db.(beginner); ok {
		var err error
		if tx, err = b.BeginTx(queryCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
			return err
		}
		defer rollbackWarn(tx, "tx")
		exec = tx
	}

	if err := (&SQLStore{db: exec, isPostgres: s.isPostgres}).InsertMessage(queryCtx, msg); err != nil {
		return err
	}
	var deletedID string
	err := exec.QueryRowContext(queryCtx, `DELETE FROM one_shot_schedules WHERE id = $1 RETURNING id;`, scheduleID).Scan(&deletedID)
	if err == sql.ErrNoRows {
		return fmt.Errorf("one-shot schedule %s not found or already consumed", scheduleID)
	}
	if err != nil {
		return err
	}
	if tx != nil {
		return tx.Commit()
	}
	return nil
}

func scanCronRows(rows *sql.Rows) ([]CronSchedule, error) {
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

func (s *SQLStore) queryCronSchedules(ctx context.Context, suffixSQL string, args ...any) ([]CronSchedule, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := "SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(NULLIF(effort, ''), 'medium') FROM cron_schedules WHERE enabled = TRUE"
	if suffixSQL != "" {
		query += " " + suffixSQL
	}
	rows, err := s.db.QueryContext(queryCtx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCronRows(rows)
}

func (s *SQLStore) GetDueCronSchedules(ctx context.Context) ([]CronSchedule, error) {
	return s.queryCronSchedules(ctx, "AND next_run_at <= $1", time.Now().UTC())
}

func (s *SQLStore) GetAllCronSchedules(ctx context.Context, targetID string) ([]CronSchedule, error) {
	if targetID != "" {
		return s.queryCronSchedules(ctx, "AND target_id = $1 ORDER BY created_at ASC", targetID)
	}
	return s.queryCronSchedules(ctx, "ORDER BY created_at ASC")
}

func (s *SQLStore) UpdateCronNextRun(ctx context.Context, id string, nextRunAt time.Time) error {
	if id == "" {
		return nil
	}
	_, err := s.execWithTimeout(ctx, 5*time.Second, `UPDATE cron_schedules SET next_run_at = $1 WHERE id = $2`, nextRunAt, id)
	return err
}

func (s *SQLStore) CreateScheduleRun(ctx context.Context, run ScheduleRun) error {
	if run.ID == "" {
		return fmt.Errorf("schedule run id cannot be empty")
	}
	if run.StartedAt.IsZero() {
		run.StartedAt = time.Now().UTC()
	}
	if run.Status == "" {
		run.Status = "enqueued"
	}
	var completedAtVal any
	if run.CompletedAt != nil {
		completedAtVal = *run.CompletedAt
	}
	query := `INSERT INTO schedule_runs (id, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, completed_at, duration_ms, error, effort, model) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`
	_, err := s.execWithTimeout(ctx, 5*time.Second, query, run.ID, run.ScheduleID, run.ScheduleType, run.MessageID, run.TargetID, run.ThreadID, run.Title, run.Prompt, run.Status, run.StartedAt, completedAtVal, run.DurationMs, run.Error, normalizeScheduleEffort(run.Effort), run.Model)
	return err
}

func (s *SQLStore) UpdateScheduleRunStatus(ctx context.Context, params UpdateRunParams) error {
	if params.RunID == "" {
		return fmt.Errorf("schedule run id cannot be empty")
	}
	var sets []string
	var args []any
	addSet := func(col string, val any) {
		sets = append(sets, fmt.Sprintf("%s = $%d", col, len(args)+1))
		args = append(args, val)
	}
	if params.Status != "" { addSet("status", params.Status) }
	if params.MessageID != "" { addSet("message_id", params.MessageID) }
	if !params.CompletedAt.IsZero() { addSet("completed_at", params.CompletedAt) }
	if params.DurationMs != 0 { addSet("duration_ms", params.DurationMs) }
	if params.Model != "" { addSet("model", params.Model) }
	if params.Error != "" {
		addSet("error", params.Error)
	} else if params.Status == "completed" {
		sets = append(sets, "error = ''")
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, params.RunID)
	_, err := s.execWithTimeout(ctx, 5*time.Second, fmt.Sprintf("UPDATE schedule_runs SET %s WHERE id = $%d", strings.Join(sets, ", "), len(args)), args...)
	return err
}

func (s *SQLStore) GetScheduleRunsPaginated(ctx context.Context, limit, offset int, scheduleID, status string) ([]ScheduleRun, int, error) {
	if err := s.checkDB(); err != nil {
		return nil, 0, err
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	var whereClauses []string
	var args []any
	if sID := strings.TrimSpace(scheduleID); sID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("schedule_id = $%d", len(args)+1))
		args = append(args, sID)
	}
	if st := strings.TrimSpace(status); st != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", len(args)+1))
		args = append(args, st)
	}
	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = " WHERE " + strings.Join(whereClauses, " AND ")
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()
	var total int
	if err := s.db.QueryRowContext(queryCtx, "SELECT COUNT(*) FROM schedule_runs"+whereSQL, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count schedule runs: %w", err)
	}
	selectQuery := fmt.Sprintf(`SELECT id, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, completed_at, duration_ms, error, COALESCE(NULLIF(effort, ''), 'medium'), COALESCE(model, '') FROM schedule_runs%s ORDER BY started_at DESC, id DESC LIMIT $%d OFFSET $%d`, whereSQL, len(args)+1, len(args)+2)
	rows, err := s.db.QueryContext(queryCtx, selectQuery, append(args, limit, offset)...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query schedule runs: %w", err)
	}
	defer rows.Close()
	runs := make([]ScheduleRun, 0)
	for rows.Next() {
		var r ScheduleRun
		var completedAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.ScheduleID, &r.ScheduleType, &r.MessageID, &r.TargetID, &r.ThreadID, &r.Title, &r.Prompt, &r.Status, &r.StartedAt, &completedAt, &r.DurationMs, &r.Error, &r.Effort, &r.Model); err != nil {
			return nil, 0, fmt.Errorf("failed to scan schedule run: %w", err)
		}
		if completedAt.Valid {
			r.CompletedAt = &completedAt.Time
		}
		runs = append(runs, r)
	}
	return runs, total, rows.Err()
}

func (s *SQLStore) GetScheduleSummaryMetrics(ctx context.Context) (ScheduleSummaryMetrics, error) {
	if err := s.checkDB(); err != nil {
		return ScheduleSummaryMetrics{}, err
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()
	var metrics ScheduleSummaryMetrics
	if err := s.db.QueryRowContext(queryCtx, "SELECT COUNT(*) FROM cron_schedules WHERE enabled = TRUE").Scan(&metrics.CronCount); err != nil {
		return metrics, fmt.Errorf("failed to count cron schedules: %w", err)
	}
	if err := s.db.QueryRowContext(queryCtx, "SELECT COUNT(*) FROM one_shot_schedules").Scan(&metrics.OneShotCount); err != nil {
		return metrics, fmt.Errorf("failed to count one-shot schedules: %w", err)
	}
	metrics.TotalActive = metrics.CronCount + metrics.OneShotCount

	var cronNext, oneShotNext sql.NullTime
	if err := s.db.QueryRowContext(queryCtx, "SELECT next_run_at FROM cron_schedules WHERE enabled = TRUE ORDER BY next_run_at ASC LIMIT 1").Scan(&cronNext); err != nil && err != sql.ErrNoRows {
		return metrics, fmt.Errorf("failed to query next cron run: %w", err)
	}
	if err := s.db.QueryRowContext(queryCtx, "SELECT run_at FROM one_shot_schedules ORDER BY run_at ASC LIMIT 1").Scan(&oneShotNext); err != nil && err != sql.ErrNoRows {
		return metrics, fmt.Errorf("failed to query next one-shot run: %w", err)
	}
	if cronNext.Valid {
		metrics.NextRunAt = &cronNext.Time
	}
	if oneShotNext.Valid && (metrics.NextRunAt == nil || oneShotNext.Time.Before(*metrics.NextRunAt)) {
		metrics.NextRunAt = &oneShotNext.Time
	}

	var totalRuns, completedRuns int
	if err := s.db.QueryRowContext(queryCtx, "SELECT COUNT(*), COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0) FROM schedule_runs WHERE started_at >= $1", time.Now().UTC().Add(-24*time.Hour)).Scan(&totalRuns, &completedRuns); err != nil {
		return metrics, fmt.Errorf("failed to query 24h run metrics: %w", err)
	}
	metrics.TotalRuns24h = totalRuns
	if totalRuns == 0 {
		metrics.SuccessRate24h = 100.0
	} else {
		metrics.SuccessRate24h = math.Round((float64(completedRuns)/float64(totalRuns))*1000.0) / 10.0
	}
	return metrics, nil
}

func (s *SQLStore) ReconcileOrphanedScheduleRuns(ctx context.Context) (int64, error) {
	return s.execRowsAffected(ctx, 5*time.Second, `UPDATE schedule_runs SET status = 'failed', error = 'Interrupted by server restart', completed_at = $1 WHERE status IN ('enqueued', 'running')`, time.Now().UTC())
}

func (s *SQLStore) PruneScheduleRuns(ctx context.Context, maxCount int, maxAge time.Duration) (int64, error) {
	if maxCount <= 0 {
		maxCount = 1000
	}
	if maxAge <= 0 {
		maxAge = 30 * 24 * time.Hour
	}
	return s.execRowsAffected(ctx, 10*time.Second, `DELETE FROM schedule_runs WHERE started_at < $1 OR id NOT IN (SELECT id FROM schedule_runs ORDER BY started_at DESC, id DESC LIMIT $2)`, time.Now().UTC().Add(-maxAge), maxCount)
}
