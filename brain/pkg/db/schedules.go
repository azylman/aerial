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
}

type UpdateRunParams struct {
	RunID       string    `json:"run_id"`
	MessageID   string    `json:"message_id"`
	Status      string    `json:"status"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMs  int64     `json:"duration_ms"`
	Error       string    `json:"error"`
}

type ScheduleSummaryMetrics struct {
	TotalActive    int        `json:"total_active"`
	CronCount      int        `json:"cron_count"`
	OneShotCount   int        `json:"one_shot_count"`
	TotalRuns24h   int        `json:"total_runs_24h"`
	NextRunAt      *time.Time `json:"next_run_at"`
	SuccessRate24h float64    `json:"success_rate_24h"`
}

func CreateOneShotSchedule(database DBTX, s OneShotSchedule) error {
	if database == nil {
		return nil
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at) VALUES ($1, $2, $3, $4, $5)`
	_, err := database.ExecContext(ctx, query, s.ID, s.ThreadID, s.Prompt, s.RunAt, s.CreatedAt)
	return err
}

func GetDueOneShotSchedules(database DBTX) ([]OneShotSchedule, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `SELECT id, thread_id, prompt, run_at, created_at FROM one_shot_schedules WHERE run_at <= $1`
	rows, err := database.QueryContext(ctx, query, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []OneShotSchedule
	for rows.Next() {
		var s OneShotSchedule
		if err := rows.Scan(&s.ID, &s.ThreadID, &s.Prompt, &s.RunAt, &s.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, s)
	}
	return results, nil
}

func DeleteOneShotSchedule(database DBTX, id string) error {
	if database == nil || id == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := database.ExecContext(ctx, `DELETE FROM one_shot_schedules WHERE id = $1`, id)
	return err
}

func InsertMessageAndConsumeOneShot(database DBTX, scheduleID string, msg Message) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if msg.ID == "" {
		return fmt.Errorf("message id cannot be empty")
	}
	if msg.Status == "" {
		msg.Status = StatusPending
	}
	now := time.Now().UTC()
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.UpdatedAt.IsZero() {
		msg.UpdatedAt = now
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exec := database
	type beginner interface {
		BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	}
	var tx *sql.Tx
	if b, ok := database.(beginner); ok {
		var err error
		tx, err = b.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		exec = tx
	}

	insertQuery := `
	INSERT INTO messages (id, thread_id, guild_id, author_id, author_name, content, summary, status, retry_count, restart_count, error_message, response_text, schedule_run_id, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
	ON CONFLICT (id) DO NOTHING;
	`
	if _, err := exec.ExecContext(ctx, insertQuery, msg.ID, msg.ThreadID, msg.GuildID, msg.AuthorID, msg.AuthorName, msg.Content, msg.Summary, msg.Status, msg.RetryCount, msg.RestartCount, msg.ErrorMessage, msg.ResponseText, msg.ScheduleRunID, msg.CreatedAt, msg.UpdatedAt); err != nil {
		return err
	}

	var deletedID string
	deleteQuery := `DELETE FROM one_shot_schedules WHERE id = $1 RETURNING id;`
	err := exec.QueryRowContext(ctx, deleteQuery, scheduleID).Scan(&deletedID)
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

func GetAllOneShotSchedules(database DBTX, threadID string) ([]OneShotSchedule, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `SELECT id, thread_id, prompt, run_at, created_at FROM one_shot_schedules`
	var rows *sql.Rows
	var err error
	if threadID != "" {
		query += ` WHERE thread_id = $1 ORDER BY run_at ASC`
		rows, err = database.QueryContext(ctx, query, threadID)
	} else {
		query += ` ORDER BY run_at ASC`
		rows, err = database.QueryContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []OneShotSchedule
	for rows.Next() {
		var s OneShotSchedule
		if err := rows.Scan(&s.ID, &s.ThreadID, &s.Prompt, &s.RunAt, &s.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, s)
	}
	return results, nil
}

func CreateCronSchedule(database DBTX, c CronSchedule) error {
	if database == nil {
		return nil
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	if c.Timezone == "" {
		c.Timezone = "America/Los_Angeles"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	_, err := database.ExecContext(ctx, query, c.ID, c.TargetID, c.TitlePrefix, c.CronExpr, c.Prompt, c.Timezone, c.NextRunAt, c.Enabled, c.CreatedAt)
	return err
}

func GetDueCronSchedules(database DBTX) ([]CronSchedule, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at FROM cron_schedules WHERE enabled = TRUE AND next_run_at <= $1`
	rows, err := database.QueryContext(ctx, query, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []CronSchedule
	for rows.Next() {
		var c CronSchedule
		if err := rows.Scan(&c.ID, &c.TargetID, &c.TitlePrefix, &c.CronExpr, &c.Prompt, &c.Timezone, &c.NextRunAt, &c.Enabled, &c.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, c)
	}
	return results, nil
}

func GetAllCronSchedules(database DBTX, targetID string) ([]CronSchedule, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at FROM cron_schedules WHERE enabled = TRUE`
	var rows *sql.Rows
	var err error
	if targetID != "" {
		query += ` AND target_id = $1 ORDER BY created_at ASC`
		rows, err = database.QueryContext(ctx, query, targetID)
	} else {
		query += ` ORDER BY created_at ASC`
		rows, err = database.QueryContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []CronSchedule
	for rows.Next() {
		var c CronSchedule
		if err := rows.Scan(&c.ID, &c.TargetID, &c.TitlePrefix, &c.CronExpr, &c.Prompt, &c.Timezone, &c.NextRunAt, &c.Enabled, &c.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, c)
	}
	return results, nil
}

func DeleteCronSchedule(database DBTX, id string) error {
	if database == nil || id == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := database.ExecContext(ctx, `DELETE FROM cron_schedules WHERE id = $1`, id)
	return err
}

func UpdateCronNextRun(database DBTX, id string, nextRunAt time.Time) error {
	if database == nil || id == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := database.ExecContext(ctx, `UPDATE cron_schedules SET next_run_at = $1 WHERE id = $2`, nextRunAt, id)
	return err
}

func CreateScheduleRun(database DBTX, run ScheduleRun) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if run.ID == "" {
		return fmt.Errorf("schedule run id cannot be empty")
	}
	if run.Status == "" {
		run.Status = "enqueued"
	}
	if run.StartedAt.IsZero() {
		run.StartedAt = time.Now().UTC()
	}

	var completedAtVal interface{}
	if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
		completedAtVal = *run.CompletedAt
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	INSERT INTO schedule_runs (id, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, completed_at, duration_ms, error)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`
	_, err := database.ExecContext(ctx, query,
		run.ID,
		run.ScheduleID,
		run.ScheduleType,
		run.MessageID,
		run.TargetID,
		run.ThreadID,
		run.Title,
		run.Prompt,
		run.Status,
		run.StartedAt,
		completedAtVal,
		run.DurationMs,
		run.Error,
	)
	return err
}

func UpdateScheduleRunStatus(database DBTX, params UpdateRunParams) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if params.RunID == "" {
		return fmt.Errorf("schedule run id cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var sets []string
	var args []interface{}
	idx := 1

	if params.Status != "" {
		sets = append(sets, fmt.Sprintf("status = $%d", idx))
		args = append(args, params.Status)
		idx++
	}
	if params.MessageID != "" {
		sets = append(sets, fmt.Sprintf("message_id = $%d", idx))
		args = append(args, params.MessageID)
		idx++
	}
	if !params.CompletedAt.IsZero() {
		sets = append(sets, fmt.Sprintf("completed_at = $%d", idx))
		args = append(args, params.CompletedAt)
		idx++
	}
	if params.DurationMs != 0 {
		sets = append(sets, fmt.Sprintf("duration_ms = $%d", idx))
		args = append(args, params.DurationMs)
		idx++
	}
	if params.Error != "" {
		sets = append(sets, fmt.Sprintf("error = $%d", idx))
		args = append(args, params.Error)
		idx++
	} else if params.Status == "completed" {
		sets = append(sets, "error = ''")
	}

	if len(sets) == 0 {
		return nil
	}

	query := fmt.Sprintf("UPDATE schedule_runs SET %s WHERE id = $%d", strings.Join(sets, ", "), idx)
	args = append(args, params.RunID)

	_, err := database.ExecContext(ctx, query, args...)
	return err
}

func GetScheduleRunsPaginated(database DBTX, limit, offset int, scheduleID, status string) ([]ScheduleRun, int, error) {
	if database == nil {
		return nil, 0, fmt.Errorf("database is nil")
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	var whereClauses []string
	var args []interface{}
	argIdx := 1

	if strings.TrimSpace(scheduleID) != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("schedule_id = $%d", argIdx))
		args = append(args, strings.TrimSpace(scheduleID))
		argIdx++
	}
	if strings.TrimSpace(status) != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, strings.TrimSpace(status))
		argIdx++
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	countQuery := "SELECT COUNT(*) FROM schedule_runs" + whereSQL
	var total int
	if err := database.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count schedule runs: %w", err)
	}

	selectQuery := fmt.Sprintf(`
		SELECT id, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, completed_at, duration_ms, error
		FROM schedule_runs
		%s
		ORDER BY started_at DESC, id DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, argIdx, argIdx+1)

	queryArgs := append(args, limit, offset)
	rows, err := database.QueryContext(ctx, selectQuery, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query schedule runs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	runs := make([]ScheduleRun, 0)
	for rows.Next() {
		var r ScheduleRun
		var completedAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.ScheduleID, &r.ScheduleType, &r.MessageID, &r.TargetID, &r.ThreadID, &r.Title, &r.Prompt, &r.Status, &r.StartedAt, &completedAt, &r.DurationMs, &r.Error); err != nil {
			return nil, 0, fmt.Errorf("failed to scan schedule run: %w", err)
		}
		if completedAt.Valid {
			r.CompletedAt = &completedAt.Time
		}
		runs = append(runs, r)
	}

	return runs, total, nil
}

func GetScheduleSummaryMetrics(database DBTX) (ScheduleSummaryMetrics, error) {
	if database == nil {
		return ScheduleSummaryMetrics{}, fmt.Errorf("database is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var metrics ScheduleSummaryMetrics

	// 1. Cron count (enabled only)
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM cron_schedules WHERE enabled = TRUE").Scan(&metrics.CronCount); err != nil {
		return metrics, fmt.Errorf("failed to count cron schedules: %w", err)
	}

	// 2. One-shot count
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM one_shot_schedules").Scan(&metrics.OneShotCount); err != nil {
		return metrics, fmt.Errorf("failed to count one-shot schedules: %w", err)
	}

	metrics.TotalActive = metrics.CronCount + metrics.OneShotCount

	// 3. Next run timestamp
	var cronNext, oneShotNext sql.NullTime
	errCron := database.QueryRowContext(ctx, "SELECT next_run_at FROM cron_schedules WHERE enabled = TRUE ORDER BY next_run_at ASC LIMIT 1").Scan(&cronNext)
	if errCron != nil && errCron != sql.ErrNoRows {
		return metrics, fmt.Errorf("failed to query next cron run: %w", errCron)
	}
	errOneShot := database.QueryRowContext(ctx, "SELECT run_at FROM one_shot_schedules ORDER BY run_at ASC LIMIT 1").Scan(&oneShotNext)
	if errOneShot != nil && errOneShot != sql.ErrNoRows {
		return metrics, fmt.Errorf("failed to query next one-shot run: %w", errOneShot)
	}

	if cronNext.Valid && oneShotNext.Valid {
		if cronNext.Time.Before(oneShotNext.Time) {
			metrics.NextRunAt = &cronNext.Time
		} else {
			metrics.NextRunAt = &oneShotNext.Time
		}
	} else if cronNext.Valid {
		metrics.NextRunAt = &cronNext.Time
	} else if oneShotNext.Valid {
		metrics.NextRunAt = &oneShotNext.Time
	}

	// 4. 24-hour run stats and success rate
	cutoff24h := time.Now().UTC().Add(-24 * time.Hour)
	runs24hQuery := `
	SELECT 
		COUNT(*),
		COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0)
	FROM schedule_runs
	WHERE started_at >= $1
	`
	var totalRuns, completedRuns int
	if err := database.QueryRowContext(ctx, runs24hQuery, cutoff24h).Scan(&totalRuns, &completedRuns); err != nil {
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

func ReconcileOrphanedScheduleRuns(database DBTX) (int64, error) {
	if database == nil {
		return 0, fmt.Errorf("database is nil")
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	UPDATE schedule_runs
	SET status = 'failed',
	    error = 'Interrupted by server restart',
	    completed_at = $1
	WHERE status IN ('enqueued', 'running')
	`
	res, err := database.ExecContext(ctx, query, now)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func PruneScheduleRuns(database DBTX, maxCount int, maxAge time.Duration) (int64, error) {
	if database == nil {
		return 0, fmt.Errorf("database is nil")
	}
	if maxCount <= 0 {
		maxCount = 1000
	}
	if maxAge <= 0 {
		maxAge = 30 * 24 * time.Hour
	}
	cutoff := time.Now().UTC().Add(-maxAge)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
	DELETE FROM schedule_runs
	WHERE started_at < $1
	   OR id NOT IN (
		   SELECT id FROM schedule_runs
		   ORDER BY started_at DESC, id DESC
		   LIMIT $2
	   )
	`
	res, err := database.ExecContext(ctx, query, cutoff, maxCount)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
