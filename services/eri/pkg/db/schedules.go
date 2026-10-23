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

type HeartbeatSchedule struct {
	ID          string    `json:"id"`
	TargetID    string    `json:"target_id"`
	TitlePrefix string    `json:"title_prefix"`
	CronExpr    string    `json:"cron_expr"`
	Prompt      string    `json:"prompt"`
	Timezone    string    `json:"timezone"`
	Effort      string    `json:"effort"`
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

type ScheduleSummaryMetrics struct {
	TotalActive    int        `json:"total_active"`
	CronCount      int        `json:"cron_count"`
	HeartbeatCount int        `json:"heartbeat_count"`
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

func (s *SQLStore) GetAllOneShotSchedules(ctx context.Context, threadID string) ([]OneShotSchedule, error) {
	if threadID != "" {
		return s.queryOneShotSchedules(ctx, "WHERE thread_id = $1 ORDER BY run_at ASC", threadID)
	}
	return s.queryOneShotSchedules(ctx, "ORDER BY run_at ASC")
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

func (s *SQLStore) GetAllCronSchedules(ctx context.Context, targetID string) ([]CronSchedule, error) {
	if targetID != "" {
		return s.queryCronSchedules(ctx, "AND target_id = $1 ORDER BY created_at ASC", targetID)
	}
	return s.queryCronSchedules(ctx, "ORDER BY created_at ASC")
}

func scanHeartbeatRows(rows *sql.Rows) ([]HeartbeatSchedule, error) {
	var results []HeartbeatSchedule
	for rows.Next() {
		var h HeartbeatSchedule
		if err := rows.Scan(&h.ID, &h.TargetID, &h.TitlePrefix, &h.CronExpr, &h.Prompt, &h.Timezone, &h.Effort, &h.NextRunAt, &h.Enabled, &h.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, h)
	}
	return results, rows.Err()
}

func (s *SQLStore) queryHeartbeatSchedules(ctx context.Context, suffixSQL string, args ...any) ([]HeartbeatSchedule, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := "SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, COALESCE(NULLIF(effort, ''), 'low'), next_run_at, enabled, created_at FROM heartbeat_schedules WHERE enabled = TRUE"
	if suffixSQL != "" {
		query += " " + suffixSQL
	}
	rows, err := s.db.QueryContext(queryCtx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanHeartbeatRows(rows)
}

func (s *SQLStore) GetAllHeartbeatSchedules(ctx context.Context, targetID string) ([]HeartbeatSchedule, error) {
	if targetID != "" {
		return s.queryHeartbeatSchedules(ctx, "AND target_id = $1 ORDER BY created_at ASC", targetID)
	}
	return s.queryHeartbeatSchedules(ctx, "ORDER BY created_at ASC")
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
	if err := s.db.QueryRowContext(queryCtx, "SELECT COUNT(*) FROM heartbeat_schedules WHERE enabled = TRUE").Scan(&metrics.HeartbeatCount); err != nil {
		return metrics, fmt.Errorf("failed to count heartbeat schedules: %w", err)
	}
	if err := s.db.QueryRowContext(queryCtx, "SELECT COUNT(*) FROM one_shot_schedules").Scan(&metrics.OneShotCount); err != nil {
		return metrics, fmt.Errorf("failed to count one-shot schedules: %w", err)
	}
	metrics.TotalActive = metrics.CronCount + metrics.HeartbeatCount + metrics.OneShotCount

	var cronNext, heartbeatNext, oneShotNext sql.NullTime
	if err := s.db.QueryRowContext(queryCtx, "SELECT next_run_at FROM cron_schedules WHERE enabled = TRUE ORDER BY next_run_at ASC LIMIT 1").Scan(&cronNext); err != nil && err != sql.ErrNoRows {
		return metrics, fmt.Errorf("failed to query next cron run: %w", err)
	}
	if err := s.db.QueryRowContext(queryCtx, "SELECT next_run_at FROM heartbeat_schedules WHERE enabled = TRUE ORDER BY next_run_at ASC LIMIT 1").Scan(&heartbeatNext); err != nil && err != sql.ErrNoRows {
		return metrics, fmt.Errorf("failed to query next heartbeat run: %w", err)
	}
	if err := s.db.QueryRowContext(queryCtx, "SELECT run_at FROM one_shot_schedules ORDER BY run_at ASC LIMIT 1").Scan(&oneShotNext); err != nil && err != sql.ErrNoRows {
		return metrics, fmt.Errorf("failed to query next one-shot run: %w", err)
	}
	if cronNext.Valid {
		metrics.NextRunAt = &cronNext.Time
	}
	if heartbeatNext.Valid && (metrics.NextRunAt == nil || heartbeatNext.Time.Before(*metrics.NextRunAt)) {
		metrics.NextRunAt = &heartbeatNext.Time
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
