package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
)

var sqliteTestInitHook func(string) (*sql.DB, error)

// Domain row types (CronSchedule, OneShotSchedule, HeartbeatSchedule,
// EventTrigger, ScheduleRun, ScheduleRunJSON, ScheduleSummaryMetrics) now
// live in internal/model; pure.go keeps zero-logic aliases for this package.

// OpenDB opens the scheduler database from cfg. SQLite DSNs keep routing
// through the test hook until the event-trigger half moves to internal/store;
// all other DSNs delegate to internal/store.
func OpenDB(ctx context.Context, cfg *Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	return openDB(ctx, cfg.DatabaseURL)
}

// NewDB opens the scheduler database from cfg.
func NewDB(ctx context.Context, cfg *Config) (*sql.DB, error) {
	return OpenDB(ctx, cfg)
}

func openDB(ctx context.Context, dsn string) (*sql.DB, error) {
	trimmed := strings.TrimSpace(dsn)
	trimmed = strings.TrimPrefix(trimmed, "sqlite://")
	if sqliteTestInitHook != nil && (trimmed == ":memory:" || strings.Contains(trimmed, ".db") || strings.Contains(trimmed, "sqlite") || strings.HasPrefix(trimmed, "file:")) {
		return sqliteTestInitHook(trimmed)
	}
	return store.NewDB(ctx, &Config{DatabaseURL: dsn})
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
	return store.NormalizeScheduleEffort(effort)
}

// InsertCronSchedule persists a new cron schedule row.
func InsertCronSchedule(ctx context.Context, database *sql.DB, c CronSchedule) error {
	return store.InsertCronSchedule(ctx, database, c)
}

// InsertOneShotSchedule persists a new one-shot schedule row.
func InsertOneShotSchedule(ctx context.Context, database *sql.DB, s OneShotSchedule) error {
	return store.InsertOneShotSchedule(ctx, database, s)
}

// InsertHeartbeatSchedule persists a new heartbeat schedule row.
func InsertHeartbeatSchedule(ctx context.Context, database *sql.DB, h HeartbeatSchedule) error {
	return store.InsertHeartbeatSchedule(ctx, database, h)
}

// ListCronSchedules lists enabled cron schedules, optionally filtered by target.
func ListCronSchedules(ctx context.Context, database *sql.DB, targetID string) ([]CronSchedule, error) {
	return store.ListCronSchedules(ctx, database, targetID)
}

// ListOneShotSchedules lists one-shot schedules, optionally filtered by thread.
func ListOneShotSchedules(ctx context.Context, database *sql.DB, targetID string) ([]OneShotSchedule, error) {
	return store.ListOneShotSchedules(ctx, database, targetID)
}

// ListHeartbeatSchedules lists enabled heartbeat schedules, optionally filtered by target.
func ListHeartbeatSchedules(ctx context.Context, database *sql.DB, targetID string) ([]HeartbeatSchedule, error) {
	return store.ListHeartbeatSchedules(ctx, database, targetID)
}

// GetCronSchedule fetches a cron schedule by id.
func GetCronSchedule(ctx context.Context, database *sql.DB, id string) (*CronSchedule, error) {
	return store.GetCronSchedule(ctx, database, id)
}

// GetOneShotSchedule fetches a one-shot schedule by id.
func GetOneShotSchedule(ctx context.Context, database *sql.DB, id string) (*OneShotSchedule, error) {
	return store.GetOneShotSchedule(ctx, database, id)
}

// GetHeartbeatSchedule fetches a heartbeat schedule by id.
func GetHeartbeatSchedule(ctx context.Context, database *sql.DB, id string) (*HeartbeatSchedule, error) {
	return store.GetHeartbeatSchedule(ctx, database, id)
}

// UpdateHeartbeatSchedule replaces a heartbeat schedule row.
func UpdateHeartbeatSchedule(ctx context.Context, database *sql.DB, h HeartbeatSchedule) error {
	return store.UpdateHeartbeatSchedule(ctx, database, h)
}

// UpdateCronSchedule patches selected cron schedule columns.
func UpdateCronSchedule(ctx context.Context, database *sql.DB, id string, effort, cronExpr, prompt, titlePrefix, timezone *string, nextRunAt *time.Time) error {
	return store.UpdateCronSchedule(ctx, database, id, effort, cronExpr, prompt, titlePrefix, timezone, nextRunAt)
}

// DeleteSchedule deletes a schedule id from every schedule table.
func DeleteSchedule(ctx context.Context, database *sql.DB, scheduleID string) (bool, error) {
	return store.DeleteSchedule(ctx, database, scheduleID)
}

// GetDueCronSchedules lists enabled cron schedules whose next run is due.
func GetDueCronSchedules(ctx context.Context, database *sql.DB, now time.Time) ([]CronSchedule, error) {
	return store.GetDueCronSchedules(ctx, database, now)
}

// AdvanceCronNextRun moves a cron schedule's next run timestamp.
func AdvanceCronNextRun(ctx context.Context, database *sql.DB, id string, nextRun time.Time) error {
	return store.AdvanceCronNextRun(ctx, database, id, nextRun)
}

// GetDueHeartbeatSchedules lists enabled heartbeat schedules whose next run is due.
func GetDueHeartbeatSchedules(ctx context.Context, database *sql.DB, now time.Time) ([]HeartbeatSchedule, error) {
	return store.GetDueHeartbeatSchedules(ctx, database, now)
}

// AdvanceHeartbeatNextRun moves a heartbeat schedule's next run timestamp.
func AdvanceHeartbeatNextRun(ctx context.Context, database *sql.DB, id string, nextRun time.Time) error {
	return store.AdvanceHeartbeatNextRun(ctx, database, id, nextRun)
}

// AdvanceHeartbeatLastCompletedAt advances a heartbeat's watermark monotonically.
func AdvanceHeartbeatLastCompletedAt(ctx context.Context, database *sql.DB, id string, completedAt time.Time) error {
	return store.AdvanceHeartbeatLastCompletedAt(ctx, database, id, completedAt)
}

// GetDueOneShotSchedules lists one-shot schedules whose run time is due.
func GetDueOneShotSchedules(ctx context.Context, database *sql.DB, now time.Time) ([]OneShotSchedule, error) {
	return store.GetDueOneShotSchedules(ctx, database, now)
}

func deleteByID(ctx context.Context, database *sql.DB, query, emptyErrMsg, errPrefix, id string) (bool, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return false, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		if emptyErrMsg != "" {
			return false, fmt.Errorf("%s", emptyErrMsg)
		}
		return false, fmt.Errorf("id cannot be empty")
	}
	res, err := database.ExecContext(ctx, query, id)
	if err != nil {
		if errPrefix != "" {
			return false, fmt.Errorf("%s: %w", errPrefix, err)
		}
		return false, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		if errPrefix != "" {
			return false, fmt.Errorf("%s: failed to determine rows affected: %w", errPrefix, err)
		}
		return false, fmt.Errorf("failed to determine rows affected: %w", err)
	}
	return rowsAffected > 0, nil
}

// DeleteOneShotSchedule removes a one-shot schedule by id.
func DeleteOneShotSchedule(ctx context.Context, database *sql.DB, id string) (bool, error) {
	return store.DeleteOneShotSchedule(ctx, database, id)
}

// CreateScheduleRun records a new schedule execution attempt.
func CreateScheduleRun(ctx context.Context, database *sql.DB, r ScheduleRun) error {
	return store.CreateScheduleRun(ctx, database, r)
}

// HasRunningHeartbeatRun reports whether a heartbeat has a running run since a cutoff.
func HasRunningHeartbeatRun(ctx context.Context, database *sql.DB, scheduleID string, since time.Time) (bool, error) {
	return store.HasRunningHeartbeatRun(ctx, database, scheduleID, since)
}

// GetScheduleRun fetches a schedule run by id.
func GetScheduleRun(ctx context.Context, database *sql.DB, id string) (*ScheduleRun, error) {
	return store.GetScheduleRun(ctx, database, id)
}

// UpdateScheduleRunOutcome records the terminal outcome of a schedule run.
func UpdateScheduleRunOutcome(ctx context.Context, database *sql.DB, id, status, errStr string, completedAt time.Time, durationMs int64) error {
	return store.UpdateScheduleRunOutcome(ctx, database, id, status, errStr, completedAt, durationMs)
}

// PruneScheduleRuns deletes runs older than maxAge or beyond maxCount newest.
func PruneScheduleRuns(ctx context.Context, database *sql.DB, maxCount int, maxAge time.Duration, now time.Time) (int64, error) {
	return store.PruneScheduleRuns(ctx, database, maxCount, maxAge, now)
}

// GetScheduleSummaryMetrics computes summary metrics across all schedules and runs in the last 24 hours.
func GetScheduleSummaryMetrics(ctx context.Context, database *sql.DB) (ScheduleSummaryMetrics, error) {
	return store.GetScheduleSummaryMetrics(ctx, database)
}

// GetScheduleRunsPaginated fetches a paginated, filtered slice of schedule runs and the total count.
func GetScheduleRunsPaginated(ctx context.Context, database *sql.DB, limit, offset int, scheduleID, status string) ([]ScheduleRunJSON, int, error) {
	return store.GetScheduleRunsPaginated(ctx, database, limit, offset, scheduleID, status)
}

var (
	vanadisMu     sync.RWMutex
	vanadisURL    string
	vanadisClient = tracing.Client(&http.Client{Timeout: 5 * time.Second})
)

// SetVanadisURL sets the base URL for Vanadis HTTP API calls.
func SetVanadisURL(u string) {
	vanadisMu.Lock()
	defer vanadisMu.Unlock()
	vanadisURL = strings.TrimRight(u, "/")
}

// GetVanadisURL gets the base URL for Vanadis HTTP API calls.
func GetVanadisURL() string {
	vanadisMu.RLock()
	defer vanadisMu.RUnlock()
	return vanadisURL
}

// firstNonEmpty delegates to the exported model.FirstNonEmpty seam so
// store callers keep compiling; the logic lives in internal/model.
func firstNonEmpty(vals ...string) string {
	return model.FirstNonEmpty(vals...)
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
	baseURL := strings.TrimSpace(GetVanadisURL())
	switch source {
	case sourceVanadisBuild, sourceVanadisChunk:
		return validateVanadisTarget(ctx, baseURL, source, target)
	}
	return nil
}

func validateVanadisTarget(ctx context.Context, baseURL, source, target string) error {
	if baseURL == "" {
		return fmt.Errorf("vanadis URL is not configured")
	}
	pathPrefix := "builds"
	terminalStates := map[string]bool{
		statusCompleted: true, statusMerged: true, statusFailed: true, "cancelled": true, "canceled": true,
	}
	entityName := "vanadis build"
	if source == sourceVanadisChunk {
		pathPrefix = "chunks"
		terminalStates = map[string]bool{
			statusMerged: true, statusCompleted: true, statusFailed: true,
		}
		entityName = "vanadis chunk"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/%s/%s", baseURL, pathPrefix, url.PathEscape(target)), http.NoBody)
	if err != nil {
		return fmt.Errorf("create %s request: %w", entityName, err)
	}
	resp, err := vanadisClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to query %s %q: %w", entityName, target, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%s %q not found", entityName, target)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("failed to query %s %q: HTTP %d", entityName, target, resp.StatusCode)
	}
	var res struct {
		Build struct {
			Status string `json:"status"`
		} `json:"build"`
		Chunk struct {
			Status string `json:"status"`
		} `json:"chunk"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return fmt.Errorf("failed to decode %s %q response: %w", entityName, target, err)
	}
	var status string
	if source == sourceVanadisChunk {
		status = firstNonEmpty(res.Chunk.Status, res.Status)
	} else {
		status = firstNonEmpty(res.Build.Status, res.Status)
	}
	if terminalStates[strings.ToLower(strings.TrimSpace(status))] {
		return fmt.Errorf("%s %q is already in terminal state %q", entityName, target, status)
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
		effort = effortMedium
	}
	query := `INSERT INTO event_triggers (id, channel_id, source, target, event_type, prompt, effort, created_at, persistent) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	_, err = database.ExecContext(ctx, query, t.ID, t.ChannelID, t.Source, t.Target, t.EventType, t.Prompt, effort, t.CreatedAt, t.Persistent)
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
		query = `SELECT id, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium'), created_at, persistent FROM event_triggers WHERE channel_id = $1 ORDER BY created_at ASC`
		rows, err = database.QueryContext(ctx, query, channelID)
	} else {
		query = `SELECT id, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium'), created_at, persistent FROM event_triggers ORDER BY created_at ASC`
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
		if err := rows.Scan(&t.ID, &t.ChannelID, &t.Source, &t.Target, &t.EventType, &t.Prompt, &t.Effort, &t.CreatedAt, &t.Persistent); err != nil {
			return nil, err
		}
		results = append(results, t)
	}
	return results, rows.Err()
}

func buildEventTriggerFilterArgs(source string, targets []string, eventType string) (string, []any, bool) {
	source, eventType = strings.ToLower(strings.TrimSpace(source)), strings.ToLower(strings.TrimSpace(eventType))
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
		return "", nil, false
	}
	placeholders := make([]string, len(cleanTargets))
	args := make([]any, 0, 2+len(cleanTargets))
	args = append(args, source, eventType)
	for i, t := range cleanTargets {
		placeholders[i] = fmt.Sprintf("$%d", i+3)
		args = append(args, t)
	}
	return strings.Join(placeholders, ", "), args, true
}

var isSQLiteDBFn = isSQLiteDB

func isSQLiteDB(db *sql.DB) bool {
	if db == nil {
		return false
	}
	return strings.Contains(strings.ToLower(fmt.Sprintf("%T", db.Driver())), "sqlite")
}

func claimEventTriggersSQLite(ctx context.Context, database *sql.DB, placeholders string, args []any) ([]EventTrigger, error) {
	tx, txErr := database.BeginTx(ctx, nil)
	if txErr != nil {
		return nil, txErr
	}
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			log.Printf("[Haro] failed to rollback claim transaction: %v", rErr)
		}
	}()

	selectQuery := fmt.Sprintf(`SELECT id, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium'), created_at, persistent FROM event_triggers WHERE source = $1 AND event_type = $2 AND target IN (%s) AND (persistent = FALSE OR (updated_at IS NULL OR updated_at < datetime('now', '-5 seconds'))) ORDER BY created_at ASC`, placeholders)
	rows, qErr := tx.QueryContext(ctx, selectQuery, args...)
	if qErr != nil {
		return nil, qErr
	}
	triggers, scanErr := scanEventTriggerRows(rows)
	if scanErr != nil {
		return nil, scanErr
	}

	deleteQuery := fmt.Sprintf(`DELETE FROM event_triggers WHERE source = $1 AND event_type = $2 AND target IN (%s) AND persistent = FALSE`, placeholders)
	if _, dErr := tx.ExecContext(ctx, deleteQuery, args...); dErr != nil {
		return nil, dErr
	}

	updateQuery := fmt.Sprintf(`UPDATE event_triggers SET updated_at = CURRENT_TIMESTAMP WHERE source = $1 AND event_type = $2 AND target IN (%s) AND persistent = TRUE AND (updated_at IS NULL OR updated_at < datetime('now', '-5 seconds'))`, placeholders)
	if _, uErr := tx.ExecContext(ctx, updateQuery, args...); uErr != nil {
		return nil, uErr
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return triggers, nil
}

// ClaimEventTriggers atomically deletes and returns matching event triggers from event_triggers.
func ClaimEventTriggers(ctx context.Context, database *sql.DB, source string, targets []string, eventType string) ([]EventTrigger, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	placeholders, args, ok := buildEventTriggerFilterArgs(source, targets, eventType)
	if !ok {
		return nil, nil
	}

	if isSQLiteDBFn(database) {
		return claimEventTriggersSQLite(ctx, database, placeholders, args)
	}

	rawQuery := fmt.Sprintf(`WITH deleted AS (
		DELETE FROM event_triggers WHERE source = $1 AND event_type = $2 AND target IN (%s) AND persistent = FALSE RETURNING id, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium') AS effort, created_at, persistent
	), updated_persistent AS (
		UPDATE event_triggers SET updated_at = CURRENT_TIMESTAMP WHERE source = $1 AND event_type = $2 AND target IN (%s) AND persistent = TRUE AND (updated_at IS NULL OR updated_at < CURRENT_TIMESTAMP - INTERVAL '5 seconds') RETURNING id, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium') AS effort, created_at, persistent
	) SELECT id, channel_id, source, target, event_type, prompt, effort, created_at, persistent FROM deleted UNION ALL SELECT id, channel_id, source, target, event_type, prompt, effort, created_at, persistent FROM updated_persistent ORDER BY created_at ASC`, placeholders, placeholders)
	rows, err := database.QueryContext(ctx, rawQuery, args...)
	if err != nil {
		return nil, err
	}
	return scanEventTriggerRows(rows)
}

// DeleteEventTrigger removes an event trigger by id.
func DeleteEventTrigger(ctx context.Context, database *sql.DB, id string) (bool, error) {
	return deleteByID(ctx, database, "DELETE FROM event_triggers WHERE id = $1", "id cannot be empty", "failed to delete event trigger", id)
}
