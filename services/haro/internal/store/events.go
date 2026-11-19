package store

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
)

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
	case model.SourceVanadisBuild, model.SourceVanadisChunk:
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
		model.StatusCompleted: true, model.StatusMerged: true, model.StatusFailed: true, "cancelled": true, "canceled": true,
	}
	entityName := "vanadis build"
	if source == model.SourceVanadisChunk {
		pathPrefix = "chunks"
		terminalStates = map[string]bool{
			model.StatusMerged: true, model.StatusCompleted: true, model.StatusFailed: true,
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
	if source == model.SourceVanadisChunk {
		status = model.FirstNonEmpty(res.Chunk.Status, res.Status)
	} else {
		status = model.FirstNonEmpty(res.Build.Status, res.Status)
	}
	if terminalStates[strings.ToLower(strings.TrimSpace(status))] {
		return fmt.Errorf("%s %q is already in terminal state %q", entityName, target, status)
	}
	return nil
}

// InsertEventTrigger persists a new event trigger in the database.
func InsertEventTrigger(ctx context.Context, database *sql.DB, t model.EventTrigger) error {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return err
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now().UTC()
	}
	effort, err := model.NormalizeEffort(t.Effort)
	if err != nil {
		effort = model.EffortMedium
	}
	query := `INSERT INTO event_triggers (id, channel_id, source, target, event_type, prompt, effort, created_at, persistent) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	_, err = database.ExecContext(ctx, query, t.ID, t.ChannelID, t.Source, t.Target, t.EventType, t.Prompt, effort, t.CreatedAt, t.Persistent)
	return err
}

// ListEventTriggers retrieves event triggers, optionally filtered by channel_id.
func ListEventTriggers(ctx context.Context, database *sql.DB, channelID string) ([]model.EventTrigger, error) {
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

func scanEventTriggerRows(rows *sql.Rows) ([]model.EventTrigger, error) {
	defer rows.Close()
	results := []model.EventTrigger{}
	for rows.Next() {
		var t model.EventTrigger
		if err := rows.Scan(&t.ID, &t.ChannelID, &t.Source, &t.Target, &t.EventType, &t.Prompt, &t.Effort, &t.CreatedAt, &t.Persistent); err != nil {
			return nil, err
		}
		results = append(results, t)
	}
	return results, rows.Err()
}

// BuildEventTriggerFilterArgs builds the target-IN-list SQL fragment for
// event-trigger claims. It reports false when source, event type, or the
// deduplicated target list is empty. The unexported root wrapper delegates
// here so handler tests keep compiling until the haro-07b purge.
func BuildEventTriggerFilterArgs(source string, targets []string, eventType string) (string, []any, bool) {
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

func claimEventTriggersSQLite(ctx context.Context, database *sql.DB, placeholders string, args []any) ([]model.EventTrigger, error) {
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
func ClaimEventTriggers(ctx context.Context, database *sql.DB, source string, targets []string, eventType string) ([]model.EventTrigger, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	placeholders, args, ok := BuildEventTriggerFilterArgs(source, targets, eventType)
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
