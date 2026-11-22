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

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/haro/internal/model"
)

var (
	vanadisMu          sync.RWMutex
	vanadisURL         string
	vanadisClient      = tracing.Client(&http.Client{Timeout: 5 * time.Second})
	vanadisTokenSource = k8sauth.NewTokenSource("", nil)
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
	vanadisMu.RLock()
	ts := vanadisTokenSource
	vanadisMu.RUnlock()
	if ts != nil {
		if tok, tokErr := ts.Token(); tokErr == nil && tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
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
		status = strutil.FirstNonEmpty(res.Chunk.Status, res.Status)
	} else {
		status = strutil.FirstNonEmpty(res.Build.Status, res.Status)
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
	t.Agent = model.NormalizeAgent(t.Agent)
	query := `INSERT INTO event_triggers (id, agent, channel_id, source, target, event_type, prompt, effort, created_at, persistent) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	_, err = database.ExecContext(ctx, query, t.ID, t.Agent, t.ChannelID, t.Source, t.Target, t.EventType, t.Prompt, effort, t.CreatedAt, t.Persistent)
	return err
}

// ListEventTriggers retrieves event triggers, optionally filtered by channel_id and agent.
func ListEventTriggers(ctx context.Context, database *sql.DB, channelID string, agent ...string) ([]model.EventTrigger, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	channelID = strings.TrimSpace(channelID)
	agentFilter := ""
	if len(agent) > 0 {
		agentFilter = strings.TrimSpace(agent[0])
	}

	var query string
	var rows *sql.Rows
	var args []any
	base := `SELECT id, agent, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium'), created_at, persistent FROM event_triggers`
	whereParts := make([]string, 0, 2)
	if channelID != "" {
		args = append(args, channelID)
		whereParts = append(whereParts, fmt.Sprintf("channel_id = $%d", len(args)))
	}
	if agentFilter != "" && agentFilter != "*" {
		args = append(args, agentFilter)
		whereParts = append(whereParts, fmt.Sprintf("agent = $%d", len(args)))
	}
	if len(whereParts) > 0 {
		query = fmt.Sprintf("%s WHERE %s ORDER BY created_at ASC", base, strings.Join(whereParts, " AND "))
	} else {
		query = base + " ORDER BY created_at ASC"
	}

	rows, err = database.QueryContext(ctx, query, args...)
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
		if err := rows.Scan(&t.ID, &t.Agent, &t.ChannelID, &t.Source, &t.Target, &t.EventType, &t.Prompt, &t.Effort, &t.CreatedAt, &t.Persistent); err != nil {
			return nil, err
		}
		t.Agent = model.NormalizeAgent(t.Agent)
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

func claimEventTriggersTx(ctx context.Context, database *sql.DB, placeholders string, args []any) ([]model.EventTrigger, error) {
	tx, txErr := database.BeginTx(ctx, nil)
	if txErr != nil {
		return nil, txErr
	}
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			log.Printf("[Haro] failed to rollback claim transaction: %v", rErr)
		}
	}()

	now := time.Now().UTC()
	cutoff := now.Add(-5 * time.Second)
	cutoffPlaceholder := fmt.Sprintf("$%d", len(args)+1)
	nowPlaceholder := fmt.Sprintf("$%d", len(args)+2)
	argsWithCutoff := make([]any, len(args), len(args)+2)
	copy(argsWithCutoff, args)
	argsWithCutoff = append(argsWithCutoff, cutoff, now)

	selectQuery := fmt.Sprintf(`SELECT id, agent, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium') AS effort, created_at, persistent FROM event_triggers WHERE source = $1 AND event_type = $2 AND target IN (%s) AND (persistent = FALSE OR (updated_at IS NULL OR updated_at < %s)) ORDER BY created_at ASC`, placeholders, cutoffPlaceholder)
	rows, qErr := tx.QueryContext(ctx, selectQuery, argsWithCutoff[:len(args)+1]...)
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

	updateQuery := fmt.Sprintf(`UPDATE event_triggers SET updated_at = %s WHERE source = $1 AND event_type = $2 AND target IN (%s) AND persistent = TRUE AND (updated_at IS NULL OR updated_at < %s)`, nowPlaceholder, placeholders, cutoffPlaceholder)
	if _, uErr := tx.ExecContext(ctx, updateQuery, argsWithCutoff...); uErr != nil {
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
		return claimEventTriggersTx(ctx, database, placeholders, args)
	}

	rawQuery := fmt.Sprintf(`WITH deleted AS (
		DELETE FROM event_triggers WHERE source = $1 AND event_type = $2 AND target IN (%s) AND persistent = FALSE RETURNING id, agent, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium') AS effort, created_at, persistent
	), updated_persistent AS (
		UPDATE event_triggers SET updated_at = CURRENT_TIMESTAMP WHERE source = $1 AND event_type = $2 AND target IN (%s) AND persistent = TRUE AND (updated_at IS NULL OR updated_at < CURRENT_TIMESTAMP - INTERVAL '5 seconds') RETURNING id, agent, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium') AS effort, created_at, persistent
	) SELECT id, agent, channel_id, source, target, event_type, prompt, effort, created_at, persistent FROM deleted UNION ALL SELECT id, agent, channel_id, source, target, event_type, prompt, effort, created_at, persistent FROM updated_persistent ORDER BY created_at ASC`, placeholders, placeholders)
	rows, err := database.QueryContext(ctx, rawQuery, args...)
	if err != nil {
		return nil, err
	}
	return scanEventTriggerRows(rows)
}

// GetEventTrigger fetches an event trigger by id, including its updated_at cooldown timestamp when present.
func GetEventTrigger(ctx context.Context, database *sql.DB, id string) (*model.EventTrigger, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, fmt.Errorf("id cannot be empty")
	}
	const query = `SELECT id, agent, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium'), created_at, updated_at, persistent FROM event_triggers WHERE id = $1`
	var trig model.EventTrigger
	var updatedAt sql.NullTime
	if scanErr := database.QueryRowContext(ctx, query, id).Scan(&trig.ID, &trig.Agent, &trig.ChannelID, &trig.Source, &trig.Target, &trig.EventType, &trig.Prompt, &trig.Effort, &trig.CreatedAt, &updatedAt, &trig.Persistent); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil, fmt.Errorf("event watch %q not found: %w", id, sql.ErrNoRows)
		}
		return nil, scanErr
	}
	if updatedAt.Valid {
		ts := updatedAt.Time.UTC()
		trig.UpdatedAt = &ts
	}
	trig.Agent = model.NormalizeAgent(trig.Agent)
	return &trig, nil
}

// DeleteEventTrigger removes an event trigger by id.
func DeleteEventTrigger(ctx context.Context, database *sql.DB, id string) (bool, error) {
	return deleteByID(ctx, database, "DELETE FROM event_triggers WHERE id = $1", "id cannot be empty", "failed to delete event trigger", id)
}

// QueryMatchingEventTriggers queries matching event triggers without claiming or deleting them.
// It is intentionally read-only so that a transient dispatch error in HaroEventWatch.Fire Step 1
// does not mutate or delete the trigger before eri accepts the turn. Duplicate deliveries of the
// same event are deduplicated via a per-(trigger_id, event_id) Restate Idempotency-Key, and
// concurrent distinct events matching the same persistent trigger within the 5-second cooldown
// window are strictly serialized by the HaroEventWatch Virtual Object key (trigger_id), where
// Step 1 re-verifies trig.UpdatedAt against the 5-second cooldown after Step 2 of the prior
// invocation calls ConsumeEventTrigger.
func QueryMatchingEventTriggers(ctx context.Context, database *sql.DB, source string, targets []string, eventType string) ([]model.EventTrigger, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return nil, err
	}
	placeholders, args, ok := BuildEventTriggerFilterArgs(source, targets, eventType)
	if !ok {
		return nil, nil
	}

	cutoffPlaceholder := fmt.Sprintf("$%d", len(args)+1)
	args = append(args, time.Now().UTC().Add(-5*time.Second))
	selectQuery := fmt.Sprintf(`SELECT id, agent, channel_id, source, target, event_type, prompt, COALESCE(NULLIF(effort, ''), 'medium') AS effort, created_at, persistent FROM event_triggers WHERE source = $1 AND event_type = $2 AND target IN (%s) AND (persistent = FALSE OR (updated_at IS NULL OR updated_at < %s)) ORDER BY created_at ASC`, placeholders, cutoffPlaceholder)

	rows, qErr := database.QueryContext(ctx, selectQuery, args...)
	if qErr != nil {
		return nil, qErr
	}
	return scanEventTriggerRows(rows)
}

// ConsumeEventTrigger deletes a non-persistent trigger or updates updated_at on a persistent trigger.
// It is executed in Step 2 of HaroEventWatch.Fire (serialized per trigger_id) after Step 1 dispatch succeeds.
// When now is provided, updated_at is set to the provided timestamp to maintain a consistent clock source;
// otherwise it defaults to time.Now().UTC().
func ConsumeEventTrigger(ctx context.Context, database *sql.DB, id string, now ...time.Time) (bool, error) {
	ctx, err := normCtxAndDB(ctx, database)
	if err != nil {
		return false, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return false, errors.New("id cannot be empty")
	}

	res, dErr := database.ExecContext(ctx, "DELETE FROM event_triggers WHERE id = $1 AND persistent = FALSE", id)
	if dErr != nil {
		return false, dErr
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows > 0 {
		return true, nil
	}
	ts := time.Now().UTC()
	if len(now) > 0 && !now[0].IsZero() {
		ts = now[0].UTC()
	}
	uRes, uErr := database.ExecContext(ctx, "UPDATE event_triggers SET updated_at = $1 WHERE id = $2 AND persistent = TRUE", ts, id)
	if uErr != nil {
		return false, uErr
	}
	rows, err = uRes.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}
