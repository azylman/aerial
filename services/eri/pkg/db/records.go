package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Tables addressed by the by-ID record helpers. Values are code constants, never user input.
const (
	messagesTable   = "messages"
	dispatchesTable = "dispatches"
)

const (
	activeStatusesSQL  = "('PENDING', 'PROCESSING')"
	pendingStatusesSQL = "('PENDING')"
)

// NormalizeRecordIDs trims IDs, drops empty ones and removes duplicates, keeping first-seen order.
func NormalizeRecordIDs(ids []string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" && !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}

// sqlPlaceholders returns "$start, $start+1, ..." for n positional parameters.
func sqlPlaceholders(start, n int) string {
	ps := make([]string, n)
	for i := range ps {
		ps[i] = fmt.Sprintf("$%d", start+i)
	}
	return strings.Join(ps, ", ")
}

// inInputOrder returns the members of changed in the order they appear in ids.
func inInputOrder(ids, changed []string) []string {
	out := make([]string, 0, len(changed))
	for _, id := range ids {
		if slices.Contains(changed, id) {
			out = append(out, id)
		}
	}
	return out
}

// escapeLike escapes LIKE wildcards so the value matches literally under ESCAPE '\'.
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

func appendIDArgs(args []any, ids []string) []any {
	for _, id := range ids {
		args = append(args, id)
	}
	return args
}

func (s *SQLStore) queryStrings(ctx context.Context, query string, args ...any) ([]string, error) {
	queryCtx, cancel := withTimeout(ctx, defaultExecTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(queryCtx, query, args...)
	if err != nil {
		return nil, err
	}
	return scanNonEmptyStrings(rows)
}

// updateRecordsByID applies "SET <set>" to the agent's rows in table whose id is in ids and
// whose status is in statuses, returning the changed IDs in input order.
func (s *SQLStore) updateRecordsByID(ctx context.Context, table, set, statuses string, ids []string, setArgs ...any) ([]string, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	ids = NormalizeRecordIDs(ids)
	if len(ids) == 0 {
		return nil, nil
	}
	args := append(slices.Clone(setArgs), s.effectiveAgent())
	agentIdx := len(args)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE agent = $%d AND status IN %s AND id IN (%s) RETURNING id",
		table, set, agentIdx, statuses, sqlPlaceholders(agentIdx+1, len(ids)))
	changed, err := s.queryStrings(ctx, query, appendIDArgs(args, ids)...)
	if err != nil {
		return nil, fmt.Errorf("failed updating %s by id: %w", table, err)
	}
	return inInputOrder(ids, changed), nil
}

func (s *SQLStore) failRecords(ctx context.Context, table, statuses string, ids []string, reason string) ([]string, error) {
	return s.updateRecordsByID(ctx, table, "status = 'FAILED', error_message = $1, updated_at = $2", statuses, ids, reason, time.Now().UTC())
}

func (s *SQLStore) skipRecords(ctx context.Context, table string, ids []string, reason string) ([]string, error) {
	return s.updateRecordsByID(ctx, table, "status = 'COMPLETED', error_message = $1, updated_at = $2", activeStatusesSQL, ids, reason, time.Now().UTC())
}

// beginAttempt starts attempt ordinal on one row. Two guarded single-row UPDATEs keep it
// race-free without reading pre-update values back: retry_count only ever grows, so a row the
// first statement skips (retry_count >= ordinal) always matches the second.
func (s *SQLStore) beginAttempt(ctx context.Context, table, id string, ordinal int) (AttemptStart, error) {
	if err := s.checkDB(); err != nil {
		return AttemptStart{}, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return AttemptStart{}, fmt.Errorf("%s id cannot be empty", table)
	}
	if ordinal < 1 {
		return AttemptStart{}, fmt.Errorf("attempt ordinal must be >= 1, got %d", ordinal)
	}
	agent := s.effectiveAgent()
	queryCtx, cancel := withTimeout(ctx, defaultExecTimeout)
	defer cancel()
	now := time.Now().UTC()
	var start AttemptStart
	err := s.db.QueryRowContext(queryCtx, fmt.Sprintf(`UPDATE %s SET retry_count = $1, updated_at = $2 WHERE agent = $3 AND id = $4 AND retry_count < $1 RETURNING retry_count, restart_count`, table),
		ordinal, now, agent, id).Scan(&start.RetryCount, &start.RestartCount)
	if err == nil {
		return start, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return AttemptStart{}, fmt.Errorf("failed beginning attempt %d for %s %s: %w", ordinal, table, id, err)
	}
	err = s.db.QueryRowContext(queryCtx, fmt.Sprintf(`UPDATE %s SET restart_count = restart_count + 1, updated_at = $1 WHERE agent = $2 AND id = $3 AND retry_count >= $4 RETURNING retry_count, restart_count`, table),
		now, agent, id, ordinal).Scan(&start.RetryCount, &start.RestartCount)
	if errors.Is(err, sql.ErrNoRows) {
		return AttemptStart{}, fmt.Errorf("%s %s: %w", table, id, ErrRecordNotFound)
	}
	if err != nil {
		return AttemptStart{}, fmt.Errorf("failed re-entering attempt %d for %s %s: %w", ordinal, table, id, err)
	}
	start.Reentry = true
	return start, nil
}

// GetMessagesByID returns the agent's messages with the given IDs in input order; missing IDs are skipped.
func (s *SQLStore) GetMessagesByID(ctx context.Context, ids []string) ([]Message, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	ids = NormalizeRecordIDs(ids)
	if len(ids) == 0 {
		return nil, nil
	}
	queryCtx, cancel := withTimeout(ctx, defaultExecTimeout)
	defer cancel()
	query := fmt.Sprintf("SELECT %s FROM messages WHERE agent = $1 AND id IN (%s)", messageSelectCols, sqlPlaceholders(2, len(ids)))
	rows, err := s.db.QueryContext(queryCtx, query, appendIDArgs([]any{s.effectiveAgent()}, ids)...)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages by id: %w", err)
	}
	found, err := scanMessages(rows, true)
	if err != nil {
		return nil, fmt.Errorf("failed to scan messages by id: %w", err)
	}
	out := make([]Message, 0, len(found))
	for _, id := range ids {
		if i := slices.IndexFunc(found, func(m Message) bool { return m.ID == id }); i >= 0 {
			out = append(out, found[i])
		}
	}
	return out, nil
}

// FailMessagesIfActive marks PENDING/PROCESSING messages FAILED with reason and returns the IDs changed.
func (s *SQLStore) FailMessagesIfActive(ctx context.Context, ids []string, reason string) ([]string, error) {
	return s.failRecords(ctx, messagesTable, activeStatusesSQL, ids, reason)
}

// FailMessagesIfPending marks PENDING messages FAILED with reason and returns the IDs changed.
func (s *SQLStore) FailMessagesIfPending(ctx context.Context, ids []string, reason string) ([]string, error) {
	return s.failRecords(ctx, messagesTable, pendingStatusesSQL, ids, reason)
}

// SkipMessagesIfActive records PENDING/PROCESSING messages COMPLETED with reason (a skipped
// turn) and returns the IDs changed; rows already terminal are left untouched.
func (s *SQLStore) SkipMessagesIfActive(ctx context.Context, ids []string, reason string) ([]string, error) {
	return s.skipRecords(ctx, messagesTable, ids, reason)
}

// SkipDispatchesIfActive records PENDING/PROCESSING dispatches COMPLETED with reason (a skipped
// turn) and returns the IDs changed; rows already terminal are left untouched.
func (s *SQLStore) SkipDispatchesIfActive(ctx context.Context, ids []string, reason string) ([]string, error) {
	return s.skipRecords(ctx, dispatchesTable, ids, reason)
}

// RequeueProcessingMessages moves PROCESSING messages back to PENDING, counting the deferral in
// retry_count (restart_count untouched), and returns the IDs changed; a replay changes nothing.
func (s *SQLStore) RequeueProcessingMessages(ctx context.Context, ids []string, reason string) ([]string, error) {
	return s.updateRecordsByID(ctx, messagesTable, "status = 'PENDING', turn_id = '', retry_count = retry_count + 1, error_message = $1, updated_at = $2", "('PROCESSING')", ids, reason, time.Now().UTC())
}

// BeginMessageAttempt records the start of attempt ordinal for a message (see beginAttempt).
func (s *SQLStore) BeginMessageAttempt(ctx context.Context, id string, ordinal int) (AttemptStart, error) {
	return s.beginAttempt(ctx, messagesTable, id, ordinal)
}

// FailDispatchesIfActive marks PENDING/PROCESSING dispatches FAILED with reason and returns the IDs changed.
func (s *SQLStore) FailDispatchesIfActive(ctx context.Context, ids []string, reason string) ([]string, error) {
	return s.failRecords(ctx, dispatchesTable, activeStatusesSQL, ids, reason)
}

// FailDispatchesIfPending marks PENDING dispatches FAILED with reason and returns the IDs changed.
func (s *SQLStore) FailDispatchesIfPending(ctx context.Context, ids []string, reason string) ([]string, error) {
	return s.failRecords(ctx, dispatchesTable, pendingStatusesSQL, ids, reason)
}

// BeginDispatchAttempt records the start of attempt ordinal for a dispatch (see beginAttempt).
func (s *SQLStore) BeginDispatchAttempt(ctx context.Context, id string, ordinal int) (AttemptStart, error) {
	return s.beginAttempt(ctx, dispatchesTable, id, ordinal)
}

// ListActiveDispatchSessionKeys returns the distinct, sorted, non-empty session_id values of
// PENDING/PROCESSING dispatches matching dispatchID, or belonging to the Vanadis build buildID.
func (s *SQLStore) ListActiveDispatchSessionKeys(ctx context.Context, dispatchID, buildID string) ([]string, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	dID, bID := strings.TrimSpace(dispatchID), strings.TrimSpace(buildID)
	if dID == "" && bID == "" {
		return nil, nil
	}
	escaped := escapeLike(bID)
	return s.queryStrings(ctx, `SELECT DISTINCT session_id FROM dispatches
	WHERE agent = $1 AND status IN ('PENDING', 'PROCESSING') AND session_id != '' AND (
		($2 != '' AND id = $2) OR
		($3 != '' AND (
			COALESCE(vanadis_chunk_id, '') = $3 OR
			COALESCE(vanadis_chunk_id, '') LIKE $4 ESCAPE '\' OR
			session_id = $5 OR
			session_id LIKE $6 ESCAPE '\'
		))
	)
	ORDER BY session_id ASC`,
		s.effectiveAgent(), dID, bID, escaped+"-chunk-%", "vanadis-"+bID, "vanadis-"+escaped+"-chunk-%")
}
