package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Dispatch struct {
	ID             string    `json:"id"`
	RowID          int64     `json:"row_id,omitempty"`
	Source         string    `json:"source"`
	SessionID      string    `json:"session_id,omitempty"`
	ConversationID string    `json:"conversation_id,omitempty"`
	Prompt         string    `json:"prompt"`
	Summary        string    `json:"summary"`
	Effort         string    `json:"effort,omitempty"`
	Model          string    `json:"model,omitempty"`
	Status         string    `json:"status"`
	RetryCount     int       `json:"retry_count"`
	RestartCount   int       `json:"restart_count"`
	ErrorMessage   string    `json:"error_message,omitempty"`
	ResponseText   string    `json:"response_text,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

const dispatchSelectCols = "id, COALESCE(row_id, 0) AS row_id, source, session_id, conversation_id, prompt, COALESCE(summary, '') AS summary, COALESCE(effort, '') AS effort, COALESCE(model, '') AS model, status, retry_count, restart_count, COALESCE(error_message, '') AS error_message, COALESCE(response_text, '') AS response_text, created_at, updated_at"

func scanDispatch(scanner interface{ Scan(dest ...any) error }) (Dispatch, error) {
	var d Dispatch
	var errMsg, respText sql.NullString
	if err := scanner.Scan(
		&d.ID, &d.RowID, &d.Source, &d.SessionID, &d.ConversationID,
		&d.Prompt, &d.Summary, &d.Effort, &d.Model, &d.Status,
		&d.RetryCount, &d.RestartCount, &errMsg, &respText,
		&d.CreatedAt, &d.UpdatedAt,
	); err != nil {
		return Dispatch{}, err
	}
	d.ErrorMessage, d.ResponseText = errMsg.String, respText.String
	return d, nil
}

func scanDispatches(rows *sql.Rows) ([]Dispatch, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	results := make([]Dispatch, 0)
	for rows.Next() {
		d, err := scanDispatch(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, d)
	}
	return results, rows.Err()
}

func (s *SQLStore) execDispatchUpdate(ctx context.Context, id, query string, args ...any) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("dispatch id cannot be empty")
	}
	return s.execWithTimeout(ctx, query, args...)
}

func (s *SQLStore) InsertDispatch(ctx context.Context, d Dispatch) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if strings.TrimSpace(d.ID) == "" {
		return fmt.Errorf("dispatch id cannot be empty")
	}
	if d.Status == "" {
		d.Status = StatusPending
	}
	if strings.TrimSpace(d.Summary) == "" {
		d.Summary = CleanTaskSummary(d.Prompt)
	}
	now := time.Now().UTC()
	if d.CreatedAt.IsZero() {
		d.CreatedAt = now
	}
	if d.UpdatedAt.IsZero() {
		d.UpdatedAt = now
	}

	query := `INSERT INTO dispatches (
		id, source, session_id, conversation_id,
		prompt, summary, effort, model, status,
		retry_count, restart_count, error_message, response_text,
		created_at, updated_at
	) VALUES (
		$1, $2, $3, $4,
		$5, $6, $7, $8, $9,
		$10, $11, $12, $13,
		$14, $15
	) ON CONFLICT (id) DO NOTHING;`

	return s.execWithTimeout(ctx, query,
		d.ID, d.Source, d.SessionID, d.ConversationID,
		d.Prompt, d.Summary, d.Effort, d.Model, d.Status,
		d.RetryCount, d.RestartCount, d.ErrorMessage, d.ResponseText,
		d.CreatedAt, d.UpdatedAt,
	)
}

func (s *SQLStore) GetDispatch(ctx context.Context, id string) (*Dispatch, error) {
	if err := s.checkDB(); err != nil || strings.TrimSpace(id) == "" {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := fmt.Sprintf("SELECT %s FROM dispatches WHERE id = $1", dispatchSelectCols)
	row := s.db.QueryRowContext(queryCtx, query, strings.TrimSpace(id))
	d, err := scanDispatch(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get dispatch %s: %w", id, err)
	}
	return &d, nil
}

func (s *SQLStore) ClaimPendingDispatch(ctx context.Context, id string) (bool, error) {
	if err := s.checkDB(); err != nil || strings.TrimSpace(id) == "" {
		return false, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `UPDATE dispatches SET status = 'PROCESSING', updated_at = $1 WHERE id = $2 AND status = 'PENDING' RETURNING id`
	var claimedID string
	err := s.db.QueryRowContext(queryCtx, query, time.Now().UTC(), strings.TrimSpace(id)).Scan(&claimedID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to claim dispatch %s: %w", id, err)
	}
	return claimedID != "", nil
}

func (s *SQLStore) UpdateDispatchStatus(ctx context.Context, id, status, errorMsg string) error {
	return s.execDispatchUpdate(ctx, id, `UPDATE dispatches SET status = $1, error_message = $2, updated_at = $3 WHERE id = $4`, status, errorMsg, time.Now().UTC(), id)
}

func (s *SQLStore) UpdateDispatchCompleted(ctx context.Context, id, responseText string, conversationID ...string) error {
	now := time.Now().UTC()
	if len(conversationID) > 0 && strings.TrimSpace(conversationID[0]) != "" {
		return s.execDispatchUpdate(ctx, id, `UPDATE dispatches SET status = 'COMPLETED', response_text = $1, conversation_id = $2, error_message = '', updated_at = $3 WHERE id = $4`, responseText, strings.TrimSpace(conversationID[0]), now, id)
	}
	return s.execDispatchUpdate(ctx, id, `UPDATE dispatches SET status = 'COMPLETED', response_text = $1, error_message = '', updated_at = $2 WHERE id = $3`, responseText, now, id)
}

func (s *SQLStore) IncrementDispatchRetry(ctx context.Context, id, errorMsg string) error {
	return s.execDispatchUpdate(ctx, id, `UPDATE dispatches SET retry_count = retry_count + 1, error_message = $1, updated_at = $2 WHERE id = $3`, errorMsg, time.Now().UTC(), id)
}

func (s *SQLStore) ResetDispatchToPendingWithRestart(ctx context.Context, id, reason string) error {
	return s.execDispatchUpdate(ctx, id, `UPDATE dispatches SET restart_count = restart_count + 1, status = 'PENDING', error_message = $1, updated_at = $2 WHERE id = $3 AND status = 'PROCESSING'`, reason, time.Now().UTC(), id)
}

func (s *SQLStore) GetPendingOrProcessingDispatches(ctx context.Context, limit int) ([]Dispatch, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	query := fmt.Sprintf("SELECT %s FROM dispatches WHERE status IN ('PENDING', 'PROCESSING') ORDER BY created_at ASC", dispatchSelectCols)
	rows, err := s.db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, err
	}
	results, err := scanDispatches(rows)
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func (s *SQLStore) GetRecentSessionDispatches(ctx context.Context, sessionID string, limit int) ([]Dispatch, error) {
	if err := s.checkDB(); err != nil || strings.TrimSpace(sessionID) == "" {
		return nil, err
	}
	if limit <= 0 {
		limit = 10
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := fmt.Sprintf("SELECT %s FROM (SELECT %s FROM dispatches WHERE session_id = $1 AND status = 'COMPLETED' ORDER BY created_at DESC LIMIT $2) recent ORDER BY created_at ASC", dispatchSelectCols, dispatchSelectCols)
	rows, err := s.db.QueryContext(queryCtx, query, strings.TrimSpace(sessionID), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent session dispatches: %w", err)
	}
	return scanDispatches(rows)
}

func (s *SQLStore) HasActiveDispatchTurn(ctx context.Context, sessionID, id string) (bool, error) {
	if err := s.checkDB(); err != nil {
		return false, err
	}
	sID := strings.TrimSpace(sessionID)
	tID := strings.TrimSpace(id)
	if sID == "" && tID == "" {
		return false, nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `SELECT EXISTS(
		SELECT 1 FROM dispatches
		WHERE status IN ('PENDING', 'PROCESSING')
		  AND (($1 != '' AND session_id = $1) OR ($2 != '' AND id = $2))
	)`
	var exists bool
	err := s.db.QueryRowContext(queryCtx, query, sID, tID).Scan(&exists)
	return exists, err
}

func (s *SQLStore) CancelPendingDispatches(ctx context.Context, dispatchID, buildID, reason string) ([]string, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	dID := strings.TrimSpace(dispatchID)
	bID := strings.TrimSpace(buildID)
	if dID == "" && bID == "" {
		return nil, nil
	}

	buildPattern := ""
	if bID != "" {
		buildPattern = "Build ID: " + bID + "\n"
	}

	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `UPDATE dispatches
	SET status = 'FAILED', error_message = $1, updated_at = $2
	WHERE status = 'PENDING' AND (
		($3 != '' AND id = $3) OR
		($4 != '' AND prompt LIKE '%' || $4 || '%')
	)
	RETURNING id;`

	rows, err := s.db.QueryContext(queryCtx, query, reason, time.Now().UTC(), dID, buildPattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cancelledIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		cancelledIDs = append(cancelledIDs, id)
	}
	return cancelledIDs, rows.Err()
}
