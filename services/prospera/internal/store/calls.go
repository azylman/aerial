package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/services/prospera/internal/model"
)

func (*PGStore) insertCallTx(ctx context.Context, tx *sql.Tx, call model.VoiceCall) error {
	now := time.Now().UTC()
	startedAt := call.StartedAt
	if startedAt.IsZero() {
		startedAt = now
	}
	updatedAt := call.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}

	briefJSON, err := json.Marshal(call.Brief)
	if err != nil {
		return fmt.Errorf("store: marshal brief: %w", err)
	}
	instrJSON, err := json.Marshal(call.OwnerInstructions)
	if err != nil {
		return fmt.Errorf("store: marshal owner instructions: %w", err)
	}
	if call.OwnerInstructions == nil {
		instrJSON = []byte("[]")
	}
	transcriptJSON, err := json.Marshal(call.Transcript)
	if err != nil {
		return fmt.Errorf("store: marshal transcript: %w", err)
	}
	if call.Transcript == nil {
		transcriptJSON = []byte("[]")
	}

	reportStatus := call.ReportStatus
	if reportStatus == "" {
		reportStatus = reportStatusPending
	}

	query := `INSERT INTO voice_calls (
		call_id, provider_call_sid, direction, state, end_reason,
		from_number, to_number, callback_of_call_id, discord_channel_id, discord_thread_id,
		brief, owner_instructions, transcript, live_transcript_cursor, answered_by,
		voicemail_recording_url, report_status, report_summary, started_at, answered_at,
		ended_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5,
		$6, $7, $8, $9, $10,
		$11, $12, $13, $14, $15,
		$16, $17, $18, $19, $20,
		$21, $22
	)`

	var providerSID, endReason, callbackOfID, discordChan, discordThread, answeredBy, vmURL, repSummary sql.NullString
	if call.ProviderCallSID != "" {
		providerSID = sql.NullString{String: call.ProviderCallSID, Valid: true}
	}
	if call.EndReason != "" {
		endReason = sql.NullString{String: call.EndReason, Valid: true}
	}
	if call.CallbackOfCallID != "" {
		callbackOfID = sql.NullString{String: call.CallbackOfCallID, Valid: true}
	}
	if call.DiscordChannelID != "" {
		discordChan = sql.NullString{String: call.DiscordChannelID, Valid: true}
	}
	if call.DiscordThreadID != "" {
		discordThread = sql.NullString{String: call.DiscordThreadID, Valid: true}
	}
	if call.AnsweredBy != "" {
		answeredBy = sql.NullString{String: call.AnsweredBy, Valid: true}
	}
	if call.VoicemailRecordingURL != "" {
		vmURL = sql.NullString{String: call.VoicemailRecordingURL, Valid: true}
	}
	if call.ReportSummary != "" {
		repSummary = sql.NullString{String: call.ReportSummary, Valid: true}
	}

	var ansAt, endAt sql.NullTime
	if call.AnsweredAt != nil {
		ansAt = sql.NullTime{Time: *call.AnsweredAt, Valid: true}
	}
	if call.EndedAt != nil {
		endAt = sql.NullTime{Time: *call.EndedAt, Valid: true}
	}

	_, err = tx.ExecContext(ctx, query,
		call.CallID, providerSID, call.Direction, call.State, endReason,
		call.FromNumber, call.ToNumber, callbackOfID, discordChan, discordThread,
		string(briefJSON), string(instrJSON), string(transcriptJSON), call.LiveTranscriptCursor, answeredBy,
		vmURL, reportStatus, repSummary, startedAt, ansAt,
		endAt, updatedAt,
	)
	if err != nil {
		return fmt.Errorf("store: insert call: %w", err)
	}
	return nil
}

func rollbackTx(tx *sql.Tx) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		return
	}
}

// CreateOutboundCall inserts a new outbound call record enforcing concurrency and duplicate destination limits.
func (s *PGStore) CreateOutboundCall(ctx context.Context, call model.VoiceCall, maxConcurrent int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer rollbackTx(tx)

	var activeCount int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM voice_calls WHERE ended_at IS NULL").Scan(&activeCount); err != nil {
		return fmt.Errorf("store: check active count: %w", err)
	}
	if maxConcurrent > 0 && activeCount >= maxConcurrent {
		return ErrMaxConcurrentCalls
	}

	var dupCount int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM voice_calls WHERE to_number = $1 AND ended_at IS NULL", call.ToNumber).Scan(&dupCount); err != nil {
		return fmt.Errorf("store: check duplicate destination: %w", err)
	}
	if dupCount > 0 {
		return ErrDuplicateActiveDest
	}

	if err := s.insertCallTx(ctx, tx, call); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit tx: %w", err)
	}
	return nil
}

// CreateInboundCall inserts a new inbound call record.
func (s *PGStore) CreateInboundCall(ctx context.Context, call model.VoiceCall) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer rollbackTx(tx)

	if err := s.insertCallTx(ctx, tx, call); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit tx: %w", err)
	}
	return nil
}

// BindProviderCallSID binds a Twilio provider Call SID to an existing call record.
func (s *PGStore) BindProviderCallSID(ctx context.Context, callID, providerSID string) error {
	res, err := s.db.ExecContext(ctx,
		"UPDATE voice_calls SET provider_call_sid = $1, updated_at = $2 WHERE call_id = $3",
		providerSID, time.Now().UTC(), callID,
	)
	if err != nil {
		return fmt.Errorf("store: bind provider sid: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: rows affected: %w", err)
	}
	if rows == 0 {
		return ErrCallNotFound
	}
	return nil
}

// GetCall retrieves a call by internal CallID.
func (s *PGStore) GetCall(ctx context.Context, callID string) (*model.VoiceCall, error) {
	query := fmt.Sprintf("SELECT %s FROM voice_calls WHERE call_id = $1", selectCallFields)
	row := s.db.QueryRowContext(ctx, query, callID)
	return scanCall(row)
}

// GetCallByProviderSID retrieves a call by external Twilio provider Call SID.
func (s *PGStore) GetCallByProviderSID(ctx context.Context, providerSID string) (*model.VoiceCall, error) {
	query := fmt.Sprintf("SELECT %s FROM voice_calls WHERE provider_call_sid = $1", selectCallFields)
	row := s.db.QueryRowContext(ctx, query, providerSID)
	return scanCall(row)
}

// ListCalls lists the most recent calls up to limit.
func (s *PGStore) ListCalls(ctx context.Context, limit int) ([]model.VoiceCall, error) {
	if limit <= 0 {
		limit = 50
	}
	query := fmt.Sprintf("SELECT %s FROM voice_calls ORDER BY started_at DESC LIMIT $1", selectCallFields)
	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list calls: %w", err)
	}
	defer rows.Close()

	var calls []model.VoiceCall
	for rows.Next() {
		c, err := scanCall(rows)
		if err != nil {
			return nil, err
		}
		calls = append(calls, *c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate calls: %w", err)
	}
	return calls, nil
}

// FindRecentOutboundCall searches for a recent outbound call to fromE164 within the specified duration window.
func (s *PGStore) FindRecentOutboundCall(ctx context.Context, fromE164 string, window time.Duration) (*model.VoiceCall, error) {
	threshold := time.Now().UTC().Add(-window)
	query := fmt.Sprintf("SELECT %s FROM voice_calls WHERE direction = 'outbound' AND to_number = $1 AND started_at >= $2 ORDER BY started_at DESC LIMIT 1", selectCallFields)
	row := s.db.QueryRowContext(ctx, query, fromE164, threshold)
	call, err := scanCall(row)
	if err != nil {
		if errors.Is(err, ErrCallNotFound) {
			return nil, nil //nolint:nilnil // absent recent call is a valid empty result, not an error
		}
		return nil, err
	}
	return call, nil
}

// UpdateCallState updates call lifecycle state, end reason, and answering party.
func (s *PGStore) UpdateCallState(ctx context.Context, callID, state, endReason, answeredBy string) error {
	now := time.Now().UTC()
	var setEnded bool
	switch state {
	case "completed", callStateFailed, "busy", "no-answer", "canceled":
		setEnded = true
	}

	var query string
	var args []any
	switch {
	case setEnded:
		query = `UPDATE voice_calls SET 
			state = $1, 
			end_reason = CASE WHEN $2 != '' THEN $2 ELSE end_reason END, 
			answered_by = CASE WHEN $3 != '' THEN $3 ELSE answered_by END,
			ended_at = COALESCE(ended_at, $4),
			updated_at = $5
			WHERE call_id = $6`
		args = []any{state, endReason, answeredBy, now, now, callID}
	case state == callStateInProgress:
		query = `UPDATE voice_calls SET 
			state = $1, 
			end_reason = CASE WHEN $2 != '' THEN $2 ELSE end_reason END, 
			answered_by = CASE WHEN $3 != '' THEN $3 ELSE answered_by END,
			answered_at = COALESCE(answered_at, $4),
			updated_at = $5
			WHERE call_id = $6`
		args = []any{state, endReason, answeredBy, now, now, callID}
	default:
		query = `UPDATE voice_calls SET 
			state = $1, 
			end_reason = CASE WHEN $2 != '' THEN $2 ELSE end_reason END, 
			answered_by = CASE WHEN $3 != '' THEN $3 ELSE answered_by END,
			updated_at = $4
			WHERE call_id = $5`
		args = []any{state, endReason, answeredBy, now, callID}
	}

	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("store: update call state: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: rows affected: %w", err)
	}
	if rows == 0 {
		return ErrCallNotFound
	}
	return nil
}

func appendCallJSONSlice[T any](ctx context.Context, s *PGStore, callID, column, errLabel string, item T) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer rollbackTx(tx)

	query := fmt.Sprintf("SELECT %s FROM voice_calls WHERE call_id = $1", column)
	if !s.isSQLite {
		query += " FOR UPDATE"
	}

	var raw string
	err = tx.QueryRowContext(ctx, query, callID).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrCallNotFound
		}
		return fmt.Errorf("store: get %s: %w", errLabel, err)
	}

	list := make([]T, 0, 1)
	if raw != "" {
		if err = json.Unmarshal([]byte(raw), &list); err != nil {
			return fmt.Errorf("store: unmarshal %s: %w", errLabel, err)
		}
	}
	list = append(list, item)

	updatedJSON, err := json.Marshal(list)
	if err != nil {
		return fmt.Errorf("store: marshal %s: %w", errLabel, err)
	}

	updateQuery := fmt.Sprintf("UPDATE voice_calls SET %s = $1, updated_at = $2 WHERE call_id = $3", column)
	_, err = tx.ExecContext(ctx, updateQuery,
		string(updatedJSON), time.Now().UTC(), callID,
	)
	if err != nil {
		return fmt.Errorf("store: update %s: %w", errLabel, err)
	}

	return tx.Commit()
}

// AppendTranscriptTurn transactionally appends a dialogue turn to the call's transcript.
func (s *PGStore) AppendTranscriptTurn(ctx context.Context, callID string, turn model.TranscriptTurn) error {
	return appendCallJSONSlice(ctx, s, callID, "transcript", "transcript", turn)
}

// AdvanceLiveTranscriptCursor atomically advances the live delivery transcript cursor via compare-and-swap.
func (s *PGStore) AdvanceLiveTranscriptCursor(ctx context.Context, callID string, fromCursor, toCursor int) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		"UPDATE voice_calls SET live_transcript_cursor = $1, updated_at = $2 WHERE call_id = $3 AND live_transcript_cursor = $4",
		toCursor, time.Now().UTC(), callID, fromCursor,
	)
	if err != nil {
		return false, fmt.Errorf("store: advance cursor: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: rows affected: %w", err)
	}
	return rows > 0, nil
}

// AppendOwnerInstruction appends a mid-call guidance instruction from the owner.
func (s *PGStore) AppendOwnerInstruction(ctx context.Context, callID string, instr model.OwnerInstruction) error {
	return appendCallJSONSlice(ctx, s, callID, "owner_instructions", "owner instructions", instr)
}

// SetVoicemailRecording sets the recording URL for a voicemail call.
func (s *PGStore) SetVoicemailRecording(ctx context.Context, callID, recordingURL string) error {
	res, err := s.db.ExecContext(ctx,
		"UPDATE voice_calls SET voicemail_recording_url = $1, updated_at = $2 WHERE call_id = $3",
		recordingURL, time.Now().UTC(), callID,
	)
	if err != nil {
		return fmt.Errorf("store: set voicemail recording: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: rows affected: %w", err)
	}
	if rows == 0 {
		return ErrCallNotFound
	}
	return nil
}

// TransitionReportStatus atomically transitions report_status from one of allowedFrom to toStatus.
// If allowedFrom is empty, the transition is unconditional.
func (s *PGStore) TransitionReportStatus(ctx context.Context, callID string, allowedFrom []string, toStatus, summary string) (bool, error) {
	now := time.Now().UTC()
	var query string
	var args []any

	if len(allowedFrom) == 0 {
		query = `UPDATE voice_calls SET 
			report_status = $1, 
			report_summary = CASE WHEN $2 != '' THEN $2 ELSE report_summary END,
			updated_at = $3
			WHERE call_id = $4`
		args = []any{toStatus, summary, now, callID}
	} else {
		placeholders := make([]string, len(allowedFrom))
		args = append(args, toStatus, summary, now, callID)
		for i, st := range allowedFrom {
			placeholders[i] = fmt.Sprintf("$%d", i+5)
			args = append(args, st)
		}
		query = fmt.Sprintf(`UPDATE voice_calls SET 
			report_status = $1, 
			report_summary = CASE WHEN $2 != '' THEN $2 ELSE report_summary END,
			updated_at = $3
			WHERE call_id = $4 AND report_status IN (%s)`, strings.Join(placeholders, ", "))
	}

	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("store: transition report status: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: rows affected: %w", err)
	}
	return rows > 0, nil
}

// ReconcileOrphanedCalls marks calls with ended_at IS NULL as ended if they have no provider SID or are inactive.
func (s *PGStore) ReconcileOrphanedCalls(ctx context.Context, checkActive func(ctx context.Context, providerSID string) (bool, error)) (int, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT call_id, provider_call_sid FROM voice_calls WHERE ended_at IS NULL")
	if err != nil {
		return 0, fmt.Errorf("store: query unended calls: %w", err)
	}
	defer rows.Close()

	type unendedCall struct {
		callID      string
		providerSID sql.NullString
	}
	var unended []unendedCall
	for rows.Next() {
		var u unendedCall
		if err := rows.Scan(&u.callID, &u.providerSID); err != nil {
			return 0, fmt.Errorf("store: scan unended call: %w", err)
		}
		unended = append(unended, u)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: iterate unended calls: %w", err)
	}

	reconciled := 0
	now := time.Now().UTC()

	for _, u := range unended {
		if !u.providerSID.Valid || strings.TrimSpace(u.providerSID.String) == "" {
			_, err := s.db.ExecContext(ctx,
				"UPDATE voice_calls SET state = 'failed', end_reason = 'orphan_no_provider_sid', ended_at = $1, updated_at = $1 WHERE call_id = $2 AND ended_at IS NULL",
				now, u.callID,
			)
			if err == nil {
				reconciled++
			}
			continue
		}

		if checkActive != nil {
			active, err := checkActive(ctx, u.providerSID.String)
			if err != nil {
				continue
			}
			if !active {
				_, err := s.db.ExecContext(ctx,
					"UPDATE voice_calls SET state = 'completed', end_reason = 'reconciled_orphan', ended_at = $1, updated_at = $1 WHERE call_id = $2 AND ended_at IS NULL",
					now, u.callID,
				)
				if err == nil {
					reconciled++
				}
			}
		}
	}

	return reconciled, nil
}
