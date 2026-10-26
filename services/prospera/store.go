package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	ErrMaxConcurrentCalls  = errors.New("store: max concurrent calls limit reached")
	ErrDuplicateActiveDest = errors.New("store: active call to destination number already exists")
	ErrCallNotFound        = errors.New("store: call not found")
	ErrMessageNotFound     = errors.New("store: voice message not found")
)

// TranscriptTurn represents a single speaker turn in a call transcript.
type TranscriptTurn struct {
	Role      string    `json:"role"`
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
}

// OwnerInstruction captures mid-call guidance from the account owner.
type OwnerInstruction struct {
	Instruction string    `json:"instruction"`
	Mode        string    `json:"mode"`
	CreatedAt   time.Time `json:"created_at"`
}

// VoiceCall represents a persistent phone call record.
type VoiceCall struct {
	CallID                string             `json:"call_id"`
	ProviderCallSID       string             `json:"provider_call_sid,omitempty"`
	Direction             string             `json:"direction"`
	State                 string             `json:"state"`
	EndReason             string             `json:"end_reason,omitempty"`
	FromNumber            string             `json:"from_number"`
	ToNumber              string             `json:"to_number"`
	CallbackOfCallID      string             `json:"callback_of_call_id,omitempty"`
	DiscordChannelID      string             `json:"discord_channel_id,omitempty"`
	DiscordThreadID       string             `json:"discord_thread_id,omitempty"`
	Brief                 CallBrief          `json:"brief"`
	OwnerInstructions     []OwnerInstruction `json:"owner_instructions"`
	Transcript            []TranscriptTurn   `json:"transcript"`
	LiveTranscriptCursor  int                `json:"live_transcript_cursor"`
	AnsweredBy            string             `json:"answered_by,omitempty"`
	VoicemailRecordingURL string             `json:"voicemail_recording_url,omitempty"`
	ReportStatus          string             `json:"report_status"`
	ReportSummary         string             `json:"report_summary,omitempty"`
	StartedAt             time.Time          `json:"started_at"`
	AnsweredAt            *time.Time         `json:"answered_at,omitempty"`
	EndedAt               *time.Time         `json:"ended_at,omitempty"`
	UpdatedAt             time.Time          `json:"updated_at"`
}

// VoiceMessage represents an SMS or MMS message associated with Prospera.
type VoiceMessage struct {
	MessageSID       string    `json:"message_sid"`
	Direction        string    `json:"direction"`
	FromNumber       string    `json:"from_number"`
	ToNumber         string    `json:"to_number"`
	Body             string    `json:"body"`
	MediaURLs        []string  `json:"media_urls"`
	RelatedCallID    string    `json:"related_call_id,omitempty"`
	DiscordDelivered bool      `json:"discord_delivered"`
	CreatedAt        time.Time `json:"created_at"`
}

// Store defines persistence operations for Prospera calls and messages.
type Store interface {
	CreateOutboundCall(ctx context.Context, call VoiceCall, maxConcurrent int) error
	CreateInboundCall(ctx context.Context, call VoiceCall) error
	BindProviderCallSID(ctx context.Context, callID, providerSID string) error
	GetCall(ctx context.Context, callID string) (*VoiceCall, error)
	GetCallByProviderSID(ctx context.Context, providerSID string) (*VoiceCall, error)
	ListCalls(ctx context.Context, limit int) ([]VoiceCall, error)
	FindRecentOutboundCall(ctx context.Context, fromE164 string, window time.Duration) (*VoiceCall, error)
	UpdateCallState(ctx context.Context, callID, state, endReason, answeredBy string) error
	AppendTranscriptTurn(ctx context.Context, callID string, turn TranscriptTurn) error
	AdvanceLiveTranscriptCursor(ctx context.Context, callID string, fromCursor, toCursor int) (bool, error)
	AppendOwnerInstruction(ctx context.Context, callID string, instr OwnerInstruction) error
	SetVoicemailRecording(ctx context.Context, callID, recordingURL string) error
	TransitionReportStatus(ctx context.Context, callID string, allowedFrom []string, toStatus, summary string) (bool, error)
	ReconcileOrphanedCalls(ctx context.Context, checkActive func(ctx context.Context, providerSID string) (bool, error)) (int, error)
	SaveVoiceMessage(ctx context.Context, msg VoiceMessage) (bool, error)
	MarkVoiceMessageDelivered(ctx context.Context, messageSID string) error
	ListVoiceMessages(ctx context.Context, limit int) ([]VoiceMessage, error)
	Close() error
}

// PGStore implements Store backed by PostgreSQL (or SQLite in tests).
type PGStore struct {
	db *sql.DB
}

// NewPGStore constructs a new PGStore wrapping a sql.DB instance.
func NewPGStore(db *sql.DB) *PGStore {
	return &PGStore{db: db}
}

// OpenDB opens a database connection from a connection string.
func OpenDB(databaseURL string) (*sql.DB, error) {
	trimmed := strings.TrimSpace(databaseURL)
	if trimmed == "" {
		return nil, fmt.Errorf("store: connection string cannot be empty")
	}
	if strings.HasPrefix(trimmed, "sqlite://") || strings.HasPrefix(trimmed, "file:") || strings.Contains(trimmed, ":memory:") {
		sqlitePath := strings.TrimPrefix(trimmed, "sqlite://")
		return sql.Open("sqlite", sqlitePath)
	}
	if !strings.HasPrefix(trimmed, "postgres://") && !strings.HasPrefix(trimmed, "postgresql://") {
		return nil, fmt.Errorf("store: unsupported database scheme in %q: only postgres:// or postgresql:// supported", trimmed)
	}
	return sql.Open("pgx", trimmed)
}

const selectCallFields = `call_id, provider_call_sid, direction, state, end_reason,
	from_number, to_number, callback_of_call_id, discord_channel_id, discord_thread_id,
	brief, owner_instructions, transcript, live_transcript_cursor, answered_by,
	voicemail_recording_url, report_status, report_summary, started_at, answered_at,
	ended_at, updated_at`

func nullStringVal(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func unmarshalCallJSON(c *VoiceCall, briefRaw, instrRaw, transRaw string) error {
	if briefRaw != "" {
		if err := json.Unmarshal([]byte(briefRaw), &c.Brief); err != nil {
			return fmt.Errorf("store: unmarshal brief: %w", err)
		}
	}
	if instrRaw != "" {
		if err := json.Unmarshal([]byte(instrRaw), &c.OwnerInstructions); err != nil {
			return fmt.Errorf("store: unmarshal owner instructions: %w", err)
		}
	}
	if c.OwnerInstructions == nil {
		c.OwnerInstructions = []OwnerInstruction{}
	}
	if transRaw != "" {
		if err := json.Unmarshal([]byte(transRaw), &c.Transcript); err != nil {
			return fmt.Errorf("store: unmarshal transcript: %w", err)
		}
	}
	if c.Transcript == nil {
		c.Transcript = []TranscriptTurn{}
	}
	return nil
}

func scanCall(scanner interface{ Scan(...any) error }) (*VoiceCall, error) {
	var c VoiceCall
	var providerSID, endReason, callbackOfID, discordChan, discordThread, answeredBy, vmURL, repSummary sql.NullString
	var ansAt, endAt sql.NullTime
	var briefRaw, instrRaw, transRaw string

	err := scanner.Scan(
		&c.CallID, &providerSID, &c.Direction, &c.State, &endReason,
		&c.FromNumber, &c.ToNumber, &callbackOfID, &discordChan, &discordThread,
		&briefRaw, &instrRaw, &transRaw, &c.LiveTranscriptCursor, &answeredBy,
		&vmURL, &c.ReportStatus, &repSummary, &c.StartedAt, &ansAt,
		&endAt, &c.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrCallNotFound
		}
		return nil, fmt.Errorf("store: scan call: %w", err)
	}

	c.ProviderCallSID = nullStringVal(providerSID)
	c.EndReason = nullStringVal(endReason)
	c.CallbackOfCallID = nullStringVal(callbackOfID)
	c.DiscordChannelID = nullStringVal(discordChan)
	c.DiscordThreadID = nullStringVal(discordThread)
	c.AnsweredBy = nullStringVal(answeredBy)
	c.VoicemailRecordingURL = nullStringVal(vmURL)
	c.ReportSummary = nullStringVal(repSummary)

	if ansAt.Valid {
		c.AnsweredAt = &ansAt.Time
	}
	if endAt.Valid {
		c.EndedAt = &endAt.Time
	}

	if err := unmarshalCallJSON(&c, briefRaw, instrRaw, transRaw); err != nil {
		return nil, err
	}

	return &c, nil
}

func (s *PGStore) insertCallTx(ctx context.Context, tx *sql.Tx, call VoiceCall) error {
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
		reportStatus = "pending"
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

// CreateOutboundCall inserts a new outbound call record enforcing concurrency and duplicate destination limits.
func (s *PGStore) CreateOutboundCall(ctx context.Context, call VoiceCall, maxConcurrent int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }() //nolint:errcheck

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
func (s *PGStore) CreateInboundCall(ctx context.Context, call VoiceCall) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }() //nolint:errcheck

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
func (s *PGStore) GetCall(ctx context.Context, callID string) (*VoiceCall, error) {
	query := fmt.Sprintf("SELECT %s FROM voice_calls WHERE call_id = $1", selectCallFields)
	row := s.db.QueryRowContext(ctx, query, callID)
	return scanCall(row)
}

// GetCallByProviderSID retrieves a call by external Twilio provider Call SID.
func (s *PGStore) GetCallByProviderSID(ctx context.Context, providerSID string) (*VoiceCall, error) {
	query := fmt.Sprintf("SELECT %s FROM voice_calls WHERE provider_call_sid = $1", selectCallFields)
	row := s.db.QueryRowContext(ctx, query, providerSID)
	return scanCall(row)
}

// ListCalls lists the most recent calls up to limit.
func (s *PGStore) ListCalls(ctx context.Context, limit int) ([]VoiceCall, error) {
	if limit <= 0 {
		limit = 50
	}
	query := fmt.Sprintf("SELECT %s FROM voice_calls ORDER BY started_at DESC LIMIT $1", selectCallFields)
	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list calls: %w", err)
	}
	defer rows.Close()

	var calls []VoiceCall
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
func (s *PGStore) FindRecentOutboundCall(ctx context.Context, fromE164 string, window time.Duration) (*VoiceCall, error) {
	threshold := time.Now().UTC().Add(-window)
	query := fmt.Sprintf("SELECT %s FROM voice_calls WHERE direction = 'outbound' AND to_number = $1 AND started_at >= $2 ORDER BY started_at DESC LIMIT 1", selectCallFields)
	row := s.db.QueryRowContext(ctx, query, fromE164, threshold)
	call, err := scanCall(row)
	if err != nil {
		if errors.Is(err, ErrCallNotFound) {
			return nil, nil
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
	case "completed", "failed", "busy", "no-answer", "canceled":
		setEnded = true
	}

	var query string
	var args []any
	if setEnded {
		query = `UPDATE voice_calls SET 
			state = $1, 
			end_reason = CASE WHEN $2 != '' THEN $2 ELSE end_reason END, 
			answered_by = CASE WHEN $3 != '' THEN $3 ELSE answered_by END,
			ended_at = COALESCE(ended_at, $4),
			updated_at = $5
			WHERE call_id = $6`
		args = []any{state, endReason, answeredBy, now, now, callID}
	} else if state == "in-progress" {
		query = `UPDATE voice_calls SET 
			state = $1, 
			end_reason = CASE WHEN $2 != '' THEN $2 ELSE end_reason END, 
			answered_by = CASE WHEN $3 != '' THEN $3 ELSE answered_by END,
			answered_at = COALESCE(answered_at, $4),
			updated_at = $5
			WHERE call_id = $6`
		args = []any{state, endReason, answeredBy, now, now, callID}
	} else {
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

// AppendTranscriptTurn transactionally appends a dialogue turn to the call's transcript.
func (s *PGStore) AppendTranscriptTurn(ctx context.Context, callID string, turn TranscriptTurn) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }() //nolint:errcheck

	var transRaw string
	err = tx.QueryRowContext(ctx, "SELECT transcript FROM voice_calls WHERE call_id = $1", callID).Scan(&transRaw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrCallNotFound
		}
		return fmt.Errorf("store: get transcript: %w", err)
	}

	var turns []TranscriptTurn
	if transRaw != "" {
		if err := json.Unmarshal([]byte(transRaw), &turns); err != nil {
			return fmt.Errorf("store: unmarshal transcript: %w", err)
		}
	}
	turns = append(turns, turn)

	updatedJSON, err := json.Marshal(turns)
	if err != nil {
		return fmt.Errorf("store: marshal transcript: %w", err)
	}

	_, err = tx.ExecContext(ctx, "UPDATE voice_calls SET transcript = $1, updated_at = $2 WHERE call_id = $3",
		string(updatedJSON), time.Now().UTC(), callID,
	)
	if err != nil {
		return fmt.Errorf("store: update transcript: %w", err)
	}

	return tx.Commit()
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
func (s *PGStore) AppendOwnerInstruction(ctx context.Context, callID string, instr OwnerInstruction) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }() //nolint:errcheck

	var instrRaw string
	err = tx.QueryRowContext(ctx, "SELECT owner_instructions FROM voice_calls WHERE call_id = $1", callID).Scan(&instrRaw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrCallNotFound
		}
		return fmt.Errorf("store: get owner instructions: %w", err)
	}

	var list []OwnerInstruction
	if instrRaw != "" {
		if err := json.Unmarshal([]byte(instrRaw), &list); err != nil {
			return fmt.Errorf("store: unmarshal owner instructions: %w", err)
		}
	}
	list = append(list, instr)

	updatedJSON, err := json.Marshal(list)
	if err != nil {
		return fmt.Errorf("store: marshal owner instructions: %w", err)
	}

	_, err = tx.ExecContext(ctx, "UPDATE voice_calls SET owner_instructions = $1, updated_at = $2 WHERE call_id = $3",
		string(updatedJSON), time.Now().UTC(), callID,
	)
	if err != nil {
		return fmt.Errorf("store: update owner instructions: %w", err)
	}

	return tx.Commit()
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

// SaveVoiceMessage inserts a voice message record. Returns true if inserted, false if already exists (idempotent).
func (s *PGStore) SaveVoiceMessage(ctx context.Context, msg VoiceMessage) (bool, error) {
	mediaJSON, err := json.Marshal(msg.MediaURLs)
	if err != nil {
		return false, fmt.Errorf("store: marshal media urls: %w", err)
	}
	if msg.MediaURLs == nil {
		mediaJSON = []byte("[]")
	}

	createdAt := msg.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	var relCallID sql.NullString
	if msg.RelatedCallID != "" {
		relCallID = sql.NullString{String: msg.RelatedCallID, Valid: true}
	}

	query := `INSERT INTO voice_messages (
		message_sid, direction, from_number, to_number, body,
		media_urls, related_call_id, discord_delivered, created_at
	) VALUES (
		$1, $2, $3, $4, $5,
		$6, $7, $8, $9
	) ON CONFLICT (message_sid) DO NOTHING`

	res, err := s.db.ExecContext(ctx, query,
		msg.MessageSID, msg.Direction, msg.FromNumber, msg.ToNumber, msg.Body,
		string(mediaJSON), relCallID, msg.DiscordDelivered, createdAt,
	)
	if err != nil {
		return false, fmt.Errorf("store: insert message: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: check rows affected: %w", err)
	}
	return rows > 0, nil
}

// MarkVoiceMessageDelivered updates discord_delivered to true for a message.
func (s *PGStore) MarkVoiceMessageDelivered(ctx context.Context, messageSID string) error {
	res, err := s.db.ExecContext(ctx,
		"UPDATE voice_messages SET discord_delivered = TRUE WHERE message_sid = $1",
		messageSID,
	)
	if err != nil {
		return fmt.Errorf("store: mark message delivered: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: rows affected: %w", err)
	}
	if rows == 0 {
		return ErrMessageNotFound
	}
	return nil
}

// ListVoiceMessages lists recent voice messages ordered by created_at DESC.
func (s *PGStore) ListVoiceMessages(ctx context.Context, limit int) ([]VoiceMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `SELECT message_sid, direction, from_number, to_number, body,
		media_urls, related_call_id, discord_delivered, created_at
		FROM voice_messages ORDER BY created_at DESC LIMIT $1`
	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("store: query voice messages: %w", err)
	}
	defer rows.Close()

	var msgs []VoiceMessage
	for rows.Next() {
		var m VoiceMessage
		var mediaRaw string
		var relCallID sql.NullString
		if err := rows.Scan(
			&m.MessageSID, &m.Direction, &m.FromNumber, &m.ToNumber, &m.Body,
			&mediaRaw, &relCallID, &m.DiscordDelivered, &m.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("store: scan voice message: %w", err)
		}
		if relCallID.Valid {
			m.RelatedCallID = relCallID.String
		}
		if mediaRaw != "" {
			if err := json.Unmarshal([]byte(mediaRaw), &m.MediaURLs); err != nil {
				return nil, fmt.Errorf("store: unmarshal media urls: %w", err)
			}
		}
		if m.MediaURLs == nil {
			m.MediaURLs = []string{}
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate voice messages: %w", err)
	}
	return msgs, nil
}

// Close closes the underlying database pool.
func (s *PGStore) Close() error {
	return s.db.Close()
}
