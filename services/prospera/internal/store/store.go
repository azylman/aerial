package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/services/prospera/internal/model"
)

const (
	callStateFailed     = "failed"
	callStateInProgress = "in-progress"
	reportStatusPending = "pending"
)

var (
	ErrMaxConcurrentCalls  = errors.New("store: max concurrent calls limit reached")
	ErrDuplicateActiveDest = errors.New("store: active call to destination number already exists")
	ErrCallNotFound        = errors.New("store: call not found")
	ErrMessageNotFound     = errors.New("store: voice message not found")
)

// Store defines persistence operations for Prospera calls and messages.
type Store interface {
	CreateOutboundCall(ctx context.Context, call model.VoiceCall, maxConcurrent int) error
	CreateInboundCall(ctx context.Context, call model.VoiceCall) error
	BindProviderCallSID(ctx context.Context, callID, providerSID string) error
	GetCall(ctx context.Context, callID string) (*model.VoiceCall, error)
	GetCallByProviderSID(ctx context.Context, providerSID string) (*model.VoiceCall, error)
	ListCalls(ctx context.Context, limit int) ([]model.VoiceCall, error)
	FindRecentOutboundCall(ctx context.Context, fromE164 string, window time.Duration) (*model.VoiceCall, error)
	UpdateCallState(ctx context.Context, callID, state, endReason, answeredBy string) error
	AppendTranscriptTurn(ctx context.Context, callID string, turn model.TranscriptTurn) error
	AdvanceLiveTranscriptCursor(ctx context.Context, callID string, fromCursor, toCursor int) (bool, error)
	AppendOwnerInstruction(ctx context.Context, callID string, instr model.OwnerInstruction) error
	SetVoicemailRecording(ctx context.Context, callID, recordingURL string) error
	TransitionReportStatus(ctx context.Context, callID string, allowedFrom []string, toStatus, summary string) (bool, error)
	ReconcileOrphanedCalls(ctx context.Context, checkActive func(ctx context.Context, providerSID string) (bool, error)) (int, error)
	SaveVoiceMessage(ctx context.Context, msg model.VoiceMessage) (bool, error)
	MarkVoiceMessageDelivered(ctx context.Context, messageSID string) error
	ListVoiceMessages(ctx context.Context, limit int) ([]model.VoiceMessage, error)
	Close() error
}

// PGStore implements Store backed by PostgreSQL (or SQLite in tests).
type PGStore struct {
	db       *sql.DB
	isSQLite bool
}

// NewPGStore constructs a new PGStore wrapping a sql.DB instance.
func NewPGStore(db *sql.DB) *PGStore {
	var isSQLite bool
	if db != nil && db.Driver() != nil {
		driverName := fmt.Sprintf("%T", db.Driver())
		if strings.Contains(strings.ToLower(driverName), "sqlite") {
			isSQLite = true
		}
	}
	return &PGStore{db: db, isSQLite: isSQLite}
}

// IsSQLite reports whether the underlying database is SQLite.
func (s *PGStore) IsSQLite() bool {
	return s.isSQLite
}

// OpenDB opens a database connection from a connection string: sqlite://,
// file: and :memory: URLs open SQLite, everything else goes through
// pgutil.OpenSQL (postgres:// or postgresql:// only, pinged at open).
func OpenDB(ctx context.Context, databaseURL string) (*sql.DB, error) {
	trimmed := strings.TrimSpace(databaseURL)
	if strings.HasPrefix(trimmed, "sqlite://") || strings.HasPrefix(trimmed, "file:") || strings.Contains(trimmed, ":memory:") {
		sqlitePath := strings.TrimPrefix(trimmed, "sqlite://")
		return sql.Open("sqlite", sqlitePath)
	}
	return pgutil.OpenSQL(ctx, trimmed, pgutil.SQLOptions{})
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

func unmarshalCallJSON(c *model.VoiceCall, briefRaw, instrRaw, transRaw string) error {
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
		c.OwnerInstructions = []model.OwnerInstruction{}
	}
	if transRaw != "" {
		if err := json.Unmarshal([]byte(transRaw), &c.Transcript); err != nil {
			return fmt.Errorf("store: unmarshal transcript: %w", err)
		}
	}
	if c.Transcript == nil {
		c.Transcript = []model.TranscriptTurn{}
	}
	return nil
}

func scanCall(scanner interface{ Scan(dest ...any) error }) (*model.VoiceCall, error) {
	var c model.VoiceCall
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

// Close closes the underlying database pool.
func (s *PGStore) Close() error {
	return s.db.Close()
}
