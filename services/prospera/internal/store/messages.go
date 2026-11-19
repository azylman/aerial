package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/azylman/aerial/services/prospera/internal/model"
)

// SaveVoiceMessage inserts a voice message record. Returns true if inserted, false if already exists (idempotent).
func (s *PGStore) SaveVoiceMessage(ctx context.Context, msg model.VoiceMessage) (bool, error) {
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
func (s *PGStore) ListVoiceMessages(ctx context.Context, limit int) ([]model.VoiceMessage, error) {
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

	var msgs []model.VoiceMessage
	for rows.Next() {
		var m model.VoiceMessage
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
