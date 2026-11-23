package db

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/metrics"
)

func recordTranscriptQuery(op string, start time.Time, errp *error) {
	status := "success"
	if errp != nil && *errp != nil {
		status = "error"
	}
	metrics.RecordDBQuery(op, status, time.Since(start))
}

// ResolveTranscriptStack returns configuredStack if non-empty, or derives the stack name
// from the database name in databaseURL's path.
func ResolveTranscriptStack(databaseURL, configuredStack string) (string, error) {
	if s := strings.TrimSpace(configuredStack); s != "" {
		return s, nil
	}
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		return "", fmt.Errorf("transcripts: database URL cannot be empty")
	}
	var dbName string
	if strings.Contains(databaseURL, "://") {
		u, err := url.Parse(databaseURL)
		if err != nil {
			return "", fmt.Errorf("transcripts: parse database URL: %w", err)
		}
		dbName = strings.TrimPrefix(u.Path, "/")
		if dbName != "" {
			dbName = path.Base(dbName)
		}
	} else {
		trimmed := strings.Trim(databaseURL, ":")
		if idx := strings.IndexAny(trimmed, "?;"); idx != -1 {
			trimmed = trimmed[:idx]
		}
		dbName = strings.TrimSuffix(path.Base(trimmed), ".db")
	}
	if dbName == "" {
		return "", fmt.Errorf("transcripts: cannot derive stack from database URL without a database name; set transcripts.stack")
	}
	return dbName, nil
}

// SQLTranscriptStore persists transcript rows into a (possibly shared) transcript index.
// Writes are always scoped to stack; reads take an explicit stack filter where empty means all stacks.
type SQLTranscriptStore struct {
	db    DBTX
	stack string
}

// NewSQLTranscriptStore binds a transcript store to database, writing rows labeled with stack.
// An empty stack yields a read-only store.
func NewSQLTranscriptStore(database DBTX, stack string) *SQLTranscriptStore {
	return &SQLTranscriptStore{db: database, stack: strings.TrimSpace(stack)}
}

func (s *SQLTranscriptStore) checkDB() error {
	if s == nil || isDBTXNil(s.db) {
		return ErrNilDatabase
	}
	return nil
}

// checkWriter guards stack-scoped writes: a read-only store (empty stack) can search but never write.
func (s *SQLTranscriptStore) checkWriter() error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if s.stack == "" {
		return fmt.Errorf("transcript store stack cannot be empty for stack-scoped writes")
	}
	return nil
}

// GetKnownTurnSessionIDs queries conversation IDs that have participated in turn messaging or non-heartbeat sessions.
func (s *SQLTranscriptStore) GetKnownTurnSessionIDs(ctx context.Context) (results map[string]bool, err error) {
	defer recordTranscriptQuery("get_known_turn_session_ids", time.Now(), &err)
	if checkWriterErr := s.checkWriter(); checkWriterErr != nil {
		return nil, checkWriterErr
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `SELECT conversation_id, 1 AS allowed FROM messages WHERE conversation_id <> ''
UNION
SELECT conversation_id, 1 AS allowed FROM sessions WHERE conversation_id <> '' AND session_id NOT LIKE 'heartbeat-%'
UNION
SELECT previous_conversation_id, 1 AS allowed FROM sessions WHERE previous_conversation_id <> '' AND session_id NOT LIKE 'heartbeat-%'
UNION
SELECT conversation_id, CASE WHEN source = 'heartbeat' THEN 0 ELSE 1 END AS allowed FROM dispatches WHERE conversation_id <> ''
UNION
SELECT conversation_id, 0 AS allowed FROM sessions WHERE conversation_id <> '' AND session_id LIKE 'heartbeat-%'
UNION
SELECT previous_conversation_id, 0 AS allowed FROM sessions WHERE previous_conversation_id <> '' AND session_id LIKE 'heartbeat-%'`

	rows, err := s.db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("query known turn session ids: %w", err)
	}
	defer rows.Close()

	var hasAnyRow bool
	for rows.Next() {
		var (
			convID  string
			allowed int
		)
		if err := rows.Scan(&convID, &allowed); err != nil {
			return nil, fmt.Errorf("scan known turn session id: %w", err)
		}
		if !hasAnyRow {
			hasAnyRow = true
			results = make(map[string]bool)
		}
		if allowed == 1 {
			if trimmed := strings.TrimSpace(convID); trimmed != "" {
				results[trimmed] = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate known turn session ids: %w", err)
	}
	if !hasAnyRow {
		return nil, nil //nolint:nilnil // nil map signifies empty turn tables across all turn sources
	}
	return results, nil
}
