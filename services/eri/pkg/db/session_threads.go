package db

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// darkResolveThreadAnchor keeps ResolveThreadForSession reachable in the
// production call graph until the turn-end publishing cutover wires live
// callers. The call below is a validated no-op: a zero store fails its nil
// database check before any I/O, so this performs no database work and
// changes no behavior. The cutover chunk removes this anchor when it wires
// the live publisher.
var _ = func() bool {
	_, errResolve := (&SQLStore{}).ResolveThreadForSession(context.Background(), "")
	_ = errResolve
	return true
}()

// ResolveThreadForSession returns the Discord thread holding the given agent
// session ID, matching either the current or the previous conversation ID so
// rotations keep resolving. Empty input resolves empty with no error.
func (s *SQLStore) ResolveThreadForSession(ctx context.Context, sessionID string) (string, error) {
	if err := s.checkDB(); err != nil {
		return "", err
	}
	if strings.TrimSpace(sessionID) == "" {
		return "", nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var thread sql.NullString
	err := s.db.QueryRowContext(queryCtx,
		`SELECT session_id FROM sessions WHERE conversation_id = $1 OR previous_conversation_id = $1 ORDER BY updated_at DESC LIMIT 1`,
		strings.TrimSpace(sessionID)).Scan(&thread)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return thread.String, err
}
