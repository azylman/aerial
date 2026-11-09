package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

// TranscriptTarget locates the transcript index (DSN) and the stack label this instance writes under.
type TranscriptTarget struct {
	DSN   string
	Stack string
}

// ResolveTranscriptTarget derives the transcript index target from the stack's own databaseURL.
// Stack is tc.Stack, else the database name in databaseURL's path. DSN is databaseURL with its
// database path swapped to tc.Database when that names a different database.
func ResolveTranscriptTarget(databaseURL string, tc config.TranscriptsConfig) (TranscriptTarget, error) {
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		return TranscriptTarget{}, fmt.Errorf("transcripts: database URL cannot be empty")
	}
	u, err := url.Parse(databaseURL)
	if err != nil {
		return TranscriptTarget{}, fmt.Errorf("transcripts: parse database URL: %w", err)
	}
	ownDB := strings.TrimPrefix(u.Path, "/")
	if ownDB != "" {
		ownDB = path.Base(ownDB)
	}
	target := TranscriptTarget{DSN: databaseURL, Stack: strings.TrimSpace(tc.Stack)}
	if target.Stack == "" {
		target.Stack = ownDB
	}
	if target.Stack == "" {
		return TranscriptTarget{}, fmt.Errorf("transcripts: cannot derive stack from database URL without a database name; set transcripts.stack")
	}
	if shared := strings.TrimSpace(tc.Database); shared != "" && shared != ownDB {
		if u.Scheme == "" {
			return TranscriptTarget{}, fmt.Errorf("transcripts: database URL must be a URL to swap to database %q", shared)
		}
		u.Path = "/" + shared
		u.RawPath = ""
		target.DSN = u.String()
	}
	return target, nil
}

// routedTranscriptStore resolves the transcript target JIT from the live config on every call so a
// config hot-reload retargets the index without a restart. The primary pool is reused when the target
// DSN is the stack's own DatabaseURL; other DSNs get one lazily opened pool each.
type routedTranscriptStore struct {
	cfg     *config.Config
	primary *sql.DB
	open    func(string) (*sql.DB, error)

	mu    sync.Mutex
	pools map[string]*sql.DB
}

// NewRoutedTranscriptStore returns a TranscriptStore that writes under the configured stack into the
// configured transcript database, opening extra pools via open (db.Open in production).
func NewRoutedTranscriptStore(cfg *config.Config, primary *sql.DB, open func(string) (*sql.DB, error)) TranscriptStore {
	return &routedTranscriptStore{cfg: cfg, primary: primary, open: open, pools: make(map[string]*sql.DB)}
}

func (r *routedTranscriptStore) resolve() (*SQLTranscriptStore, error) {
	if r.cfg == nil {
		return nil, fmt.Errorf("transcripts: config cannot be nil")
	}
	cur := r.cfg.Current()
	if cur == nil {
		return nil, fmt.Errorf("transcripts: config snapshot is nil")
	}
	target, err := ResolveTranscriptTarget(cur.DatabaseURL, cur.Transcripts)
	if err != nil {
		return nil, err
	}
	if target.DSN == strings.TrimSpace(cur.DatabaseURL) {
		return NewSQLTranscriptStore(r.primary, target.Stack), nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	pool, ok := r.pools[target.DSN]
	if !ok {
		if r.open == nil {
			return nil, fmt.Errorf("transcripts: no opener configured for shared transcript database")
		}
		if pool, err = r.open(target.DSN); err != nil {
			return nil, fmt.Errorf("transcripts: open shared transcript database: %w", err)
		}
		r.pools[target.DSN] = pool
	}
	return NewSQLTranscriptStore(pool, target.Stack), nil
}

// Close releases every lazily opened shared pool; the primary pool belongs to the caller.
func (r *routedTranscriptStore) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	errs := make([]error, 0, len(r.pools))
	for dsn, pool := range r.pools {
		errs = append(errs, pool.Close())
		delete(r.pools, dsn)
	}
	return errors.Join(errs...)
}

func (r *routedTranscriptStore) GetSessionSyncStates(ctx context.Context) (map[string]SessionSyncState, error) {
	s, err := r.resolve()
	if err != nil {
		return nil, err
	}
	return s.GetSessionSyncStates(ctx)
}

func (r *routedTranscriptStore) UpsertSessionSummary(ctx context.Context, summary SessionSummary) error {
	s, err := r.resolve()
	if err != nil {
		return err
	}
	return s.UpsertSessionSummary(ctx, summary)
}

func (r *routedTranscriptStore) SetSessionEngine(ctx context.Context, sessionID, engine string) error {
	s, err := r.resolve()
	if err != nil {
		return err
	}
	return s.SetSessionEngine(ctx, sessionID, engine)
}

func (r *routedTranscriptStore) BatchInsertTranscriptSteps(ctx context.Context, steps []TranscriptStep) error {
	s, err := r.resolve()
	if err != nil {
		return err
	}
	return s.BatchInsertTranscriptSteps(ctx, steps)
}

func (r *routedTranscriptStore) BackfillToolKinds(ctx context.Context, mapper func(string) string) (int64, error) {
	s, err := r.resolve()
	if err != nil {
		return 0, err
	}
	return s.BackfillToolKinds(ctx, mapper)
}

func (r *routedTranscriptStore) SearchSessionSummaries(ctx context.Context, stack string, embedding []float32, queryText string, limit int, minScore float64) ([]SessionSummary, error) {
	s, err := r.resolve()
	if err != nil {
		return nil, err
	}
	return s.SearchSessionSummaries(ctx, stack, embedding, queryText, limit, minScore)
}

func (r *routedTranscriptStore) SearchTranscriptSteps(ctx context.Context, stack, queryText, sessionFilter, toolFilter string, limit int, since ...time.Time) ([]TranscriptStep, error) {
	s, err := r.resolve()
	if err != nil {
		return nil, err
	}
	return s.SearchTranscriptSteps(ctx, stack, queryText, sessionFilter, toolFilter, limit, since...)
}

func (r *routedTranscriptStore) GetTranscriptStepRange(ctx context.Context, stack, sessionID string, startStep, endStep int) ([]TranscriptStep, error) {
	s, err := r.resolve()
	if err != nil {
		return nil, err
	}
	return s.GetTranscriptStepRange(ctx, stack, sessionID, startStep, endStep)
}
