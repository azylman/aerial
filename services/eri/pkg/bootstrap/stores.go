package bootstrap

import (
	"database/sql"
	"fmt"
	"io"
	"log"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

// InitDefaultStores opens the database with open and builds the agent-scoped
// SQL store and transcript store on it. The returned cleanup closes both stores
// and then the database.
func InitDefaultStores(open func(*config.Config) (*sql.DB, error), cfg *config.Config, cur *config.ConfigData, agentName string) (db.Store, db.TranscriptStore, func(), error) {
	database, err := open(cfg)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	stack, err := db.ResolveTranscriptStack(cur.DatabaseURL, cur.Transcripts.Stack)
	if err != nil {
		_ = database.Close()
		return nil, nil, nil, fmt.Errorf("failed to resolve transcript stack: %w", err)
	}
	store := db.NewSQLStore(database).WithAgent(db.ResolveAgentName(agentName))
	txStore := db.NewSQLTranscriptStore(database, stack)
	cleanup := func() {
		for _, s := range []any{txStore, store} {
			if closer, ok := s.(io.Closer); ok {
				_ = closer.Close()
			}
		}
		if closeErr := database.Close(); closeErr != nil {
			log.Printf("Error closing database: %v", closeErr)
		}
	}
	return store, txStore, cleanup, nil
}
