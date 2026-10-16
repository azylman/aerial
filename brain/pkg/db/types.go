package db

import "time"

// VanadisBuild represents a multi-chunk autonomous engineering build in vanadis_builds.
type VanadisBuild struct {
	ID              string    `json:"id"`
	Repo            string    `json:"repo"`
	ThreadID        string    `json:"thread_id"`
	Tier            int       `json:"tier"`
	Objective       string    `json:"objective"`
	Status          string    `json:"status"`
	StatusMessageID string    `json:"status_message_id,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// VanadisChunk represents an individual ordered task chunk within a VanadisBuild.
type VanadisChunk struct {
	ID           string    `json:"id"`
	BuildID      string    `json:"build_id"`
	ChunkIndex   int       `json:"chunk_index"`
	Repo         string    `json:"repo"`
	Title        string    `json:"title"`
	Instructions string    `json:"instructions"`
	TargetFiles  []string  `json:"target_files"`
	Status       string    `json:"status"`
	PRNumber     int       `json:"pr_number,omitempty"`
	DiffStat     string    `json:"diff_stat,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}
