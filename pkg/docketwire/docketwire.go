package docketwire

import (
	"time"
)

// TaskSummary is a compact task representation.
type TaskSummary struct {
	ID              int64      `json:"id"`
	Title           string     `json:"title"`
	Status          string     `json:"status"`
	ParentID        *int64     `json:"parent_id,omitempty"`
	BlockedBy       []int64    `json:"blocked_by,omitempty"`
	Blocks          []int64    `json:"blocks,omitempty"`
	ClaimedByAgent  string     `json:"claimed_by_agent,omitempty"`
	ClaimedByTurnID string     `json:"claimed_by_turn_id,omitempty"`
	ClaimedAt       *time.Time `json:"claimed_at,omitempty"`
}

// ScoredTaskSummary couples a task with its search relevance score.
type ScoredTaskSummary struct {
	ID              int64      `json:"id"`
	Title           string     `json:"title"`
	Status          string     `json:"status"`
	ParentID        *int64     `json:"parent_id,omitempty"`
	Score           float64    `json:"score"`
	ClaimedByAgent  string     `json:"claimed_by_agent,omitempty"`
	ClaimedByTurnID string     `json:"claimed_by_turn_id,omitempty"`
	ClaimedAt       *time.Time `json:"claimed_at,omitempty"`
}

// TaskComment represents a comment on a task.
type TaskComment struct {
	ID        int64     `json:"id"`
	Author    string    `json:"author,omitempty"`
	Comment   string    `json:"comment"`
	CreatedAt time.Time `json:"created_at"`
}

// HydratedTask contains full task details, relations, and comments.
type HydratedTask struct {
	ID              int64         `json:"id"`
	Title           string        `json:"title"`
	Status          string        `json:"status"`
	Description     string        `json:"description,omitempty"`
	CreatedAt       time.Time     `json:"created_at"`
	UpdatedAt       time.Time     `json:"updated_at"`
	ClosedAt        *time.Time    `json:"closed_at,omitempty"`
	Parent          *TaskSummary  `json:"parent,omitempty"`
	Subtasks        []TaskSummary `json:"subtasks,omitempty"`
	Blocks          []TaskSummary `json:"blocks,omitempty"`
	BlockedBy       []TaskSummary `json:"blocked_by,omitempty"`
	Related         []TaskSummary `json:"related,omitempty"`
	Comments        []TaskComment `json:"comments,omitempty"`
	ClaimedByAgent  string        `json:"claimed_by_agent,omitempty"`
	ClaimedByTurnID string        `json:"claimed_by_turn_id,omitempty"`
	ClaimedAt       *time.Time    `json:"claimed_at,omitempty"`
}
