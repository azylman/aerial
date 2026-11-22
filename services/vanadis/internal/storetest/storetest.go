// Package storetest provides an in-memory store.VanadisStore fake and compact
// build/chunk constructors shared by vanadis package tests. It imports only
// internal/model so every vanadis package (store included) can use it.
package storetest

import (
	"context"
	"sort"

	"github.com/azylman/aerial/services/vanadis/internal/model"
)

// Store is an airgapped VanadisStore seeded directly by tests. GetErr fails
// GetBuild and GetChunk; ListErr fails ListBuilds.
type Store struct {
	Builds          map[string]model.VanadisBuild
	Chunks          map[string][]model.VanadisChunk
	GetErr, ListErr error
}

// New returns an empty Store with initialized maps.
func New() *Store {
	return &Store{Builds: make(map[string]model.VanadisBuild), Chunks: make(map[string][]model.VanadisChunk)}
}

// GetBuild returns the build and its chunks, or model.ErrBuildNotFound.
func (s *Store) GetBuild(_ context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s.GetErr != nil {
		return nil, nil, s.GetErr
	}
	b, ok := s.Builds[buildID]
	if !ok {
		return nil, nil, model.ErrBuildNotFound
	}
	return &b, s.Chunks[buildID], nil
}

// ListBuilds returns build summaries newest first (ties broken by descending
// ID), capped at limit; limits outside 1..100 mean 50, like the Postgres store.
func (s *Store) ListBuilds(_ context.Context, limit int) ([]model.VanadisBuildEntry, error) {
	if s.ListErr != nil {
		return nil, s.ListErr
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	builds := make([]model.VanadisBuild, 0, len(s.Builds))
	for _, b := range s.Builds {
		builds = append(builds, b)
	}
	sort.Slice(builds, func(i, j int) bool {
		return builds[i].CreatedAt.After(builds[j].CreatedAt) || (builds[i].CreatedAt.Equal(builds[j].CreatedAt) && builds[i].ID > builds[j].ID)
	})
	if len(builds) > limit {
		builds = builds[:limit]
	}
	entries := make([]model.VanadisBuildEntry, len(builds))
	for i, b := range builds {
		entries[i] = model.SummarizeBuildEntry(b, s.Chunks[b.ID])
	}
	return entries, nil
}

// GetChunk returns the chunk and its build, model.ErrChunkNotFound when no
// build holds the chunk, or model.ErrBuildNotFound when its build is missing.
func (s *Store) GetChunk(_ context.Context, chunkID string) (*model.VanadisChunk, *model.VanadisBuild, error) {
	if s.GetErr != nil {
		return nil, nil, s.GetErr
	}
	for buildID, chunks := range s.Chunks {
		for _, c := range chunks {
			if c.ID != chunkID {
				continue
			}
			b, ok := s.Builds[buildID]
			if !ok {
				return nil, nil, model.ErrBuildNotFound
			}
			return &c, &b, nil
		}
	}
	return nil, nil, model.ErrChunkNotFound
}

// Build returns a tier-2 build.
func Build(id, threadID, title, objective, status string) model.VanadisBuild {
	return model.VanadisBuild{ID: id, ThreadID: threadID, Title: title, Objective: objective, Status: status, Tier: 2}
}

// Chunk returns a PR chunk in the aerial repo with empty (non-nil) list fields;
// an empty effort means "medium".
func Chunk(id, buildID string, idx int, title, instr, effort, status string) model.VanadisChunk {
	if effort == "" {
		effort = "medium"
	}
	return model.VanadisChunk{
		ID: id, BuildID: buildID, ChunkIndex: idx, Mode: "pr", Repo: "aerial", Title: title, Instructions: instr, Effort: effort, Status: status,
		TargetFiles: []string{}, TargetSymbols: []string{}, AcceptanceCriteria: []string{}, DependsOn: []string{},
	}
}
