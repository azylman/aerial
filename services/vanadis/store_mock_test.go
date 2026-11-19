package main

import (
	"context"
	"errors"
	"slices"
	"sort"
)

type memoryVanadisStore struct {
	builds                     map[string]VanadisBuild
	chunks                     map[string][]VanadisChunk
	createErr, getErr, listErr error
}

func newMemoryVanadisStore() *memoryVanadisStore {
	return &memoryVanadisStore{builds: make(map[string]VanadisBuild), chunks: make(map[string][]VanadisChunk)}
}

func (m *memoryVanadisStore) CreateBuild(_ context.Context, build VanadisBuild, chunks []VanadisChunk) error {
	if m.createErr != nil {
		return m.createErr
	}
	normChunks, err := normalizeAndValidateBuildDAG(chunks)
	if err != nil {
		return err
	}
	m.builds[build.ID], m.chunks[build.ID] = build, normChunks
	return nil
}

func (m *memoryVanadisStore) GetBuild(_ context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	b, ok := m.builds[buildID]
	if !ok {
		return nil, nil, ErrBuildNotFound
	}
	return &b, m.chunks[buildID], nil
}

func (m *memoryVanadisStore) ListBuilds(_ context.Context, limit int) ([]VanadisBuildEntry, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	bList := make([]VanadisBuild, 0, len(m.builds))
	for _, b := range m.builds {
		bList = append(bList, b)
	}
	sort.Slice(bList, func(i, j int) bool {
		return bList[i].CreatedAt.After(bList[j].CreatedAt) || (bList[i].CreatedAt.Equal(bList[j].CreatedAt) && bList[i].ID > bList[j].ID)
	})
	if len(bList) > limit {
		bList = bList[:limit]
	}
	entries := make([]VanadisBuildEntry, len(bList))
	for i, b := range bList {
		entries[i] = SummarizeBuildEntry(b, m.chunks[b.ID])
	}
	return entries, nil
}

func (m *memoryVanadisStore) GetChunk(_ context.Context, chunkID string) (*VanadisChunk, *VanadisBuild, error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	for bID, list := range m.chunks {
		for _, c := range list {
			if c.ID == chunkID {
				b, ok := m.builds[bID]
				if !ok {
					return nil, nil, ErrBuildNotFound
				}
				return &c, &b, nil
			}
		}
	}
	return nil, nil, ErrChunkNotFound
}

func reposMatch(a, b string) bool {
	if b = normalizeRepo(b); b == "" {
		return false
	}
	return slices.Contains(repoMatchCandidates(a), b)
}

func (m *memoryVanadisStore) GetChunkByPR(_ context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return &VanadisChunkPRLookup{Matched: false}, nil
	}
	for bID, list := range m.chunks {
		b := m.builds[bID]
		if b.Status == "cancelled" {
			continue
		}
		for _, c := range list {
			if reposMatch(c.Repo, repo) && c.PRNumber == prNumber {
				return &VanadisChunkPRLookup{
					Matched: true,
					ChunkID: c.ID,
					BuildID: bID,
					Effort:  capChunkEffort(c.Effort),
				}, nil
			}
		}
	}
	return &VanadisChunkPRLookup{Matched: false}, nil
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read error") }

func mkBuild(id, threadID, title, objective, status string) VanadisBuild {
	return VanadisBuild{ID: id, ThreadID: threadID, Title: title, Objective: objective, Status: status, Tier: 2}
}

func mkChunk(id, buildID string, idx int, title, instr, effort, status string) VanadisChunk {
	if effort == "" {
		effort = "medium"
	}
	return VanadisChunk{ID: id, BuildID: buildID, ChunkIndex: idx, Mode: "pr", Repo: normalizeRepo("aerial"), Title: title, Instructions: instr, Effort: effort, Status: status, TargetFiles: []string{}, TargetSymbols: []string{}, AcceptanceCriteria: []string{}, DependsOn: []string{}}
}
