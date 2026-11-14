package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

var noopHTTPClient = &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
})}

type mockRow struct {
	b   *VanadisBuild
	err error
}

func fillBuildDest(b VanadisBuild, dest []any) {
	*dest[0].(*string), *dest[1].(*string), *dest[2].(*int), *dest[3].(*string), *dest[4].(*string), *dest[5].(*string), *dest[6].(*string), *dest[7].(*string), *dest[8].(*time.Time), *dest[9].(*time.Time) = b.ID, b.ThreadID, b.Tier, b.Title, b.Objective, b.SharedContext, b.Status, b.StatusMessageID, b.CreatedAt, b.UpdatedAt
}

func (r *mockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.b != nil && len(dest) == 10 {
		fillBuildDest(*r.b, dest)
	}
	return nil
}

type mockRows struct {
	pgx.Rows

	items            []VanadisChunk
	builds           []VanadisBuild
	idx              int
	scanErr, rowsErr error
}

func (r *mockRows) Close()     {}
func (r *mockRows) Err() error { return r.rowsErr }
func (r *mockRows) Next() bool {
	if len(r.builds) > 0 {
		r.idx++
		return r.idx <= len(r.builds)
	}
	if r.idx < len(r.items) {
		r.idx++
		return true
	}
	return false
}

func (r *mockRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if len(r.builds) > 0 {
		fillBuildDest(r.builds[r.idx-1], dest)
		return nil
	}
	if len(r.items) > 0 {
		c := r.items[r.idx-1]
		if len(dest) == 10 {
			*dest[0].(*string), *dest[1].(*int), *dest[2].(*string), *dest[3].(*string), *dest[4].(*string), *dest[5].(*string), *dest[6].(*int), *dest[7].(*sql.NullTime), *dest[8].(*sql.NullTime), *dest[9].(*time.Time) = c.ID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Status, c.PRNumber, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.UpdatedAt
			return nil
		}
		if len(dest) == 17 {
			*dest[0].(*string), *dest[1].(*string), *dest[2].(*int), *dest[3].(*string), *dest[4].(*string), *dest[5].(*string), *dest[6].(*string), *dest[7].(*[]string), *dest[8].(*[]string), *dest[9].(*string), *dest[10].(*[]string), *dest[11].(*string), *dest[12].(*int), *dest[13].(*sql.NullTime), *dest[14].(*sql.NullTime), *dest[15].(*time.Time), *dest[16].(*time.Time) = c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt
			return nil
		}
	}
	return nil
}

type mockTx struct {
	pgx.Tx

	execFn                 func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	queryFn                func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	queryRowFn             func(ctx context.Context, sql string, args ...any) pgx.Row
	commitErr, rollbackErr error
}

func (t *mockTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if t.execFn != nil {
		return t.execFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (t *mockTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if t.queryFn != nil {
		return t.queryFn(ctx, sql, args...)
	}
	return &mockRows{}, nil
}

func (t *mockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if t.queryRowFn != nil {
		return t.queryRowFn(ctx, sql, args...)
	}
	return &mockRow{}
}
func (t *mockTx) Commit(_ context.Context) error   { return t.commitErr }
func (t *mockTx) Rollback(_ context.Context) error { return t.rollbackErr }

type mockDB struct {
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	beginFn    func(ctx context.Context) (pgx.Tx, error)
}

func (db *mockDB) Exec(ctx context.Context, s string, a ...any) (pgconn.CommandTag, error) {
	if db.execFn != nil {
		return db.execFn(ctx, s, a...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (db *mockDB) Query(ctx context.Context, s string, a ...any) (pgx.Rows, error) {
	if db.queryFn != nil {
		return db.queryFn(ctx, s, a...)
	}
	return &mockRows{}, nil
}

func (db *mockDB) QueryRow(ctx context.Context, s string, a ...any) pgx.Row {
	if db.queryRowFn != nil {
		return db.queryRowFn(ctx, s, a...)
	}
	return &mockRow{}
}

func (db *mockDB) Begin(ctx context.Context) (pgx.Tx, error) {
	if db.beginFn != nil {
		return db.beginFn(ctx)
	}
	return &mockTx{}, nil
}

type memoryVanadisStore struct {
	builds                                                                                      map[string]VanadisBuild
	chunks                                                                                      map[string][]VanadisChunk
	createErr, getErr, listErr, updateChunkErr, updateBuildErr, appendChunksErr, removeChunkErr error
	stallNudges                                                                                 map[string]int
}

func newMemoryVanadisStore() *memoryVanadisStore {
	return &memoryVanadisStore{builds: make(map[string]VanadisBuild), chunks: make(map[string][]VanadisChunk)}
}

func (m *memoryVanadisStore) CreateBuild(_ context.Context, build VanadisBuild, chunks []VanadisChunk) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.builds[build.ID], m.chunks[build.ID] = build, chunks
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

func (m *memoryVanadisStore) UpdateChunkStatus(_ context.Context, chunkID, status string) error {
	if m.updateChunkErr != nil {
		return m.updateChunkErr
	}
	if status == "failed" && m.updateBuildErr != nil {
		return m.updateBuildErr
	}
	now := time.Now().UTC()
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != chunkID {
				continue
			}
			list[i].Status = status
			if (status == "running" || status == "coding") && list[i].StartedAt.IsZero() {
				list[i].StartedAt = now
			}
			if isTerminalChunkStatus(status) && list[i].CompletedAt.IsZero() {
				list[i].CompletedAt = now
			} else if !isTerminalChunkStatus(status) {
				list[i].CompletedAt = time.Time{}
			}
			list[i].UpdatedAt = now
			m.chunks[bID] = list
			return nil
		}
	}
	return ErrChunkNotFound
}

func (m *memoryVanadisStore) setBuild(id string, fn func(*VanadisBuild)) error {
	if m.updateBuildErr != nil {
		return m.updateBuildErr
	}
	b, ok := m.builds[id]
	if !ok {
		return ErrBuildNotFound
	}
	fn(&b)
	m.builds[id] = b
	return nil
}

func (m *memoryVanadisStore) UpdateBuildStatus(_ context.Context, id, status string) error {
	return m.setBuild(id, func(b *VanadisBuild) { b.Status = status })
}

func (m *memoryVanadisStore) UpdateBuildStatusMessageID(_ context.Context, id, msgID string) error {
	return m.setBuild(id, func(b *VanadisBuild) { b.StatusMessageID = msgID })
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
				hasRemaining := slices.ContainsFunc(list, func(c2 VanadisChunk) bool { return c2.ChunkIndex > c.ChunkIndex })
				summaries := make([]VanadisChunkSummary, 0, len(list))
				for _, c2 := range list {
					summaries = append(summaries, VanadisChunkSummary{ID: c2.ID, ChunkIndex: c2.ChunkIndex, Mode: c2.Mode, Status: c2.Status})
				}
				return &VanadisChunkPRLookup{
					Matched:            true,
					ChunkID:            c.ID,
					BuildID:            bID,
					Effort:             capChunkEffort(c.Effort),
					HasRemainingChunks: hasRemaining,
					NextChunkMode:      nextChunkModeAfter(summaries, c.ID, c.ChunkIndex),
				}, nil
			}
		}
	}
	return &VanadisChunkPRLookup{Matched: false}, nil
}

func (m *memoryVanadisStore) AttachChunkPR(_ context.Context, chunkID string, prNumber int, repo string) (string, error) {
	if chunkID, repo = strings.TrimSpace(chunkID), normalizeRepo(repo); chunkID == "" || prNumber <= 0 {
		return "", errors.New("chunk id and valid pr number are required")
	}
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != chunkID {
				continue
			}
			if list[i].Status == "cancelled" || m.builds[bID].Status == "cancelled" || m.builds[bID].Status == "completed" {
				return "", ErrBuildConflict
			}
			if repo != "" && !reposMatch(list[i].Repo, repo) {
				return "", fmt.Errorf("chunk %s repo %q does not match %q", chunkID, list[i].Repo, repo)
			}
			list[i].PRNumber = prNumber
			if list[i].Status != "merged" && list[i].Status != "failed" {
				list[i].Status = "pr_opened"
			}
			if list[i].StartedAt.IsZero() {
				list[i].StartedAt = time.Now().UTC()
			}
			list[i].UpdatedAt = time.Now().UTC()
			m.chunks[bID] = list
			return m.builds[bID].ThreadID, nil
		}
	}
	return "", ErrChunkNotFound
}

func (m *memoryVanadisStore) findChunkByPR(repo string, prNumber int) (string, int, bool) {
	for bID, list := range m.chunks {
		for i := range list {
			if reposMatch(list[i].Repo, repo) && list[i].PRNumber == prNumber {
				return bID, i, true
			}
		}
	}
	return "", -1, false
}

func summarizeChunkList(list []VanadisChunk) []VanadisChunkSummary {
	summaries := make([]VanadisChunkSummary, len(list))
	for i, c := range list {
		summaries[i] = VanadisChunkSummary{
			ID:          c.ID,
			ChunkIndex:  c.ChunkIndex,
			Repo:        c.Repo,
			Title:       c.Title,
			Status:      c.Status,
			PRNumber:    c.PRNumber,
			StartedAt:   c.StartedAt,
			CompletedAt: c.CompletedAt,
			UpdatedAt:   c.UpdatedAt,
		}
	}
	return summaries
}

func findNextUnmergedChunkIdx(list []VanadisChunk, currentChunkIdx int) int {
	nextChunkIdx := -1
	fallbackIdx := -1
	for _, cs := range list {
		if cs.Status == "merged" {
			continue
		}
		if cs.ChunkIndex > currentChunkIdx && (nextChunkIdx == -1 || cs.ChunkIndex < nextChunkIdx) {
			nextChunkIdx = cs.ChunkIndex
		} else if fallbackIdx == -1 {
			fallbackIdx = cs.ChunkIndex
		}
	}
	if nextChunkIdx == -1 {
		return fallbackIdx
	}
	return nextChunkIdx
}

func (m *memoryVanadisStore) activateNextPendingChunk(buildID string, nextIdx int, now time.Time) []VanadisChunkSummary {
	list := m.chunks[buildID]
	for ci := range list {
		if list[ci].ChunkIndex == nextIdx && list[ci].Status == "pending" {
			list[ci].Status = "running"
			if list[ci].StartedAt.IsZero() {
				list[ci].StartedAt = now
			}
			list[ci].UpdatedAt = now
			m.chunks[buildID] = list
			break
		}
	}
	return summarizeChunkList(m.chunks[buildID])
}

func (m *memoryVanadisStore) AdvanceChunkByPR(_ context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return &VanadisAdvancementResult{Matched: false}, nil
	}
	bID, idx, ok := m.findChunkByPR(repo, prNumber)
	if !ok {
		return &VanadisAdvancementResult{Matched: false}, nil
	}
	b := m.builds[bID]
	list := m.chunks[bID]
	target := list[idx]
	if b.Status == "cancelled" {
		return &VanadisAdvancementResult{
			Matched:          true,
			Advanced:         false,
			BuildID:          bID,
			ThreadID:         b.ThreadID,
			Tier:             b.Tier,
			Title:            firstNonEmpty(b.Title, b.Objective),
			Objective:        b.Objective,
			BuildStatus:      b.Status,
			StatusMessageID:  b.StatusMessageID,
			MergedChunkIndex: target.ChunkIndex,
			MergedChunkTitle: target.Title,
			NextChunkIndex:   -1,
			BuildCompleted:   b.Status == "completed",
		}, nil
	}

	wasMerged := target.Status == "merged"
	now := time.Now().UTC()
	list[idx].Status = "merged"
	if list[idx].CompletedAt.IsZero() {
		list[idx].CompletedAt = now
	}
	list[idx].UpdatedAt = now
	m.chunks[bID] = list

	summaries := summarizeChunkList(list)
	nextChunkIdx := findNextUnmergedChunkIdx(list, target.ChunkIndex)

	if nextChunkIdx >= 0 {
		if b.Status == "failed" {
			b.Status = "running"
			m.builds[bID] = b
		}
		updatedSummaries := m.activateNextPendingChunk(bID, nextChunkIdx, now)
		return &VanadisAdvancementResult{
			Matched:          true,
			Advanced:         !wasMerged,
			BuildID:          bID,
			ThreadID:         b.ThreadID,
			Tier:             b.Tier,
			Title:            firstNonEmpty(b.Title, b.Objective),
			Objective:        b.Objective,
			BuildStatus:      b.Status,
			StatusMessageID:  b.StatusMessageID,
			MergedChunkIndex: target.ChunkIndex,
			MergedChunkTitle: target.Title,
			NextChunkIndex:   nextChunkIdx,
			BuildCompleted:   false,
			Chunks:           updatedSummaries,
		}, nil
	}

	b.Status = "completed"
	m.builds[bID] = b
	return &VanadisAdvancementResult{
		Matched:          true,
		Advanced:         !wasMerged,
		BuildID:          bID,
		ThreadID:         b.ThreadID,
		Tier:             b.Tier,
		Title:            firstNonEmpty(b.Title, b.Objective),
		Objective:        b.Objective,
		BuildStatus:      "completed",
		StatusMessageID:  b.StatusMessageID,
		MergedChunkIndex: target.ChunkIndex,
		MergedChunkTitle: target.Title,
		NextChunkIndex:   -1,
		BuildCompleted:   true,
		Chunks:           summaries,
	}, nil
}

func (m *memoryVanadisStore) FailChunkByPR(_ context.Context, repo string, prNumber int) (string, string, bool, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", "", false, nil
	}
	for bID, list := range m.chunks {
		for i := range list {
			if reposMatch(list[i].Repo, repo) && list[i].PRNumber == prNumber && list[i].Status != "merged" && list[i].Status != "failed" {
				list[i].Status = "failed"
				now := time.Now().UTC()
				if list[i].CompletedAt.IsZero() {
					list[i].CompletedAt = now
				}
				list[i].UpdatedAt = now
				m.chunks[bID] = list
				b := m.builds[bID]
				if b.Status == "running" {
					b.Status = "failed"
					m.builds[bID] = b
				}
				return bID, list[i].ID, true, nil
			}
		}
	}
	return "", "", false, nil
}

func (m *memoryVanadisStore) findChunkByID(chunkID string) (string, int, bool) {
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID == chunkID {
				return bID, i, true
			}
		}
	}
	return "", -1, false
}

func (m *memoryVanadisStore) CompleteActionChunk(_ context.Context, chunkID string) (*VanadisAdvancementResult, error) {
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil, errors.New("chunk id cannot be empty")
	}
	bID, idx, ok := m.findChunkByID(chunkID)
	if !ok {
		return nil, ErrChunkNotFound
	}
	b := m.builds[bID]
	list := m.chunks[bID]
	target := list[idx]
	res := &VanadisAdvancementResult{
		Matched:          true,
		Advanced:         false,
		BuildID:          bID,
		ThreadID:         b.ThreadID,
		Tier:             b.Tier,
		Title:            firstNonEmpty(b.Title, b.Objective),
		Objective:        b.Objective,
		BuildStatus:      b.Status,
		StatusMessageID:  b.StatusMessageID,
		MergedChunkIndex: target.ChunkIndex,
		MergedChunkTitle: target.Title,
		ChunkMode:        target.Mode,
		NextChunkIndex:   -1,
		BuildCompleted:   b.Status == "completed",
	}
	if target.Mode != "action" || target.Status != "running" || b.Status != "running" {
		return res, nil
	}

	now := time.Now().UTC()
	list[idx].Status = "completed"
	list[idx].CompletedAt = now
	list[idx].UpdatedAt = now
	m.chunks[bID] = list

	nextIdx := -1
	for j := idx + 1; j < len(list); j++ {
		if !isChunkDoneStatus(list[j].Status) {
			nextIdx = list[j].ChunkIndex
			break
		}
	}

	res.Advanced = true
	res.Chunks = summarizeChunkList(list)
	if nextIdx >= 0 {
		_ = m.activateNextPendingChunk(bID, nextIdx, now)
		res.NextChunkIndex = nextIdx
		res.BuildCompleted = false
		return res, nil
	}

	b.Status = "completed"
	b.UpdatedAt = now
	m.builds[bID] = b
	res.BuildStatus = "completed"
	res.BuildCompleted = true
	return res, nil
}

func (m *memoryVanadisStore) ClaimStalledChunk(_ context.Context, chunkID string) (StalledChunkVerdict, *VanadisBuild, *VanadisChunk, error) {
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != chunkID {
				continue
			}
			b, c := m.builds[bID], list[i]
			verdict := judgeStalledChunk(c, b.Status, m.stallNudges[chunkID])
			if verdict == StalledChunkVerdictNudge {
				if m.stallNudges == nil {
					m.stallNudges = make(map[string]int)
				}
				m.stallNudges[chunkID]++
			}
			return verdict, &b, &c, nil
		}
	}
	return "", nil, nil, ErrChunkNotFound
}

func (m *memoryVanadisStore) FailChunk(_ context.Context, chunkID, reason string) (*VanadisBuild, *VanadisChunk, bool, error) {
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil, nil, false, errors.New("chunk id cannot be empty")
	}
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != chunkID {
				continue
			}
			b := m.builds[bID]
			if isChunkDoneStatus(list[i].Status) || list[i].Status == "failed" || b.Status == "completed" || b.Status == "cancelled" {
				return &b, &list[i], false, nil
			}
			now := time.Now().UTC()
			list[i].Status = "failed"
			list[i].CompletedAt = now
			list[i].UpdatedAt = now
			m.chunks[bID] = list
			b.Status = "failed"
			b.UpdatedAt = now
			m.builds[bID] = b
			return &b, &list[i], true, nil
		}
	}
	return nil, nil, false, ErrChunkNotFound
}

func (m *memoryVanadisStore) UpdateBuild(_ context.Context, buildID string, req UpdateBuildRequest) (*VanadisBuild, error) {
	if m.updateBuildErr != nil {
		return nil, m.updateBuildErr
	}
	buildID = strings.TrimSpace(buildID)
	title := strings.TrimSpace(req.Title)
	obj := strings.TrimSpace(req.Objective)
	sharedCtx := strings.TrimSpace(req.SharedContext)
	if buildID == "" || (title == "" && obj == "" && sharedCtx == "") {
		return nil, fmt.Errorf("%w: build id and at least one non-empty field are required", ErrInvalidBuildInput)
	}
	b, ok := m.builds[buildID]
	if !ok {
		return nil, ErrBuildNotFound
	}
	if b.Status != "running" {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running)", ErrBuildConflict, buildID, b.Status)
	}
	if title != "" {
		b.Title = title
	}
	if obj != "" {
		b.Objective = obj
	}
	if sharedCtx != "" {
		b.SharedContext = sharedCtx
	}
	b.UpdatedAt = time.Now().UTC()
	m.builds[buildID] = b
	return &b, nil
}

func (m *memoryVanadisStore) UpdateChunk(_ context.Context, buildID, chunkRef string, req UpdateChunkRequest) (*VanadisChunk, error) {
	if m.updateChunkErr != nil {
		return nil, m.updateChunkErr
	}
	buildID, chunkRef = strings.TrimSpace(buildID), strings.TrimSpace(chunkRef)
	normEffort, err := validateUpdateChunkInput(buildID, chunkRef, req)
	if err != nil {
		return nil, err
	}
	b, ok := m.builds[buildID]
	if !ok {
		return nil, ErrBuildNotFound
	}
	if b.Status != "running" {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running)", ErrBuildConflict, buildID, b.Status)
	}
	list := m.chunks[buildID]
	idx := resolveChunkRef(list, chunkRef)
	if idx < 0 {
		return nil, ErrChunkNotFound
	}
	c := list[idx]
	if c.Status != "pending" {
		return nil, fmt.Errorf("%w: chunk %s has status %q (must be pending)", ErrBuildConflict, c.ID, c.Status)
	}
	if err := applyChunkUpdates(&c, req, normEffort); err != nil {
		return nil, err
	}
	c.UpdatedAt = time.Now().UTC()
	list[idx] = c
	m.chunks[buildID] = list
	return &c, nil
}

func (m *memoryVanadisStore) AppendChunks(_ context.Context, buildID string, inputs []CreateChunkInput) ([]VanadisChunk, error) {
	if m.appendChunksErr != nil {
		return nil, m.appendChunksErr
	}
	buildID = strings.TrimSpace(buildID)
	if err := validateAppendChunksInputs(buildID, inputs); err != nil {
		return nil, err
	}
	b, ok := m.builds[buildID]
	if !ok {
		return nil, ErrBuildNotFound
	}
	if b.Status != "running" {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running)", ErrBuildConflict, buildID, b.Status)
	}
	existing := m.chunks[buildID]
	usedIDs := make(map[string]bool, len(existing)+len(inputs))
	for _, c := range existing {
		usedIDs[c.ID] = true
	}
	nextIdx := 0
	if len(existing) > 0 {
		last := existing[len(existing)-1]
		nextIdx = last.ChunkIndex + 1
	}
	now := time.Now().UTC()
	appended := make([]VanadisChunk, 0, len(inputs))
	for _, in := range inputs {
		newChunk, err := buildAppendedChunk(buildID, in, nextIdx, usedIDs, now)
		if err != nil {
			return nil, err
		}
		appended = append(appended, newChunk)
		existing = append(existing, newChunk)
		nextIdx++
	}
	m.chunks[buildID] = existing
	return appended, nil
}

func (m *memoryVanadisStore) RemoveChunk(_ context.Context, buildID, chunkRef string) (*VanadisChunk, []VanadisChunk, error) {
	if m.removeChunkErr != nil {
		return nil, nil, m.removeChunkErr
	}
	buildID, chunkRef = strings.TrimSpace(buildID), strings.TrimSpace(chunkRef)
	if buildID == "" || chunkRef == "" {
		return nil, nil, fmt.Errorf("%w: build id and chunk ref are required", ErrInvalidBuildInput)
	}
	b, ok := m.builds[buildID]
	if !ok {
		return nil, nil, ErrBuildNotFound
	}
	if b.Status != "running" {
		return nil, nil, fmt.Errorf("%w: build %s has status %q (must be running)", ErrBuildConflict, buildID, b.Status)
	}
	existing := m.chunks[buildID]
	removeIdx := resolveChunkRef(existing, chunkRef)
	if removeIdx < 0 {
		return nil, nil, ErrChunkNotFound
	}
	removed := existing[removeIdx]
	if removed.Status != "pending" {
		return nil, nil, fmt.Errorf("%w: chunk %s has status %q (must be pending)", ErrBuildConflict, removed.ID, removed.Status)
	}
	if len(existing) <= 1 {
		return nil, nil, fmt.Errorf("%w: cannot remove the only chunk of build %s", ErrBuildConflict, buildID)
	}
	for i := removeIdx + 1; i < len(existing); i++ {
		existing[i].ChunkIndex--
	}
	remaining := make([]VanadisChunk, 0, len(existing)-1)
	remaining = append(remaining, existing[:removeIdx]...)
	remaining = append(remaining, existing[removeIdx+1:]...)
	m.chunks[buildID] = remaining
	return &removed, remaining, nil
}

type mockRiverInserter struct {
	inserted []ExecuteChunkArgs
	err      error
}

func (m *mockRiverInserter) Insert(_ context.Context, args river.JobArgs, _ *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	if a, ok := args.(ExecuteChunkArgs); ok {
		m.inserted = append(m.inserted, a)
	}
	return &rivertype.JobInsertResult{}, nil
}

type mockRiverLifecycle struct {
	mockRiverInserter

	startErr, stopErr error
}

func (m *mockRiverLifecycle) Start(_ context.Context) error { return m.startErr }
func (m *mockRiverLifecycle) Stop(_ context.Context) error  { return m.stopErr }

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read error") }

func newMockStore(db mockDB) *PostgresVanadisStore {
	return NewPostgresVanadisStore(&db)
}

func mkBuild(id, threadID, title, objective, status string) VanadisBuild {
	return VanadisBuild{ID: id, ThreadID: threadID, Title: title, Objective: objective, Status: status, Tier: 2}
}

func mkChunk(id, buildID string, idx int, title, instr, effort, status string) VanadisChunk {
	if effort == "" {
		effort = "medium"
	}
	return VanadisChunk{ID: id, BuildID: buildID, ChunkIndex: idx, Mode: "pr", Repo: normalizeRepo("aerial"), Title: title, Instructions: instr, Effort: effort, Status: status, TargetFiles: []string{}, TargetSymbols: []string{}, AcceptanceCriteria: []string{}}
}
