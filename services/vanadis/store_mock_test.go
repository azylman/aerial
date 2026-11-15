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

	"github.com/azylman/aerial/pkg/pubsub"
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

func fillBuildDest(b VanadisBuild, dest []any) error {
	if len(dest) == 11 {
		return scanValues(dest, b.ID, b.ThreadID, b.Tier, b.Title, b.Objective, b.SharedContext, b.Status, b.StatusMessageID, b.Quiet, b.CreatedAt, b.UpdatedAt)
	}
	return scanValues(dest, b.ID, b.ThreadID, b.Tier, b.Title, b.Objective, b.SharedContext, b.Status, b.StatusMessageID, b.CreatedAt, b.UpdatedAt)
}

func (r *mockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.b != nil && (len(dest) == 10 || len(dest) == 11) {
		return fillBuildDest(*r.b, dest)
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
		return fillBuildDest(r.builds[r.idx-1], dest)
	}
	if len(r.items) > 0 {
		c := r.items[r.idx-1]
		if len(dest) == 10 {
			return scanValues(dest, c.ID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Status, c.PRNumber, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.UpdatedAt)
		}
		if len(dest) == 18 {
			return scanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt)
		}
		if len(dest) == 25 {
			rawCIChecks := []byte(`{"lint":"passed"}`)
			rawRolloutState := []byte(`{"expected_jobs":["eri"]}`)
			return scanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, c.DependsOn, c.CIFailCount, rawCIChecks, rawRolloutState, c.Banner, c.MergeAwakeableID, c.DeployAwakeableID, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt)
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

func (m *memoryVanadisStore) SetChunkAwakeables(_ context.Context, chunkID, mergeAwakeableID, deployAwakeableID string) error {
	if m.updateChunkErr != nil {
		return m.updateChunkErr
	}
	now := time.Now().UTC()
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != chunkID {
				continue
			}
			list[i].MergeAwakeableID = mergeAwakeableID
			list[i].DeployAwakeableID = deployAwakeableID
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

func (m *memoryVanadisStore) AttachChunkPR(_ context.Context, req AttachChunkPRRequest) (string, string, string, error) {
	if m.updateChunkErr != nil {
		return "", "", "", m.updateChunkErr
	}
	chunkID := strings.TrimSpace(req.ChunkID)
	if req.PRNumber <= 0 {
		return "", "", "", errors.New("chunk id or repo, and valid pr number are required")
	}
	if chunkID != "" {
		return m.attachToExistingChunk(chunkID, req)
	}
	return m.attachStandalonePR(req)
}

func (m *memoryVanadisStore) attachToExistingChunk(chunkID string, req AttachChunkPRRequest) (string, string, string, error) {
	repo := normalizeRepo(req.Repo)
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != chunkID {
				continue
			}
			if list[i].Status == "cancelled" || m.builds[bID].Status == "cancelled" || m.builds[bID].Status == "completed" {
				return "", "", "", ErrBuildConflict
			}
			if list[i].Mode == "action" {
				if repo == "" {
					return "", "", "", errors.New("repo is required to attach PR to action chunk")
				}
				list[i].Repo = repo
			} else if repo != "" && !reposMatch(list[i].Repo, repo) {
				return "", "", "", fmt.Errorf("chunk %s repo %q does not match %q", chunkID, list[i].Repo, repo)
			}
			list[i].PRNumber = req.PRNumber
			if list[i].Status != "merged" && list[i].Status != "failed" {
				list[i].Status = "pr_opened"
			}
			if list[i].StartedAt.IsZero() {
				list[i].StartedAt = time.Now().UTC()
			}
			list[i].UpdatedAt = time.Now().UTC()
			m.chunks[bID] = list
			return bID, list[i].ID, m.builds[bID].ThreadID, nil
		}
	}
	return "", "", "", ErrChunkNotFound
}

func (m *memoryVanadisStore) attachStandalonePR(req AttachChunkPRRequest) (string, string, string, error) {
	repo := normalizeRepo(req.Repo)
	if repo == "" {
		return "", "", "", errors.New("repo is required for standalone attach")
	}
	for bID, list := range m.chunks {
		b := m.builds[bID]
		if b.Status == "cancelled" || b.Status == "completed" {
			continue
		}
		for _, c := range list {
			if c.Status != "cancelled" && reposMatch(c.Repo, repo) && c.PRNumber == req.PRNumber {
				return bID, c.ID, b.ThreadID, nil
			}
		}
	}
	buildID := generateBuildID()
	newChunkID := buildID + "-chunk-1"
	threadID := normalizeDiscordThreadID(req.TargetID)
	title := firstNonEmpty(strings.TrimSpace(req.Title), fmt.Sprintf("PR #%d", req.PRNumber))
	objective := firstNonEmpty(strings.TrimSpace(req.Title), fmt.Sprintf("Track PR #%d on %s", req.PRNumber, repo))
	isQuiet := req.Quiet || strings.HasPrefix(strings.TrimSpace(req.Branch), "dependabot/")
	now := time.Now().UTC()

	b := VanadisBuild{
		ID:        buildID,
		ThreadID:  threadID,
		Tier:      0,
		Title:     title,
		Objective: objective,
		Status:    "running",
		Quiet:     isQuiet,
		CreatedAt: now,
		UpdatedAt: now,
	}
	c := VanadisChunk{
		ID:           newChunkID,
		BuildID:      buildID,
		ChunkIndex:   0,
		Mode:         "pr",
		Repo:         repo,
		Title:        title,
		Instructions: objective,
		Status:       "pr_opened",
		PRNumber:     req.PRNumber,
		DependsOn:    []string{},
		StartedAt:    now,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	m.builds[buildID] = b
	m.chunks[buildID] = []VanadisChunk{c}
	return buildID, newChunkID, threadID, nil
}

func (m *memoryVanadisStore) FailChunkByPR(_ context.Context, repo string, prNumber int) (string, string, bool, error) {
	if m.updateChunkErr != nil {
		return "", "", false, m.updateChunkErr
	}
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
	if target.Mode != "action" || target.Status != "running" || b.Status != "running" || target.PRNumber > 0 {
		return res, nil
	}

	now := time.Now().UTC()
	list[idx].Status = "completed"
	list[idx].CompletedAt = now
	list[idx].UpdatedAt = now
	m.chunks[bID] = list

	allDone := true
	for _, c := range list {
		if c.Status != "completed" && c.Status != "merged" {
			allDone = false
			break
		}
	}
	if allDone {
		b.Status = "completed"
		b.UpdatedAt = now
		m.builds[bID] = b
		res.BuildCompleted = true
		res.BuildStatus = "completed"
	}

	res.Advanced = true
	return res, nil
}

func (m *memoryVanadisStore) ClaimStalledChunk(_ context.Context, chunkID string, dispatchID ...string) (StalledChunkVerdict, *VanadisBuild, *VanadisChunk, error) {
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != chunkID {
				continue
			}
			b, c := m.builds[bID], list[i]
			nudges := m.stallNudges[chunkID]
			var dispID string
			if len(dispatchID) > 0 {
				dispID = strings.TrimSpace(dispatchID[0])
			}
			if dispID != "" {
				expectedDispID := fmt.Sprintf("vanadis-%s-turn-%d", c.ID, nudges)
				if dispID != expectedDispID {
					return StalledChunkVerdictNoop, &b, &c, nil
				}
			}
			verdict := judgeStalledChunk(c, b.Status, nudges)
			if verdict == StalledChunkVerdictNudge {
				if m.stallNudges == nil {
					m.stallNudges = make(map[string]int)
				}
				m.stallNudges[chunkID]++
				list[i].StallNudges = m.stallNudges[chunkID]
				c.StallNudges = list[i].StallNudges
				m.chunks[bID] = list
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
	normDeps, err := validateUpdateChunkDAG(list, idx, c)
	if err != nil {
		return nil, err
	}
	c.DependsOn = normDeps
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
	var prevChunkID string
	if len(existing) > 0 {
		last := existing[len(existing)-1]
		nextIdx = last.ChunkIndex + 1
		prevChunkID = last.ID
	}
	now := time.Now().UTC()
	appended := make([]VanadisChunk, 0, len(inputs))
	for _, in := range inputs {
		newChunk, err := buildAppendedChunk(buildID, in, nextIdx, prevChunkID, usedIDs, now)
		if err != nil {
			return nil, err
		}
		appended = append(appended, newChunk)
		prevChunkID = newChunk.ID
		nextIdx++
	}

	normAppended, err := validateAppendChunksDAG(existing, appended)
	if err != nil {
		return nil, err
	}
	existing = append(existing, normAppended...)
	m.chunks[buildID] = existing
	return normAppended, nil
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
	remaining := make([]VanadisChunk, 0, len(existing)-1)
	remaining = append(remaining, existing[:removeIdx]...)
	remaining = append(remaining, existing[removeIdx+1:]...)
	relinkDependenciesAfterRemoval(remaining, removed.ID, removed.DependsOn)
	for i := range remaining {
		remaining[i].ChunkIndex = i
	}
	m.chunks[buildID] = remaining
	return &removed, remaining, nil
}

func (m *memoryVanadisStore) RecordPRCheckStatus(_ context.Context, chunkID, checkName, checkState string) error {
	cID := strings.TrimSpace(chunkID)
	cName := strings.TrimSpace(checkName)
	cState := strings.TrimSpace(checkState)
	if cID == "" || cName == "" {
		return errors.New("chunk_id and check_name cannot be empty")
	}
	now := time.Now().UTC()
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != cID {
				continue
			}
			if list[i].CIChecks == nil {
				list[i].CIChecks = make(map[string]string)
			}
			list[i].CIChecks[cName] = cState
			if cState == "passed" && list[i].Status == "ci_failed" {
				hasFailed := false
				for _, state := range list[i].CIChecks {
					if state == "failed" {
						hasFailed = true
						break
					}
				}
				if !hasFailed {
					list[i].Status = "pr_opened"
					list[i].Banner = ""
				}
			}
			list[i].UpdatedAt = now
			m.chunks[bID] = list
			return nil
		}
	}
	return ErrChunkNotFound
}

func (m *memoryVanadisStore) RecordPRRolloutState(_ context.Context, chunkID, status string, rolloutState *pubsub.PRRolloutState) error {
	now := time.Now().UTC()
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != chunkID {
				continue
			}
			list[i].RolloutState = rolloutState
			if status != "" {
				list[i].Status = status
			}
			list[i].UpdatedAt = now
			m.chunks[bID] = list
			return nil
		}
	}
	return ErrChunkNotFound
}

func (m *memoryVanadisStore) ResetChunkForSynchronize(_ context.Context, chunkID, buildID string) error {
	now := time.Now().UTC()
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != chunkID {
				continue
			}
			list[i].CIFailCount = 0
			list[i].CIChecks = make(map[string]string)
			list[i].Banner = ""
			if list[i].Status == "failed" {
				list[i].Status = "pr_opened"
				list[i].CompletedAt = time.Time{}
			}
			list[i].UpdatedAt = now
			m.chunks[bID] = list
			if b, ok := m.builds[buildID]; ok {
				if b.Status == "failed" {
					b.Status = "running"
					b.UpdatedAt = now
					m.builds[buildID] = b
				}
			}
			return nil
		}
	}
	return ErrChunkNotFound
}

func (m *memoryVanadisStore) IncrementChunkCIFailCount(_ context.Context, chunkID, banner string) (int, error) {
	now := time.Now().UTC()
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != chunkID {
				continue
			}
			if list[i].Status == "failed" || list[i].Status == "completed" || list[i].Status == "merged" {
				return list[i].CIFailCount, nil
			}
			list[i].CIFailCount++
			list[i].Status = "ci_failed"
			list[i].Banner = banner
			list[i].UpdatedAt = now
			m.chunks[bID] = list
			return list[i].CIFailCount, nil
		}
	}
	return 0, ErrChunkNotFound
}

func (m *memoryVanadisStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	now := time.Now().UTC()
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID != chunkID {
				continue
			}
			if status != "" {
				list[i].Status = status
			}
			list[i].Banner = banner
			list[i].UpdatedAt = now
			m.chunks[bID] = list
			return nil
		}
	}
	return ErrChunkNotFound
}

type mockChunkBus struct {
	pubsub.Bus

	published     []ExecuteChunkArgs
	widgetCalls   int
	widgetBuildID string
	widgetBanner  string
	err           error
}

func (m *mockChunkBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	if m.err != nil {
		return m.err
	}
	if a, ok := data.(ExecuteChunkArgs); ok {
		m.published = append(m.published, a)
		return nil
	}
	if strings.HasPrefix(subject, "aerial.coalesced.widget_sync.build.") {
		m.widgetCalls++
		if a, ok := data.(pubsub.WidgetSyncJobArgs); ok {
			m.widgetBuildID = a.BuildID
			m.widgetBanner = a.Banner
		}
		return nil
	}
	if strings.HasPrefix(subject, "aerial.events.") {
		return nil
	}
	if m.Bus != nil {
		return m.Bus.Publish(ctx, subject, msgID, data)
	}
	return errors.New("mockChunkBus: unhandled publish subject")
}

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
