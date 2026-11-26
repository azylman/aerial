package db

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// Fixture row IDs and values referenced by several cases.
const (
	mPendA          = "m-pend-a"
	mPendB          = "m-pend-b"
	mProc           = "m-proc"
	mDone           = "m-done"
	mFail           = "m-fail"
	dKey            = "d-key"
	dProc           = "d-proc"
	dSess           = "d-sess"
	idMissing       = "missing"
	failReason      = "stop"
	b1Chunk2Session = "vanadis-b1-chunk-2"
	sess1           = "sess-1"
	oldErr          = "old"
)

var recordBase = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

// recordAt returns recordBase + offset seconds (created_at and updated_at of a fixture row).
func recordAt(offset int) time.Time { return recordBase.Add(time.Duration(offset) * time.Second) }

// recordMessages is the hand-derived message fixture shared by every record case.
var recordMessages = []Message{
	{ID: mPendA, ThreadID: "t1", Content: mPendA, Status: StatusPending, CreatedAt: recordAt(2), UpdatedAt: recordAt(2)},
	{ID: mPendB, ThreadID: "t1", Content: mPendB, Status: StatusPending, CreatedAt: recordAt(1), UpdatedAt: recordAt(1)},
	{ID: mProc, ThreadID: "t1", Content: mProc, Status: StatusProcessing, RetryCount: 1, CreatedAt: recordAt(3), UpdatedAt: recordAt(3)},
	{ID: mDone, ThreadID: "t1", Content: mDone, Status: StatusCompleted, RetryCount: 1, CreatedAt: recordAt(4), UpdatedAt: recordAt(4)},
	{ID: mFail, ThreadID: "t1", Content: mFail, Status: StatusFailed, ErrorMessage: oldErr, CreatedAt: recordAt(5), UpdatedAt: recordAt(5)},
	{ID: "m-other", ThreadID: "t2", Content: "m-other", Status: StatusPending, CreatedAt: recordAt(6), UpdatedAt: recordAt(6)},
}

// recordDispatches mirrors every message row as a dispatch (session mirror-<thread>) so the
// status-guard tables read the same IDs, then adds the session-key fixture rows.
var recordDispatches = []Dispatch{
	{ID: mPendA, Source: "test", SessionID: "mirror-t1", Prompt: mPendA, Status: StatusPending, CreatedAt: recordAt(2), UpdatedAt: recordAt(2)},
	{ID: mPendB, Source: "test", SessionID: "mirror-t1", Prompt: mPendB, Status: StatusPending, CreatedAt: recordAt(1), UpdatedAt: recordAt(1)},
	{ID: mProc, Source: "test", SessionID: "mirror-t1", Prompt: mProc, Status: StatusProcessing, RetryCount: 1, CreatedAt: recordAt(3), UpdatedAt: recordAt(3)},
	{ID: mDone, Source: "test", SessionID: "mirror-t1", Prompt: mDone, Status: StatusCompleted, RetryCount: 1, CreatedAt: recordAt(4), UpdatedAt: recordAt(4)},
	{ID: mFail, Source: "test", SessionID: "mirror-t1", Prompt: mFail, Status: StatusFailed, ErrorMessage: oldErr, CreatedAt: recordAt(5), UpdatedAt: recordAt(5)},
	{ID: "m-other", Source: "test", SessionID: "mirror-t2", Prompt: "m-other", Status: StatusPending, CreatedAt: recordAt(6), UpdatedAt: recordAt(6)},
	{ID: dSess, Source: "test", SessionID: sess1, Prompt: dSess, Status: StatusPending, CreatedAt: recordAt(3), UpdatedAt: recordAt(3)},
	{ID: "d-sess-early", Source: "test", SessionID: sess1, Prompt: "d-sess-early", Status: StatusPending, CreatedAt: recordAt(1), UpdatedAt: recordAt(1)},
	{ID: dProc, Source: "test", SessionID: sess1, Prompt: dProc, Status: StatusProcessing, RetryCount: 1, CreatedAt: recordAt(4), UpdatedAt: recordAt(4)},
	{ID: "d-done", Source: "test", SessionID: sess1, Prompt: "d-done", Status: StatusCompleted, ErrorMessage: oldErr, CreatedAt: recordAt(5), UpdatedAt: recordAt(5)},
	{ID: dKey, Source: "test", Prompt: dKey, Status: StatusPending, CreatedAt: recordAt(2), UpdatedAt: recordAt(2)},
	{ID: "d-chunkonly", Source: "test", SessionID: "other-s", VanadisChunkID: "c9", Prompt: "d-chunkonly", Status: StatusPending, CreatedAt: recordAt(6), UpdatedAt: recordAt(6)},
	{ID: "d-b1-chunk", Source: "test", SessionID: b1Chunk2Session, VanadisChunkID: "b1-chunk-2", Prompt: "d-b1-chunk", Status: StatusPending, CreatedAt: recordAt(7), UpdatedAt: recordAt(7)},
	{ID: "d-b1-build", Source: "test", SessionID: "vanadis-b1", Prompt: "d-b1-build", Status: StatusProcessing, CreatedAt: recordAt(8), UpdatedAt: recordAt(8)},
	{ID: "d-b1-chunkcol", Source: "test", SessionID: "custom-s", VanadisChunkID: "b1", Prompt: "d-b1-chunkcol", Status: StatusPending, CreatedAt: recordAt(9), UpdatedAt: recordAt(9)},
	{ID: "d-b1x", Source: "test", SessionID: "vanadis-b1x-chunk-1", VanadisChunkID: "b1x-chunk-1", Prompt: "d-b1x", Status: StatusPending, CreatedAt: recordAt(10), UpdatedAt: recordAt(10)},
	{ID: "d-wild", Source: "test", SessionID: "vanadis-bA1-chunk-1", VanadisChunkID: "bA1-chunk-1", Prompt: "d-wild", Status: StatusPending, CreatedAt: recordAt(11), UpdatedAt: recordAt(11)},
	{ID: "d-b1-done", Source: "test", SessionID: "vanadis-b1-chunk-3", VanadisChunkID: "b1-chunk-3", Prompt: "d-b1-done", Status: StatusFailed, CreatedAt: recordAt(12), UpdatedAt: recordAt(12)},
}

// newRecordStore opens a private database seeded with the record fixture,
// runs sabotageSQL and optionally closes the handle.
func newRecordStore(t *testing.T, sabotageSQL string, closeDB bool) *SQLStore {
	t.Helper()
	store, database := newTestStore(t)
	seedMessages(t, store, recordMessages)
	seedDispatches(t, store, recordDispatches)
	execSQL(t, database, sabotageSQL)
	if closeDB {
		if err := database.Close(); err != nil {
			t.Fatalf("close test database: %v", err)
		}
	}
	return store
}

// recordState is the observable (status, error, retry, restart) tuple of one row.
type recordState struct {
	status, errMsg string
	retry, restart int
}

func messageState(t *testing.T, s *SQLStore, id string) recordState {
	t.Helper()
	m, err := s.GetMessage(context.Background(), id)
	if err != nil || m == nil {
		t.Fatalf("GetMessage(%s): %v", id, err)
	}
	return recordState{m.Status, m.ErrorMessage, m.RetryCount, m.RestartCount}
}

func dispatchState(t *testing.T, s *SQLStore, id string) recordState {
	t.Helper()
	d, err := s.GetDispatch(context.Background(), id)
	if err != nil || d == nil {
		t.Fatalf("GetDispatch(%s): %v", id, err)
	}
	return recordState{d.Status, d.ErrorMessage, d.RetryCount, d.RestartCount}
}

func wantIDs(t *testing.T, op string, got, want []string) {
	t.Helper()
	if len(got) != len(want) || (len(want) > 0 && !slices.Equal(got, want)) {
		t.Errorf("%s = %v, want %v", op, got, want)
	}
}

func wantErrContaining(t *testing.T, op string, err error, want string) bool {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", op, err)
		}
		return false
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("%s error = %v, want containing %q", op, err, want)
	}
	return true
}

func TestGetMessagesByID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		ids          []string
		sabotageSQL  string
		closeDB      bool
		wantErr      string
		wantIDs      []string
		wantStatuses []string
	}{
		{name: "input order, missing and duplicate ids skipped", ids: []string{mDone, idMissing, mPendA, mDone, " "}, wantIDs: []string{mDone, mPendA}, wantStatuses: []string{StatusCompleted, StatusPending}},
		{name: "no ids", ids: nil},
		{name: "closed handle fails the query", ids: []string{mDone}, closeDB: true, wantErr: "failed to query messages by id"},
		{name: "dropped table fails the query", ids: []string{mDone}, sabotageSQL: `DROP TABLE messages`, wantErr: "failed to query messages by id"},
		{name: "unscannable row fails the scan", ids: []string{mDone}, sabotageSQL: `ALTER TABLE messages ALTER COLUMN created_at DROP NOT NULL; UPDATE messages SET created_at = NULL`, wantErr: "failed to scan messages by id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newRecordStore(t, tc.sabotageSQL, tc.closeDB)
			got, err := s.GetMessagesByID(context.Background(), tc.ids)
			if wantErrContaining(t, "GetMessagesByID", err, tc.wantErr) {
				return
			}
			var ids, statuses []string
			for _, m := range got {
				ids, statuses = append(ids, m.ID), append(statuses, m.Status)
			}
			wantIDs(t, "GetMessagesByID ids", ids, tc.wantIDs)
			wantIDs(t, "GetMessagesByID statuses", statuses, tc.wantStatuses)
		})
	}
}

// recordTransitionCase is one row of a guarded by-ID status transition table
// (Fail*IfActive, Fail*IfPending, Skip*IfActive, RequeueProcessingMessages).
type recordTransitionCase struct {
	name        string
	ids         []string
	calls       int
	sabotageSQL string
	wantErr     string
	wantChanged []string
	want        map[string]recordState
}

type recordTransition func(s *SQLStore, ctx context.Context, ids []string, reason string) ([]string, error)

// runRecordTransitions runs one transition method's table: seed, sabotage,
// call it (calls times, default once), then assert the IDs changed by the
// last call and every listed row's persisted state.
func runRecordTransitions(t *testing.T, transition recordTransition, reason string, state func(*testing.T, *SQLStore, string) recordState, tests []recordTransitionCase) {
	t.Helper()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newRecordStore(t, tc.sabotageSQL, false)
			var got []string
			var err error
			for range max(tc.calls, 1) {
				if got, err = transition(s, context.Background(), tc.ids, reason); err != nil {
					break
				}
			}
			if wantErrContaining(t, "transition", err, tc.wantErr) {
				return
			}
			wantIDs(t, "changed", got, tc.wantChanged)
			ids := make([]string, 0, len(tc.want))
			for id := range tc.want {
				ids = append(ids, id)
			}
			slices.Sort(ids)
			for _, id := range ids {
				if st := state(t, s, id); st != tc.want[id] {
					t.Errorf("row %s state = %+v, want %+v", id, st, tc.want[id])
				}
			}
		})
	}
}

// ifActiveCases is the row set for the transitions guarded on PENDING or
// PROCESSING (Fail*IfActive -> FAILED, Skip*IfActive -> COMPLETED); the
// mirrored fixture gives messages and dispatches the same IDs and states.
// sqlstate picks the sabotage class (retryable 40001 or terminal 23505).
func ifActiveCases(table, toStatus, sqlstate string) []recordTransitionCase {
	return []recordTransitionCase{
		{
			name: "moves pending and processing, leaves terminal rows", ids: []string{mProc, mPendA, mDone, mFail, idMissing},
			wantChanged: []string{mProc, mPendA},
			want: map[string]recordState{
				mProc: {toStatus, failReason, 1, 0}, mPendA: {toStatus, failReason, 0, 0},
				mDone: {StatusCompleted, "", 1, 0}, mFail: {StatusFailed, oldErr, 0, 0}, mPendB: {StatusPending, "", 0, 0},
			},
		},
		{name: "no ids", ids: []string{}, want: map[string]recordState{mPendA: {StatusPending, "", 0, 0}}},
		{name: "update failure surfaces", ids: []string{mPendA}, sabotageSQL: failTriggerSQL(table, "UPDATE", sqlstate), wantErr: "failed updating " + table + " by id"},
	}
}

// failIfPendingCases is the FailMessagesIfPending / FailDispatchesIfPending row set.
func failIfPendingCases(table string) []recordTransitionCase {
	return []recordTransitionCase{
		{
			name: "leaves processing rows untouched", ids: []string{mProc, mPendA, mDone},
			wantChanged: []string{mPendA},
			want: map[string]recordState{
				mProc: {StatusProcessing, "", 1, 0}, mPendA: {StatusFailed, failReason, 0, 0}, mDone: {StatusCompleted, "", 1, 0},
			},
		},
		{name: "retryable update failure surfaces", ids: []string{mPendA}, sabotageSQL: failTriggerSQL(table, "UPDATE", "08006"), wantErr: "failed updating " + table + " by id"},
	}
}

func skipIfPendingCases(table string) []recordTransitionCase {
	return []recordTransitionCase{
		{
			name: "leaves processing and terminal rows untouched", ids: []string{mProc, mPendA, mDone, mFail},
			wantChanged: []string{mPendA},
			want: map[string]recordState{
				mProc: {StatusProcessing, "", 1, 0}, mPendA: {StatusCompleted, failReason, 0, 0}, mDone: {StatusCompleted, "", 1, 0}, mFail: {StatusFailed, oldErr, 0, 0},
			},
		},
		{name: "retryable update failure surfaces", ids: []string{mPendA}, sabotageSQL: failTriggerSQL(table, "UPDATE", "08006"), wantErr: "failed updating " + table + " by id"},
	}
}

func TestFailMessagesIfActive(t *testing.T) {
	t.Parallel()
	runRecordTransitions(t, (*SQLStore).FailMessagesIfActive, failReason, messageState, ifActiveCases("messages", StatusFailed, "40001"))
}

func TestFailMessagesIfPending(t *testing.T) {
	t.Parallel()
	runRecordTransitions(t, (*SQLStore).FailMessagesIfPending, failReason, messageState, failIfPendingCases("messages"))
}

func TestSkipMessagesIfActive(t *testing.T) {
	t.Parallel()
	runRecordTransitions(t, (*SQLStore).SkipMessagesIfActive, failReason, messageState, ifActiveCases("messages", StatusCompleted, "23505"))
}

func TestSkipMessagesIfPending(t *testing.T) {
	t.Parallel()
	runRecordTransitions(t, (*SQLStore).SkipMessagesIfPending, failReason, messageState, skipIfPendingCases("messages"))
}

func TestFailDispatchesIfActive(t *testing.T) {
	t.Parallel()
	runRecordTransitions(t, (*SQLStore).FailDispatchesIfActive, failReason, dispatchState, ifActiveCases("dispatches", StatusFailed, "40001"))
}

func TestFailDispatchesIfPending(t *testing.T) {
	t.Parallel()
	runRecordTransitions(t, (*SQLStore).FailDispatchesIfPending, failReason, dispatchState, failIfPendingCases("dispatches"))
}

func TestSkipDispatchesIfActive(t *testing.T) {
	t.Parallel()
	runRecordTransitions(t, (*SQLStore).SkipDispatchesIfActive, failReason, dispatchState, ifActiveCases("dispatches", StatusCompleted, "23505"))
}

func TestRequeueProcessingMessages(t *testing.T) {
	t.Parallel()
	runRecordTransitions(t, (*SQLStore).RequeueProcessingMessages, "deferred", messageState, []recordTransitionCase{
		{
			name: "processing row requeued with retry bump, others untouched", ids: []string{mPendA, mProc, mDone}, calls: 1,
			wantChanged: []string{mProc},
			want: map[string]recordState{
				mProc: {StatusPending, "deferred", 2, 0}, mPendA: {StatusPending, "", 0, 0}, mDone: {StatusCompleted, "", 1, 0},
			},
		},
		{
			name: "replayed call changes nothing and keeps retry count", ids: []string{mProc}, calls: 2,
			wantChanged: nil,
			want:        map[string]recordState{mProc: {StatusPending, "deferred", 2, 0}},
		},
		{name: "update failure surfaces", ids: []string{mProc}, sabotageSQL: failTriggerSQL("messages", "UPDATE", "40001"), wantErr: "failed updating messages by id"},
	})
}

// beginAttemptCase is one row of a BeginMessageAttempt/BeginDispatchAttempt
// table: ordinals run sequentially, then concurrent ordinals run at once.
type beginAttemptCase struct {
	name           string
	id             string
	ordinals       []int
	concurrent     []int
	sabotageSQL    string
	want           AttemptStart
	wantConcurrent []AttemptStart
	wantNotFound   bool
	wantErr        string
}

type beginAttempt func(s *SQLStore, ctx context.Context, id string, ordinal int) (AttemptStart, error)

// runConcurrentAttempts calls begin once per ordinal at the same time and
// returns the results ordered by restart count.
func runConcurrentAttempts(t *testing.T, s *SQLStore, begin beginAttempt, id string, ordinals []int) []AttemptStart {
	t.Helper()
	results := make([]AttemptStart, len(ordinals))
	errs := make([]error, len(ordinals))
	var wg sync.WaitGroup
	for i, ord := range ordinals {
		wg.Go(func() { results[i], errs[i] = begin(s, context.Background(), id, ord) })
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatalf("concurrent BeginAttempt: %v", err)
	}
	slices.SortFunc(results, func(a, b AttemptStart) int { return a.RestartCount - b.RestartCount })
	return results
}

func runBeginAttempts(t *testing.T, begin beginAttempt, state func(*testing.T, *SQLStore, string) recordState, tests []beginAttemptCase) {
	t.Helper()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newRecordStore(t, tc.sabotageSQL, false)
			var got AttemptStart
			var err error
			for _, ord := range tc.ordinals {
				if got, err = begin(s, context.Background(), tc.id, ord); err != nil {
					break
				}
			}
			if tc.wantNotFound && !errors.Is(err, ErrRecordNotFound) {
				t.Fatalf("BeginAttempt err = %v, want ErrRecordNotFound", err)
			}
			if wantErrContaining(t, "BeginAttempt", err, tc.wantErr) {
				return
			}
			if len(tc.concurrent) > 0 {
				if results := runConcurrentAttempts(t, s, begin, tc.id, tc.concurrent); !slices.Equal(results, tc.wantConcurrent) {
					t.Errorf("concurrent BeginAttempt = %+v, want %+v", results, tc.wantConcurrent)
				}
			} else if got != tc.want {
				t.Errorf("BeginAttempt = %+v, want %+v", got, tc.want)
			}
			if st := state(t, s, tc.id); st.retry != tc.want.RetryCount || st.restart != tc.want.RestartCount {
				t.Errorf("persisted retry/restart = %d/%d, want %d/%d", st.retry, st.restart, tc.want.RetryCount, tc.want.RestartCount)
			}
		})
	}
}

// beginAttemptCases is the BeginMessageAttempt / BeginDispatchAttempt row set:
// pendingID has retry_count 0, processingID has retry_count 1 in the fixture.
func beginAttemptCases(table, pendingID, processingID string) []beginAttemptCase {
	return []beginAttemptCase{
		{name: "fresh row ordinals 1,2,3", id: pendingID, ordinals: []int{1, 2, 3}, want: AttemptStart{RetryCount: 3}},
		{name: "repeated ordinal is crash re-entry", id: pendingID, ordinals: []int{1, 1}, want: AttemptStart{Reentry: true, RetryCount: 1, RestartCount: 1}},
		{name: "persisted retry count above ordinal is re-entry", id: processingID, ordinals: []int{1}, want: AttemptStart{Reentry: true, RetryCount: 1, RestartCount: 1}},
		{name: "next ordinal after persisted retries starts fresh", id: processingID, ordinals: []int{3}, want: AttemptStart{RetryCount: 3}},
		{
			name: "two concurrent attempts with one ordinal: one starts, one re-enters", id: pendingID, concurrent: []int{1, 1},
			want:           AttemptStart{Reentry: true, RetryCount: 1, RestartCount: 1},
			wantConcurrent: []AttemptStart{{RetryCount: 1}, {Reentry: true, RetryCount: 1, RestartCount: 1}},
		},
		{name: "missing row", id: idMissing, ordinals: []int{1}, wantNotFound: true, wantErr: "record not found"},
		{name: "zero ordinal rejected", id: pendingID, ordinals: []int{0}, wantErr: "attempt ordinal must be >= 1"},
		{name: "empty id rejected", id: " ", ordinals: []int{1}, wantErr: "id cannot be empty"},
		{name: "fresh attempt update failure", id: pendingID, ordinals: []int{1}, sabotageSQL: failTriggerSQL(table, "UPDATE", "40001"), wantErr: "failed beginning attempt 1"},
		{name: "re-entry update failure", id: processingID, ordinals: []int{1}, sabotageSQL: failTriggerSQL(table, "UPDATE", "40001"), wantErr: "failed re-entering attempt 1"},
	}
}

func TestBeginMessageAttempt(t *testing.T) {
	t.Parallel()
	runBeginAttempts(t, (*SQLStore).BeginMessageAttempt, messageState, beginAttemptCases("messages", mPendA, mProc))
}

func TestBeginDispatchAttempt(t *testing.T) {
	t.Parallel()
	runBeginAttempts(t, (*SQLStore).BeginDispatchAttempt, dispatchState, beginAttemptCases("dispatches", dSess, dProc))
}

func TestListActiveDispatchSessionKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, dispatchID, buildID string
		closeDB                   bool
		wantErr                   string
		want                      []string
	}{
		{name: "dispatch id", dispatchID: dProc, want: []string{sess1}},
		{name: "build id matches chunk column, chunk prefix, build session; sorted, terminal excluded", buildID: "b1", want: []string{"custom-s", "vanadis-b1", b1Chunk2Session}},
		{name: "dispatch and build combined", dispatchID: dSess, buildID: "c9", want: []string{"other-s", sess1}},
		{name: "like wildcards in build id match literally", buildID: "b_1", want: nil},
		{name: "terminal dispatch id", dispatchID: "d-done", want: nil},
		{name: "empty session id excluded", dispatchID: dKey, want: nil},
		{name: "nothing requested", want: nil},
		{name: "closed handle fails the query", dispatchID: dProc, closeDB: true, wantErr: "database is closed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newRecordStore(t, "", tc.closeDB)
			got, err := s.ListActiveDispatchSessionKeys(context.Background(), tc.dispatchID, tc.buildID)
			if wantErrContaining(t, "ListActiveDispatchSessionKeys", err, tc.wantErr) {
				return
			}
			wantIDs(t, "ListActiveDispatchSessionKeys", got, tc.want)
		})
	}
}
