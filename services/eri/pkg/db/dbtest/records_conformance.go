package dbtest

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
)

// Fixture row IDs and values referenced by several cases.
const (
	mPendA          = "m-pend-a"
	mPendB          = "m-pend-b"
	mProc           = "m-proc"
	mDone           = "m-done"
	mFail           = "m-fail"
	mOther          = "m-other"
	dKey            = "d-key"
	dProc           = "d-proc"
	dSess           = "d-sess"
	idMissing       = "missing"
	failReason      = "stop"
	b1Chunk2Session = "vanadis-b1-chunk-2"
	sess1           = "sess-1"
	oldErr          = "old"
)

// RecordStoreFactory returns a fresh, empty store for one conformance case.
type RecordStoreFactory func(t *testing.T) db.Store

var recordBase = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

type recordRow struct {
	id, thread, session, chunk, status, errMsg string
	retry, restart, offset                     int
}

// Hand-derived fixture shared by every conformance case: created_at = recordBase + offset seconds.
var recordMessages = []recordRow{
	{id: mPendA, thread: "t1", status: db.StatusPending, offset: 2},
	{id: mPendB, thread: "t1", status: db.StatusPending, offset: 1},
	{id: mProc, thread: "t1", status: db.StatusProcessing, retry: 1, offset: 3},
	{id: mDone, thread: "t1", status: db.StatusCompleted, retry: 1, offset: 4},
	{id: mFail, thread: "t1", status: db.StatusFailed, errMsg: oldErr, offset: 5},
	{id: mOther, thread: "t2", status: db.StatusPending, offset: 6},
}

var recordDispatches = []recordRow{
	{id: dSess, session: sess1, status: db.StatusPending, offset: 3},
	{id: "d-sess-early", session: sess1, status: db.StatusPending, offset: 1},
	{id: dProc, session: sess1, status: db.StatusProcessing, retry: 1, offset: 4},
	{id: "d-done", session: sess1, status: db.StatusCompleted, errMsg: oldErr, offset: 5},
	{id: dKey, status: db.StatusPending, offset: 2},
	{id: "d-chunkonly", session: "other-s", chunk: "c9", status: db.StatusPending, offset: 6},
	{id: "d-b1-chunk", session: b1Chunk2Session, chunk: "b1-chunk-2", status: db.StatusPending, offset: 7},
	{id: "d-b1-build", session: "vanadis-b1", status: db.StatusProcessing, offset: 8},
	{id: "d-b1-chunkcol", session: "custom-s", chunk: "b1", status: db.StatusPending, offset: 9},
	{id: "d-b1x", session: "vanadis-b1x-chunk-1", chunk: "b1x-chunk-1", status: db.StatusPending, offset: 10},
	{id: "d-wild", session: "vanadis-bA1-chunk-1", chunk: "bA1-chunk-1", status: db.StatusPending, offset: 11},
	{id: "d-b1-done", session: "vanadis-b1-chunk-3", chunk: "b1-chunk-3", status: db.StatusFailed, offset: 12},
}

func seedRecords(t *testing.T, s db.Store) {
	t.Helper()
	ctx := context.Background()
	for _, r := range recordMessages {
		ts := recordBase.Add(time.Duration(r.offset) * time.Second)
		if err := s.InsertMessage(ctx, db.Message{ID: r.id, ThreadID: r.thread, Content: r.id, Status: r.status, ErrorMessage: r.errMsg, RetryCount: r.retry, RestartCount: r.restart, CreatedAt: ts, UpdatedAt: ts}); err != nil {
			t.Fatalf("seed message %s: %v", r.id, err)
		}
	}
	// Every message row is mirrored as a dispatch (session mirror-<thread>) so status-guard cases share one table.
	mirrors := make([]recordRow, 0, len(recordMessages))
	for _, r := range recordMessages {
		r.session = "mirror-" + r.thread
		mirrors = append(mirrors, r)
	}
	for _, r := range append(mirrors, recordDispatches...) {
		ts := recordBase.Add(time.Duration(r.offset) * time.Second)
		if _, err := s.InsertDispatch(ctx, db.Dispatch{ID: r.id, Source: "test", SessionID: r.session, VanadisChunkID: r.chunk, Prompt: r.id, Status: r.status, ErrorMessage: r.errMsg, RetryCount: r.retry, RestartCount: r.restart, CreatedAt: ts, UpdatedAt: ts}); err != nil {
			t.Fatalf("seed dispatch %s: %v", r.id, err)
		}
	}
}

func newSeeded(t *testing.T, factory RecordStoreFactory) db.Store {
	t.Helper()
	s := factory(t)
	seedRecords(t, s)
	return s
}

func wantIDs(t *testing.T, op string, got, want []string) {
	t.Helper()
	if len(got) != len(want) || (len(want) > 0 && !slices.Equal(got, want)) {
		t.Errorf("%s = %v, want %v", op, got, want)
	}
}

// recordState is the observable (status, error, retry, restart) tuple of one row.
type recordState struct {
	status, errMsg string
	retry, restart int
}

func messageState(t *testing.T, s db.Store, id string) recordState {
	t.Helper()
	m, err := s.GetMessage(context.Background(), id)
	if err != nil || m == nil {
		t.Fatalf("GetMessage(%s): %v", id, err)
	}
	return recordState{m.Status, m.ErrorMessage, m.RetryCount, m.RestartCount}
}

func dispatchState(t *testing.T, s db.Store, id string) recordState {
	t.Helper()
	d, err := s.GetDispatch(context.Background(), id)
	if err != nil || d == nil {
		t.Fatalf("GetDispatch(%s): %v", id, err)
	}
	return recordState{d.Status, d.ErrorMessage, d.RetryCount, d.RestartCount}
}

func wantStates(t *testing.T, s db.Store, get func(*testing.T, db.Store, string) recordState, want map[string]recordState) {
	t.Helper()
	ids := make([]string, 0, len(want))
	for id := range want {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if got := get(t, s, id); got != want[id] {
			t.Errorf("row %s state = %+v, want %+v", id, got, want[id])
		}
	}
}

// RunRecordStoreConformance exercises the by-ID queue record methods against a store implementation.
func RunRecordStoreConformance(t *testing.T, factory RecordStoreFactory) {
	t.Helper()
	t.Run("ListPendingMessageIDsForThread", func(t *testing.T) { testListPendingMessageIDs(t, factory) })
	t.Run("GetMessagesByID", func(t *testing.T) { testGetMessagesByID(t, factory) })
	t.Run("FailMessages", func(t *testing.T) { testFailRecords(t, factory, false) })
	t.Run("FailDispatches", func(t *testing.T) { testFailRecords(t, factory, true) })
	t.Run("RequeueProcessingMessages", func(t *testing.T) { testRequeueProcessingMessages(t, factory) })
	t.Run("BeginMessageAttempt", func(t *testing.T) { testBeginAttempt(t, factory, false) })
	t.Run("BeginDispatchAttempt", func(t *testing.T) { testBeginAttempt(t, factory, true) })
	t.Run("ListPendingDispatchIDsForSession", func(t *testing.T) { testListPendingDispatchIDs(t, factory) })
	t.Run("ListActiveDispatchSessionKeys", func(t *testing.T) { testListActiveDispatchSessionKeys(t, factory) })
}

func testListPendingMessageIDs(t *testing.T, factory RecordStoreFactory) {
	t.Helper()
	t.Parallel()
	s := newSeeded(t, factory)
	tests := []struct {
		name, thread string
		want         []string
	}{
		{"pending rows oldest first, active and terminal rows excluded", "t1", []string{mPendB, mPendA}},
		{"other thread", "t2", []string{mOther}},
		{"padded thread id", "  t2 ", []string{mOther}},
		{"unknown thread", "t-none", nil},
		{"empty thread", "", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.ListPendingMessageIDsForThread(context.Background(), tc.thread)
			if err != nil {
				t.Fatalf("ListPendingMessageIDsForThread: %v", err)
			}
			wantIDs(t, "ListPendingMessageIDsForThread", got, tc.want)
		})
	}
}

func testGetMessagesByID(t *testing.T, factory RecordStoreFactory) {
	t.Helper()
	t.Parallel()
	s := newSeeded(t, factory)
	tests := []struct {
		name         string
		ids          []string
		wantIDs      []string
		wantStatuses []string
	}{
		{"input order, missing and duplicate ids skipped", []string{mDone, idMissing, mPendA, mDone, " "}, []string{mDone, mPendA}, []string{db.StatusCompleted, db.StatusPending}},
		{"no ids", nil, nil, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.GetMessagesByID(context.Background(), tc.ids)
			if err != nil {
				t.Fatalf("GetMessagesByID: %v", err)
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

func testFailRecords(t *testing.T, factory RecordStoreFactory, dispatch bool) {
	t.Helper()
	t.Parallel()
	tests := []struct {
		name        string
		pendingOnly bool
		ids         []string
		wantChanged []string
		want        map[string]recordState
	}{
		{
			name: "if active fails pending and processing, leaves terminal rows", ids: []string{mProc, mPendA, mDone, mFail, idMissing},
			wantChanged: []string{mProc, mPendA},
			want: map[string]recordState{
				mProc: {db.StatusFailed, failReason, 1, 0}, mPendA: {db.StatusFailed, failReason, 0, 0},
				mDone: {db.StatusCompleted, "", 1, 0}, mFail: {db.StatusFailed, oldErr, 0, 0}, mPendB: {db.StatusPending, "", 0, 0},
			},
		},
		{
			name: "if pending leaves processing rows untouched", pendingOnly: true, ids: []string{mProc, mPendA, mDone},
			wantChanged: []string{mPendA},
			want: map[string]recordState{
				mProc: {db.StatusProcessing, "", 1, 0}, mPendA: {db.StatusFailed, failReason, 0, 0}, mDone: {db.StatusCompleted, "", 1, 0},
			},
		},
		{name: "no ids", ids: []string{}, wantChanged: nil, want: map[string]recordState{mPendA: {db.StatusPending, "", 0, 0}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newSeeded(t, factory)
			ops := map[[2]bool]func(context.Context, []string, string) ([]string, error){
				{false, false}: s.FailMessagesIfActive, {false, true}: s.FailMessagesIfPending,
				{true, false}: s.FailDispatchesIfActive, {true, true}: s.FailDispatchesIfPending,
			}
			get := messageState
			if dispatch {
				get = dispatchState
			}
			got, err := ops[[2]bool{dispatch, tc.pendingOnly}](context.Background(), tc.ids, failReason)
			if err != nil {
				t.Fatalf("Fail records: %v", err)
			}
			wantIDs(t, "Fail records changed", got, tc.wantChanged)
			wantStates(t, s, get, tc.want)
		})
	}
}

func testRequeueProcessingMessages(t *testing.T, factory RecordStoreFactory) {
	t.Helper()
	t.Parallel()
	tests := []struct {
		name        string
		ids         []string
		calls       int
		wantChanged []string
		want        map[string]recordState
	}{
		{
			name: "processing row requeued with retry bump, others untouched", ids: []string{mPendA, mProc, mDone}, calls: 1,
			wantChanged: []string{mProc},
			want: map[string]recordState{
				mProc: {db.StatusPending, "deferred", 2, 0}, mPendA: {db.StatusPending, "", 0, 0}, mDone: {db.StatusCompleted, "", 1, 0},
			},
		},
		{
			name: "replayed call changes nothing and keeps retry count", ids: []string{mProc}, calls: 2,
			wantChanged: nil,
			want:        map[string]recordState{mProc: {db.StatusPending, "deferred", 2, 0}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newSeeded(t, factory)
			var got []string
			for range tc.calls {
				var err error
				if got, err = s.RequeueProcessingMessages(context.Background(), tc.ids, "deferred"); err != nil {
					t.Fatalf("RequeueProcessingMessages: %v", err)
				}
			}
			wantIDs(t, "RequeueProcessingMessages changed (last call)", got, tc.wantChanged)
			wantStates(t, s, messageState, tc.want)
		})
	}
}

func testBeginAttempt(t *testing.T, factory RecordStoreFactory, dispatch bool) {
	t.Helper()
	t.Parallel()
	tests := []struct {
		name, msgID, dispID string
		ordinals            []int
		want                db.AttemptStart
		wantNotFound        bool
		wantErr             bool
	}{
		{name: "fresh row ordinals 1,2,3", msgID: mPendA, dispID: dSess, ordinals: []int{1, 2, 3}, want: db.AttemptStart{RetryCount: 3}},
		{name: "repeated ordinal is crash re-entry", msgID: mPendA, dispID: dSess, ordinals: []int{1, 1}, want: db.AttemptStart{Reentry: true, RetryCount: 1, RestartCount: 1}},
		{name: "persisted retry count above ordinal is re-entry", msgID: mProc, dispID: dProc, ordinals: []int{1}, want: db.AttemptStart{Reentry: true, RetryCount: 1, RestartCount: 1}},
		{name: "next ordinal after persisted retries starts fresh", msgID: mProc, dispID: dProc, ordinals: []int{3}, want: db.AttemptStart{RetryCount: 3}},
		{name: "missing row", msgID: idMissing, dispID: idMissing, ordinals: []int{1}, wantNotFound: true, wantErr: true},
		{name: "zero ordinal rejected", msgID: mPendA, dispID: dSess, ordinals: []int{0}, wantErr: true},
		{name: "empty id rejected", msgID: " ", dispID: " ", ordinals: []int{1}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newSeeded(t, factory)
			op, id, get := s.BeginMessageAttempt, tc.msgID, messageState
			if dispatch {
				op, id, get = s.BeginDispatchAttempt, tc.dispID, dispatchState
			}
			var got db.AttemptStart
			var err error
			for _, ord := range tc.ordinals {
				if got, err = op(context.Background(), id, ord); err != nil {
					break
				}
			}
			if (err != nil) != tc.wantErr || (tc.wantNotFound && !errors.Is(err, db.ErrRecordNotFound)) {
				t.Fatalf("BeginAttempt err = %v, wantErr %v notFound %v", err, tc.wantErr, tc.wantNotFound)
			}
			if tc.wantErr {
				return
			}
			if got != tc.want {
				t.Errorf("BeginAttempt = %+v, want %+v", got, tc.want)
			}
			if st := get(t, s, id); st.retry != tc.want.RetryCount || st.restart != tc.want.RestartCount {
				t.Errorf("persisted retry/restart = %d/%d, want %d/%d", st.retry, st.restart, tc.want.RetryCount, tc.want.RestartCount)
			}
		})
	}
}

func testListPendingDispatchIDs(t *testing.T, factory RecordStoreFactory) {
	t.Helper()
	t.Parallel()
	s := newSeeded(t, factory)
	tests := []struct {
		name, key string
		want      []string
	}{
		{"session id oldest first, processing and terminal excluded", sess1, []string{"d-sess-early", dSess}},
		{"vanadis chunk key form", "vanadis-c9", []string{"d-chunkonly"}},
		{"session equal to vanadis chunk form", b1Chunk2Session, []string{"d-b1-chunk"}},
		{"dispatch id key form", "dispatch-d-key", []string{dKey}},
		{"unknown key", "nope", nil},
		{"empty key", " ", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.ListPendingDispatchIDsForSession(context.Background(), tc.key)
			if err != nil {
				t.Fatalf("ListPendingDispatchIDsForSession: %v", err)
			}
			wantIDs(t, "ListPendingDispatchIDsForSession", got, tc.want)
		})
	}
}

func testListActiveDispatchSessionKeys(t *testing.T, factory RecordStoreFactory) {
	t.Helper()
	t.Parallel()
	s := newSeeded(t, factory)
	tests := []struct {
		name, dispatchID, buildID string
		want                      []string
	}{
		{"dispatch id", dProc, "", []string{sess1}},
		{"build id matches chunk column, chunk prefix, build session; sorted, terminal excluded", "", "b1", []string{"custom-s", "vanadis-b1", b1Chunk2Session}},
		{"dispatch and build combined", dSess, "c9", []string{"other-s", sess1}},
		{"like wildcards in build id match literally", "", "b_1", nil},
		{"terminal dispatch id", "d-done", "", nil},
		{"empty session id excluded", dKey, "", nil},
		{"nothing requested", "", "", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.ListActiveDispatchSessionKeys(context.Background(), tc.dispatchID, tc.buildID)
			if err != nil {
				t.Fatalf("ListActiveDispatchSessionKeys: %v", err)
			}
			wantIDs(t, "ListActiveDispatchSessionKeys", got, tc.want)
		})
	}
}
