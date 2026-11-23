package store_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/store"
	"github.com/azylman/aerial/services/prospera/internal/testutil"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}

// seedEndedCall inserts a finished call that FK columns
// (callback_of_call_id, related_call_id) can reference.
func seedEndedCall(t *testing.T, db *sql.DB, callID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO voice_calls (call_id, direction, state, from_number, to_number, ended_at)
		VALUES ($1, 'outbound', 'completed', '+15105550100', '+15105550198', NOW())`, callID); err != nil {
		t.Fatalf("seed call %s: %v", callID, err)
	}
}

func TestPGStore_CallLifecycle(t *testing.T) {
	t.Parallel()
	db := testutil.OpenTestDB(t)
	seedEndedCall(t, db, "call-orig")
	st := store.NewPGStore(db)
	ctx := context.Background()

	testLifecycleCreateAndFetch(ctx, t, st)
	testLifecycleBindProviderSID(ctx, t, st)
	testLifecycleUpdateStateAndTurns(ctx, t, st)
	testLifecycleInstructionsAndCompletion(ctx, t, st)
}

func testLifecycleCreateAndFetch(ctx context.Context, t *testing.T, st *store.PGStore) {
	t.Helper()
	now := time.Now().UTC()
	answered := now
	ended := now

	call := model.VoiceCall{
		CallID:           "call-123",
		Direction:        "outbound",
		State:            "initiated",
		EndReason:        "user_hangup",
		FromNumber:       "+15105550100",
		ToNumber:         "+15105550199",
		CallbackOfCallID: "call-orig",
		DiscordChannelID: "prospera",
		DiscordThreadID:  "thread-123",
		Brief: model.CallBrief{
			Task:      "Test errand",
			Approvals: "Zero spend",
		},
		OwnerInstructions: []model.OwnerInstruction{{Instruction: "start", Mode: "guidance", CreatedAt: now}},
		Transcript:        []model.TranscriptTurn{{Role: "system", Text: "init", Timestamp: now}},
		ReportStatus:      "pending",
		ReportSummary:     "summary-initial",
		StartedAt:         now,
		AnsweredAt:        &answered,
		EndedAt:           &ended,
		UpdatedAt:         now,
	}

	if err := st.CreateOutboundCall(ctx, call, 5); err != nil {
		t.Fatalf("CreateOutboundCall() error = %v", err)
	}

	fetched, err := st.GetCall(ctx, "call-123")
	if err != nil {
		t.Fatalf("GetCall() error = %v", err)
	}
	if fetched.CallID != "call-123" || fetched.State != "initiated" || fetched.Brief.Task != "Test errand" {
		t.Errorf("GetCall() returned unexpected call: %+v", fetched)
	}
	if fetched.CallbackOfCallID != "call-orig" || fetched.DiscordThreadID != "thread-123" {
		t.Errorf("expected optional fields populated: %+v", fetched)
	}

	// Non-existent call
	_, err = st.GetCall(ctx, "non-existent")
	if !errors.Is(err, store.ErrCallNotFound) {
		t.Errorf("GetCall(non-existent) err = %v, want ErrCallNotFound", err)
	}
}

func testLifecycleBindProviderSID(ctx context.Context, t *testing.T, st *store.PGStore) {
	t.Helper()
	if err := st.BindProviderCallSID(ctx, "call-123", "CA111222333"); err != nil {
		t.Fatalf("BindProviderCallSID() error = %v", err)
	}
	if err := st.BindProviderCallSID(ctx, "non-existent", "CA999"); !errors.Is(err, store.ErrCallNotFound) {
		t.Errorf("BindProviderCallSID(non-existent) err = %v, want ErrCallNotFound", err)
	}

	fetchedBySID, err := st.GetCallByProviderSID(ctx, "CA111222333")
	if err != nil {
		t.Fatalf("GetCallByProviderSID() error = %v", err)
	}
	if fetchedBySID.CallID != "call-123" {
		t.Errorf("GetCallByProviderSID() CallID = %q, want call-123", fetchedBySID.CallID)
	}

	_, err = st.GetCallByProviderSID(ctx, "CA_nonexistent")
	if !errors.Is(err, store.ErrCallNotFound) {
		t.Errorf("GetCallByProviderSID(non-existent) err = %v, want ErrCallNotFound", err)
	}
}

func testLifecycleUpdateStateAndTurns(ctx context.Context, t *testing.T, st *store.PGStore) {
	t.Helper()
	if err := st.UpdateCallState(ctx, "call-123", "ringing", "", ""); err != nil {
		t.Fatalf("UpdateCallState(ringing) error = %v", err)
	}

	if err := st.UpdateCallState(ctx, "call-123", "in-progress", "", "human"); err != nil {
		t.Fatalf("UpdateCallState(in-progress) error = %v", err)
	}
	updatedCall, _ := st.GetCall(ctx, "call-123")
	if updatedCall.State != "in-progress" || updatedCall.AnsweredBy != "human" || updatedCall.AnsweredAt == nil {
		t.Errorf("expected in-progress call with answered_by=human, got: %+v", updatedCall)
	}

	turn1 := model.TranscriptTurn{Role: "model", Text: "Hello, this is Prospera", Timestamp: time.Now().UTC()}
	turn2 := model.TranscriptTurn{Role: "user", Text: "Hi, how can I help?", Timestamp: time.Now().UTC()}
	if err := st.AppendTranscriptTurn(ctx, "call-123", turn1); err != nil {
		t.Fatalf("AppendTranscriptTurn(turn1) error = %v", err)
	}
	if err := st.AppendTranscriptTurn(ctx, "call-123", turn2); err != nil {
		t.Fatalf("AppendTranscriptTurn(turn2) error = %v", err)
	}
	if err := st.AppendTranscriptTurn(ctx, "non-existent", turn1); !errors.Is(err, store.ErrCallNotFound) {
		t.Errorf("AppendTranscriptTurn(non-existent) err = %v, want ErrCallNotFound", err)
	}

	withTurns, _ := st.GetCall(ctx, "call-123")
	if len(withTurns.Transcript) < 2 {
		t.Errorf("unexpected transcript turns: %+v", withTurns.Transcript)
	}

	advanced, err := st.AdvanceLiveTranscriptCursor(ctx, "call-123", 0, 2)
	if err != nil || !advanced {
		t.Fatalf("AdvanceLiveTranscriptCursor(0 -> 2) err = %v, advanced = %v", err, advanced)
	}
	staleAdvance, err := st.AdvanceLiveTranscriptCursor(ctx, "call-123", 0, 3)
	if err != nil || staleAdvance {
		t.Fatalf("stale AdvanceLiveTranscriptCursor should return false, got advanced=%v, err=%v", staleAdvance, err)
	}
}

func testLifecycleInstructionsAndCompletion(ctx context.Context, t *testing.T, st *store.PGStore) {
	t.Helper()
	instr := model.OwnerInstruction{Instruction: "Ask about reservations", Mode: "guidance", CreatedAt: time.Now().UTC()}
	if err := st.AppendOwnerInstruction(ctx, "call-123", instr); err != nil {
		t.Fatalf("AppendOwnerInstruction() error = %v", err)
	}
	if err := st.AppendOwnerInstruction(ctx, "non-existent", instr); !errors.Is(err, store.ErrCallNotFound) {
		t.Errorf("AppendOwnerInstruction(non-existent) err = %v, want ErrCallNotFound", err)
	}

	if err := st.SetVoicemailRecording(ctx, "call-123", "https://api.twilio.com/recordings/RE123"); err != nil {
		t.Fatalf("SetVoicemailRecording() error = %v", err)
	}
	if err := st.SetVoicemailRecording(ctx, "non-existent", "https://url"); !errors.Is(err, store.ErrCallNotFound) {
		t.Errorf("SetVoicemailRecording(non-existent) err = %v, want ErrCallNotFound", err)
	}

	if err := st.UpdateCallState(ctx, "call-123", "failed", "network_error", ""); err != nil {
		t.Fatalf("UpdateCallState(failed) error = %v", err)
	}

	if err := st.UpdateCallState(ctx, "call-123", "completed", "completed_normally", ""); err != nil {
		t.Fatalf("UpdateCallState(completed) error = %v", err)
	}
	if err := st.UpdateCallState(ctx, "non-existent", "completed", "", ""); !errors.Is(err, store.ErrCallNotFound) {
		t.Errorf("UpdateCallState(non-existent) err = %v, want ErrCallNotFound", err)
	}

	endedCall, _ := st.GetCall(ctx, "call-123")
	if endedCall.State != "completed" || endedCall.EndedAt == nil || endedCall.VoicemailRecordingURL != "https://api.twilio.com/recordings/RE123" {
		t.Errorf("unexpected ended call: %+v", endedCall)
	}

	list, err := st.ListCalls(ctx, 0)
	if err != nil {
		t.Fatalf("ListCalls() error = %v", err)
	}
	if len(list) != 2 {
		t.Errorf("len(list) = %d, want 2 (call-123 and its seeded call-orig)", len(list))
	}
}

func TestPGStore_DefaultTimestampsAndEmptySlices(t *testing.T) {
	t.Parallel()
	st := testutil.SetUpTestStore(t)
	ctx := context.Background()

	call := model.VoiceCall{
		CallID:     "call-zero-times",
		Direction:  "inbound",
		State:      "ringing",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
	}

	if err := st.CreateInboundCall(ctx, call); err != nil {
		t.Fatalf("CreateInboundCall() error = %v", err)
	}

	fetched, err := st.GetCall(ctx, "call-zero-times")
	if err != nil {
		t.Fatalf("GetCall() error = %v", err)
	}
	if fetched.StartedAt.IsZero() || fetched.UpdatedAt.IsZero() {
		t.Errorf("expected non-zero default timestamps: %+v", fetched)
	}
	if fetched.ReportStatus != "pending" {
		t.Errorf("expected default report_status='pending', got %q", fetched.ReportStatus)
	}
}

func TestPGStore_ConcurrencyAndDuplicates(t *testing.T) {
	t.Parallel()
	st := testutil.SetUpTestStore(t)
	ctx := context.Background()

	call1 := model.VoiceCall{
		CallID:     "call-out-1",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550101",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := st.CreateOutboundCall(ctx, call1, 1); err != nil {
		t.Fatalf("first CreateOutboundCall failed: %v", err)
	}

	call2 := model.VoiceCall{
		CallID:     "call-out-2",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550102",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := st.CreateOutboundCall(ctx, call2, 1); !errors.Is(err, store.ErrMaxConcurrentCalls) {
		t.Errorf("CreateOutboundCall over limit err = %v, want ErrMaxConcurrentCalls", err)
	}

	callDup := model.VoiceCall{
		CallID:     "call-out-dup",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550101",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := st.CreateOutboundCall(ctx, callDup, 10); !errors.Is(err, store.ErrDuplicateActiveDest) {
		t.Errorf("CreateOutboundCall duplicate dest err = %v, want ErrDuplicateActiveDest", err)
	}

	inbound := model.VoiceCall{
		CallID:     "call-in-1",
		Direction:  "inbound",
		State:      "ringing",
		FromNumber: "+15105550999",
		ToNumber:   "+15105550100",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := st.CreateInboundCall(ctx, inbound); err != nil {
		t.Fatalf("CreateInboundCall failed: %v", err)
	}

	if err := st.UpdateCallState(ctx, "call-out-1", "completed", "done", ""); err != nil {
		t.Fatalf("UpdateCallState failed: %v", err)
	}
	callNew := model.VoiceCall{
		CallID:     "call-out-3",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550101",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := st.CreateOutboundCall(ctx, callNew, 5); err != nil {
		t.Fatalf("CreateOutboundCall after completion failed: %v", err)
	}
}

func TestPGStore_FindRecentOutboundCall(t *testing.T) {
	t.Parallel()
	st := testutil.SetUpTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()

	c1 := model.VoiceCall{
		CallID:     "recent-call",
		Direction:  "outbound",
		State:      "completed",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550111",
		StartedAt:  now.Add(-48 * time.Hour),
		UpdatedAt:  now.Add(-48 * time.Hour),
	}
	c2 := model.VoiceCall{
		CallID:     "old-call",
		Direction:  "outbound",
		State:      "completed",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550222",
		StartedAt:  now.Add(-240 * time.Hour),
		UpdatedAt:  now.Add(-240 * time.Hour),
	}

	_ = st.CreateInboundCall(ctx, c1)
	_ = st.CreateInboundCall(ctx, c2)

	found, err := st.FindRecentOutboundCall(ctx, "+15105550111", 168*time.Hour)
	if err != nil {
		t.Fatalf("FindRecentOutboundCall error = %v", err)
	}
	if found == nil || found.CallID != "recent-call" {
		t.Errorf("expected recent-call, got %+v", found)
	}

	notFound, err := st.FindRecentOutboundCall(ctx, "+15105550222", 168*time.Hour)
	if err != nil {
		t.Fatalf("FindRecentOutboundCall error = %v", err)
	}
	if notFound != nil {
		t.Errorf("expected nil for 10-day old call, got %+v", notFound)
	}

	unknown, err := st.FindRecentOutboundCall(ctx, "+15105550999", 168*time.Hour)
	if err != nil {
		t.Fatalf("FindRecentOutboundCall error = %v", err)
	}
	if unknown != nil {
		t.Errorf("expected nil for unknown number, got %+v", unknown)
	}
}

func TestPGStore_TransitionReportStatus(t *testing.T) {
	t.Parallel()
	st := testutil.SetUpTestStore(t)
	ctx := context.Background()

	call := model.VoiceCall{
		CallID:       "call-status-test",
		Direction:    "outbound",
		State:        "in-progress",
		FromNumber:   "+15105550100",
		ToNumber:     "+15105550199",
		ReportStatus: "pending",
		StartedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	_ = st.CreateInboundCall(ctx, call)

	ok, err := st.TransitionReportStatus(ctx, "call-status-test", []string{"pending"}, "processing", "")
	if err != nil || !ok {
		t.Fatalf("TransitionReportStatus(pending -> processing) ok=%v, err=%v", ok, err)
	}

	callVM := model.VoiceCall{
		CallID:       "call-vm-test",
		Direction:    "inbound",
		State:        "completed",
		FromNumber:   "+15105550888",
		ToNumber:     "+15105550100",
		ReportStatus: "awaiting_recording",
		StartedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	_ = st.CreateInboundCall(ctx, callVM)

	blocked, err := st.TransitionReportStatus(ctx, "call-vm-test", []string{"pending"}, "processing", "")
	if err != nil || blocked {
		t.Fatalf("expected blocked transition when awaiting_recording, got ok=%v, err=%v", blocked, err)
	}

	uncondOk, err := st.TransitionReportStatus(ctx, "call-vm-test", nil, "delivered", "voicemail received")
	if err != nil || !uncondOk {
		t.Fatalf("unconditional transition failed: ok=%v, err=%v", uncondOk, err)
	}
	vmCall, _ := st.GetCall(ctx, "call-vm-test")
	if vmCall.ReportStatus != "delivered" || vmCall.ReportSummary != "voicemail received" {
		t.Errorf("unexpected call after unconditional transition: %+v", vmCall)
	}
}

func TestPGStore_ListUnfinishedReportCalls(t *testing.T) {
	t.Parallel()
	st := testutil.SetUpTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()

	// Initial empty check
	calls, err := st.ListUnfinishedReportCalls(ctx, 0)
	if err != nil {
		t.Fatalf("ListUnfinishedReportCalls() initial error = %v", err)
	}
	if len(calls) != 0 {
		t.Fatalf("expected 0 calls, got %d", len(calls))
	}

	ended := now
	cPending := model.VoiceCall{
		CallID:       "call-pending",
		Direction:    "outbound",
		State:        "completed",
		FromNumber:   "+15105550100",
		ToNumber:     "+15105550101",
		ReportStatus: "pending",
		StartedAt:    now.Add(-2 * time.Minute),
		EndedAt:      &ended,
		UpdatedAt:    now.Add(-2 * time.Minute),
	}
	cProcessing := model.VoiceCall{
		CallID:       "call-processing",
		Direction:    "outbound",
		State:        "completed",
		FromNumber:   "+15105550100",
		ToNumber:     "+15105550102",
		ReportStatus: "processing",
		StartedAt:    now.Add(-1 * time.Minute),
		EndedAt:      &ended,
		UpdatedAt:    now.Add(-1 * time.Minute),
	}
	cDelivered := model.VoiceCall{
		CallID:       "call-delivered",
		Direction:    "outbound",
		State:        "completed",
		FromNumber:   "+15105550100",
		ToNumber:     "+15105550103",
		ReportStatus: "delivered",
		StartedAt:    now,
		EndedAt:      &ended,
		UpdatedAt:    now,
	}
	cActive := model.VoiceCall{
		CallID:       "call-active",
		Direction:    "outbound",
		State:        "in-progress",
		FromNumber:   "+15105550100",
		ToNumber:     "+15105550104",
		ReportStatus: "pending",
		StartedAt:    now,
		UpdatedAt:    now,
	}

	_ = st.CreateInboundCall(ctx, cPending)
	_ = st.CreateInboundCall(ctx, cProcessing)
	_ = st.CreateInboundCall(ctx, cDelivered)
	_ = st.CreateInboundCall(ctx, cActive)

	calls, err = st.ListUnfinishedReportCalls(ctx, 10)
	if err != nil {
		t.Fatalf("ListUnfinishedReportCalls() error = %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("expected 2 unfinished calls, got %d", len(calls))
	}
	if calls[0].CallID != "call-pending" || calls[1].CallID != "call-processing" {
		t.Fatalf("unexpected calls order: %s, %s", calls[0].CallID, calls[1].CallID)
	}

	// Limit test
	limited, err := st.ListUnfinishedReportCalls(ctx, 1)
	if err != nil || len(limited) != 1 {
		t.Fatalf("ListUnfinishedReportCalls(limit=1) len=%d, err=%v", len(limited), err)
	}
}

func TestPGStore_ReconcileOrphanedCalls(t *testing.T) {
	t.Parallel()
	st := testutil.SetUpTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()

	c1 := model.VoiceCall{
		CallID:     "orphan-no-sid",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550101",
		StartedAt:  now,
		UpdatedAt:  now,
	}
	c2 := model.VoiceCall{
		CallID:          "active-call",
		ProviderCallSID: "CA_active",
		Direction:       "outbound",
		State:           "in-progress",
		FromNumber:      "+15105550100",
		ToNumber:        "+15105550102",
		StartedAt:       now,
		UpdatedAt:       now,
	}
	c3 := model.VoiceCall{
		CallID:          "inactive-call",
		ProviderCallSID: "CA_inactive",
		Direction:       "outbound",
		State:           "in-progress",
		FromNumber:      "+15105550100",
		ToNumber:        "+15105550103",
		StartedAt:       now,
		UpdatedAt:       now,
	}
	cErr := model.VoiceCall{
		CallID:          "err-call",
		ProviderCallSID: "CA_err",
		Direction:       "outbound",
		State:           "in-progress",
		FromNumber:      "+15105550100",
		ToNumber:        "+15105550105",
		StartedAt:       now,
		UpdatedAt:       now,
	}
	ended := now
	c4 := model.VoiceCall{
		CallID:          "already-ended",
		ProviderCallSID: "CA_ended",
		Direction:       "outbound",
		State:           "completed",
		FromNumber:      "+15105550100",
		ToNumber:        "+15105550104",
		StartedAt:       now,
		EndedAt:         &ended,
		UpdatedAt:       now,
	}

	_ = st.CreateInboundCall(ctx, c1)
	_ = st.CreateInboundCall(ctx, c2)
	_ = st.CreateInboundCall(ctx, c3)
	_ = st.CreateInboundCall(ctx, cErr)
	_ = st.CreateInboundCall(ctx, c4)

	checkActive := func(_ context.Context, providerSID string) (bool, error) {
		if providerSID == "CA_err" {
			return false, sql.ErrConnDone
		}
		if providerSID == "CA_active" {
			return true, nil
		}
		return false, nil
	}

	reconciled, err := st.ReconcileOrphanedCalls(ctx, checkActive)
	if err != nil {
		t.Fatalf("ReconcileOrphanedCalls error = %v", err)
	}
	if reconciled != 2 {
		t.Errorf("reconciled = %d, want 2", reconciled)
	}

	rec1, _ := st.GetCall(ctx, "orphan-no-sid")
	if rec1.State != "failed" || rec1.EndReason != "orphan_no_provider_sid" || rec1.EndedAt == nil {
		t.Errorf("unexpected reconciled c1: %+v", rec1)
	}

	rec3, _ := st.GetCall(ctx, "inactive-call")
	if rec3.State != "completed" || rec3.EndReason != "reconciled_orphan" || rec3.EndedAt == nil {
		t.Errorf("unexpected reconciled c3: %+v", rec3)
	}

	act2, _ := st.GetCall(ctx, "active-call")
	if act2.EndedAt != nil {
		t.Errorf("active call c2 should not be ended: %+v", act2)
	}
}

func TestPGStore_VoiceMessages(t *testing.T) {
	t.Parallel()
	st := testutil.SetUpTestStore(t)
	ctx := context.Background()

	msg := model.VoiceMessage{
		MessageSID:       "SM123456",
		Direction:        "inbound",
		FromNumber:       "+15105550199",
		ToNumber:         "+15105550100",
		Body:             "Booking confirmed for 7pm",
		MediaURLs:        []string{"https://media.twilio.com/img1.jpg"},
		DiscordDelivered: false,
		CreatedAt:        time.Now().UTC(),
	}

	inserted, err := st.SaveVoiceMessage(ctx, msg)
	if err != nil || !inserted {
		t.Fatalf("SaveVoiceMessage(new) inserted = %v, err = %v", inserted, err)
	}

	dupInserted, err := st.SaveVoiceMessage(ctx, msg)
	if err != nil || dupInserted {
		t.Fatalf("SaveVoiceMessage(duplicate) dupInserted = %v, err = %v", dupInserted, err)
	}

	// Message with empty media and zero created at
	msgZero := model.VoiceMessage{
		MessageSID: "SM777",
		Direction:  "outbound",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		Body:       "reminder",
	}
	if _, err = st.SaveVoiceMessage(ctx, msgZero); err != nil {
		t.Fatalf("SaveVoiceMessage(msgZero) error = %v", err)
	}

	msgs, err := st.ListVoiceMessages(ctx, 0)
	if err != nil {
		t.Fatalf("ListVoiceMessages() error = %v", err)
	}
	if len(msgs) != 2 {
		t.Errorf("len(msgs) = %d, want 2", len(msgs))
	}

	if err := st.MarkVoiceMessageDelivered(ctx, "SM123456"); err != nil {
		t.Fatalf("MarkVoiceMessageDelivered error = %v", err)
	}
	if err := st.MarkVoiceMessageDelivered(ctx, "non-existent"); !errors.Is(err, store.ErrMessageNotFound) {
		t.Errorf("MarkVoiceMessageDelivered(non-existent) err = %v, want ErrMessageNotFound", err)
	}

	msgsAfter, _ := st.ListVoiceMessages(ctx, 10)
	var foundSM bool
	for _, m := range msgsAfter {
		if m.MessageSID == "SM123456" && m.DiscordDelivered {
			foundSM = true
		}
	}
	if !foundSM {
		t.Errorf("expected SM123456 to be marked delivered: %+v", msgsAfter)
	}
}

func TestPGStore_DatabaseErrors(t *testing.T) {
	t.Parallel()
	db := testutil.OpenTestDB(t)
	st := store.NewPGStore(db)
	if err := st.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	ctx := context.Background()

	_, getErr := st.GetCall(ctx, "id")
	_, bySIDErr := st.GetCallByProviderSID(ctx, "sid")
	_, listErr := st.ListCalls(ctx, 10)
	_, recentErr := st.FindRecentOutboundCall(ctx, "+15105550100", time.Hour)
	_, cursorErr := st.AdvanceLiveTranscriptCursor(ctx, "id", 0, 1)
	_, transitionErr := st.TransitionReportStatus(ctx, "id", nil, "delivered", "")
	_, unfinishedErr := st.ListUnfinishedReportCalls(ctx, 10)
	_, reconcileErr := st.ReconcileOrphanedCalls(ctx, nil)
	_, saveErr := st.SaveVoiceMessage(ctx, model.VoiceMessage{MessageSID: "SM1"})
	_, listMsgErr := st.ListVoiceMessages(ctx, 10)

	results := []struct {
		op  string
		err error
	}{
		{op: "CreateOutboundCall", err: st.CreateOutboundCall(ctx, model.VoiceCall{}, 1)},
		{op: "CreateInboundCall", err: st.CreateInboundCall(ctx, model.VoiceCall{})},
		{op: "BindProviderCallSID", err: st.BindProviderCallSID(ctx, "id", "sid")},
		{op: "GetCall", err: getErr},
		{op: "GetCallByProviderSID", err: bySIDErr},
		{op: "ListCalls", err: listErr},
		{op: "FindRecentOutboundCall", err: recentErr},
		{op: "UpdateCallState", err: st.UpdateCallState(ctx, "id", "in-progress", "", "")},
		{op: "AppendTranscriptTurn", err: st.AppendTranscriptTurn(ctx, "id", model.TranscriptTurn{})},
		{op: "AdvanceLiveTranscriptCursor", err: cursorErr},
		{op: "AppendOwnerInstruction", err: st.AppendOwnerInstruction(ctx, "id", model.OwnerInstruction{})},
		{op: "SetVoicemailRecording", err: st.SetVoicemailRecording(ctx, "id", "url")},
		{op: "TransitionReportStatus", err: transitionErr},
		{op: "ListUnfinishedReportCalls", err: unfinishedErr},
		{op: "ReconcileOrphanedCalls", err: reconcileErr},
		{op: "SaveVoiceMessage", err: saveErr},
		{op: "MarkVoiceMessageDelivered", err: st.MarkVoiceMessageDelivered(ctx, "SM1")},
		{op: "ListVoiceMessages", err: listMsgErr},
	}
	for _, r := range results {
		if r.err == nil || !strings.Contains(r.err.Error(), "sql: database is closed") {
			t.Errorf("%s on closed handle err = %v, want sql: database is closed", r.op, r.err)
		}
	}
}

func TestOpenDB(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{name: "postgres dsn opens and pings", url: pgtest.NewDSN(t), wantErr: false},
		{name: "postgres dsn with surrounding space", url: "  " + pgtest.NewDSN(t) + "\n", wantErr: false},
		{name: "empty url", url: "   ", wantErr: true},
		{name: "unsupported scheme", url: "invalid://url", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, err := store.OpenDB(context.Background(), tc.url)
			if (err != nil) != tc.wantErr {
				t.Fatalf("OpenDB(%q) err = %v, wantErr = %v", tc.url, err, tc.wantErr)
			}
			if db != nil {
				_ = db.Close()
			}
		})
	}
}

func TestPGStore_EmptyJSONDefaults(t *testing.T) {
	t.Parallel()
	st := testutil.SetUpTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	call := model.VoiceCall{
		CallID:     "call-empty-json",
		Direction:  "inbound",
		State:      "ringing",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		StartedAt:  now,
		UpdatedAt:  now,
	}
	if err := st.CreateInboundCall(ctx, call); err != nil {
		t.Fatalf("CreateInboundCall() error = %v", err)
	}

	fetched, err := st.GetCall(ctx, "call-empty-json")
	if err != nil {
		t.Fatalf("GetCall() error = %v", err)
	}
	if fetched.OwnerInstructions == nil {
		t.Errorf("expected non-nil owner instructions, got nil")
	}
	if fetched.Transcript == nil {
		t.Errorf("expected non-nil transcript, got nil")
	}
	if len(fetched.OwnerInstructions) != 0 || len(fetched.Transcript) != 0 {
		t.Errorf("expected empty slices, got %+v / %+v", fetched.OwnerInstructions, fetched.Transcript)
	}
}

func TestPGStore_SaveVoiceMessage_Concurrent(t *testing.T) {
	t.Parallel()
	st := testutil.SetUpTestStore(t)
	ctx := context.Background()

	const numGoroutines = 8
	var wg sync.WaitGroup
	var insertedCount int64
	concurMsg := model.VoiceMessage{
		MessageSID: "SM_CONCURRENT",
		Direction:  "inbound",
		FromNumber: "+15105550111",
		ToNumber:   "+15105550222",
		Body:       "concurrent race test",
	}
	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			ins, err := st.SaveVoiceMessage(ctx, concurMsg)
			if err != nil {
				t.Errorf("SaveVoiceMessage(concur) error = %v", err)
				return
			}
			if ins {
				atomic.AddInt64(&insertedCount, 1)
			}
		}()
	}
	wg.Wait()
	if insertedCount != 1 {
		t.Fatalf("expected exactly 1 insert to succeed, got %d", insertedCount)
	}
}

func TestPGStore_MissingCallErrorsAndFullFields(t *testing.T) {
	t.Parallel()
	db := testutil.OpenTestDB(t)
	seedEndedCall(t, db, "call-prior")
	st := store.NewPGStore(db)
	ctx := context.Background()

	now := time.Now().UTC()
	callFull := model.VoiceCall{
		CallID:                "call-full-fields",
		ProviderCallSID:       "CA-full-1",
		Direction:             "outbound",
		State:                 "completed",
		EndReason:             "normal",
		FromNumber:            "+15105550100",
		ToNumber:              "+15105550199",
		CallbackOfCallID:      "call-prior",
		DiscordChannelID:      "chan-1",
		DiscordThreadID:       "thread-1",
		AnsweredBy:            "human",
		VoicemailRecordingURL: "https://rec.wav",
		ReportSummary:         "summary text",
		AnsweredAt:            &now,
		EndedAt:               &now,
	}
	if err := st.CreateInboundCall(ctx, callFull); err != nil {
		t.Fatalf("CreateInboundCall(callFull) err: %v", err)
	}

	fetched, err := st.GetCall(ctx, "call-full-fields")
	if err != nil {
		t.Fatalf("GetCall err: %v", err)
	}
	if fetched.AnsweredBy != "human" || fetched.VoicemailRecordingURL != "https://rec.wav" || fetched.ReportSummary != "summary text" {
		t.Fatalf("unexpected fetched call fields: %+v", fetched)
	}

	// Missing call operations
	missingID := "call-missing-999"
	if err := st.AppendTranscriptTurn(ctx, missingID, model.TranscriptTurn{Role: "user", Text: "hi"}); !errors.Is(err, store.ErrCallNotFound) {
		t.Fatalf("AppendTranscriptTurn missing call: %v, want ErrCallNotFound", err)
	}
	if err := st.AppendOwnerInstruction(ctx, missingID, model.OwnerInstruction{Instruction: "say hi"}); !errors.Is(err, store.ErrCallNotFound) {
		t.Fatalf("AppendOwnerInstruction missing call: %v, want ErrCallNotFound", err)
	}
	if err := st.SetVoicemailRecording(ctx, missingID, "http://rec"); !errors.Is(err, store.ErrCallNotFound) {
		t.Fatalf("SetVoicemailRecording missing call: %v, want ErrCallNotFound", err)
	}
	if ok, err := st.AdvanceLiveTranscriptCursor(ctx, missingID, 0, 1); err != nil || ok {
		t.Fatalf("AdvanceLiveTranscriptCursor missing call: ok=%v, err=%v", ok, err)
	}
	if err := st.BindProviderCallSID(ctx, missingID, "CA-123"); !errors.Is(err, store.ErrCallNotFound) {
		t.Fatalf("BindProviderCallSID missing call: %v, want ErrCallNotFound", err)
	}
	if err := st.UpdateCallState(ctx, missingID, "completed", "", ""); !errors.Is(err, store.ErrCallNotFound) {
		t.Fatalf("UpdateCallState missing call: %v, want ErrCallNotFound", err)
	}

	if err := st.CreateInboundCall(ctx, callFull); err == nil {
		t.Fatal("expected error inserting duplicate CallID in CreateInboundCall")
	}
	callFullDupOut := callFull
	callFullDupOut.ToNumber = "+15105550998"
	if err := st.CreateOutboundCall(ctx, callFullDupOut, 5); err == nil {
		t.Fatal("expected error inserting duplicate CallID in CreateOutboundCall")
	}
}

// TestPGStore_SabotagedCallRows: JSONB only stores valid JSON, so corruption
// is well-formed JSON of the wrong shape, a sabotaged column or a constraint
// written into the test's own database.
func TestPGStore_SabotagedCallRows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		sabotageSQL   string
		wantGetErr    string
		wantListErr   string
		wantAppendErr string
	}{
		{name: "brief not an object", sabotageSQL: `UPDATE voice_calls SET brief = '[1]'`, wantGetErr: "store: unmarshal brief", wantListErr: "store: unmarshal brief"},
		{name: "owner instructions not a list", sabotageSQL: `UPDATE voice_calls SET owner_instructions = '{}'`, wantGetErr: "store: unmarshal owner instructions", wantListErr: "store: unmarshal owner instructions", wantAppendErr: "store: unmarshal owner instructions"},
		{name: "transcript not a list", sabotageSQL: `UPDATE voice_calls SET transcript = '{}'`, wantGetErr: "store: unmarshal transcript", wantListErr: "store: unmarshal transcript"},
		{name: "cursor column unscannable", sabotageSQL: `ALTER TABLE voice_calls ALTER COLUMN live_transcript_cursor TYPE TEXT USING 'x'`, wantGetErr: "store: scan call", wantListErr: "store: scan call"},
		{name: "owner instructions column dropped", sabotageSQL: `ALTER TABLE voice_calls DROP COLUMN owner_instructions`, wantGetErr: "store: scan call", wantListErr: "store: list calls", wantAppendErr: "store: get owner instructions"},
		{name: "owner instructions append rejected", sabotageSQL: `ALTER TABLE voice_calls ADD CONSTRAINT no_instructions CHECK (jsonb_array_length(owner_instructions) = 0)`, wantAppendErr: "store: update owner instructions"},
		{name: "json null slices default empty", sabotageSQL: `UPDATE voice_calls SET transcript = 'null', owner_instructions = 'null'`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := testutil.OpenTestDB(t)
			st := store.NewPGStore(db)
			seed := model.VoiceCall{CallID: "corr-1", Direction: "inbound", State: "ringing", FromNumber: "+15105550100", ToNumber: "+15105550199"}
			if err := st.CreateInboundCall(ctx, seed); err != nil {
				t.Fatalf("CreateInboundCall() error = %v", err)
			}
			if _, err := db.ExecContext(ctx, tc.sabotageSQL); err != nil {
				t.Fatalf("sabotage %q: %v", tc.sabotageSQL, err)
			}

			fetched, err := st.GetCall(ctx, "corr-1")
			assertErrContains(t, "GetCall", err, tc.wantGetErr)
			_, err = st.ListCalls(ctx, 10)
			assertErrContains(t, "ListCalls", err, tc.wantListErr)
			err = st.AppendOwnerInstruction(ctx, "corr-1", model.OwnerInstruction{Instruction: "hi"})
			assertErrContains(t, "AppendOwnerInstruction", err, tc.wantAppendErr)
			if tc.wantGetErr == "" && (fetched.Transcript == nil || fetched.OwnerInstructions == nil) {
				t.Errorf("GetCall() slices = %v / %v, want non-nil", fetched.Transcript, fetched.OwnerInstructions)
			}
		})
	}
}

func TestPGStore_CorruptMessageJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		sabotageSQL string
		wantErr     string
	}{
		{name: "media urls not a list", sabotageSQL: `UPDATE voice_messages SET media_urls = '{}'`, wantErr: "store: unmarshal media urls"},
		{name: "created at unscannable", sabotageSQL: `ALTER TABLE voice_messages ALTER COLUMN created_at TYPE TEXT USING 'not-a-timestamp'`, wantErr: "store: scan voice message"},
		{name: "json null media defaults empty", sabotageSQL: `UPDATE voice_messages SET media_urls = 'null'`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := testutil.OpenTestDB(t)
			seedEndedCall(t, db, "corr-1")
			st := store.NewPGStore(db)
			msg := model.VoiceMessage{MessageSID: "SM_CORR", Direction: "inbound", FromNumber: "+15105550199", ToNumber: "+15105550100", Body: "corruption probe", RelatedCallID: "corr-1"}
			if _, err := st.SaveVoiceMessage(ctx, msg); err != nil {
				t.Fatalf("SaveVoiceMessage() error = %v", err)
			}
			if _, err := db.ExecContext(ctx, tc.sabotageSQL); err != nil {
				t.Fatalf("sabotage %q: %v", tc.sabotageSQL, err)
			}

			msgs, err := st.ListVoiceMessages(ctx, 10)
			assertErrContains(t, "ListVoiceMessages", err, tc.wantErr)
			if tc.wantErr != "" {
				return
			}
			if len(msgs) != 1 || msgs[0].MediaURLs == nil || msgs[0].RelatedCallID != "corr-1" {
				t.Errorf("ListVoiceMessages() = %+v, want one message with non-nil media and related call corr-1", msgs)
			}
		})
	}
}

// assertErrContains checks err against want: want == "" means no error.
func assertErrContains(t *testing.T, op string, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Errorf("%s error = %v, want nil", op, err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("%s error = %v, want containing %q", op, err, want)
	}
}

// rejectCommitSQL installs a deferred constraint trigger in the test's own
// database, so every transaction inserting a call fails at COMMIT.
const rejectCommitSQL = `
CREATE FUNCTION reject_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'commit rejected by test trigger';
END $$;
CREATE CONSTRAINT TRIGGER reject_commit AFTER INSERT ON voice_calls
	DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_commit();`

func TestPGStore_CommitErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := testutil.OpenTestDB(t)
	st := store.NewPGStore(db)
	if _, err := db.ExecContext(ctx, rejectCommitSQL); err != nil {
		t.Fatalf("install commit trigger: %v", err)
	}

	outbound := model.VoiceCall{CallID: "commit-out", Direction: "outbound", State: "initiated", FromNumber: "+15105550100", ToNumber: "+15105550199"}
	assertErrContains(t, "CreateOutboundCall", st.CreateOutboundCall(ctx, outbound, 5), "store: commit tx")
	inbound := model.VoiceCall{CallID: "commit-in", Direction: "inbound", State: "ringing", FromNumber: "+15105550199", ToNumber: "+15105550100"}
	assertErrContains(t, "CreateInboundCall", st.CreateInboundCall(ctx, inbound), "store: commit tx")

	calls, err := st.ListCalls(ctx, 10)
	if err != nil || len(calls) != 0 {
		t.Errorf("ListCalls() after rejected commits = %d calls, err = %v; want 0, nil", len(calls), err)
	}
}

// failOnSecondRowSQL swaps each table for a view whose key column raises on
// one row. The planner evaluates the (non-volatile) function above the sort
// and LIMIT, per emitted row, so the first row streams and the failure
// surfaces mid-iteration (rows.Err), not at Query.
const failOnSecondRowSQL = `
CREATE FUNCTION fail_on(id TEXT, bad TEXT) RETURNS TEXT LANGUAGE plpgsql STABLE AS $$
BEGIN
	IF id = bad THEN
		RAISE EXCEPTION 'iteration fault on %', id;
	END IF;
	RETURN id;
END $$;
ALTER TABLE voice_calls RENAME TO voice_calls_base;
CREATE VIEW voice_calls AS SELECT
	fail_on(call_id, 'fault-next-2') AS call_id, provider_call_sid, direction, state, end_reason,
	from_number, to_number, callback_of_call_id, discord_channel_id, discord_thread_id,
	brief, owner_instructions, transcript, live_transcript_cursor, answered_by,
	voicemail_recording_url, report_status, report_summary, started_at, answered_at,
	ended_at, updated_at
	FROM voice_calls_base;
ALTER TABLE voice_messages RENAME TO voice_messages_base;
CREATE VIEW voice_messages AS SELECT
	fail_on(message_sid, 'FAULT_NEXT_2') AS message_sid, direction, from_number, to_number, body,
	media_urls, related_call_id, discord_delivered, created_at
	FROM voice_messages_base;`

func TestPGStore_IterationErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := testutil.OpenTestDB(t)
	st := store.NewPGStore(db)

	now := time.Now().UTC()
	// fault-next-1 is newest and inserted first, so it is the first row both
	// in started_at DESC order and in heap order.
	for _, c := range []model.VoiceCall{
		{CallID: "fault-next-1", Direction: "inbound", State: "ringing", FromNumber: "+15105550100", ToNumber: "+15105550199", StartedAt: now},
		{CallID: "fault-next-2", Direction: "inbound", State: "ringing", FromNumber: "+15105550101", ToNumber: "+15105550199", StartedAt: now.Add(-time.Minute)},
	} {
		if err := st.CreateInboundCall(ctx, c); err != nil {
			t.Fatalf("CreateInboundCall(%s) error = %v", c.CallID, err)
		}
	}
	for _, m := range []model.VoiceMessage{
		{MessageSID: "FAULT_NEXT_1", Direction: "inbound", FromNumber: "+15105550199", ToNumber: "+15105550100", Body: "first", CreatedAt: now},
		{MessageSID: "FAULT_NEXT_2", Direction: "inbound", FromNumber: "+15105550199", ToNumber: "+15105550100", Body: "second", CreatedAt: now.Add(-time.Minute)},
	} {
		if _, err := st.SaveVoiceMessage(ctx, m); err != nil {
			t.Fatalf("SaveVoiceMessage(%s) error = %v", m.MessageSID, err)
		}
	}
	if _, err := db.ExecContext(ctx, failOnSecondRowSQL); err != nil {
		t.Fatalf("install failing views: %v", err)
	}

	_, err := st.ListCalls(ctx, 10)
	assertErrContains(t, "ListCalls", err, "store: iterate calls")
	_, err = st.ListVoiceMessages(ctx, 10)
	assertErrContains(t, "ListVoiceMessages", err, "store: iterate voice messages")
	_, err = st.ReconcileOrphanedCalls(ctx, nil)
	assertErrContains(t, "ReconcileOrphanedCalls", err, "store: iterate unended calls")
}
