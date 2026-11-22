package store_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/store"
	"github.com/azylman/aerial/services/prospera/internal/testutil"
)

func TestPGStore_CallLifecycle(t *testing.T) {
	t.Parallel()
	st := testutil.SetUpTestStore(t)
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
	if len(list) != 1 {
		t.Errorf("len(list) = %d, want 1", len(list))
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
	db, _ := sql.Open("sqlite", ":memory:")
	st := store.NewPGStore(db)
	_ = db.Close()
	ctx := context.Background()

	_ = st.CreateOutboundCall(ctx, model.VoiceCall{}, 1)
	_ = st.CreateInboundCall(ctx, model.VoiceCall{})
	_ = st.BindProviderCallSID(ctx, "id", "sid")
	_, _ = st.GetCall(ctx, "id")
	_, _ = st.GetCallByProviderSID(ctx, "sid")
	_, _ = st.ListCalls(ctx, 10)
	_, _ = st.FindRecentOutboundCall(ctx, "+15105550100", time.Hour)
	_ = st.UpdateCallState(ctx, "id", "in-progress", "", "")
	_ = st.AppendTranscriptTurn(ctx, "id", model.TranscriptTurn{})
	_, _ = st.AdvanceLiveTranscriptCursor(ctx, "id", 0, 1)
	_ = st.AppendOwnerInstruction(ctx, "id", model.OwnerInstruction{})
	_ = st.SetVoicemailRecording(ctx, "id", "url")
	_, _ = st.TransitionReportStatus(ctx, "id", nil, "done", "")
	_, _ = st.ReconcileOrphanedCalls(ctx, nil)
	_, _ = st.SaveVoiceMessage(ctx, model.VoiceMessage{MessageSID: "SM1"})
	_ = st.MarkVoiceMessageDelivered(ctx, "SM1")
	_, _ = st.ListVoiceMessages(ctx, 10)
}

func TestOpenDB(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{name: "sqlite memory prefix", url: "sqlite://:memory:", wantErr: false},
		{name: "sqlite file prefix", url: "file:test.db?mode=memory", wantErr: false},
		{name: "sqlite memory keyword", url: ":memory:", wantErr: false},
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
	st := testutil.SetUpTestStore(t)
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

func TestPGStore_CorruptCallJSON(t *testing.T) {
	t.Parallel()
	db := testutil.OpenTestSQLiteDB(t)
	st := store.NewPGStore(db)
	ctx := context.Background()

	now := time.Now().UTC()
	seed := model.VoiceCall{
		CallID:     "corr-1",
		Direction:  "inbound",
		State:      "ringing",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		StartedAt:  now,
		UpdatedAt:  now,
	}
	if err := st.CreateInboundCall(ctx, seed); err != nil {
		t.Fatalf("CreateInboundCall() error = %v", err)
	}
	corruptCallJSON(t, db, `brief = '{bad'`)
	if _, err := st.GetCall(ctx, "corr-1"); err == nil || !strings.Contains(err.Error(), "store: unmarshal brief") {
		t.Errorf("GetCall corrupt brief err = %v, want unmarshal brief error", err)
	}
	if _, err := st.ListCalls(ctx, 10); err == nil || !strings.Contains(err.Error(), "store: unmarshal brief") {
		t.Errorf("ListCalls corrupt brief err = %v, want unmarshal brief error", err)
	}

	corruptCallJSON(t, db, `brief = '{}', owner_instructions = '{bad'`)
	if _, err := st.GetCall(ctx, "corr-1"); err == nil || !strings.Contains(err.Error(), "store: unmarshal owner instructions") {
		t.Errorf("GetCall corrupt owner instructions err = %v, want unmarshal error", err)
	}

	corruptCallJSON(t, db, `owner_instructions = '[]', transcript = '{bad'`)
	if _, err := st.GetCall(ctx, "corr-1"); err == nil || !strings.Contains(err.Error(), "store: unmarshal transcript") {
		t.Errorf("GetCall corrupt transcript err = %v, want unmarshal error", err)
	}
	corruptCallJSON(t, db, `transcript = '[]', owner_instructions = '{bad'`)
	if err := st.AppendOwnerInstruction(ctx, "corr-1", model.OwnerInstruction{Instruction: "hi"}); err == nil || !strings.Contains(err.Error(), "store: unmarshal owner instructions") {
		t.Errorf("AppendOwnerInstruction corrupt JSON err = %v, want unmarshal error", err)
	}

	corruptCallJSON(t, db, `transcript = 'null', owner_instructions = 'null'`)
	fetched, err := st.GetCall(ctx, "corr-1")
	if err != nil {
		t.Fatalf("GetCall null JSON error = %v", err)
	}
	if fetched.Transcript == nil || fetched.OwnerInstructions == nil {
		t.Errorf("expected non-nil slices for null JSON, got %+v", fetched)
	}
}

func TestPGStore_CorruptMessageJSON(t *testing.T) {
	t.Parallel()
	db := testutil.OpenTestSQLiteDB(t)
	st := store.NewPGStore(db)
	ctx := context.Background()

	msg := model.VoiceMessage{
		MessageSID:    "SM_CORR",
		Direction:     "inbound",
		FromNumber:    "+15105550199",
		ToNumber:      "+15105550100",
		Body:          "corruption probe",
		RelatedCallID: "corr-1",
		CreatedAt:     time.Now().UTC(),
	}
	if _, err := st.SaveVoiceMessage(ctx, msg); err != nil {
		t.Fatalf("SaveVoiceMessage() error = %v", err)
	}

	if _, err := db.Exec("UPDATE voice_messages SET media_urls = '{bad' WHERE message_sid = 'SM_CORR'"); err != nil {
		t.Fatalf("corrupt media_urls error = %v", err)
	}
	if _, err := st.ListVoiceMessages(ctx, 10); err == nil || !strings.Contains(err.Error(), "store: unmarshal media urls") {
		t.Errorf("ListVoiceMessages corrupt media err = %v, want unmarshal error", err)
	}

	if _, err := db.Exec("UPDATE voice_messages SET media_urls = 'null' WHERE message_sid = 'SM_CORR'"); err != nil {
		t.Fatalf("null out media_urls error = %v", err)
	}
	msgs, err := st.ListVoiceMessages(ctx, 10)
	if err != nil {
		t.Fatalf("ListVoiceMessages null media error = %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	if msgs[0].MediaURLs == nil {
		t.Errorf("expected non-nil media urls for null JSON, got nil")
	}
	if msgs[0].RelatedCallID != "corr-1" {
		t.Errorf("RelatedCallID = %q, want corr-1", msgs[0].RelatedCallID)
	}

	if _, err := db.Exec("UPDATE voice_messages SET created_at = 'not-a-timestamp' WHERE message_sid = 'SM_CORR'"); err != nil {
		t.Fatalf("corrupt created_at error = %v", err)
	}
	if _, err := st.ListVoiceMessages(ctx, 10); err == nil || !strings.Contains(err.Error(), "store: scan voice message") {
		t.Errorf("ListVoiceMessages corrupt timestamp err = %v, want scan error", err)
	}
}

func corruptCallJSON(t *testing.T, db *sql.DB, setClause string) {
	t.Helper()
	if _, err := db.Exec("UPDATE voice_calls SET " + setClause + " WHERE call_id = 'corr-1'"); err != nil {
		t.Fatalf("corrupt voice_calls (%s) error = %v", setClause, err)
	}
}

// errFaultInjected is returned by the fault-injecting test driver.
var errFaultInjected = errors.New("test: injected driver fault")

// faultKind selects which driver operation a faultDriver sabotages.
type faultKind int

const (
	faultNone faultKind = iota
	faultCommit
	faultRowsAffected
	faultNext
)

// faultSetting is shared between a test and its faultDriver so the test can
// arm the fault after seeding through the same pool.
type faultSetting struct {
	kind faultKind
}

// faultDriver wraps the sqlite driver to inject deterministic driver-level
// failures. It exercises error branches that healthy databases never hit
// (failed commits, failed rows-affected checks, and mid-iteration query
// failures).
type faultDriver struct {
	base    driver.Driver
	setting *faultSetting
}

func (d *faultDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	return &faultConn{Conn: conn, setting: d.setting}, nil
}

// faultConn delegates every operation to sqlite except the armed fault.
type faultConn struct {
	driver.Conn

	setting *faultSetting
}

func (c *faultConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *faultConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	beginner, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, driver.ErrSkip
	}
	tx, err := beginner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &faultTx{Tx: tx, setting: c.setting}, nil
}

func (c *faultConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	res, err := execer.ExecContext(ctx, query, args)
	if err != nil || c.setting.kind != faultRowsAffected {
		return res, err
	}
	return faultResult{Result: res}, nil
}

func (c *faultConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rows, err := queryer.QueryContext(ctx, query, args)
	if err != nil || c.setting.kind != faultNext {
		return rows, err
	}
	return &faultRows{Rows: rows}, nil
}

// faultTx delegates to the sqlite transaction except the armed fault.
type faultTx struct {
	driver.Tx

	setting *faultSetting
}

func (t *faultTx) Commit() error {
	if t.setting.kind == faultCommit {
		return errFaultInjected
	}
	return t.Tx.Commit()
}

// faultResult poisons RowsAffected while delegating the rest.
type faultResult struct {
	driver.Result
}

func (faultResult) RowsAffected() (int64, error) {
	return 0, errFaultInjected
}

// faultRows fails iteration on the first row.
type faultRows struct {
	driver.Rows
}

func (*faultRows) Next(dest []driver.Value) error {
	return errFaultInjected
}

var (
	_ driver.Driver         = (*faultDriver)(nil)
	_ driver.Conn           = (*faultConn)(nil)
	_ driver.ExecerContext  = (*faultConn)(nil)
	_ driver.QueryerContext = (*faultConn)(nil)
	_ driver.Tx             = (*faultTx)(nil)
	_ driver.Result         = faultResult{}
	_ driver.Rows           = (*faultRows)(nil)
)

// openFaultPool registers a fault-injecting driver under name, opens a
// private in-memory database on it, and clones the canonical test schema
// into it.
func openFaultPool(t *testing.T, name string, setting *faultSetting) *sql.DB {
	t.Helper()
	sql.Register(name, &faultDriver{base: &sqlite.Driver{}, setting: setting})
	db, err := sql.Open(name, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open(%s) error = %v", name, err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	cloneTestSchema(t, db)
	return db
}

// cloneTestSchema copies the canonical table DDL from a fresh test database
// into dst without duplicating the DDL text.
func cloneTestSchema(t *testing.T, dst *sql.DB) {
	t.Helper()
	src := testutil.OpenTestSQLiteDB(t)
	rows, err := src.Query("SELECT sql FROM sqlite_master WHERE type = 'table' AND sql IS NOT NULL")
	if err != nil {
		t.Fatalf("read test schema error = %v", err)
	}
	defer rows.Close()
	var stmts []string
	for rows.Next() {
		var ddl string
		if err := rows.Scan(&ddl); err != nil {
			t.Fatalf("scan test schema error = %v", err)
		}
		stmts = append(stmts, ddl)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate test schema error = %v", err)
	}
	for _, ddl := range stmts {
		if _, err := dst.Exec(ddl); err != nil {
			t.Fatalf("apply test schema error = %v", err)
		}
	}
}

func TestPGStore_FaultCommitErrors(t *testing.T) {
	// A failed driver commit leaves the underlying transaction open, so
	// each faulted commit runs on its own pool.
	setting := &faultSetting{}
	ctx := context.Background()
	now := time.Now().UTC()

	outboundDB := openFaultPool(t, "fault-commit-out", setting)
	setting.kind = faultCommit
	outboundStore := store.NewPGStore(outboundDB)
	call := model.VoiceCall{
		CallID:     "fault-commit-1",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		StartedAt:  now,
		UpdatedAt:  now,
	}
	if err := outboundStore.CreateOutboundCall(ctx, call, 5); err == nil || !strings.Contains(err.Error(), "store: commit tx") {
		t.Errorf("CreateOutboundCall fault commit err = %v, want commit tx error", err)
	}

	inboundDB := openFaultPool(t, "fault-commit-in", setting)
	inboundStore := store.NewPGStore(inboundDB)
	inbound := model.VoiceCall{
		CallID:     "fault-commit-2",
		Direction:  "inbound",
		State:      "ringing",
		FromNumber: "+15105550199",
		ToNumber:   "+15105550100",
		StartedAt:  now,
		UpdatedAt:  now,
	}
	if err := inboundStore.CreateInboundCall(ctx, inbound); err == nil || !strings.Contains(err.Error(), "store: commit tx") {
		t.Errorf("CreateInboundCall fault commit err = %v, want commit tx error", err)
	}
}

func TestPGStore_FaultRowsAffectedErrors(t *testing.T) {
	setting := &faultSetting{}
	db := openFaultPool(t, "fault-rows", setting)
	st := store.NewPGStore(db)
	ctx := context.Background()

	now := time.Now().UTC()
	seed := model.VoiceCall{
		CallID:     "fault-rows-1",
		Direction:  "inbound",
		State:      "ringing",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		StartedAt:  now,
		UpdatedAt:  now,
	}
	if err := st.CreateInboundCall(ctx, seed); err != nil {
		t.Fatalf("CreateInboundCall() error = %v", err)
	}
	seedMsg := model.VoiceMessage{
		MessageSID: "FAULT_ROWS_1",
		Direction:  "inbound",
		FromNumber: "+15105550199",
		ToNumber:   "+15105550100",
		Body:       "fault probe",
		CreatedAt:  now,
	}
	if _, err := st.SaveVoiceMessage(ctx, seedMsg); err != nil {
		t.Fatalf("SaveVoiceMessage() error = %v", err)
	}

	setting.kind = faultRowsAffected
	if err := st.BindProviderCallSID(ctx, "fault-rows-1", "CA1"); err == nil || !strings.Contains(err.Error(), "store: rows affected") {
		t.Errorf("BindProviderCallSID fault rows err = %v, want rows affected error", err)
	}
	if err := st.UpdateCallState(ctx, "fault-rows-1", "completed", "done", ""); err == nil || !strings.Contains(err.Error(), "store: rows affected") {
		t.Errorf("UpdateCallState fault rows err = %v, want rows affected error", err)
	}
	if _, err := st.AdvanceLiveTranscriptCursor(ctx, "fault-rows-1", 0, 1); err == nil || !strings.Contains(err.Error(), "store: rows affected") {
		t.Errorf("AdvanceLiveTranscriptCursor fault rows err = %v, want rows affected error", err)
	}
	if err := st.SetVoicemailRecording(ctx, "fault-rows-1", "https://rec"); err == nil || !strings.Contains(err.Error(), "store: rows affected") {
		t.Errorf("SetVoicemailRecording fault rows err = %v, want rows affected error", err)
	}
	if _, err := st.TransitionReportStatus(ctx, "fault-rows-1", nil, "delivered", ""); err == nil || !strings.Contains(err.Error(), "store: rows affected") {
		t.Errorf("TransitionReportStatus fault rows err = %v, want rows affected error", err)
	}
	if err := st.MarkVoiceMessageDelivered(ctx, "FAULT_ROWS_1"); err == nil || !strings.Contains(err.Error(), "store: rows affected") {
		t.Errorf("MarkVoiceMessageDelivered fault rows err = %v, want rows affected error", err)
	}
	if _, err := st.SaveVoiceMessage(ctx, model.VoiceMessage{MessageSID: "FAULT_ROWS_2"}); err == nil || !strings.Contains(err.Error(), "store: check rows affected") {
		t.Errorf("SaveVoiceMessage fault rows err = %v, want check rows affected error", err)
	}
}

func TestPGStore_FaultIterationErrors(t *testing.T) {
	setting := &faultSetting{}
	db := openFaultPool(t, "fault-next", setting)
	st := store.NewPGStore(db)
	ctx := context.Background()

	now := time.Now().UTC()
	seed := model.VoiceCall{
		CallID:     "fault-next-1",
		Direction:  "inbound",
		State:      "ringing",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		StartedAt:  now,
		UpdatedAt:  now,
	}
	if err := st.CreateInboundCall(ctx, seed); err != nil {
		t.Fatalf("CreateInboundCall() error = %v", err)
	}
	if _, err := st.SaveVoiceMessage(ctx, model.VoiceMessage{MessageSID: "FAULT_NEXT_1", CreatedAt: now}); err != nil {
		t.Fatalf("SaveVoiceMessage() error = %v", err)
	}

	setting.kind = faultNext
	if _, err := st.ListCalls(ctx, 10); err == nil || !strings.Contains(err.Error(), "store: iterate calls") {
		t.Errorf("ListCalls fault iteration err = %v, want iterate calls error", err)
	}
	if _, err := st.ListVoiceMessages(ctx, 10); err == nil || !strings.Contains(err.Error(), "store: iterate voice messages") {
		t.Errorf("ListVoiceMessages fault iteration err = %v, want iterate error", err)
	}
	if _, err := st.ReconcileOrphanedCalls(ctx, nil); err == nil || !strings.Contains(err.Error(), "store: iterate unended calls") {
		t.Errorf("ReconcileOrphanedCalls fault iteration err = %v, want iterate error", err)
	}
}
