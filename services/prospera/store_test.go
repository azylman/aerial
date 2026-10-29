package main

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func setUpTestStore(t *testing.T) *PGStore {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)

	schema := `
	CREATE TABLE voice_calls (
		call_id TEXT PRIMARY KEY,
		provider_call_sid TEXT UNIQUE,
		direction TEXT NOT NULL,
		state TEXT NOT NULL,
		end_reason TEXT,
		from_number TEXT NOT NULL,
		to_number TEXT NOT NULL,
		callback_of_call_id TEXT,
		discord_channel_id TEXT,
		discord_thread_id TEXT,
		brief TEXT NOT NULL DEFAULT '{}',
		owner_instructions TEXT NOT NULL DEFAULT '[]',
		transcript TEXT NOT NULL DEFAULT '[]',
		live_transcript_cursor INTEGER NOT NULL DEFAULT 0,
		answered_by TEXT,
		voicemail_recording_url TEXT,
		report_status TEXT NOT NULL DEFAULT 'pending',
		report_summary TEXT,
		started_at TIMESTAMP NOT NULL,
		answered_at TIMESTAMP,
		ended_at TIMESTAMP,
		updated_at TIMESTAMP NOT NULL
	);

	CREATE TABLE voice_messages (
		message_sid TEXT PRIMARY KEY,
		direction TEXT NOT NULL,
		from_number TEXT NOT NULL,
		to_number TEXT NOT NULL,
		body TEXT NOT NULL,
		media_urls TEXT NOT NULL DEFAULT '[]',
		related_call_id TEXT,
		discord_delivered BOOLEAN NOT NULL DEFAULT FALSE,
		created_at TIMESTAMP NOT NULL
	);
	`

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		t.Fatalf("failed to create test schema: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return NewPGStore(db)
}

func TestPGStore_SQLiteDetection(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	if !store.isSQLite {
		t.Errorf("store.isSQLite = false, want true for SQLite test store")
	}
}

func TestPGStore_CallLifecycle(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	answered := now
	ended := now

	call := VoiceCall{
		CallID:           "call-123",
		Direction:        "outbound",
		State:            "initiated",
		EndReason:        "user_hangup",
		FromNumber:       "+15105550100",
		ToNumber:         "+15105550199",
		CallbackOfCallID: "call-orig",
		DiscordChannelID: "prospera",
		DiscordThreadID:  "thread-123",
		Brief: CallBrief{
			Task:      "Test errand",
			Approvals: "Zero spend",
		},
		OwnerInstructions: []OwnerInstruction{{Instruction: "start", Mode: "guidance", CreatedAt: now}},
		Transcript:        []TranscriptTurn{{Role: "system", Text: "init", Timestamp: now}},
		ReportStatus:      "pending",
		ReportSummary:     "summary-initial",
		StartedAt:         now,
		AnsweredAt:        &answered,
		EndedAt:           &ended,
		UpdatedAt:         now,
	}

	if err := store.CreateOutboundCall(ctx, call, 5); err != nil {
		t.Fatalf("CreateOutboundCall() error = %v", err)
	}

	fetched, err := store.GetCall(ctx, "call-123")
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
	_, err = store.GetCall(ctx, "non-existent")
	if !errors.Is(err, ErrCallNotFound) {
		t.Errorf("GetCall(non-existent) err = %v, want ErrCallNotFound", err)
	}

	// Bind provider SID
	if err := store.BindProviderCallSID(ctx, "call-123", "CA111222333"); err != nil {
		t.Fatalf("BindProviderCallSID() error = %v", err)
	}
	if err := store.BindProviderCallSID(ctx, "non-existent", "CA999"); !errors.Is(err, ErrCallNotFound) {
		t.Errorf("BindProviderCallSID(non-existent) err = %v, want ErrCallNotFound", err)
	}

	fetchedBySID, err := store.GetCallByProviderSID(ctx, "CA111222333")
	if err != nil {
		t.Fatalf("GetCallByProviderSID() error = %v", err)
	}
	if fetchedBySID.CallID != "call-123" {
		t.Errorf("GetCallByProviderSID() CallID = %q, want call-123", fetchedBySID.CallID)
	}

	_, err = store.GetCallByProviderSID(ctx, "CA_nonexistent")
	if !errors.Is(err, ErrCallNotFound) {
		t.Errorf("GetCallByProviderSID(non-existent) err = %v, want ErrCallNotFound", err)
	}

	// Update call state to ringing
	if err := store.UpdateCallState(ctx, "call-123", "ringing", "", ""); err != nil {
		t.Fatalf("UpdateCallState(ringing) error = %v", err)
	}

	// Update call state to in-progress
	if err := store.UpdateCallState(ctx, "call-123", "in-progress", "", "human"); err != nil {
		t.Fatalf("UpdateCallState(in-progress) error = %v", err)
	}
	updatedCall, _ := store.GetCall(ctx, "call-123")
	if updatedCall.State != "in-progress" || updatedCall.AnsweredBy != "human" || updatedCall.AnsweredAt == nil {
		t.Errorf("expected in-progress call with answered_by=human, got: %+v", updatedCall)
	}

	// Append transcript turns
	turn1 := TranscriptTurn{Role: "model", Text: "Hello, this is Prospera", Timestamp: time.Now().UTC()}
	turn2 := TranscriptTurn{Role: "user", Text: "Hi, how can I help?", Timestamp: time.Now().UTC()}
	if err := store.AppendTranscriptTurn(ctx, "call-123", turn1); err != nil {
		t.Fatalf("AppendTranscriptTurn(turn1) error = %v", err)
	}
	if err := store.AppendTranscriptTurn(ctx, "call-123", turn2); err != nil {
		t.Fatalf("AppendTranscriptTurn(turn2) error = %v", err)
	}
	if err := store.AppendTranscriptTurn(ctx, "non-existent", turn1); !errors.Is(err, ErrCallNotFound) {
		t.Errorf("AppendTranscriptTurn(non-existent) err = %v, want ErrCallNotFound", err)
	}

	withTurns, _ := store.GetCall(ctx, "call-123")
	if len(withTurns.Transcript) < 2 {
		t.Errorf("unexpected transcript turns: %+v", withTurns.Transcript)
	}

	// Advance live transcript cursor (CAS)
	advanced, err := store.AdvanceLiveTranscriptCursor(ctx, "call-123", 0, 2)
	if err != nil || !advanced {
		t.Fatalf("AdvanceLiveTranscriptCursor(0 -> 2) err = %v, advanced = %v", err, advanced)
	}
	staleAdvance, err := store.AdvanceLiveTranscriptCursor(ctx, "call-123", 0, 3)
	if err != nil || staleAdvance {
		t.Fatalf("stale AdvanceLiveTranscriptCursor should return false, got advanced=%v, err=%v", staleAdvance, err)
	}

	// Append owner instruction
	instr := OwnerInstruction{Instruction: "Ask about reservations", Mode: "guidance", CreatedAt: time.Now().UTC()}
	if err := store.AppendOwnerInstruction(ctx, "call-123", instr); err != nil {
		t.Fatalf("AppendOwnerInstruction() error = %v", err)
	}
	if err := store.AppendOwnerInstruction(ctx, "non-existent", instr); !errors.Is(err, ErrCallNotFound) {
		t.Errorf("AppendOwnerInstruction(non-existent) err = %v, want ErrCallNotFound", err)
	}

	// Set voicemail recording
	if err := store.SetVoicemailRecording(ctx, "call-123", "https://api.twilio.com/recordings/RE123"); err != nil {
		t.Fatalf("SetVoicemailRecording() error = %v", err)
	}
	if err := store.SetVoicemailRecording(ctx, "non-existent", "https://url"); !errors.Is(err, ErrCallNotFound) {
		t.Errorf("SetVoicemailRecording(non-existent) err = %v, want ErrCallNotFound", err)
	}

	// Update call state to failed
	if err := store.UpdateCallState(ctx, "call-123", "failed", "network_error", ""); err != nil {
		t.Fatalf("UpdateCallState(failed) error = %v", err)
	}

	// Update call state to completed
	if err := store.UpdateCallState(ctx, "call-123", "completed", "completed_normally", ""); err != nil {
		t.Fatalf("UpdateCallState(completed) error = %v", err)
	}
	if err := store.UpdateCallState(ctx, "non-existent", "completed", "", ""); !errors.Is(err, ErrCallNotFound) {
		t.Errorf("UpdateCallState(non-existent) err = %v, want ErrCallNotFound", err)
	}

	endedCall, _ := store.GetCall(ctx, "call-123")
	if endedCall.State != "completed" || endedCall.EndedAt == nil || endedCall.VoicemailRecordingURL != "https://api.twilio.com/recordings/RE123" {
		t.Errorf("unexpected ended call: %+v", endedCall)
	}

	// List calls with default limit
	list, err := store.ListCalls(ctx, 0)
	if err != nil {
		t.Fatalf("ListCalls() error = %v", err)
	}
	if len(list) != 1 {
		t.Errorf("len(list) = %d, want 1", len(list))
	}
}

func TestPGStore_DefaultTimestampsAndEmptySlices(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	ctx := context.Background()

	call := VoiceCall{
		CallID:     "call-zero-times",
		Direction:  "inbound",
		State:      "ringing",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
	}

	if err := store.CreateInboundCall(ctx, call); err != nil {
		t.Fatalf("CreateInboundCall() error = %v", err)
	}

	fetched, err := store.GetCall(ctx, "call-zero-times")
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
	store := setUpTestStore(t)
	ctx := context.Background()

	call1 := VoiceCall{
		CallID:     "call-out-1",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550101",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := store.CreateOutboundCall(ctx, call1, 1); err != nil {
		t.Fatalf("first CreateOutboundCall failed: %v", err)
	}

	call2 := VoiceCall{
		CallID:     "call-out-2",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550102",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := store.CreateOutboundCall(ctx, call2, 1); !errors.Is(err, ErrMaxConcurrentCalls) {
		t.Errorf("CreateOutboundCall over limit err = %v, want ErrMaxConcurrentCalls", err)
	}

	callDup := VoiceCall{
		CallID:     "call-out-dup",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550101",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := store.CreateOutboundCall(ctx, callDup, 10); !errors.Is(err, ErrDuplicateActiveDest) {
		t.Errorf("CreateOutboundCall duplicate dest err = %v, want ErrDuplicateActiveDest", err)
	}

	inbound := VoiceCall{
		CallID:     "call-in-1",
		Direction:  "inbound",
		State:      "ringing",
		FromNumber: "+15105550999",
		ToNumber:   "+15105550100",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := store.CreateInboundCall(ctx, inbound); err != nil {
		t.Fatalf("CreateInboundCall failed: %v", err)
	}

	if err := store.UpdateCallState(ctx, "call-out-1", "completed", "done", ""); err != nil {
		t.Fatalf("UpdateCallState failed: %v", err)
	}
	callNew := VoiceCall{
		CallID:     "call-out-3",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550101",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := store.CreateOutboundCall(ctx, callNew, 5); err != nil {
		t.Fatalf("CreateOutboundCall after completion failed: %v", err)
	}
}

func TestPGStore_FindRecentOutboundCall(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()

	c1 := VoiceCall{
		CallID:     "recent-call",
		Direction:  "outbound",
		State:      "completed",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550111",
		StartedAt:  now.Add(-48 * time.Hour),
		UpdatedAt:  now.Add(-48 * time.Hour),
	}
	c2 := VoiceCall{
		CallID:     "old-call",
		Direction:  "outbound",
		State:      "completed",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550222",
		StartedAt:  now.Add(-240 * time.Hour),
		UpdatedAt:  now.Add(-240 * time.Hour),
	}

	_ = store.CreateInboundCall(ctx, c1)
	_ = store.CreateInboundCall(ctx, c2)

	found, err := store.FindRecentOutboundCall(ctx, "+15105550111", 168*time.Hour)
	if err != nil {
		t.Fatalf("FindRecentOutboundCall error = %v", err)
	}
	if found == nil || found.CallID != "recent-call" {
		t.Errorf("expected recent-call, got %+v", found)
	}

	notFound, err := store.FindRecentOutboundCall(ctx, "+15105550222", 168*time.Hour)
	if err != nil {
		t.Fatalf("FindRecentOutboundCall error = %v", err)
	}
	if notFound != nil {
		t.Errorf("expected nil for 10-day old call, got %+v", notFound)
	}

	unknown, err := store.FindRecentOutboundCall(ctx, "+15105550999", 168*time.Hour)
	if err != nil {
		t.Fatalf("FindRecentOutboundCall error = %v", err)
	}
	if unknown != nil {
		t.Errorf("expected nil for unknown number, got %+v", unknown)
	}
}

func TestPGStore_TransitionReportStatus(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	ctx := context.Background()

	call := VoiceCall{
		CallID:       "call-status-test",
		Direction:    "outbound",
		State:        "in-progress",
		FromNumber:   "+15105550100",
		ToNumber:     "+15105550199",
		ReportStatus: "pending",
		StartedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	_ = store.CreateInboundCall(ctx, call)

	ok, err := store.TransitionReportStatus(ctx, "call-status-test", []string{"pending"}, "processing", "")
	if err != nil || !ok {
		t.Fatalf("TransitionReportStatus(pending -> processing) ok=%v, err=%v", ok, err)
	}

	callVM := VoiceCall{
		CallID:       "call-vm-test",
		Direction:    "inbound",
		State:        "completed",
		FromNumber:   "+15105550888",
		ToNumber:     "+15105550100",
		ReportStatus: "awaiting_recording",
		StartedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	_ = store.CreateInboundCall(ctx, callVM)

	blocked, err := store.TransitionReportStatus(ctx, "call-vm-test", []string{"pending"}, "processing", "")
	if err != nil || blocked {
		t.Fatalf("expected blocked transition when awaiting_recording, got ok=%v, err=%v", blocked, err)
	}

	uncondOk, err := store.TransitionReportStatus(ctx, "call-vm-test", nil, "delivered", "voicemail received")
	if err != nil || !uncondOk {
		t.Fatalf("unconditional transition failed: ok=%v, err=%v", uncondOk, err)
	}
	vmCall, _ := store.GetCall(ctx, "call-vm-test")
	if vmCall.ReportStatus != "delivered" || vmCall.ReportSummary != "voicemail received" {
		t.Errorf("unexpected call after unconditional transition: %+v", vmCall)
	}
}

func TestPGStore_ReconcileOrphanedCalls(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()

	c1 := VoiceCall{
		CallID:     "orphan-no-sid",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550101",
		StartedAt:  now,
		UpdatedAt:  now,
	}
	c2 := VoiceCall{
		CallID:          "active-call",
		ProviderCallSID: "CA_active",
		Direction:       "outbound",
		State:           "in-progress",
		FromNumber:      "+15105550100",
		ToNumber:        "+15105550102",
		StartedAt:       now,
		UpdatedAt:       now,
	}
	c3 := VoiceCall{
		CallID:          "inactive-call",
		ProviderCallSID: "CA_inactive",
		Direction:       "outbound",
		State:           "in-progress",
		FromNumber:      "+15105550100",
		ToNumber:        "+15105550103",
		StartedAt:       now,
		UpdatedAt:       now,
	}
	cErr := VoiceCall{
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
	c4 := VoiceCall{
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

	_ = store.CreateInboundCall(ctx, c1)
	_ = store.CreateInboundCall(ctx, c2)
	_ = store.CreateInboundCall(ctx, c3)
	_ = store.CreateInboundCall(ctx, cErr)
	_ = store.CreateInboundCall(ctx, c4)

	checkActive := func(_ context.Context, providerSID string) (bool, error) {
		if providerSID == "CA_err" {
			return false, sql.ErrConnDone
		}
		if providerSID == "CA_active" {
			return true, nil
		}
		return false, nil
	}

	reconciled, err := store.ReconcileOrphanedCalls(ctx, checkActive)
	if err != nil {
		t.Fatalf("ReconcileOrphanedCalls error = %v", err)
	}
	if reconciled != 2 {
		t.Errorf("reconciled = %d, want 2", reconciled)
	}

	rec1, _ := store.GetCall(ctx, "orphan-no-sid")
	if rec1.State != "failed" || rec1.EndReason != "orphan_no_provider_sid" || rec1.EndedAt == nil {
		t.Errorf("unexpected reconciled c1: %+v", rec1)
	}

	rec3, _ := store.GetCall(ctx, "inactive-call")
	if rec3.State != "completed" || rec3.EndReason != "reconciled_orphan" || rec3.EndedAt == nil {
		t.Errorf("unexpected reconciled c3: %+v", rec3)
	}

	act2, _ := store.GetCall(ctx, "active-call")
	if act2.EndedAt != nil {
		t.Errorf("active call c2 should not be ended: %+v", act2)
	}
}

func TestPGStore_VoiceMessages(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	ctx := context.Background()

	msg := VoiceMessage{
		MessageSID:       "SM123456",
		Direction:        "inbound",
		FromNumber:       "+15105550199",
		ToNumber:         "+15105550100",
		Body:             "Booking confirmed for 7pm",
		MediaURLs:        []string{"https://media.twilio.com/img1.jpg"},
		DiscordDelivered: false,
		CreatedAt:        time.Now().UTC(),
	}

	inserted, err := store.SaveVoiceMessage(ctx, msg)
	if err != nil || !inserted {
		t.Fatalf("SaveVoiceMessage(new) inserted = %v, err = %v", inserted, err)
	}

	dupInserted, err := store.SaveVoiceMessage(ctx, msg)
	if err != nil || dupInserted {
		t.Fatalf("SaveVoiceMessage(duplicate) dupInserted = %v, err = %v", dupInserted, err)
	}

	// Message with empty media and zero created at
	msgZero := VoiceMessage{
		MessageSID: "SM777",
		Direction:  "outbound",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		Body:       "reminder",
	}
	if _, err := store.SaveVoiceMessage(ctx, msgZero); err != nil {
		t.Fatalf("SaveVoiceMessage(msgZero) error = %v", err)
	}

	msgs, err := store.ListVoiceMessages(ctx, 0)
	if err != nil {
		t.Fatalf("ListVoiceMessages() error = %v", err)
	}
	if len(msgs) != 2 {
		t.Errorf("len(msgs) = %d, want 2", len(msgs))
	}

	if err := store.MarkVoiceMessageDelivered(ctx, "SM123456"); err != nil {
		t.Fatalf("MarkVoiceMessageDelivered error = %v", err)
	}
	if err := store.MarkVoiceMessageDelivered(ctx, "non-existent"); !errors.Is(err, ErrMessageNotFound) {
		t.Errorf("MarkVoiceMessageDelivered(non-existent) err = %v, want ErrMessageNotFound", err)
	}

	msgsAfter, _ := store.ListVoiceMessages(ctx, 10)
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
	store := NewPGStore(db)
	_ = db.Close()
	ctx := context.Background()

	_ = store.CreateOutboundCall(ctx, VoiceCall{}, 1)
	_ = store.CreateInboundCall(ctx, VoiceCall{})
	_ = store.BindProviderCallSID(ctx, "id", "sid")
	_, _ = store.GetCall(ctx, "id")
	_, _ = store.GetCallByProviderSID(ctx, "sid")
	_, _ = store.ListCalls(ctx, 10)
	_, _ = store.FindRecentOutboundCall(ctx, "+15105550100", time.Hour)
	_ = store.UpdateCallState(ctx, "id", "in-progress", "", "")
	_ = store.AppendTranscriptTurn(ctx, "id", TranscriptTurn{})
	_, _ = store.AdvanceLiveTranscriptCursor(ctx, "id", 0, 1)
	_ = store.AppendOwnerInstruction(ctx, "id", OwnerInstruction{})
	_ = store.SetVoicemailRecording(ctx, "id", "url")
	_, _ = store.TransitionReportStatus(ctx, "id", nil, "done", "")
	_, _ = store.ReconcileOrphanedCalls(ctx, nil)
	_, _ = store.SaveVoiceMessage(ctx, VoiceMessage{MessageSID: "SM1"})
	_ = store.MarkVoiceMessageDelivered(ctx, "SM1")
	_, _ = store.ListVoiceMessages(ctx, 10)
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
		{name: "postgres scheme", url: "postgres://user:pass@localhost:5432/testdb", wantErr: false},
		{name: "postgresql scheme", url: "postgresql://user:pass@localhost:5432/testdb", wantErr: false},
		{name: "empty url", url: "   ", wantErr: true},
		{name: "unsupported scheme", url: "invalid://url", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, err := OpenDB(tc.url)
			if (err != nil) != tc.wantErr {
				t.Fatalf("OpenDB(%q) err = %v, wantErr = %v", tc.url, err, tc.wantErr)
			}
			if db != nil {
				_ = db.Close()
			}
		})
	}
}

func TestUnmarshalCallJSON(t *testing.T) {
	t.Parallel()
	var c VoiceCall
	if err := unmarshalCallJSON(&c, "{invalid", "", ""); err == nil {
		t.Error("expected error unmarshaling invalid brief JSON")
	}
	if err := unmarshalCallJSON(&c, "", "{invalid", ""); err == nil {
		t.Error("expected error unmarshaling invalid instruction JSON")
	}
	if err := unmarshalCallJSON(&c, "", "", "{invalid"); err == nil {
		t.Error("expected error unmarshaling invalid transcript JSON")
	}
	if err := unmarshalCallJSON(&c, "", "", ""); err != nil {
		t.Errorf("unexpected error on empty JSON strings: %v", err)
	}
}

func TestPGStore_SaveVoiceMessage_Concurrent(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	ctx := context.Background()

	const numGoroutines = 8
	var wg sync.WaitGroup
	var insertedCount int64
	concurMsg := VoiceMessage{
		MessageSID: "SM_CONCURRENT",
		Direction:  "inbound",
		FromNumber: "+15105550111",
		ToNumber:   "+15105550222",
		Body:       "concurrent race test",
	}
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			ins, err := store.SaveVoiceMessage(ctx, concurMsg)
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
	store := setUpTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	callFull := VoiceCall{
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
	if err := store.CreateInboundCall(ctx, callFull); err != nil {
		t.Fatalf("CreateInboundCall(callFull) err: %v", err)
	}

	fetched, err := store.GetCall(ctx, "call-full-fields")
	if err != nil {
		t.Fatalf("GetCall err: %v", err)
	}
	if fetched.AnsweredBy != "human" || fetched.VoicemailRecordingURL != "https://rec.wav" || fetched.ReportSummary != "summary text" {
		t.Fatalf("unexpected fetched call fields: %+v", fetched)
	}

	// Missing call operations
	missingID := "call-missing-999"
	if err := store.AppendTranscriptTurn(ctx, missingID, TranscriptTurn{Role: "user", Text: "hi"}); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("AppendTranscriptTurn missing call: %v, want ErrCallNotFound", err)
	}
	if err := store.AppendOwnerInstruction(ctx, missingID, OwnerInstruction{Instruction: "say hi"}); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("AppendOwnerInstruction missing call: %v, want ErrCallNotFound", err)
	}
	if err := store.SetVoicemailRecording(ctx, missingID, "http://rec"); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("SetVoicemailRecording missing call: %v, want ErrCallNotFound", err)
	}
	if ok, err := store.AdvanceLiveTranscriptCursor(ctx, missingID, 0, 1); err != nil || ok {
		t.Fatalf("AdvanceLiveTranscriptCursor missing call: ok=%v, err=%v", ok, err)
	}
	if err := store.BindProviderCallSID(ctx, missingID, "CA-123"); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("BindProviderCallSID missing call: %v, want ErrCallNotFound", err)
	}
	if err := store.UpdateCallState(ctx, missingID, "completed", "", ""); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("UpdateCallState missing call: %v, want ErrCallNotFound", err)
	}

	if err := store.CreateInboundCall(ctx, callFull); err == nil {
		t.Fatal("expected error inserting duplicate CallID in CreateInboundCall")
	}
	callFullDupOut := callFull
	callFullDupOut.ToNumber = "+15105550998"
	if err := store.CreateOutboundCall(ctx, callFullDupOut, 5); err == nil {
		t.Fatal("expected error inserting duplicate CallID in CreateOutboundCall")
	}

	_, _ = store.db.Exec("UPDATE voice_calls SET transcript = '{bad', owner_instructions = '{bad', brief = '{bad' WHERE call_id = ?", callFull.CallID)
	if err := store.AppendTranscriptTurn(ctx, callFull.CallID, TranscriptTurn{Role: "user", Text: "hi"}); err == nil {
		t.Fatal("expected unmarshal error in AppendTranscriptTurn")
	}
	if err := store.AppendOwnerInstruction(ctx, callFull.CallID, OwnerInstruction{Instruction: "hi"}); err == nil {
		t.Fatal("expected unmarshal error in AppendOwnerInstruction")
	}
	if _, err := store.ListCalls(ctx, 10); err == nil {
		t.Fatal("expected unmarshal error in ListCalls")
	}

	_, _ = store.SaveVoiceMessage(ctx, VoiceMessage{MessageSID: "SM_BAD_JSON", Direction: "inbound", FromNumber: "+1", ToNumber: "+2", Body: "b"})
	_, _ = store.db.Exec("UPDATE voice_messages SET media_urls = 'null' WHERE message_sid = 'SM_BAD_JSON'")
	_, _ = store.ListVoiceMessages(ctx, 10)
	_, _ = store.db.Exec("UPDATE voice_messages SET media_urls = '{bad' WHERE message_sid = 'SM_BAD_JSON'")
	if _, err := store.ListVoiceMessages(ctx, 10); err == nil {
		t.Fatal("expected unmarshal error in ListVoiceMessages")
	}

	store.isSQLite = false
	_ = store.AppendTranscriptTurn(ctx, callFull.CallID, TranscriptTurn{Role: "user", Text: "hi"})
	_ = store.AppendOwnerInstruction(ctx, callFull.CallID, OwnerInstruction{Instruction: "hi"})
}
