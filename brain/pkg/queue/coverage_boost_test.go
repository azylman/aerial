package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/bwmarrin/discordgo"
)

// ---------------------------------------------------------------------------
// 1. history.go Target Coverage
// ---------------------------------------------------------------------------

func TestFetchRecentThreadHistory_EdgeCases(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	ctx := context.Background()

	// 1. Limit clamping: <= 0 and > 100
	t.Run("limit clamping", func(t *testing.T) {
		// dg nil, threadID empty -> fails fallback
		_, err := FetchRecentThreadHistory(ctx, nil, database, "", 0)
		if err == nil {
			t.Errorf("expected error for empty threadID")
		}
		_, err = FetchRecentThreadHistory(ctx, nil, database, "", 150)
		if err == nil {
			t.Errorf("expected error for empty threadID")
		}
	})

	// 2. Local DB fast path
	t.Run("db fast path", func(t *testing.T) {
		now := time.Now().UTC()
		err := db.InsertMessage(database, db.Message{
			ID:         "msg-fast-1",
			ThreadID:   "thread-fast-1",
			AuthorID:   "user-1",
			AuthorName: "Alice",
			Content:    "Hello fast path",
			CreatedAt:  now,
		})
		if err != nil {
			t.Fatalf("InsertMessage failed: %v", err)
		}

		msgs, err := FetchRecentThreadHistory(ctx, nil, database, "thread-fast-1", 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(msgs) != 1 || msgs[0].Content != "Hello fast path" {
			t.Fatalf("unexpected msgs: %+v", msgs)
		}
	})

	// 3. Fallback to Discord API checks
	t.Run("fallback guards", func(t *testing.T) {
		// dg == nil
		_, err := FetchRecentThreadHistory(ctx, nil, database, "123456789012345678", 10)
		if err == nil || !strings.Contains(err.Error(), "no database messages and discord fallback unavailable") {
			t.Errorf("expected fallback unavailable error for dg==nil, got: %v", err)
		}

		dg, _ := discordgo.New("Bot test")
		// threadID empty
		_, err = FetchRecentThreadHistory(ctx, dg, database, "", 10)
		if err == nil || !strings.Contains(err.Error(), "no database messages and discord fallback unavailable") {
			t.Errorf("expected fallback unavailable error for empty threadID, got: %v", err)
		}

		// non-numeric snowflake
		_, err = FetchRecentThreadHistory(ctx, dg, database, "not-numeric", 10)
		if err == nil || !strings.Contains(err.Error(), "no database messages and discord fallback unavailable") {
			t.Errorf("expected fallback unavailable error for non-numeric, got: %v", err)
		}
	})

	// 4. Mock Discord REST ChannelMessages: error and success mappings
	t.Run("discord rest mappings", func(t *testing.T) {
		dg, _ := discordgo.New("Bot test")
		dg.State = discordgo.NewState()
		dg.State.User = &discordgo.User{
			ID:       "bot-user-123",
			Username: "aerial",
			Bot:      true,
		}

		// REST error path
		dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("simulated network failure")
		})
		_, err := FetchRecentThreadHistory(ctx, dg, database, "123456789012345678", 10)
		if err == nil {
			t.Errorf("expected error from ChannelMessages, got nil")
		}

		now := time.Now().UTC()
		rawMessages := []*discordgo.Message{
			nil, // dm == nil test
			{
				ID:        "123456789012345678",
				Content:   "Aerial speaking via botUserID",
				Timestamp: now,
				Author: &discordgo.User{
					ID:       "bot-user-123",
					Username: "some_name",
					Bot:      true,
				},
			},
			{
				ID:        "123456789012345679",
				Content:   "Aerial speaking via username aerial",
				Timestamp: now,
				Author: &discordgo.User{
					ID:       "other-bot-id",
					Username: "Aerial",
					Bot:      true,
				},
			},
			{
				ID:        "123456789012345680",
				Content:   "Other bot message",
				Timestamp: now,
				Author: &discordgo.User{
					ID:       "random-bot-id",
					Username: "MusicBot",
					Bot:      true,
				},
			},
			{
				ID:        "123456789012345681",
				Content:   "Webhook message",
				Timestamp: now,
				WebhookID: "webhook-456",
				Author:    nil,
			},
			{
				ID:        "123456789012345682", // valid snowflake
				Content:   "Zero timestamp with snowflake",
				Timestamp: time.Time{}, // zero
				Author: &discordgo.User{
					ID:       "user-789",
					Username: "Alice",
				},
			},
			{
				ID:        "invalid-snowflake-id", // invalid snowflake -> fallback time.Now()
				Content:   "Zero timestamp with non-snowflake",
				Timestamp: time.Time{}, // zero
				Author: &discordgo.User{
					ID:       "user-890",
					Username: "Bob",
				},
			},
		}

		dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			b, _ := json.Marshal(rawMessages)
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(b)),
			}, nil
		})

		history, err := FetchRecentThreadHistory(ctx, dg, database, "123456789012345678", 50)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(history) != 6 {
			t.Fatalf("expected 6 messages parsed, got %d", len(history))
		}

		if history[0].Role != "Assistant" {
			t.Errorf("expected message 0 to have Assistant role, got %s", history[0].Role)
		}
		if history[1].Role != "Assistant" {
			t.Errorf("expected message 1 to have Assistant role, got %s", history[1].Role)
		}
		if history[2].Role != "Bot" {
			t.Errorf("expected message 2 to have Bot role, got %s", history[2].Role)
		}
		if history[3].Role != "Bot" || history[3].AuthorName != "Webhook" {
			t.Errorf("expected message 3 to be Webhook Bot, got role=%s author=%s", history[3].Role, history[3].AuthorName)
		}
		if history[4].CreatedAt.IsZero() {
			t.Errorf("expected message 4 timestamp to be extracted from snowflake")
		}
		if history[5].CreatedAt.IsZero() {
			t.Errorf("expected message 5 timestamp to fallback to now")
		}
	})
}

func TestDefaultHistoryFetcher_AdditionalEdgeCases(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	ctx := context.Background()

	// 1. ChannelID empty -> falls back to DB
	fetcher := DefaultHistoryFetcher(nil, database)
	msgs, err := fetcher(ctx, "", "", 10)
	if err != nil || len(msgs) != 0 {
		t.Errorf("expected empty result without error for empty channelID, got msgs=%v, err=%v", msgs, err)
	}

	// 2. Limit <= 0 and > 100 with REST API
	dg, _ := discordgo.New("Bot test")
	dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		msgs := []*discordgo.Message{
			nil, // dm == nil test
			{
				ID:        "123456789012345678",
				Content:   "hello",
				Timestamp: time.Time{},
				WebhookID: "wh-1",
				Author:    nil,
			},
			{
				ID:        "invalid-id",
				Content:   "hello 2",
				Timestamp: time.Time{},
				Author: &discordgo.User{
					ID:       "u-1",
					Username: "Alice",
				},
			},
		}
		b, _ := json.Marshal(msgs)
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(b)),
		}, nil
	})

	fetcherREST := DefaultHistoryFetcher(dg, database)
	// beforeID non-snowflake
	res, err := fetcherREST(ctx, "123456789012345678", "non-numeric-before", -5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 messages parsed, got %d", len(res))
	}

	// limit > 100
	res, err = fetcherREST(ctx, "123456789012345678", "", 150)
	if err != nil || len(res) != 2 {
		t.Fatalf("expected 2 messages parsed with limit > 100, got %d", len(res))
	}
}

func TestFetchHistoryFromDB_EdgeCases(t *testing.T) {
	// 1. database == nil -> returns nil, nil
	msgs, err := fetchHistoryFromDB(nil, "chan-1", 10)
	if err != nil || msgs != nil {
		t.Errorf("expected nil, nil for nil database, got msgs=%v err=%v", msgs, err)
	}

	// 2. Database error (closed db)
	closedDB, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	_ = closedDB.Close()
	_, err = fetchHistoryFromDB(closedDB, "chan-1", 10)
	if err == nil {
		t.Errorf("expected error querying closed database")
	}

	// 3. Role mappings
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()
	testCases := []struct {
		authorID     string
		authorName   string
		expectedRole string
	}{
		{"assistant", "custom", "Assistant"},
		{"user-1", "aerial", "Assistant"},
		{"user-2", "assistant", "Assistant"},
		{"bot", "custom", "Bot"},
		{"user-3", "bot", "Bot"},
		{"bot-custom", "custom", "Bot"},
		{"scheduler", "custom", "Bot"},
		{"user-4", "normal_user", "User"},
	}

	for i, tc := range testCases {
		_ = db.InsertMessage(database, db.Message{
			ID:         fmt.Sprintf("msg-role-%d", i),
			ThreadID:   "role-test-chan",
			AuthorID:   tc.authorID,
			AuthorName: tc.authorName,
			Content:    "test content",
			CreatedAt:  now.Add(time.Duration(i) * time.Minute),
		})
	}

	fetched, err := fetchHistoryFromDB(database, "role-test-chan", 50)
	if err != nil {
		t.Fatalf("fetchHistoryFromDB failed: %v", err)
	}
	if len(fetched) != len(testCases) {
		t.Fatalf("expected %d messages, got %d", len(testCases), len(fetched))
	}
	for i, tc := range testCases {
		if fetched[i].Role != tc.expectedRole {
			t.Errorf("msg %d (authorID=%s, authorName=%s): expected role %s, got %s",
				i, tc.authorID, tc.authorName, tc.expectedRole, fetched[i].Role)
		}
	}
}

func TestFormatChannelHistory_AuthorAndRoleSanitization(t *testing.T) {
	now := time.Now().UTC()

	msgs := []HistoryMessage{
		{
			ID:         "msg-1",
			AuthorName: "User[\n\rName]",
			Role:       "",
			Content:    "Hello world",
			CreatedAt:  now.Add(-10 * time.Minute),
		},
		{
			ID:         "msg-2",
			AuthorName: "",
			Role:       "Assistant",
			Content:    "Aerial response",
			CreatedAt:  now.Add(-5 * time.Minute),
		},
		{
			ID:         "msg-3",
			AuthorName: "",
			Role:       "User",
			Content:    "Anonymous user message",
			CreatedAt:  now.Add(-1 * time.Minute),
		},
	}

	formatted := FormatChannelHistory(msgs)
	if strings.Contains(formatted, "User[\n\rName]") {
		t.Errorf("expected brackets and newlines to be stripped from author name")
	}
	if !strings.Contains(formatted, "[@UserName (User)]") {
		t.Errorf("expected sanitized author [@UserName (User)], got:\n%s", formatted)
	}
	if !strings.Contains(formatted, "[Aerial (Assistant)]") {
		t.Errorf("expected empty author with Assistant role to become Aerial, got:\n%s", formatted)
	}
	if !strings.Contains(formatted, "[@User (User)]") {
		t.Errorf("expected empty author with User role to become @User, got:\n%s", formatted)
	}
}

func TestSummarizeThreadHistory_AdditionalEdgeCases(t *testing.T) {
	ctx := context.Background()

	// 1. Empty msgs
	_, err := SummarizeThreadHistory(ctx, nil, "model", "thread-1", nil)
	if err == nil || !strings.Contains(err.Error(), "empty history") {
		t.Errorf("expected 'empty history' error, got: %v", err)
	}

	// 2. All messages expired -> FormatChannelHistory returns ""
	expiredMsgs := []HistoryMessage{
		{
			ID:        "msg-old",
			Content:   "ancient message",
			CreatedAt: time.Now().UTC().Add(-10 * time.Hour),
		},
	}
	_, err = SummarizeThreadHistory(ctx, nil, "model", "thread-1", expiredMsgs)
	if err == nil || !strings.Contains(err.Error(), "no valid history to summarize") {
		t.Errorf("expected 'no valid history to summarize' error, got: %v", err)
	}

	// 3. LLM returns error
	freshMsgs := []HistoryMessage{
		{
			ID:        "msg-fresh",
			Content:   "fresh message",
			CreatedAt: time.Now().UTC().Add(-5 * time.Minute),
		},
	}
	mockErrLLM := func(ctx context.Context, model, prompt string) (string, error) {
		return "", errors.New("simulated LLM generation error")
	}
	_, err = SummarizeThreadHistory(ctx, mockErrLLM, "model", "thread-err-llm", freshMsgs)
	if err == nil || !strings.Contains(err.Error(), "simulated LLM generation error") {
		t.Errorf("expected simulated LLM error, got: %v", err)
	}

	// 4. Successful extraction with surrounding text
	mockSuccessLLM := func(ctx context.Context, model, prompt string) (string, error) {
		return "Prefix banter\n<THREAD_SUMMARY>\nDeliverables: done.\n</THREAD_SUMMARY>\nTrailing banter", nil
	}
	res, err := SummarizeThreadHistory(ctx, mockSuccessLLM, "model", "thread-success-tags", freshMsgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "<THREAD_SUMMARY>\nDeliverables: done.\n</THREAD_SUMMARY>"
	if res != expected {
		t.Errorf("expected %q, got %q", expected, res)
	}
}

// ---------------------------------------------------------------------------
// 2. status_updater.go Target Coverage
// ---------------------------------------------------------------------------

func TestFormatToolStatus_AdditionalBranches(t *testing.T) {
	// Command > 24 runes truncated
	longCmd := "really_long_command_name_that_exceeds_twenty_four_runes"
	formatted := FormatToolStatus("run_command", longCmd, 1500*time.Millisecond)
	expectedCmd := string([]rune(longCmd)[:24])
	if !strings.Contains(formatted, expectedCmd) {
		t.Errorf("expected command to be truncated to %s, got: %s", expectedCmd, formatted)
	}

	// clean in ("execute_command", "bash", "sh")
	formatted = FormatToolStatus("bash", "echo hi", 500*time.Millisecond)
	if !strings.Contains(formatted, "Executing `echo hi`") {
		t.Errorf("expected Executing `echo hi`, got: %s", formatted)
	}

	formatted = FormatToolStatus("sh", "echo hi", 500*time.Millisecond)
	if !strings.Contains(formatted, "Executing `echo hi`") {
		t.Errorf("expected Executing `echo hi`, got: %s", formatted)
	}

	formatted = FormatToolStatus("execute_command", "git", 500*time.Millisecond)
	if !strings.Contains(formatted, "Executing `git`") {
		t.Errorf("expected Executing `git`, got: %s", formatted)
	}

	// Text length exceeding MaxStatusTextLength (100)
	hugeToolName := strings.Repeat("x", 120)
	formatted = FormatToolStatus(hugeToolName, "", 500*time.Millisecond)
	if len([]rune(formatted)) > MaxStatusTextLength {
		t.Errorf("expected length <= %d, got %d", MaxStatusTextLength, len([]rune(formatted)))
	}
}

func TestStatusUpdater_DefaultRESTImplementations(t *testing.T) {
	// 1. Session is nil
	uNil := NewStatusUpdater(nil, "thread-rest-nil", false)
	_, err := uNil.sendFunc("thread-rest-nil", "hello")
	if err == nil || !strings.Contains(err.Error(), "discord session is nil") {
		t.Errorf("expected 'discord session is nil' error, got: %v", err)
	}

	// 2. Session is non-nil with mock transport
	dg, _ := discordgo.New("Bot test-token")
	dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.Method {
		case http.MethodPost:
			msg := &discordgo.Message{
				ID:        "msg-rest-created",
				ChannelID: "thread-rest-live",
				Content:   "hello",
			}
			b, _ := json.Marshal(msg)
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(b)),
			}, nil
		case http.MethodPatch:
			msg := &discordgo.Message{
				ID:        "msg-rest-created",
				ChannelID: "thread-rest-live",
				Content:   "edited",
			}
			b, _ := json.Marshal(msg)
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(b)),
			}, nil
		case http.MethodDelete:
			return &http.Response{
				StatusCode: 204,
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		}
		return &http.Response{StatusCode: 400}, nil
	})

	uLive := NewStatusUpdater(dg, "thread-rest-live", false)
	newID, err := uLive.sendFunc("thread-rest-live", "hello")
	if err != nil || newID != "msg-rest-created" {
		t.Errorf("sendFunc failed: id=%s err=%v", newID, err)
	}
	err = uLive.editFunc("thread-rest-live", newID, "edited")
	if err != nil {
		t.Errorf("editFunc failed: %v", err)
	}
	err = uLive.deleteFunc("thread-rest-live", newID)
	if err != nil {
		t.Errorf("deleteFunc failed: %v", err)
	}
}

func TestStatusUpdater_NilReceiverAndGuards(t *testing.T) {
	var u *StatusUpdater
	// These should not panic
	u.MarkTurnStarted()
	u.Stop()
	u.Reset()
	u.DeleteStatusMessage()
	u.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})

	// Disabled updater should not start
	uDisabled := NewStatusUpdater(nil, "thread-1", false)
	uDisabled.Start() // should return immediately without starting goroutine

	// Start idempotency
	uEnabled := NewStatusUpdater(nil, "thread-2", true, WithStatusInterval(50*time.Millisecond))
	uEnabled.Start() // second call should be no-op
	uEnabled.Stop()
}

func TestStatusUpdater_HandleStep_Branches(t *testing.T) {
	// Guard when disabled
	uDisabled := NewStatusUpdater(nil, "thread-step-disabled", false)
	uDisabled.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	if uDisabled.phase != "" {
		t.Errorf("expected no change when enabled=false")
	}

	u := NewStatusUpdater(nil, "thread-step", true, WithStatusInterval(10*time.Hour))
	u.Stop() // stop background ticker so it doesn't race

	// Step while disabled
	u.disabled = true
	u.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	if u.phase != "" {
		t.Errorf("expected no change when disabled")
	}
	u.disabled = false

	// thinking
	u.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	if u.phase != "thinking" || !u.dirty {
		t.Errorf("expected phase thinking and dirty=true")
	}

	// tool ACTIVE
	u.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool_call",
		ToolName: "view_file",
		State:    "ACTIVE",
		ToolInfo: &runner.StepToolInfo{
			Parameters: runner.StepToolParameters{CommandLine: "ls"},
		},
	})
	if u.phase != "tool" || u.activeTool != "view_file" || u.activeCommand != "ls" {
		t.Errorf("expected activeTool=view_file activeCommand=ls")
	}

	// tool DONE with mismatched toolName -> ignored
	u.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool_call",
		ToolName: "other_tool",
		State:    "DONE",
	})
	if u.activeTool != "view_file" {
		t.Errorf("expected activeTool view_file to remain on mismatched toolName")
	}

	// tool ERROR with matching toolName -> clears to thinking
	u.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool_call",
		ToolName: "view_file",
		State:    "ERROR",
	})
	if u.activeTool != "" || u.phase != "thinking" {
		t.Errorf("expected activeTool cleared and phase thinking, got tool=%s phase=%s", u.activeTool, u.phase)
	}

	// agent_response
	u.HandleStep(&runner.StepUpdateEvent{StepType: "agent_response"})
	if u.phase != "responding" {
		t.Errorf("expected phase responding, got %s", u.phase)
	}

	// text_delta when already responding -> dirty not re-set
	u.dirty = false
	u.HandleStep(&runner.StepUpdateEvent{StepType: "text_delta"})
	if u.dirty {
		t.Errorf("expected dirty=false when already in responding phase")
	}
}

func TestStatusUpdater_CurrentStatusText_Branches(t *testing.T) {
	u := NewStatusUpdater(nil, "thread-status-text", false)

	// 1. Tool phase with empty activeTool falls through to responding
	u.phase = "tool"
	u.activeTool = ""
	text := u.currentStatusText()
	if !strings.Contains(text, "Generating response") {
		t.Errorf("expected fallthrough to Generating response, got: %s", text)
	}

	// 2. Responding phase with elapsed > 0.1s
	u.phase = "responding"
	u.turnStart = time.Now().Add(-500 * time.Millisecond)
	text = u.currentStatusText()
	if !strings.Contains(text, "Generating response") {
		t.Errorf("expected Generating response, got: %s", text)
	}

	// 3. Thinking phase with elapsed > 0.1s
	u.phase = "thinking"
	u.turnStart = time.Now().Add(-500 * time.Millisecond)
	text = u.currentStatusText()
	if !strings.Contains(text, "Thinking") {
		t.Errorf("expected Thinking, got: %s", text)
	}

	// 4. Default phase with elapsed > 0.1s
	u.phase = "unknown_phase"
	u.turnStart = time.Now().Add(-500 * time.Millisecond)
	text = u.currentStatusText()
	if !strings.Contains(text, "Aerial is cooking") {
		t.Errorf("expected Aerial is cooking, got: %s", text)
	}
}

func TestStatusUpdater_FlushEdgeCases(t *testing.T) {
	// 1. Flush when disabled
	u := NewStatusUpdater(nil, "thread-flush", false)
	u.disabled = true
	u.flush() // should return immediately

	// 2. Flush when !dirty and no message
	u.disabled = false
	u.dirty = false
	u.statusMessageID = ""
	u.phase = ""
	u.activeTool = ""
	u.flush()

	// 3. Flush when statusMessageID != "" and text == lastDelivered
	u.statusMessageID = "msg-existing"
	u.phase = "thinking"
	u.turnStart = time.Now()
	u.lastDelivered = u.currentStatusText()
	u.dirty = true
	u.flush()
	if u.dirty {
		t.Errorf("expected dirty to be cleared when text == lastDelivered")
	}

	// 4. Send error with permission error -> disables updater
	u = NewStatusUpdater(nil, "thread-flush-perm", false,
		WithStatusMockFuncs(
			func(channelID, text string) (string, error) {
				return "", errors.New("HTTP 403 Forbidden, 50013 Missing Permissions")
			},
			nil, nil,
		),
	)
	u.phase = "thinking"
	u.dirty = true
	u.turnStart = time.Now().Add(-5 * time.Second) // past debounce
	u.flush()
	if !u.disabled {
		t.Errorf("expected u.disabled=true on 403 error")
	}

	// 5. Send error transient
	u = NewStatusUpdater(nil, "thread-flush-transient", false,
		WithStatusMockFuncs(
			func(channelID, text string) (string, error) {
				return "", errors.New("502 Bad Gateway")
			},
			nil, nil,
		),
	)
	u.phase = "thinking"
	u.dirty = true
	u.turnStart = time.Now().Add(-5 * time.Second)
	u.flush()
	if u.disabled {
		t.Errorf("expected u.disabled=false on transient error")
	}

	// 6. Edit error: thread locked/archived
	u = NewStatusUpdater(nil, "thread-flush-edit-locked", false,
		WithStatusMockFuncs(
			nil,
			func(channelID, messageID, text string) error {
				return errors.New("50083 Thread is archived")
			},
			nil,
		),
	)
	u.statusMessageID = "msg-to-edit"
	u.phase = "thinking"
	u.dirty = true
	u.flush()
	if !u.disabled {
		t.Errorf("expected u.disabled=true on thread archived error during edit")
	}

	// 7. Edit error: transient error re-marks dirty
	u = NewStatusUpdater(nil, "thread-flush-edit-transient", false,
		WithStatusMockFuncs(
			nil,
			func(channelID, messageID, text string) error {
				return errors.New("502 Bad Gateway")
			},
			nil,
		),
	)
	u.statusMessageID = "msg-to-edit"
	u.phase = "thinking"
	u.dirty = true
	u.flush()
	if !u.dirty {
		t.Errorf("expected u.dirty=true on transient edit failure for self-healing")
	}
}

// ---------------------------------------------------------------------------
// 3. queue.go Target Coverage
// ---------------------------------------------------------------------------

func TestChannelCaching_FullCoverage(t *testing.T) {
	// 1. CacheDiscordChannel nil and empty
	CacheDiscordChannel(nil)
	CacheDiscordChannel(&discordgo.Channel{ID: ""})

	// 2. Parent channel caching and inheritance
	parent := &discordgo.Channel{
		ID:      "parent-guild-chan",
		Name:    "general",
		GuildID: "guild-123",
	}
	CacheDiscordChannel(parent)

	child := &discordgo.Channel{
		ID:       "child-thread-chan",
		Name:     "thread-topic",
		GuildID:  "", // empty -> should inherit parent's guild-123
		ParentID: "parent-guild-chan",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	}
	CacheDiscordChannel(child)

	snap, ok := GetCachedChannel("child-thread-chan")
	if !ok || snap.GuildID != "guild-123" || !snap.IsThread {
		t.Errorf("expected child snapshot with inherited GuildID guild-123 and IsThread=true, got: %+v", snap)
	}

	// 3. InvalidateChannelCache
	InvalidateChannelCache("child-thread-chan")
	_, ok = GetCachedChannel("child-thread-chan")
	if ok {
		t.Errorf("expected child-thread-chan to be invalidated")
	}

	// 4. IsNumericSnowflake
	if IsNumericSnowflake("") {
		t.Errorf("expected empty string to not be numeric snowflake")
	}
	if !IsNumericSnowflake("123456789012345678") {
		t.Errorf("expected digits to be numeric snowflake")
	}
	if IsNumericSnowflake("12345a678") {
		t.Errorf("expected alphanumeric string to not be numeric snowflake")
	}
	if IsNumericSnowflake("abc") {
		t.Errorf("expected letters to not be numeric snowflake")
	}
}

func TestResolveChannelSnapshot_And_EffectiveChannel(t *testing.T) {
	// 1. ResolveEffectiveChannel empty
	effID, effName, isTh := ResolveEffectiveChannel(nil, "")
	if effID != "" || effName != "" || isTh {
		t.Errorf("expected empty results for empty channelID")
	}

	// 2. Uncached, s == nil -> returns channelID, "", false
	effID, effName, isTh = ResolveEffectiveChannel(nil, "unknown-channel")
	if effID != "unknown-channel" || effName != "" || isTh {
		t.Errorf("expected fallback to channelID for unknown channel with nil session")
	}

	// 3. Cached non-thread channel
	CacheDiscordChannel(&discordgo.Channel{
		ID:   "chan-cached-norm",
		Name: "norm-chan",
	})
	effID, effName, isTh = ResolveEffectiveChannel(nil, "chan-cached-norm")
	if effID != "chan-cached-norm" || effName != "norm-chan" || isTh {
		t.Errorf("expected norm-chan, got id=%s name=%s isThread=%v", effID, effName, isTh)
	}

	// 4. Cached thread with parent in cache
	CacheDiscordChannel(&discordgo.Channel{
		ID:   "parent-for-thread",
		Name: "parent-name",
	})
	CacheDiscordChannel(&discordgo.Channel{
		ID:       "thread-with-parent",
		Name:     "sub-thread",
		ParentID: "parent-for-thread",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	effID, effName, isTh = ResolveEffectiveChannel(nil, "thread-with-parent")
	if effID != "parent-for-thread" || effName != "parent-name" || !isTh {
		t.Errorf("expected parent ID and name, got effID=%s effName=%s isTh=%v", effID, effName, isTh)
	}

	// 5. Cached thread with parent NOT in cache
	CacheDiscordChannel(&discordgo.Channel{
		ID:       "thread-with-missing-parent",
		Name:     "orphan-thread",
		ParentID: "parent-not-cached",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	effID, effName, isTh = ResolveEffectiveChannel(nil, "thread-with-missing-parent")
	if effID != "parent-not-cached" || effName != "" || !isTh {
		t.Errorf("expected parent ID and empty name, got effID=%s effName=%s isTh=%v", effID, effName, isTh)
	}

	// 6. Cached thread with empty ParentID
	CacheDiscordChannel(&discordgo.Channel{
		ID:   "thread-no-parent-id",
		Name: "no-parent-thread",
		Type: discordgo.ChannelTypeGuildPublicThread,
	})
	effID, effName, isTh = ResolveEffectiveChannel(nil, "thread-no-parent-id")
	if effID != "thread-no-parent-id" || effName != "no-parent-thread" || !isTh {
		t.Errorf("expected snap ID and name, got effID=%s effName=%s isTh=%v", effID, effName, isTh)
	}

	// 7. Resolve via session.State
	dg, _ := discordgo.New("Bot test")
	dg.State = discordgo.NewState()
	_ = dg.State.GuildAdd(&discordgo.Guild{
		ID: "guild-s",
		Channels: []*discordgo.Channel{
			{
				ID:      "chan-in-state",
				Name:    "state-channel-name",
				GuildID: "guild-s",
			},
		},
	})
	effID, effName, _ = ResolveEffectiveChannel(dg, "chan-in-state")
	if effID != "chan-in-state" || effName != "state-channel-name" {
		t.Errorf("expected resolution from State, got effID=%s effName=%s", effID, effName)
	}

	// 8. Resolve via REST singleflight
	dgREST, _ := discordgo.New("Bot test-token")
	dgREST.State = discordgo.NewState()
	dgREST.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		ch := &discordgo.Channel{
			ID:   "998877665544332211",
			Name: "rest-fetched-chan",
		}
		b, _ := json.Marshal(ch)
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(b)),
		}, nil
	})

	effID, effName, _ = ResolveEffectiveChannel(dgREST, "998877665544332211")
	if effID != "998877665544332211" || effName != "rest-fetched-chan" {
		t.Errorf("expected resolution from REST, got effID=%s effName=%s", effID, effName)
	}
}

func TestParseDBTime_AllLayoutsAndTypes(t *testing.T) {
	// nil
	if _, ok := parseDBTime(nil); ok {
		t.Errorf("expected false for nil")
	}

	// time.Time
	now := time.Now().UTC()
	if tOut, ok := parseDBTime(now); !ok || !tOut.Equal(now) {
		t.Errorf("expected time.Time to parse, got %v, %v", tOut, ok)
	}

	// *time.Time nil and non-nil
	var nilTime *time.Time
	if _, ok := parseDBTime(nilTime); ok {
		t.Errorf("expected false for nil *time.Time")
	}
	if tOut, ok := parseDBTime(&now); !ok || !tOut.Equal(now) {
		t.Errorf("expected &now to parse")
	}

	// string empty and spaces
	if _, ok := parseDBTime(""); ok {
		t.Errorf("expected false for empty string")
	}
	if _, ok := parseDBTime("   "); ok {
		t.Errorf("expected false for spaces string")
	}

	// all 10 string layouts
	layouts := []string{
		"2026-09-11T15:04:05.123456789Z",
		"2026-09-11T15:04:05Z",
		"2026-09-11 15:04:05.123456789 -0700 MST",
		"2026-09-11 15:04:05 -0700 MST",
		"2026-09-11 15:04:05.123456789-07:00",
		"2026-09-11 15:04:05.123456789",
		"2026-09-11 15:04:05.123456789Z",
		"2026-09-11 15:04:05-07:00",
		"2026-09-11 15:04:05",
		"2026-09-11T15:04:05",
	}

	for _, l := range layouts {
		if _, ok := parseDBTime(l); !ok {
			t.Errorf("failed to parse layout %q", l)
		}
	}

	// invalid string
	if _, ok := parseDBTime("not-a-valid-timestamp"); ok {
		t.Errorf("expected false for invalid time string")
	}

	// []byte
	if _, ok := parseDBTime([]byte("2026-09-11T15:04:05Z")); !ok {
		t.Errorf("failed to parse []byte time")
	}

	// unsupported type (int)
	if _, ok := parseDBTime(123456789); ok {
		t.Errorf("expected false for int type")
	}
}

func TestCoalesceBurstPrompt_EdgeCases(t *testing.T) {
	// Empty burst
	if got := CoalesceBurstPrompt(nil); got != "" {
		t.Errorf("expected empty string for nil burst, got %q", got)
	}

	// Single message
	single := []db.Message{{Content: "single msg"}}
	if got := CoalesceBurstPrompt(single); got != "single msg" {
		t.Errorf("expected 'single msg', got %q", got)
	}

	// Multiple messages: author with @, author without @, author empty, CreatedAt zero
	burst := []db.Message{
		{
			AuthorName: "@alice",
			CreatedAt:  time.Now().UTC(),
			Content:    "First message",
		},
		{
			AuthorName: "bob",
			CreatedAt:  time.Now().UTC(),
			Content:    "Second message",
		},
		{
			AuthorName: "",
			CreatedAt:  time.Time{}, // zero timestamp
			Content:    "Third message",
		},
	}

	coalesced := CoalesceBurstPrompt(burst)
	if !strings.Contains(coalesced, "<USER_REQUEST>") {
		t.Errorf("expected <USER_REQUEST> in coalesced prompt")
	}
	if !strings.Contains(coalesced, "by @alice") {
		t.Errorf("expected 'by @alice'")
	}
	if !strings.Contains(coalesced, "by @bob") {
		t.Errorf("expected 'by @bob'")
	}
	if !strings.Contains(coalesced, "by @user") {
		t.Errorf("expected 'by @user'")
	}
}

func TestExtractMessageBody_AllVariants(t *testing.T) {
	// 1. Plain text without <USER_REQUEST>
	if got := extractMessageBody("  just plain text  "); got != "just plain text" {
		t.Errorf("expected 'just plain text', got %q", got)
	}

	// 2. Envelope with - content: and delimiters
	delimiters := []string{
		"\n- timestamp: 2026-09-11",
		"\n- mentions: [aerial]",
		"\n- attachments: []",
		"\n- other_field: val",
		"\n</USER_REQUEST>",
	}
	for _, delim := range delimiters {
		envelope := fmt.Sprintf("<USER_REQUEST>\n- author: @user\n- content: Target Body Text%s", delim)
		body := extractMessageBody(envelope)
		if body != "Target Body Text" {
			t.Errorf("delim %q: expected 'Target Body Text', got %q", delim, body)
		}
	}

	// 3. Escaped tags replacement
	escaped := "<USER_REQUEST>\n- content: Check <\\/USER_REQUEST> and <\\USER_REQUEST>\n</USER_REQUEST>"
	body := extractMessageBody(escaped)
	if body != "Check </USER_REQUEST> and <USER_REQUEST>" {
		t.Errorf("expected escaped tags to be restored, got %q", body)
	}

	// 4. Envelope without - content:
	rawEnvelope := "<USER_REQUEST>\nJust body without content marker\n</USER_REQUEST>"
	if got := extractMessageBody(rawEnvelope); got != "Just body without content marker" {
		t.Errorf("expected stripped body, got %q", got)
	}
}

func TestResolveBotRoleIDs_AllVariants(t *testing.T) {
	// sess == nil
	if roles := ResolveBotRoleIDs(nil, "guild-1", "bot-1"); roles != nil {
		t.Errorf("expected nil for nil sess")
	}

	// sess.State == nil
	dg, _ := discordgo.New("Bot test")
	if roles := ResolveBotRoleIDs(dg, "guild-1", "bot-1"); roles != nil {
		t.Errorf("expected nil for nil State")
	}

	// Populated State
	dg.State = discordgo.NewState()
	dg.State.User = &discordgo.User{ID: "bot-1", Username: "AerialBot"}

	guild1 := &discordgo.Guild{
		ID: "guild-1",
		Roles: []*discordgo.Role{
			nil, // nil role
			{ID: "role-aerial", Name: "aerial"},
			{ID: "role-gundam", Name: "gundam"},
			{ID: "role-username", Name: "AerialBot"},
			{ID: "role-other", Name: "moderator"},
		},
		Members: []*discordgo.Member{
			{
				User:  &discordgo.User{ID: "bot-1"},
				Roles: []string{"role-member-assigned", ""},
			},
		},
	}
	_ = dg.State.GuildAdd(guild1)

	// With guildID specified
	roles := ResolveBotRoleIDs(dg, "guild-1", "bot-1")
	expected := []string{"role-aerial", "role-gundam", "role-member-assigned", "role-username"}
	if len(roles) != len(expected) {
		t.Fatalf("expected %d roles, got %v", len(expected), roles)
	}

	// With guildID empty (iterates all guilds)
	rolesAll := ResolveBotRoleIDs(dg, "", "bot-1")
	if len(rolesAll) != len(expected) {
		t.Fatalf("expected %d roles across all guilds, got %v", len(expected), rolesAll)
	}
}

func TestIsTier1Wake_AllVariants(t *testing.T) {
	// 1. System authors / schedule runs
	if !isTier1Wake(db.Message{AuthorID: "http-client"}, "", nil, "") {
		t.Errorf("expected http-client to wake")
	}
	if !isTier1Wake(db.Message{AuthorID: "scheduler"}, "", nil, "") {
		t.Errorf("expected scheduler to wake")
	}
	if !isTier1Wake(db.Message{ScheduleRunID: "run-123"}, "", nil, "") {
		t.Errorf("expected ScheduleRunID to wake")
	}

	// 2. Direct user mentions in content
	if !isTier1Wake(db.Message{Content: "Hey <@bot-123>"}, "bot-123", nil, "") {
		t.Errorf("expected <@bot-123> to wake")
	}
	if !isTier1Wake(db.Message{Content: "Hey <@!bot-123>"}, "bot-123", nil, "") {
		t.Errorf("expected <@!bot-123> to wake")
	}

	// 3. Direct role mentions
	if !isTier1Wake(db.Message{Content: "Alert <@&role-999>"}, "", []string{"role-999"}, "") {
		t.Errorf("expected <@&role-999> to wake")
	}

	// 4. - mentions: [ ... ]
	if !isTier1Wake(db.Message{Content: "<USER_REQUEST>\n- mentions: [aerial, alice]\n</USER_REQUEST>"}, "", nil, "") {
		t.Errorf("expected - mentions: [aerial] to wake")
	}
	if !isTier1Wake(db.Message{Content: "<USER_REQUEST>\n- mentions: [bot-123]\n</USER_REQUEST>"}, "bot-123", nil, "") {
		t.Errorf("expected - mentions: [bot-123] to wake")
	}
	if !isTier1Wake(db.Message{Content: "<USER_REQUEST>\n- mentions: [role-999]\n</USER_REQUEST>"}, "", []string{"role-999"}, "") {
		t.Errorf("expected - mentions: [role-999] to wake")
	}

	// 5. - replying_to: with author
	replyingAerial := "<USER_REQUEST>\n- replying_to:\n  author: aerial\n  content: hi\n- content: yes</USER_REQUEST>"
	if !isTier1Wake(db.Message{Content: replyingAerial}, "", nil, "") {
		t.Errorf("expected replying to aerial to wake")
	}
	replyingBotID := "<USER_REQUEST>\n- replying_to:\n  author: bot-123\n  content: hi\n- content: yes</USER_REQUEST>"
	if !isTier1Wake(db.Message{Content: replyingBotID}, "bot-123", nil, "") {
		t.Errorf("expected replying to bot-123 to wake")
	}

	// 6. Body mentions
	if !isTier1Wake(db.Message{Content: "Hey <@aerial how are you"}, "", nil, "") {
		t.Errorf("expected <@aerial to wake")
	}
	if !isTier1Wake(db.Message{Content: "Hey <@!aerial how are you"}, "", nil, "") {
		t.Errorf("expected <@!aerial to wake")
	}

	// 7. wakeMode == "mention" suppresses plain keywords
	if isTier1Wake(db.Message{Content: "hello aerial"}, "", nil, "mention") {
		t.Errorf("expected wakeMode=mention to suppress plaintext keyword")
	}

	// 8. Plain keywords vs exclusions
	if isTier1Wake(db.Message{Content: "what a nice aerial view of the city"}, "", nil, "keyword") {
		t.Errorf("expected 'aerial view' to be excluded")
	}
	if isTier1Wake(db.Message{Content: "take an aerial photo please"}, "", nil, "keyword") {
		t.Errorf("expected 'aerial photo' to be excluded")
	}
	if !isTier1Wake(db.Message{Content: "hello aerial help me"}, "", nil, "keyword") {
		t.Errorf("expected 'aerial' keyword to wake")
	}
	if !isTier1Wake(db.Message{Content: "launch the gundam unit"}, "", nil, "keyword") {
		t.Errorf("expected 'gundam' keyword to wake")
	}
	if isTier1Wake(db.Message{Content: "hello world unrelated"}, "", nil, "keyword") {
		t.Errorf("expected unrelated text to not wake")
	}
}

func TestRecoverInterrupted_AllBranches(t *testing.T) {
	// 1. database == nil or pool == nil
	RecoverInterrupted(nil, nil)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	RecoverInterrupted(database, nil)

	// 2. Reconcile orphaned schedule runs
	_ = db.CreateScheduleRun(database, db.ScheduleRun{
		ID:           "orphan-run-1",
		ScheduleID:   "sched-1",
		ScheduleType: "cron",
		Prompt:       "prompt",
		StartedAt:    time.Now().UTC().Add(-10 * time.Minute),
		Status:       "enqueued",
	})

	// Pool setup
	pool := New(nil, WorkerPoolConfig{
		DB:           database,
		MaxAttempts:  3,
		DrainTimeout: 50 * time.Millisecond,
		IdleTimeout:  50 * time.Millisecond,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "", 0, nil
		},
	})
	defer pool.Stop()

	// 3. No messages in database
	RecoverInterrupted(database, pool)

	// 4. Poison pill messages:
	// a) restart_count >= DefaultMaxRestarts (3) with http-client
	err = db.InsertMessage(database, db.Message{
		ID:           "poison-http",
		ThreadID:     "thread-poison-http",
		AuthorID:     "http-client",
		Content:      "long poison message exceeding sixty characters in total length to verify truncation behavior",
		Status:       db.StatusProcessing,
		RestartCount: 3,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC().Add(-5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	// b) retry_count >= maxAttempts (3) with discord user
	var deliveredPoison atomic.Int32
	pool.cfg.DeliveryFunc = func(s *discordgo.Session, channelID, text string) error {
		deliveredPoison.Add(1)
		return nil
	}

	err = db.InsertMessage(database, db.Message{
		ID:           "poison-discord",
		ThreadID:     "thread-poison-discord",
		AuthorID:     "user-1",
		Content:      "short poison",
		Status:       db.StatusProcessing,
		RestartCount: 1,
		RetryCount:   3,
		CreatedAt:    time.Now().UTC().Add(-4 * time.Minute),
	})
	if err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	// c) Normal processing message (should be reset to pending and recovered)
	err = db.InsertMessage(database, db.Message{
		ID:           "normal-interrupted",
		ThreadID:     "thread-normal",
		AuthorID:     "user-1",
		Content:      "normal prompt",
		Status:       db.StatusProcessing,
		RestartCount: 0,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC().Add(-3 * time.Minute),
	})
	if err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	RecoverInterrupted(database, pool)

	if deliveredPoison.Load() != 1 {
		t.Errorf("expected 1 poison pill delivery for discord user, got %d", deliveredPoison.Load())
	}

	// Verify status in DB
	msgPoisonHTTP, _ := db.GetMessage(database, "poison-http")
	if msgPoisonHTTP.Status != db.StatusFailed {
		t.Errorf("expected poison-http to be FAILED, got %s", msgPoisonHTTP.Status)
	}

	msgPoisonDiscord, _ := db.GetMessage(database, "poison-discord")
	if msgPoisonDiscord.Status != db.StatusFailed {
		t.Errorf("expected poison-discord to be FAILED, got %s", msgPoisonDiscord.Status)
	}

	// 5. Query error path
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	RecoverInterrupted(closedDB, pool)
}

func TestWorkerPool_NewPermutations(t *testing.T) {
	// 1. appCfg == nil with cfg.Model, LowEffortModel, SystemPrompt
	pool1 := New(nil, WorkerPoolConfig{
		Model:          "custom-model",
		LowEffortModel: "custom-flash",
		SystemPrompt:   "custom-prompt",
		TimeoutMinutes: 0,
		MaxAttempts:    0,
		BackoffBase:    0,
		StalenessTTL:   0,
		DrainTimeout:   0,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "out", "", 0, nil
		},
	})
	if pool1.cfg.Model != "custom-model" {
		t.Errorf("expected custom-model, got %s", pool1.cfg.Model)
	}
	if pool1.cfg.TimeoutMinutes != DefaultTimeoutMinutes {
		t.Errorf("expected DefaultTimeoutMinutes, got %d", pool1.cfg.TimeoutMinutes)
	}
	if pool1.cfg.MaxAttempts != 3 {
		t.Errorf("expected MaxAttempts=3, got %d", pool1.cfg.MaxAttempts)
	}
	if pool1.cfg.BackoffBase != 3*time.Second {
		t.Errorf("expected BackoffBase=3s, got %v", pool1.cfg.BackoffBase)
	}
	if pool1.cfg.StalenessTTL != 30*time.Minute {
		t.Errorf("expected StalenessTTL=30m, got %v", pool1.cfg.StalenessTTL)
	}
	if pool1.cfg.DrainTimeout != 10*time.Second {
		t.Errorf("expected DrainTimeout=10s, got %v", pool1.cfg.DrainTimeout)
	}

	// Test generated RunnerWithOptionsFunc with various watchdog options
	ctx := context.Background()
	_, _, _, err := pool1.cfg.RunnerWithOptionsFunc(ctx, "bin", "prompt", "sess", "key", "model", runner.WatchdogOptions{
		MaxDuration: 2 * time.Minute,
	})
	if err != nil {
		t.Errorf("unexpected error from RunnerWithOptionsFunc: %v", err)
	}
	_, _, _, err = pool1.cfg.RunnerWithOptionsFunc(ctx, "bin", "prompt", "sess", "key", "model", runner.WatchdogOptions{
		MaxDuration:       0,
		InactivityTimeout: 3 * time.Minute,
	})
	if err != nil {
		t.Errorf("unexpected error from RunnerWithOptionsFunc: %v", err)
	}
	_, _, _, err = pool1.cfg.RunnerWithOptionsFunc(ctx, "bin", "prompt", "sess", "key", "model", runner.WatchdogOptions{
		MaxDuration:       0,
		InactivityTimeout: 0,
	})
	if err != nil {
		t.Errorf("unexpected error from RunnerWithOptionsFunc: %v", err)
	}

	// 2. RunnerWithOptionsFunc provided, RunnerFunc nil
	pool2 := New(nil, WorkerPoolConfig{
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			return "opt-out", "", 0, nil
		},
	})
	out, _, _, err := pool2.cfg.RunnerFunc(ctx, "bin", "prompt", "sess", "key", "model", 15)
	if err != nil || out != "opt-out" {
		t.Errorf("expected opt-out, got out=%s err=%v", out, err)
	}

	// 3. Both runner funcs nil
	pool3 := New(nil, WorkerPoolConfig{})
	_, _, _, err = pool3.cfg.RunnerFunc(ctx, "bin", "prompt", "sess", "key", "model", 10)
	if err == nil || !strings.Contains(err.Error(), "RunnerFunc not configured") {
		t.Errorf("expected 'RunnerFunc not configured' error, got: %v", err)
	}
	_, _, _, err = pool3.cfg.RunnerWithOptionsFunc(ctx, "bin", "prompt", "sess", "key", "model", runner.WatchdogOptions{})
	if err == nil || !strings.Contains(err.Error(), "RunnerWithOptionsFunc not configured") {
		t.Errorf("expected 'RunnerWithOptionsFunc not configured' error, got: %v", err)
	}

	// 4. DeliveryWithAttachmentsFunc defaulting when DeliveryFunc is set
	var calledDelivery atomic.Int32
	pool4 := New(nil, WorkerPoolConfig{
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			calledDelivery.Add(1)
			return nil
		},
	})
	_ = pool4.cfg.DeliveryWithAttachmentsFunc(nil, "chan", "text", nil)
	if calledDelivery.Load() != 1 {
		t.Errorf("expected DeliveryFunc called from DeliveryWithAttachmentsFunc wrapper")
	}

	// 5. Classifier OnParseError callback testing
	appCfg := config.NewFromData(&config.ConfigData{
		SystemChannel: "test-alerts-chan",
		APIKey:        "test-key",
		AgyBin:        "agy",
	})
	dg, _ := discordgo.New("Bot test")
	var alertSent atomic.Int32
	pool5 := New(appCfg, WorkerPoolConfig{
		DiscordSession: dg,
		SystemAlertFunc: func(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
			alertSent.Add(1)
			return nil
		},
	})

	if pool5.cfg.Classifier != nil && pool5.cfg.Classifier.OnParseError != nil {
		longOutput := strings.Repeat("x", 700) + "\n```json\n@everyone @here\n```"
		pool5.cfg.Classifier.OnParseError("gemini-flash", longOutput, errors.New("parse error 1"))
		time.Sleep(50 * time.Millisecond)

		// Second call within 15s debounced
		pool5.cfg.Classifier.OnParseError("gemini-flash", "short", errors.New("parse error 2"))
		time.Sleep(50 * time.Millisecond)

		if alertSent.Load() != 1 {
			t.Errorf("expected exactly 1 alert sent due to debouncing, got %d", alertSent.Load())
		}
	}

	// 6. ResolveChannelPolicy defaulting with appCfg and without appCfg
	pol := pool5.cfg.ResolveChannelPolicy("c1", "general")
	if pol.Mode == "" && pol.WakeMode == "" {
		// valid struct
	}

	pool6 := New(nil, WorkerPoolConfig{})
	_ = pool6.cfg.ResolveChannelPolicy("c1", "general")

	// 7. HistoryFetcher default wrapper
	_, _ = pool6.cfg.HistoryFetcher(context.Background(), "c1", "", 10)
}

func TestWorkerPool_GettersAndStop(t *testing.T) {
	pool := New(nil, WorkerPoolConfig{
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "", 0, nil
		},
	})

	// Getters
	if pool.Classifier() == nil {
		t.Errorf("expected non-nil Classifier")
	}
	if pool.SessionManager() == nil {
		t.Errorf("expected non-nil SessionManager")
	}

	// Start
	pool.Start()

	// StopWithTimeout
	pool.StopWithTimeout(0) // should use default DrainTimeout
	// Second Stop call should return immediately
	pool.StopWithTimeout(10 * time.Millisecond)
}

func TestGetSessionLastActivity_ErrorAndEdgeCases(t *testing.T) {
	// 1. database == nil or threadID empty
	act, isCold, err := GetSessionLastActivity(nil, "thread-1")
	if err != nil || !isCold || !act.IsZero() {
		t.Errorf("expected zero, cold, nil for nil db")
	}
	act, isCold, err = GetSessionLastActivity(nil, "")
	if err != nil || !isCold || !act.IsZero() {
		t.Errorf("expected zero, cold, nil for empty threadID")
	}

	// 2. Query error on sessions table (closed database)
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	_, _, err = GetSessionLastActivity(closedDB, "thread-1")
	if err == nil {
		t.Errorf("expected error querying closed database")
	}

	// 3. Disk activity later than DB updated_at
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	tmpDir := t.TempDir()
	sessMgr := session.New(filepath.Join(tmpDir, ".gemini"), tmpDir)

	now := time.Now().UTC()
	// Insert session with turn_count > 0
	_, err = database.Exec(`INSERT INTO sessions (thread_id, internal_session_id, turn_count, updated_at)
		VALUES ($1, $2, $3, $4)`, "thread-disk-test", "sess-disk-1", 2, now.Add(-10*time.Minute).Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insert session failed: %v", err)
	}

	act, isCold, err = GetSessionLastActivity(database, "thread-disk-test", sessMgr)
	if err != nil {
		t.Fatalf("GetSessionLastActivity failed: %v", err)
	}
	if isCold {
		t.Errorf("expected isCold=false for session with turns")
	}
	if act.IsZero() {
		t.Errorf("expected non-zero activity")
	}
}

func TestWorkerPool_EnqueueSlowPathAndWorkerIdle(t *testing.T) {
	pool := New(nil, WorkerPoolConfig{
		IdleTimeout:  30 * time.Millisecond,
		DrainTimeout: 50 * time.Millisecond,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "", 0, nil
		},
	})

	// 1. Enqueue to stopped pool
	pool.stopped = true
	pool.Enqueue(db.Message{ID: "m-stopped", ThreadID: "t-stopped"})
	pool.stopped = false

	// 2. Saturate buffer and trigger context cancellation on slow path
	// Create worker state directly
	state := &threadWorkerState{ch: make(chan db.Message, 1)}
	state.ch <- db.Message{ID: "m-fill"} // channel is full
	pool.threadChs["thread-full"] = state

	// Cancel pool context
	pool.cancel()

	// Enqueue should hit the slow path and exit via <-p.ctx.Done()
	pool.Enqueue(db.Message{ID: "m-blocked", ThreadID: "thread-full"})

	pool.Stop()
}

func TestProcessBurst_AdditionalEdgeCases(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	// 1. Empty burst or cancelled context
	pool := New(nil, WorkerPoolConfig{
		DB: database,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "All good"), "", 0, nil
		},
	})
	defer pool.Stop()

	pool.processBurst(nil)

	pool.cancel()
	pool.processBurst([]db.Message{{ID: "m1", ThreadID: "t1"}})

	// 2. Global Quota Pause Lockout in processBurst
	poolQuota := New(nil, WorkerPoolConfig{
		DB: database,
		// currentAPIKey is empty -> OAuth mode
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "OK"), "", 0, nil
		},
	})
	defer poolQuota.Stop()

	// Insert pending message with ScheduleRunID
	now := time.Now().UTC()
	_ = db.CreateScheduleRun(database, db.ScheduleRun{
		ID:           "sched-run-quota",
		ScheduleID:   "sched-1",
		ScheduleType: "cron",
		Prompt:       "prompt",
		StartedAt:    now,
		Status:       "running",
	})
	_ = db.InsertMessage(database, db.Message{
		ID:            "msg-quota-pause",
		ThreadID:      "thread-quota-pause",
		AuthorID:      "user-1",
		ScheduleRunID: "sched-run-quota",
		Content:       "help me",
		Status:        db.StatusPending,
		CreatedAt:     now,
	})

	// Lock quota for 2 minutes in the future
	poolQuota.quotaLockedUntil.Store(now.Add(2 * time.Minute).Unix())

	var onCompletedStatus string
	poolQuota.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) {
		onCompletedStatus = finalStatus
	}

	msg, _ := db.GetMessage(database, "msg-quota-pause")
	poolQuota.processBurst([]db.Message{*msg})

	if onCompletedStatus != db.StatusFailed {
		t.Errorf("expected StatusFailed on quota pause lockout, got %s", onCompletedStatus)
	}

	var runStatus string
	_ = database.QueryRow(`SELECT status FROM schedule_runs WHERE id = $1`, "sched-run-quota").Scan(&runStatus)
	if runStatus != "failed" {
		t.Errorf("expected schedule run to be failed on quota pause, got %s", runStatus)
	}
}

type mockClaimStore struct {
	db.Store
	claimErr error
	claimed  bool
}

func (m *mockClaimStore) ClaimPendingMessage(ctx context.Context, id string) (bool, error) {
	return m.claimed, m.claimErr
}

func TestQueue_TargetedCoveragePush(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	// 1. StatusUpdater default sendFunc and text branches
	t.Run("StatusUpdater defaults", func(t *testing.T) {
		s, _ := discordgo.New("Bot fake")
		up := NewStatusUpdater(s, "thread-def", false)
		if _, err := up.sendFunc("c", "t"); err == nil {
			t.Errorf("expected error from default sendFunc with dummy session")
		}
		// Future turnStart exercises sec < 0.1 -> sec = 0.1 in currentStatusText
		up.turnStart = time.Now().Add(10 * time.Second)
		txt := up.currentStatusText()
		if !strings.Contains(txt, "0.1s") {
			t.Errorf("expected 0.1s in status text, got %s", txt)
		}

		// EditFunc returning message not found error
		up.phase = "thinking"
		up.dirty = true
		up.lastDelivered = "old text"
		up.statusMessageID = "msg-edit-404"
		up.editFunc = func(c, m, txt string) error {
			return errors.New("HTTP 404 Not Found, 10008 Unknown Message")
		}
		up.flush()
		if !up.disabled || up.statusMessageID != "" {
			t.Errorf("expected updater to be disabled and msgID cleared on 404")
		}
	})

	// 2. Classifier OnParseError handler
	t.Run("Classifier OnParseError", func(t *testing.T) {
		pool1 := New(nil, WorkerPoolConfig{DB: database})
		if pool1.cfg.Classifier != nil && pool1.cfg.Classifier.OnParseError != nil {
			// sess == nil
			pool1.cfg.Classifier.OnParseError("model-1", "output ``` @everyone @here", errors.New("syntax error"))
		}

		pool2 := New(nil, WorkerPoolConfig{DB: database})
		if pool2.cfg.Classifier != nil && pool2.cfg.Classifier.OnParseError != nil {
			dg, _ := discordgo.New("Bot fake")
			pool2.SetDiscordSession(dg)
			pool2.cfg.SystemAlertFunc = func(s *discordgo.Session, ch, title, text string) error {
				panic("deliberate panic in alert handler")
			}
			pool2.cfg.Classifier.OnParseError("model-1", strings.Repeat("a", 700)+"``` @everyone", errors.New("syntax error"))
			time.Sleep(30 * time.Millisecond)
		}
	})

	// 3. ClaimPendingMessage branches
	t.Run("ClaimPendingMessage branches", func(t *testing.T) {
		store := db.NewSQLStore(database)
		pool := New(nil, WorkerPoolConfig{
			DB:    database,
			Store: &mockClaimStore{Store: store, claimErr: errors.New("db lock error")},
		})
		// claimErr != nil -> continue
		pool.processBurst([]db.Message{{ID: "m-err-claim", ThreadID: "t-claim"}})

		// !claimed -> continue
		pool2 := New(nil, WorkerPoolConfig{
			DB:    database,
			Store: &mockClaimStore{Store: store, claimed: false},
		})
		pool2.processBurst([]db.Message{{ID: "m-unclaimed", ThreadID: "t-claim"}})
	})

	// 4. Stale message drop with ScheduleRunID
	t.Run("dropBurstAsStale with ScheduleRun", func(t *testing.T) {
		runID := "run-stale-burst-1"
		_ = db.CreateScheduleRun(database, db.ScheduleRun{
			ID:           runID,
			ScheduleID:   "sched-stale",
			ScheduleType: "cron",
			StartedAt:    time.Now().UTC().Add(-2 * time.Hour),
			Status:       "running",
		})
		msgID := "msg-stale-burst-1"
		_ = db.InsertMessage(database, db.Message{
			ID:            msgID,
			ThreadID:      "thread-stale-burst",
			AuthorID:      "user-1",
			ScheduleRunID: runID,
			Content:       "old prompt",
			Status:        db.StatusPending,
			CreatedAt:     time.Now().UTC().Add(-2 * time.Hour),
		})
		pool := New(nil, WorkerPoolConfig{DB: database})
		m, _ := db.GetMessage(database, msgID)
		pool.processBurst([]db.Message{*m})

		var runErr string
		_ = database.QueryRow(`SELECT error FROM schedule_runs WHERE id = $1`, runID).Scan(&runErr)
		if runErr != "[EXPIRED_STALE]" {
			t.Errorf("expected [EXPIRED_STALE] error, got %s", runErr)
		}
	})

	// 5. Channel policy modes and wakeMode variants
	t.Run("Channel policy modes and wakeModes", func(t *testing.T) {
		baseCfg := WorkerPoolConfig{
			DB: database,
			RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
				return mockJSONResponse("", "Done"), "", 0, nil
			},
		}

		// wakeMode == "all"
		pAll := New(nil, baseCfg)
		pAll.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all"}
		}
		_ = db.InsertMessage(database, db.Message{ID: "m-all-1", ThreadID: "t-all", AuthorID: "u1", Content: "c1", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
		_ = db.InsertMessage(database, db.Message{ID: "m-all-2", ThreadID: "t-all", AuthorID: "u1", Content: "c2", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
		m1, _ := db.GetMessage(database, "m-all-1")
		m2, _ := db.GetMessage(database, "m-all-2")
		pAll.processBurst([]db.Message{*m1, *m2})

		ptrF := func(v float64) *float64 { return &v }

		// threshold <= 0
		pZero := New(nil, baseCfg)
		pZero.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrF(0.0)}
		}
		_ = db.InsertMessage(database, db.Message{ID: "m-zero-1", ThreadID: "t-zero", AuthorID: "u1", Content: "c1", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
		mz, _ := db.GetMessage(database, "m-zero-1")
		pZero.processBurst([]db.Message{*mz})

		// allSkip (heuristic skip)
		pSkip := New(nil, baseCfg)
		pSkip.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrF(0.8)}
		}
		_ = db.InsertMessage(database, db.Message{ID: "m-skip-1", ThreadID: "t-skip", AuthorID: "u1", Content: "lol haha", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
		ms, _ := db.GetMessage(database, "m-skip-1")
		pSkip.processBurst([]db.Message{*ms})

		// Classifier nil
		pNoClass := New(nil, baseCfg)
		pNoClass.cfg.Classifier = nil
		pNoClass.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrF(0.8)}
		}
		_ = db.InsertMessage(database, db.Message{ID: "m-noclass-1", ThreadID: "t-noclass", AuthorID: "u1", Content: "deep complex query", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
		mnc, _ := db.GetMessage(database, "m-noclass-1")
		pNoClass.processBurst([]db.Message{*mnc})
	})

	// 6. Leading ambient, active wake, and trailing wake/ambient messages with ScheduleRunID
	t.Run("Leading and trailing message partitioning", func(t *testing.T) {
		runLead := "run-lead-part"
		_ = db.CreateScheduleRun(database, db.ScheduleRun{ID: runLead, ScheduleID: "s1", ScheduleType: "cron", StartedAt: time.Now().UTC(), Status: "running"})

		_ = db.InsertMessage(database, db.Message{ID: "m-lead", ThreadID: "t-part", AuthorID: "u1", ScheduleRunID: runLead, Content: "ambient chatter", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
		_ = db.InsertMessage(database, db.Message{ID: "m-wake", ThreadID: "t-part", AuthorID: "u1", Content: "<@aerial> wake up!", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
		_ = db.InsertMessage(database, db.Message{ID: "m-trail-amb", ThreadID: "t-part", AuthorID: "u1", Content: "more chatter", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
		_ = db.InsertMessage(database, db.Message{ID: "m-trail-wake", ThreadID: "t-part", AuthorID: "u1", Content: "<@aerial> second command", Status: db.StatusPending, CreatedAt: time.Now().UTC()})

		ml, _ := db.GetMessage(database, "m-lead")
		mw, _ := db.GetMessage(database, "m-wake")
		mta, _ := db.GetMessage(database, "m-trail-amb")
		mtw, _ := db.GetMessage(database, "m-trail-wake")

		pool := New(nil, WorkerPoolConfig{
			DB: database,
			RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
				return mockJSONResponse("", "Processed wake"), "", 0, nil
			},
		})
		pool.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "mention"}
		}

		pool.processBurst([]db.Message{*ml, *mw, *mta, *mtw})

		var leadStatus string
		_ = database.QueryRow(`SELECT status FROM schedule_runs WHERE id = $1`, runLead).Scan(&leadStatus)
		if leadStatus != "completed" {
			t.Errorf("expected schedule run to be completed, got lead=%s", leadStatus)
		}
	})

	// 7. ParseAgyOutput failure in processBurst
	t.Run("ParseAgyOutput failure", func(t *testing.T) {
		_ = db.InsertMessage(database, db.Message{ID: "m-badparse", ThreadID: "t-badparse", AuthorID: "u1", Content: "hello", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
		mb, _ := db.GetMessage(database, "m-badparse")

		pool := New(nil, WorkerPoolConfig{
			DB:          database,
			MaxAttempts: 1,
			RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
				return "definitely not json and no markers", "", 0, nil
			},
		})
		pool.processBurst([]db.Message{*mb})

		msg, _ := db.GetMessage(database, "m-badparse")
		if msg.Status != db.StatusFailed {
			t.Errorf("expected message to fail on parse error, got %s", msg.Status)
		}
	})

	// 8. Rate limit failure with ScheduleRunID
	t.Run("Rate limit failure with ScheduleRun", func(t *testing.T) {
		runRate := "run-ratelimit-1"
		_ = db.CreateScheduleRun(database, db.ScheduleRun{ID: runRate, ScheduleID: "s-rate", ScheduleType: "cron", StartedAt: time.Now().UTC(), Status: "running"})
		_ = db.InsertMessage(database, db.Message{
			ID:            "m-rate-1",
			ThreadID:      "t-rate",
			AuthorID:      "u1",
			ScheduleRunID: runRate,
			Content:       "query",
			Status:        db.StatusPending,
			CreatedAt:     time.Now().UTC(),
		})
		mr, _ := db.GetMessage(database, "m-rate-1")

		pool := New(nil, WorkerPoolConfig{
			DB:          database,
			BackoffBase: 1 * time.Millisecond,
			RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
				return "", "Resource has been exhausted: rate limit exceeded 429", 1, errors.New("exit 1")
			},
		})
		pool.processBurst([]db.Message{*mr})

		var runStatus string
		_ = database.QueryRow(`SELECT status FROM schedule_runs WHERE id = $1`, runRate).Scan(&runStatus)
		if runStatus != "failed" {
			t.Errorf("expected schedule run to fail on rate limit, got %s", runStatus)
		}
	})

	// 9. isTier1Wake body mention and replying_to non-matching variants
	t.Run("isTier1Wake additional variants", func(t *testing.T) {
		mRole := db.Message{Content: "<USER_REQUEST>\n- content: check this <@&999888>\n</USER_REQUEST>"}
		if !isTier1Wake(mRole, "123", []string{"999888"}, "mention") {
			t.Errorf("expected role mention in body to wake")
		}

		mRep := db.Message{Content: "<USER_REQUEST>\n- replying_to:\n  author: @random_user\n- content: hi\n</USER_REQUEST>"}
		if isTier1Wake(mRep, "123", nil, "mention") {
			t.Errorf("expected non-matching replying_to author to not wake")
		}

		mBodyBot := db.Message{Content: "<USER_REQUEST>\n- content: please help <@123>\n</USER_REQUEST>"}
		if !isTier1Wake(mBodyBot, "123", nil, "mention") {
			t.Errorf("expected botUserID in body to wake")
		}

		mBodyAerial := db.Message{Content: "<USER_REQUEST>\n- content: hello <@aerial>\n</USER_REQUEST>"}
		if !isTier1Wake(mBodyAerial, "123", nil, "mention") {
			t.Errorf("expected <@aerial> in body to wake")
		}
	})
}

func TestQueue_CoverageFinalSprint(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	// 1. resolveChannelSnapshot empty
	snap, ok := resolveChannelSnapshot(nil, "")
	if ok || snap.ID != "" {
		t.Errorf("expected false and empty snapshot for empty channelID")
	}

	// 2. extractMessageBody without markers
	bodyNoMarkers := extractMessageBody("<USER_REQUEST>- content: simple text without delimiter")
	if bodyNoMarkers != "simple text without delimiter" {
		t.Errorf("expected trimmed content, got %q", bodyNoMarkers)
	}

	// 3. ResolveBotRoleIDs nil guild in slice
	s, _ := discordgo.New("Bot fake")
	s.State.GuildAdd(&discordgo.Guild{ID: "g-test"})
	s.State.Guilds = append(s.State.Guilds, nil)
	_ = ResolveBotRoleIDs(s, "", "bot-123")

	// 4. isTier1Wake replying_to without author line
	mRepNoAuth := db.Message{Content: "<USER_REQUEST>\n- replying_to:\n- mentions: [user]\n- content: hi\n</USER_REQUEST>"}
	if isTier1Wake(mRepNoAuth, "bot-123", nil, "mention") {
		t.Errorf("expected false for replying_to without author line")
	}

	// 5. GetSessionLastActivity closed db error
	dbClosed, _ := db.InitDB(":memory:")
	_ = dbClosed.Close()
	if _, _, err := GetSessionLastActivity(dbClosed, "thread-closed"); err == nil {
		t.Errorf("expected error from GetSessionLastActivity on closed db")
	}

	// 6. runThreadWorker idle timer fire and channel close
	p := New(nil, WorkerPoolConfig{DB: database, IdleTimeout: 5 * time.Millisecond})
	stIdle := &threadWorkerState{ch: make(chan db.Message, 1)}
	p.mu.Lock()
	p.threadChs["t-idle-sprint"] = stIdle
	p.mu.Unlock()
	p.wg.Add(1)
	go p.runThreadWorker("t-idle-sprint", stIdle)
	time.Sleep(25 * time.Millisecond) // wait for idle timeout to fire and clean up

	stClose := &threadWorkerState{ch: make(chan db.Message)}
	p.wg.Add(1)
	go p.runThreadWorker("t-close-sprint", stClose)
	close(stClose.ch)
	p.wg.Wait()

	// 7. Enqueue on stopped pool
	p.Stop()
	p.Enqueue(db.Message{ID: "m-enqueue-stopped"})

	// 8. Policy ignored with ScheduleRunID
	runIgnored := "run-ignored-sprint"
	_ = db.CreateScheduleRun(database, db.ScheduleRun{ID: runIgnored, ScheduleID: "s-ign", ScheduleType: "cron", StartedAt: time.Now().UTC(), Status: "running"})
	_ = db.InsertMessage(database, db.Message{
		ID:            "m-ign-sprint",
		ThreadID:      "t-ign-sprint",
		AuthorID:      "u1",
		ScheduleRunID: runIgnored,
		Content:       "ignored prompt",
		Status:        db.StatusPending,
		CreatedAt:     time.Now().UTC(),
	})
	mIgn, _ := db.GetMessage(database, "m-ign-sprint")

	pIgn := New(nil, WorkerPoolConfig{
		DB:           database,
		StalenessTTL: -1, // exercises stalenessTTL <= 0 -> 30m default
	})
	pIgn.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "ignore"}
	}
	pIgn.processBurst([]db.Message{*mIgn})

	var ignStatus string
	_ = database.QueryRow(`SELECT status FROM schedule_runs WHERE id = $1`, runIgnored).Scan(&ignStatus)
	if ignStatus != "completed" {
		t.Errorf("expected ignored schedule run to be completed, got %s", ignStatus)
	}

	// 9. Default ResolveChannelPolicy fallback when nil
	_ = db.InsertMessage(database, db.Message{
		ID:        "m-def-pol",
		ThreadID:  "t-def-pol",
		AuthorID:  "u1",
		Content:   "test prompt",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	})
	mDef, _ := db.GetMessage(database, "m-def-pol")
	pDefPol := New(nil, WorkerPoolConfig{
		DB: database,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "OK"), "", 0, nil
		},
	})
	pDefPol.cfg.ResolveChannelPolicy = nil // exercises nil ResolveChannelPolicy fallback
	pDefPol.processBurst([]db.Message{*mDef})

	// 10. StopWithTimeout with drainTimeout <= 0 and p.cfg.DrainTimeout <= 0
	pDrain := New(nil, WorkerPoolConfig{DB: database, DrainTimeout: -1})
	pDrain.StopWithTimeout(0)

	// 11. Trailing classifier without configured classifier
	ptrF2 := func(v float64) *float64 { return &v }
	pTrailNoClass := New(nil, WorkerPoolConfig{
		DB: database,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "OK"), "", 0, nil
		},
	})
	pTrailNoClass.cfg.Classifier = nil
	pTrailNoClass.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: ptrF2(0.5)}
	}
	_ = db.InsertMessage(database, db.Message{ID: "m-w-lead", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "<@aerial> start", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	_ = db.InsertMessage(database, db.Message{ID: "m-trail-query", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "why is the ocean blue", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mwLead, _ := db.GetMessage(database, "m-w-lead")
	mtQuery, _ := db.GetMessage(database, "m-trail-query")
	pTrailNoClass.processBurst([]db.Message{*mwLead, *mtQuery})

	// 12. getTurnHistory slicing when turnHistoryFetched && len(turnHistory) > limit
	var hist15 []HistoryMessage
	for i := 0; i < 15; i++ {
		hist15 = append(hist15, HistoryMessage{
			ID:         fmt.Sprintf("hist-%d", i),
			AuthorName: "User",
			Role:       "User",
			Content:    fmt.Sprintf("history message %d", i),
			CreatedAt:  time.Now().UTC().Add(-time.Duration(20-i) * time.Minute),
		})
	}

	pHistSlice := New(nil, WorkerPoolConfig{
		DB: database,
		HistoryFetcher: func(ctx context.Context, channelID, beforeID string, limit int) ([]HistoryMessage, error) {
			return hist15, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "OK"), "", 0, nil
		},
	})
	_ = db.InsertMessage(database, db.Message{
		ID:        "m-hist-slice",
		ThreadID:  "t-hist-slice-123",
		AuthorID:  "u1",
		Content:   "<@aerial> hello with history",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	})
	mHistSlice, _ := db.GetMessage(database, "m-hist-slice")
	pHistSlice.processBurst([]db.Message{*mHistSlice})

	// 13. Staleness fail-open on database error during GetSessionLastActivity
	staleDB, _ := db.InitDB(":memory:")
	msgStale := db.Message{
		ID:        "m-stale-failopen",
		ThreadID:  "t-stale-failopen",
		CreatedAt: time.Now().UTC().Add(-40 * time.Minute),
		Status:    db.StatusPending,
		Content:   "stale content",
	}
	pStale := New(nil, WorkerPoolConfig{
		DB:          staleDB,
		BackoffBase: 1 * time.Millisecond,
		Store:       &mockClaimStore{Store: db.NewSQLStore(staleDB), claimed: true},
	})
	_ = staleDB.Close() // Force GetSessionLastActivity to return error
	pStale.processBurst([]db.Message{msgStale})
}



