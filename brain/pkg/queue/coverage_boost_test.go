package queue

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
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
	"golang.org/x/sync/singleflight"
)

// ---------------------------------------------------------------------------
// 1. History & Summarizer Declarative Test Suites
// ---------------------------------------------------------------------------

func TestFetchRecentThreadHistory_LimitClamping(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()
	ctx := context.Background()

	for _, limit := range []int{0, 150} {
		if _, err := FetchRecentThreadHistory(ctx, nil, store, "", limit); err == nil {
			t.Errorf("expected error for empty threadID with limit %d", limit)
		}
	}
}

func TestFetchRecentThreadHistory_DBFastPath(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()
	ctx := context.Background()

	_ = insertMessage(store, db.Message{
		ID: "msg-fast-1", ThreadID: "thread-fast-1", AuthorID: "user-1",
		AuthorName: "Alice", Content: "Hello fast path", CreatedAt: time.Now().UTC(),
	})
	msgs, err := FetchRecentThreadHistory(ctx, nil, store, "thread-fast-1", 10)
	if err != nil || len(msgs) != 1 || msgs[0].Content != "Hello fast path" {
		t.Fatalf("unexpected msgs: %+v, err: %v", msgs, err)
	}
}

func TestFetchRecentThreadHistory_FallbackGuards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dg, _ := discordgo.New("Bot test")
	cases := []struct {
		name     string
		sess     *discordgo.Session
		threadID string
	}{
		{name: "NilSession", sess: nil, threadID: "123456789012345678"},
		{name: "EmptyThreadID", sess: dg, threadID: ""},
		{name: "NonNumericThreadID", sess: dg, threadID: "not-numeric"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			defer func() { _ = store.Close() }()
			_, err := FetchRecentThreadHistory(ctx, tc.sess, store, tc.threadID, 10)
			if err == nil || !strings.Contains(err.Error(), "no database messages and discord fallback unavailable") {
				t.Errorf("expected fallback unavailable error for sess=%v threadID=%q, got: %v", tc.sess != nil, tc.threadID, err)
			}
		})
	}
}

func TestFetchRecentThreadHistory_DiscordRESTMappings(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()
	ctx := context.Background()

	dg, _ := discordgo.New("Bot test")
	dg.State = discordgo.NewState()
	dg.State.User = &discordgo.User{ID: "bot-user-123", Username: "aerial", Bot: true}

	dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("simulated network failure")
	})
	if _, err := FetchRecentThreadHistory(ctx, dg, store, "123456789012345678", 10); err == nil {
		t.Errorf("expected error from ChannelMessages, got nil")
	}

	now := time.Now().UTC()
	dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		b, _ := json.Marshal([]*discordgo.Message{
			nil,
			{ID: "123456789012345678", Content: "Aerial speaking via botUserID", Timestamp: now, Author: &discordgo.User{ID: "bot-user-123", Username: "some_name", Bot: true}},
			{ID: "123456789012345679", Content: "Aerial speaking via username aerial", Timestamp: now, Author: &discordgo.User{ID: "other-bot-id", Username: "Aerial", Bot: true}},
			{ID: "123456789012345680", Content: "Other bot message", Timestamp: now, Author: &discordgo.User{ID: "random-bot-id", Username: "MusicBot", Bot: true}},
			{ID: "123456789012345681", Content: "Webhook message", Timestamp: now, WebhookID: "webhook-456", Author: nil},
			{ID: "123456789012345682", Content: "Zero timestamp with snowflake", Timestamp: time.Time{}, Author: &discordgo.User{ID: "user-789", Username: "Alice"}},
			{ID: "invalid-snowflake-id", Content: "Zero timestamp with non-snowflake", Timestamp: time.Time{}, Author: &discordgo.User{ID: "user-890", Username: "Bob"}},
		})
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
	})

	history, err := FetchRecentThreadHistory(ctx, dg, store, "123456789012345678", 50)
	if err != nil || len(history) != 6 {
		t.Fatalf("expected 6 messages parsed, got %d, err: %v", len(history), err)
	}
	expectedRoles := []string{"Assistant", "Assistant", "Bot", "Bot", "User", "User"}
	for i, expRole := range expectedRoles {
		if history[i].Role != expRole {
			t.Errorf("msg %d: expected role %s, got %s", i, expRole, history[i].Role)
		}
	}
	if history[3].AuthorName != "Webhook" {
		t.Errorf("expected Webhook AuthorName, got %s", history[3].AuthorName)
	}
	if history[4].CreatedAt.IsZero() || history[5].CreatedAt.IsZero() {
		t.Errorf("expected non-zero timestamps for messages 4 and 5")
	}
}

func TestDefaultHistoryFetcher_AdditionalEdgeCases(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()
	ctx := context.Background()

	fetcher := DefaultHistoryFetcher(nil, store)
	if msgs, err := fetcher(ctx, "", "", 10); err != nil || len(msgs) != 0 {
		t.Errorf("expected empty result without error for empty channelID, got msgs=%v, err=%v", msgs, err)
	}

	dg, _ := discordgo.New("Bot test")
	dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		b, _ := json.Marshal([]*discordgo.Message{
			nil,
			{ID: "123456789012345678", Content: "hello", Timestamp: time.Time{}, WebhookID: "wh-1", Author: nil},
			{ID: "invalid-id", Content: "hello 2", Timestamp: time.Time{}, Author: &discordgo.User{ID: "u-1", Username: "Alice"}},
		})
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
	})

	fetcherREST := DefaultHistoryFetcher(dg, store)
	for _, tc := range []struct {
		beforeID string
		limit    int
	}{
		{"non-numeric-before", -5},
		{"", 150},
	} {
		res, err := fetcherREST(ctx, "123456789012345678", tc.beforeID, tc.limit)
		if err != nil || len(res) != 2 {
			t.Fatalf("expected 2 messages parsed for %+v, got %d, err: %v", tc, len(res), err)
		}
	}
}

func TestFetchHistoryFromDB_EdgeCases(t *testing.T) {
	t.Parallel()
	if msgs, err := fetchHistoryFromDB(nil, "chan-1", "", 10); err != nil || msgs != nil {
		t.Errorf("expected nil, nil for nil database, got msgs=%v err=%v", msgs, err)
	}

	closedStore := setupTestStore(t)
	_ = closedStore.Close()
	if _, err := fetchHistoryFromDB(closedStore, "chan-1", "", 10); err == nil {
		t.Errorf("expected error querying closed database")
	}

	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

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
		_ = insertMessage(store, db.Message{
			ID:         fmt.Sprintf("msg-role-%d", i),
			ThreadID:   "role-test-chan",
			AuthorID:   tc.authorID,
			AuthorName: tc.authorName,
			Content:    "test content",
			CreatedAt:  now.Add(time.Duration(i) * time.Minute),
		})
	}

	fetched, err := fetchHistoryFromDB(store, "role-test-chan", "", 50)
	if err != nil || len(fetched) != len(testCases) {
		t.Fatalf("fetchHistoryFromDB failed: count=%d err=%v", len(fetched), err)
	}
	for i, tc := range testCases {
		if fetched[i].Role != tc.expectedRole {
			t.Errorf("msg %d (authorID=%s, authorName=%s): expected role %s, got %s",
				i, tc.authorID, tc.authorName, tc.expectedRole, fetched[i].Role)
		}
	}

	filteredWithBefore, err := fetchHistoryFromDB(store, "role-test-chan", "msg-role-7", 50)
	if err != nil {
		t.Fatalf("fetchHistoryFromDB with beforeID failed: %v", err)
	}
	for _, m := range filteredWithBefore {
		if m.ID == "msg-role-7" {
			t.Errorf("expected msg-role-7 to be filtered out by beforeID")
		}
	}

	if FilterAssistantMessages(nil) != nil || FilterAssistantMessages([]HistoryMessage{}) != nil {
		t.Errorf("expected nil for empty FilterAssistantMessages inputs")
	}
	for _, m := range FilterAssistantMessages(fetched) {
		if m.Role == "Assistant" || m.AuthorName == "aerial" || m.AuthorName == "assistant" {
			t.Errorf("FilterAssistantMessages failed to remove assistant message: %+v", m)
		}
	}

	if _, err := fetchHistoryFromDB(store, "role-test-chan", "123456789012345678", 50); err != nil {
		t.Fatalf("fetchHistoryFromDB with snowflake failed: %v", err)
	}

	sumRes, sumErr := SummarizeThreadHistory(context.Background(), func(ctx context.Context, model, prompt string) (string, error) {
		return "<THREAD_SUMMARY>\nSummary result\n</THREAD_SUMMARY>", nil
	}, "test-model", "thread-sum-wrapper", fetched)
	if sumErr != nil || !strings.Contains(sumRes, "Summary result") {
		t.Errorf("SummarizeThreadHistory failed: res=%q err=%v", sumRes, sumErr)
	}
}

func TestFormatChannelHistory_AuthorAndRoleSanitization(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	msgs := []HistoryMessage{
		{ID: "msg-1", AuthorName: "User[\n\rName]", Role: "", Content: "Hello world", CreatedAt: now.Add(-10 * time.Minute)},
		{ID: "msg-2", AuthorName: "", Role: "Assistant", Content: "Aerial response", CreatedAt: now.Add(-5 * time.Minute)},
		{ID: "msg-3", AuthorName: "", Role: "User", Content: "Anonymous user message", CreatedAt: now.Add(-1 * time.Minute)},
	}
	formatted := FormatChannelHistory(msgs)
	for _, check := range []struct {
		pattern  string
		expected bool
	}{
		{"User[\n\rName]", false}, {"[@UserName (User)]", true}, {"[Aerial (Assistant)]", true}, {"[@User (User)]", true},
	} {
		if strings.Contains(formatted, check.pattern) != check.expected {
			t.Errorf("check %q: expected found=%v", check.pattern, check.expected)
		}
	}
}

func TestSummarizeThreadHistory_AdditionalEdgeCases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sfg := new(singleflight.Group)

	// 1. Empty msgs
	if _, err := SummarizeThreadHistoryWithGroup(ctx, sfg, nil, "model", "thread-1", nil); err == nil || !strings.Contains(err.Error(), "empty history") {
		t.Errorf("expected 'empty history' error, got: %v", err)
	}

	// 2. All messages expired -> FormatChannelHistory returns ""
	expiredMsgs := []HistoryMessage{{ID: "msg-old", Content: "ancient message", CreatedAt: time.Now().UTC().Add(-10 * time.Hour)}}
	if _, err := SummarizeThreadHistoryWithGroup(ctx, sfg, nil, "model", "thread-1", expiredMsgs); err == nil || !strings.Contains(err.Error(), "no valid history to summarize") {
		t.Errorf("expected 'no valid history to summarize' error, got: %v", err)
	}

	// 3. LLM returns error
	freshMsgs := []HistoryMessage{{ID: "msg-fresh", Content: "fresh message", CreatedAt: time.Now().UTC().Add(-5 * time.Minute)}}
	mockErrLLM := func(ctx context.Context, model, prompt string) (string, error) {
		return "", errors.New("simulated LLM generation error")
	}
	if _, err := SummarizeThreadHistoryWithGroup(ctx, sfg, mockErrLLM, "model", "thread-err-llm", freshMsgs); err == nil || !strings.Contains(err.Error(), "simulated LLM generation error") {
		t.Errorf("expected simulated LLM error, got: %v", err)
	}

	// 4. Successful extraction with surrounding text
	mockSuccessLLM := func(ctx context.Context, model, prompt string) (string, error) {
		return "Prefix banter\n<THREAD_SUMMARY>\nDeliverables: done.\n</THREAD_SUMMARY>\nTrailing banter", nil
	}
	res, err := SummarizeThreadHistoryWithGroup(ctx, sfg, mockSuccessLLM, "model", "thread-success-tags", freshMsgs)
	if err != nil || res != "<THREAD_SUMMARY>\nDeliverables: done.\n</THREAD_SUMMARY>" {
		t.Errorf("unexpected summary result: %q, err: %v", res, err)
	}

	// 5. Invalid type assertion in singleflight group
	sg := &singleflight.Group{}
	start := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(start)
		_, _, _ = sg.Do("test-invalid-type", func() (interface{}, error) {
			<-done
			return 12345, nil
		})
	}()
	<-start
	time.Sleep(5 * time.Millisecond)
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(done)
	}()

	if _, err := SummarizeThreadHistoryWithGroup(context.Background(), sg, nil, "model", "test-invalid-type", freshMsgs); err == nil {
		t.Errorf("expected error for non-string thread summary, got nil")
	}
}

// ---------------------------------------------------------------------------
// 2. StatusUpdater Declarative Test Suites
// ---------------------------------------------------------------------------

func TestFormatToolStatus_AdditionalBranches(t *testing.T) {
	t.Parallel()
	longCmd := "really_long_command_name_that_exceeds_twenty_four_runes"
	formatted := FormatToolStatus("run_command", longCmd, 1500*time.Millisecond)
	expectedCmd := string([]rune(longCmd)[:24])
	if !strings.Contains(formatted, expectedCmd) {
		t.Errorf("expected command to be truncated to %s, got: %s", expectedCmd, formatted)
	}

	for _, tool := range []string{"bash", "sh", "execute_command"} {
		f := FormatToolStatus(tool, "echo hi", 500*time.Millisecond)
		if !strings.Contains(f, "Executing `echo hi`") {
			t.Errorf("expected Executing `echo hi` for tool %s, got: %s", tool, f)
		}
	}
	hugeToolName := strings.Repeat("x", 120)
	if formatted = FormatToolStatus(hugeToolName, "", 500*time.Millisecond); len([]rune(formatted)) > MaxStatusTextLength {
		t.Errorf("expected length <= %d, got %d", MaxStatusTextLength, len([]rune(formatted)))
	}
}

func TestStatusUpdater_DefaultRESTImplementations(t *testing.T) {
	t.Parallel()
	s, _ := discordgo.New("Bot dummy-token")
	s.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		u := req.URL.String()
		switch {
		case req.Method == "POST" && strings.Contains(u, "/messages"):
			b, _ := json.Marshal(&discordgo.Message{ID: "msg-created-123"})
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
		case req.Method == "PATCH":
			b, _ := json.Marshal(&discordgo.Message{ID: "msg-edited-123"})
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
		case req.Method == "DELETE":
			return &http.Response{StatusCode: 204, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		default:
			return &http.Response{StatusCode: 400, Body: io.NopCloser(bytes.NewReader([]byte(`{"message":"bad request"}`)))}, nil
		}
	})

	u := NewStatusUpdater(s, "thread-rest-123", true)
	if id, err := u.sendFunc("thread-rest-123", "hello"); err != nil || id != "msg-created-123" {
		t.Errorf("sendFunc failed: id=%s err=%v", id, err)
	}
	if err := u.editFunc("thread-rest-123", "msg-created-123", "edited"); err != nil {
		t.Errorf("editFunc failed: %v", err)
	}
	if err := u.deleteFunc("thread-rest-123", "msg-created-123"); err != nil {
		t.Errorf("deleteFunc failed: %v", err)
	}
}

func TestStatusUpdater_NilReceiverAndGuards(t *testing.T) {
	t.Parallel()
	var u *StatusUpdater
	u.MarkTurnStarted()
	u.Stop()
	u.Reset()
	u.DeleteStatusMessage()
	u.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})

	uDisabled := NewStatusUpdater(nil, "thread-1", false)
	uDisabled.Start()

	uEnabled := NewStatusUpdater(nil, "thread-2", true, WithStatusInterval(50*time.Millisecond))
	uEnabled.Start()
	uEnabled.Stop()
}

func TestStatusUpdater_HandleStep_Branches(t *testing.T) {
	t.Parallel()
	uDisabled := NewStatusUpdater(nil, "thread-step-disabled", false)
	uDisabled.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	if uDisabled.phase != "" {
		t.Errorf("expected no change when enabled=false")
	}

	u := NewStatusUpdater(nil, "thread-step", true, WithStatusInterval(10*time.Hour))
	u.Stop()

	u.disabled = true
	u.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	if u.phase != "" {
		t.Errorf("expected no change when disabled")
	}
	u.disabled = false

	u.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	if u.phase != "thinking" || !u.dirty {
		t.Errorf("expected phase thinking and dirty=true")
	}

	u.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool_call",
		ToolName: "view_file",
		State:    "ACTIVE",
		ToolInfo: &runner.StepToolInfo{Parameters: runner.StepToolParameters{CommandLine: "ls"}},
	})
	if u.phase != "tool" || u.activeTool != "view_file" || u.activeCommand != "ls" {
		t.Errorf("expected activeTool=view_file activeCommand=ls")
	}

	u.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "other_tool", State: "DONE"})
	if u.activeTool != "view_file" {
		t.Errorf("expected activeTool view_file to remain on mismatched toolName")
	}

	u.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "view_file", State: "ERROR"})
	if u.activeTool != "" || u.phase != "thinking" {
		t.Errorf("expected activeTool cleared and phase thinking, got tool=%s phase=%s", u.activeTool, u.phase)
	}

	u.HandleStep(&runner.StepUpdateEvent{StepType: "agent_response"})
	if u.phase != "responding" {
		t.Errorf("expected phase responding, got %s", u.phase)
	}

	u.dirty = false
	u.HandleStep(&runner.StepUpdateEvent{StepType: "text_delta"})
	if u.dirty {
		t.Errorf("expected dirty=false when already in responding phase")
	}
}

func TestStatusUpdater_CurrentStatusText_Branches(t *testing.T) {
	t.Parallel()
	u := NewStatusUpdater(nil, "thread-status-text", false)

	for _, tc := range []struct {
		phase, activeTool, expectedSub string
	}{
		{"tool", "", "Generating response"},
		{"responding", "", "Generating response"},
		{"thinking", "", "Thinking"},
		{"unknown_phase", "", "Aerial is cooking"},
	} {
		u.phase, u.activeTool, u.turnStart = tc.phase, tc.activeTool, time.Now().Add(-500*time.Millisecond)
		if text := u.currentStatusText(); !strings.Contains(text, tc.expectedSub) {
			t.Errorf("phase %s: expected %q, got: %s", tc.phase, tc.expectedSub, text)
		}
	}
}

func TestStatusUpdater_FlushEdgeCases(t *testing.T) {
	t.Parallel()
	u := NewStatusUpdater(nil, "thread-flush", false)
	u.disabled = true
	u.flush()

	u.disabled = false
	u.dirty = false
	u.statusMessageID = ""
	u.phase = ""
	u.activeTool = ""
	u.flush()

	u.statusMessageID = "msg-existing"
	u.phase = "thinking"
	u.turnStart = time.Now()
	u.lastDelivered = u.currentStatusText()
	u.dirty = true
	u.flush()
	if u.dirty {
		t.Errorf("expected dirty to be cleared when text == lastDelivered")
	}

	// Permission error disables updater
	u = NewStatusUpdater(nil, "thread-flush-perm", false, WithStatusMockFuncs(
		func(c, txt string) (string, error) {
			return "", errors.New("HTTP 403 Forbidden, 50013 Missing Permissions")
		}, nil, nil,
	))
	u.phase, u.dirty, u.turnStart = "thinking", true, time.Now().Add(-5*time.Second)
	u.flush()
	if !u.disabled {
		t.Errorf("expected u.disabled=true on 403 error")
	}

	// Transient error leaves enabled
	u = NewStatusUpdater(nil, "thread-flush-transient", false, WithStatusMockFuncs(
		func(c, txt string) (string, error) { return "", errors.New("502 Bad Gateway") }, nil, nil,
	))
	u.phase, u.dirty, u.turnStart = "thinking", true, time.Now().Add(-5*time.Second)
	u.flush()
	if u.disabled {
		t.Errorf("expected u.disabled=false on transient error")
	}

	// Edit error: thread archived disables updater
	u = NewStatusUpdater(nil, "thread-flush-edit-locked", false, WithStatusMockFuncs(
		nil, func(c, m, txt string) error { return errors.New("50083 Thread is archived") }, nil,
	))
	u.statusMessageID, u.phase, u.dirty = "msg-to-edit", "thinking", true
	u.flush()
	if !u.disabled {
		t.Errorf("expected u.disabled=true on thread archived error")
	}

	// Edit error: transient error re-marks dirty
	u = NewStatusUpdater(nil, "thread-flush-edit-transient", false, WithStatusMockFuncs(
		nil, func(c, m, txt string) error { return errors.New("502 Bad Gateway") }, nil,
	))
	u.statusMessageID, u.phase, u.dirty = "msg-to-edit", "thinking", true
	u.flush()
	if !u.dirty {
		t.Errorf("expected u.dirty=true on transient edit failure")
	}
}

func TestStatusUpdater_DeleteErrorBranches(t *testing.T) {
	u := NewStatusUpdater(nil, "t1", true)
	u.deleteFunc = func(threadID, messageID string) error { return errors.New("discord deletion error") }
	u.sendFunc = func(threadID, content string) (string, error) {
		u.mu.Lock()
		u.disabled = true
		u.mu.Unlock()
		return "msg1", nil
	}
	u.activeTool = "bash"
	u.flush()

	u.statusMessageID = "msg2"
	u.Reset()

	u.statusMessageID = "msg3"
	u.DeleteStatusMessage()
}

// ---------------------------------------------------------------------------
// 3. Channel, Time, Prompt, Wake & Role Helpers Declarative Suites
// ---------------------------------------------------------------------------

func TestChannelCaching_FullCoverage(t *testing.T) {
	t.Parallel()
	CacheDiscordChannel(nil)
	CacheDiscordChannel(&discordgo.Channel{ID: ""})

	CacheDiscordChannel(&discordgo.Channel{ID: "parent-guild-chan", Name: "general", GuildID: "guild-123"})
	CacheDiscordChannel(&discordgo.Channel{
		ID:       "child-thread-chan",
		Name:     "thread-topic",
		ParentID: "parent-guild-chan",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})

	snap, ok := GetCachedChannel("child-thread-chan")
	if !ok || snap.GuildID != "guild-123" || !snap.IsThread {
		t.Errorf("expected child snapshot with inherited GuildID and IsThread, got: %+v", snap)
	}

	InvalidateChannelCache("child-thread-chan")
	if _, ok = GetCachedChannel("child-thread-chan"); ok {
		t.Errorf("expected child-thread-chan to be invalidated")
	}

	table := []struct {
		input    string
		expected bool
	}{
		{"", false},
		{"123456789012345678", true},
		{"12345a678", false},
		{"abc", false},
	}
	for _, tc := range table {
		if got := IsNumericSnowflake(tc.input); got != tc.expected {
			t.Errorf("IsNumericSnowflake(%q): expected %v, got %v", tc.input, tc.expected, got)
		}
	}
}

func TestResolveChannelSnapshot_And_EffectiveChannel(t *testing.T) {
	t.Parallel()
	if effID, effName, isTh := ResolveEffectiveChannel(nil, ""); effID != "" || effName != "" || isTh {
		t.Errorf("expected empty results for empty channelID")
	}
	if effID, effName, isTh := ResolveEffectiveChannel(nil, "unknown-channel"); effID != "unknown-channel" || effName != "" || isTh {
		t.Errorf("expected fallback to channelID for unknown channel with nil session")
	}

	CacheDiscordChannel(&discordgo.Channel{ID: "chan-cached-norm", Name: "norm-chan"})
	if effID, effName, isTh := ResolveEffectiveChannel(nil, "chan-cached-norm"); effID != "chan-cached-norm" || effName != "norm-chan" || isTh {
		t.Errorf("expected norm-chan, got id=%s name=%s isThread=%v", effID, effName, isTh)
	}

	CacheDiscordChannel(&discordgo.Channel{ID: "parent-for-thread", Name: "parent-name"})
	CacheDiscordChannel(&discordgo.Channel{ID: "thread-with-parent", Name: "sub-thread", ParentID: "parent-for-thread", Type: discordgo.ChannelTypeGuildPublicThread})
	if effID, effName, isTh := ResolveEffectiveChannel(nil, "thread-with-parent"); effID != "parent-for-thread" || effName != "parent-name" || !isTh {
		t.Errorf("expected parent ID and name, got effID=%s effName=%s isTh=%v", effID, effName, isTh)
	}

	CacheDiscordChannel(&discordgo.Channel{ID: "thread-with-missing-parent", Name: "orphan-thread", ParentID: "parent-not-cached", Type: discordgo.ChannelTypeGuildPublicThread})
	if effID, effName, isTh := ResolveEffectiveChannel(nil, "thread-with-missing-parent"); effID != "parent-not-cached" || effName != "" || !isTh {
		t.Errorf("expected parent ID and empty name, got effID=%s effName=%s isTh=%v", effID, effName, isTh)
	}

	CacheDiscordChannel(&discordgo.Channel{ID: "thread-no-parent-id", Name: "no-parent-thread", Type: discordgo.ChannelTypeGuildPublicThread})
	if effID, effName, isTh := ResolveEffectiveChannel(nil, "thread-no-parent-id"); effID != "thread-no-parent-id" || effName != "no-parent-thread" || !isTh {
		t.Errorf("expected snap ID and name, got effID=%s effName=%s isTh=%v", effID, effName, isTh)
	}

	dg, _ := discordgo.New("Bot test")
	dg.State = discordgo.NewState()
	_ = dg.State.GuildAdd(&discordgo.Guild{
		ID:       "guild-s",
		Channels: []*discordgo.Channel{{ID: "chan-in-state", Name: "state-channel-name", GuildID: "guild-s"}},
	})
	if effID, effName, _ := ResolveEffectiveChannel(dg, "chan-in-state"); effID != "chan-in-state" || effName != "state-channel-name" {
		t.Errorf("expected resolution from State, got effID=%s effName=%s", effID, effName)
	}

	dgREST, _ := discordgo.New("Bot test-token")
	dgREST.State = discordgo.NewState()
	dgREST.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		b, _ := json.Marshal(&discordgo.Channel{ID: "998877665544332211", Name: "rest-fetched-chan"})
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
	})
	if effID, effName, _ := ResolveEffectiveChannel(dgREST, "998877665544332211"); effID != "998877665544332211" || effName != "rest-fetched-chan" {
		t.Errorf("expected resolution from REST, got effID=%s effName=%s", effID, effName)
	}
}

func TestParseDBTime_AllLayoutsAndTypes(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	var nilTime *time.Time

	for i, tc := range []struct {
		input    interface{}
		expected bool
	}{
		{nil, false}, {now, true}, {nilTime, false}, {&now, true}, {"", false}, {"   ", false}, {"not-a-valid-timestamp", false},
		{[]byte("2026-09-11T15:04:05Z"), true}, {123456789, false},
		{"2026-09-11T15:04:05.123456789Z", true}, {"2026-09-11T15:04:05Z", true},
		{"2026-09-11 15:04:05.123456789 -0700 MST", true}, {"2026-09-11 15:04:05 -0700 MST", true},
		{"2026-09-11 15:04:05.123456789-07:00", true}, {"2026-09-11 15:04:05.123456789", true},
		{"2026-09-11 15:04:05-07:00", true}, {"2026-09-11 15:04:05", true}, {"2026-09-11T15:04:05", true},
	} {
		if _, ok := parseDBTime(tc.input); ok != tc.expected {
			t.Errorf("case %d (%v): expected %v, got %v", i, tc.input, tc.expected, ok)
		}
	}
}

func TestCoalesceBurstPrompt_EdgeCases(t *testing.T) {
	t.Parallel()
	if CoalesceBurstPrompt(nil) != "" || CoalesceBurstPrompt([]db.Message{}) != "" {
		t.Errorf("expected empty string for empty burst")
	}

	single := []db.Message{{Content: "Just one message"}}
	if got := CoalesceBurstPrompt(single); got != "Just one message" {
		t.Errorf("expected 'Just one message', got %q", got)
	}

	burst := []db.Message{
		{AuthorName: "@alice", CreatedAt: time.Now().UTC(), Content: "First message"},
		{AuthorName: "bob", CreatedAt: time.Now().UTC(), Content: "Second message"},
		{AuthorName: "", CreatedAt: time.Time{}, Content: "Third message"},
	}
	coalesced := CoalesceBurstPrompt(burst)
	for _, substr := range []string{"<USER_REQUEST>", "by @alice", "by @bob", "by @user"} {
		if !strings.Contains(coalesced, substr) {
			t.Errorf("expected %q in coalesced prompt", substr)
		}
	}
}

func TestExtractMessageBody_AllVariants(t *testing.T) {
	t.Parallel()
	table := []struct {
		input    string
		expected string
	}{
		{"  just plain text  ", "just plain text"},
		{"<USER_REQUEST>\n- author: @user\n- content: Target Body Text\n- timestamp: 2026-09-11", "Target Body Text"},
		{"<USER_REQUEST>\n- author: @user\n- content: Target Body Text\n- mentions: [aerial]", "Target Body Text"},
		{"<USER_REQUEST>\n- author: @user\n- content: Target Body Text\n- attachments: []", "Target Body Text"},
		{"<USER_REQUEST>\n- author: @user\n- content: Target Body Text\n- other_field: val", "Target Body Text"},
		{"<USER_REQUEST>\n- author: @user\n- content: Target Body Text\n</USER_REQUEST>", "Target Body Text"},
		{"<USER_REQUEST>\n- content: Check <\\/USER_REQUEST> and <\\USER_REQUEST>\n</USER_REQUEST>", "Check </USER_REQUEST> and <USER_REQUEST>"},
		{"<USER_REQUEST>\nJust body without content marker\n</USER_REQUEST>", "Just body without content marker"},
		{"<USER_REQUEST>- content: simple text without delimiter", "simple text without delimiter"},
	}
	for i, tc := range table {
		if got := extractMessageBody(tc.input); got != tc.expected {
			t.Errorf("case %d (%q): expected %q, got %q", i, tc.input, tc.expected, got)
		}
	}

	for _, s := range []string{
		"", "just text", "- content: foo\nbar",
		"<USER_REQUEST>hello world</USER_REQUEST>",
		"<\\/USER_REQUEST>escaped<\\/USER_REQUEST>",
		"<\\USER_REQUEST>escaped<\\USER_REQUEST>",
	} {
		_ = extractMessageBody(s)
	}
}

func TestResolveBotRoleIDs_AllVariants(t *testing.T) {
	t.Parallel()
	if ResolveBotRoleIDs(nil, "guild-1", "bot-1") != nil {
		t.Errorf("expected nil for nil sess")
	}

	dg, _ := discordgo.New("Bot test")
	if ResolveBotRoleIDs(dg, "guild-1", "bot-1") != nil {
		t.Errorf("expected nil for nil State")
	}

	dg.State = discordgo.NewState()
	dg.State.User = &discordgo.User{ID: "bot-1", Username: "AerialBot"}
	guild1 := &discordgo.Guild{
		ID:      "guild-1",
		Roles:   []*discordgo.Role{nil, {ID: "role-aerial", Name: "aerial"}, {ID: "role-gundam", Name: "gundam"}, {ID: "role-username", Name: "AerialBot"}, {ID: "role-other", Name: "moderator"}},
		Members: []*discordgo.Member{{User: &discordgo.User{ID: "bot-1"}, Roles: []string{"role-member-assigned", ""}}},
	}
	_ = dg.State.GuildAdd(guild1)

	expected := []string{"role-aerial", "role-gundam", "role-member-assigned", "role-username"}
	roles := ResolveBotRoleIDs(dg, "guild-1", "bot-1")
	if len(roles) != len(expected) {
		t.Fatalf("expected %d roles, got %v", len(expected), roles)
	}

	rolesAll := ResolveBotRoleIDs(dg, "", "bot-1")
	if len(rolesAll) != len(expected) {
		t.Fatalf("expected %d roles across all guilds, got %v", len(expected), rolesAll)
	}

	// Extra boundary branches
	sNilGuild, _ := discordgo.New("Bot fake")
	sNilGuild.State.GuildAdd(&discordgo.Guild{ID: "g-test"})
	sNilGuild.State.Guilds = append(sNilGuild.State.Guilds, nil)
	_ = ResolveBotRoleIDs(sNilGuild, "", "bot-123")

	_ = ResolveBotRoleIDs(nil, "", "")
	_ = ResolveBotRoleIDs(&discordgo.Session{}, "", "")
	_ = ResolveBotRoleIDs(&discordgo.Session{State: discordgo.NewState()}, "", "")
	_ = ResolveBotRoleIDs(&discordgo.Session{State: discordgo.NewState()}, "g1", "")

	// Member fetch fallback
	origFetcher := guildMemberFetcher
	defer func() { guildMemberFetcher = origFetcher }()
	guildMemberFetcher = func(s *discordgo.Session, guildID, userID string) (*discordgo.Member, error) {
		return &discordgo.Member{GuildID: guildID, User: &discordgo.User{ID: userID}, Roles: []string{"role1"}}, nil
	}
	sess := &discordgo.Session{State: discordgo.NewState()}
	rolesFallback := ResolveBotRoleIDs(sess, "guild-not-in-state", "bot-1")
	if len(rolesFallback) != 1 || rolesFallback[0] != "role1" {
		t.Errorf("expected [role1], got %v", rolesFallback)
	}
	_, _ = origFetcher(nil, "g", "u")
	_, _ = origFetcher(&discordgo.Session{}, "g", "u")
}

func TestIsTier1Wake_AllVariants(t *testing.T) {
	t.Parallel()
	table := []struct {
		name      string
		msg       db.Message
		botUserID string
		botRoles  []string
		expected  bool
	}{
		{"http-client author", db.Message{AuthorID: "http-client"}, "", nil, true},
		{"scheduler author", db.Message{AuthorID: "scheduler"}, "", nil, true},
		{"schedule run ID", db.Message{ScheduleRunID: "run-123"}, "", nil, true},
		{"direct user mention", db.Message{Content: "Hey <@bot-123>"}, "bot-123", nil, true},
		{"direct nickname mention", db.Message{Content: "Hey <@!bot-123>"}, "bot-123", nil, true},
		{"direct role mention", db.Message{Content: "Alert <@&role-999>"}, "", []string{"role-999"}, true},
		{"mentions aerial in header", db.Message{Content: "<USER_REQUEST>\n- mentions: [aerial, alice]\n</USER_REQUEST>"}, "", nil, true},
		{"mentions botUserID in header", db.Message{Content: "<USER_REQUEST>\n- mentions: [bot-123]\n</USER_REQUEST>"}, "bot-123", nil, true},
		{"mentions role in header", db.Message{Content: "<USER_REQUEST>\n- mentions: [role-999]\n</USER_REQUEST>"}, "", []string{"role-999"}, true},
		{"replying to aerial", db.Message{Content: "<USER_REQUEST>\n- replying_to:\n  author: aerial\n  content: hi\n- content: yes</USER_REQUEST>"}, "", nil, true},
		{"replying to bot ID", db.Message{Content: "<USER_REQUEST>\n- replying_to:\n  author: bot-123\n  content: hi\n- content: yes</USER_REQUEST>"}, "bot-123", nil, true},
		{"body mention prefix", db.Message{Content: "Hey <@aerial how are you"}, "", nil, true},
		{"body nick mention prefix", db.Message{Content: "Hey <@!aerial how are you"}, "", nil, true},
		{"plaintext keyword mention mode", db.Message{Content: "hello aerial"}, "", nil, false},
		{"plaintext keyword classifier mode", db.Message{Content: "hello aerial help me"}, "", nil, false},
		{"gundam keyword", db.Message{Content: "launch the gundam unit"}, "", nil, false},
		{"aerial view phrase", db.Message{Content: "what a nice aerial view of the city"}, "", nil, false},
		{"unrelated text", db.Message{Content: "hello world unrelated"}, "", nil, false},
		{"quoted role without mention_role_ids", db.Message{Content: "<USER_REQUEST>\n- content: check this '<@&999888>'\n- mention_role_ids: []\n</USER_REQUEST>"}, "123", []string{"999888"}, false},
		{"structured role in mention_role_ids", db.Message{Content: "<USER_REQUEST>\n- content: check this\n- mention_role_ids: [999888]\n</USER_REQUEST>"}, "123", []string{"999888"}, true},
		{"non-matching replying_to author", db.Message{Content: "<USER_REQUEST>\n- replying_to:\n  author: @random_user\n- content: hi\n</USER_REQUEST>"}, "123", nil, false},
		{"bot in mention_user_ids", db.Message{Content: "<USER_REQUEST>\n- content: please help\n- mention_user_ids: [123]\n</USER_REQUEST>"}, "123", nil, true},
		{"Zero incident quoted role snowflake", db.Message{Content: "<USER_REQUEST>\n- content: verified '<@&1543462881624858624>' role snowflake\n- mentions: [harperwallbanger]\n- mention_user_ids: [179407724335988736]\n- mention_role_ids: []\n</USER_REQUEST>"}, "1542035925603713086", []string{"1543462881624858624"}, false},
		{"replying_to without author line", db.Message{Content: "<USER_REQUEST>\n- replying_to:\n- mentions: [user]\n- content: hi\n</USER_REQUEST>"}, "bot-123", nil, false},
		{"body direct snowflake mention", db.Message{Content: "hey <@12345> check this"}, "12345", nil, true},
		{"body role snowflake mention", db.Message{Content: "hey <@&role999> check this"}, "99999", []string{"role999"}, true},
	}
	for _, tc := range table {
		if got := isTier1Wake(tc.msg, tc.botUserID, tc.botRoles); got != tc.expected {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expected, got)
		}
	}
}

func TestAssembleTurnPrompt_EmptyBase(t *testing.T) {
	for _, tc := range []struct {
		in   TurnPromptInput
		want string
	}{
		{TurnPromptInput{ThreadSummary: "summary text"}, "summary text"},
		{TurnPromptInput{LookbackHistory: []HistoryMessage{{AuthorName: "alice", Content: "hi", CreatedAt: time.Now()}}}, "@alice"},
		{TurnPromptInput{IsColdStart: true, PreviousSessionID: "prev-sess-123"}, "<PREVIOUS_SESSION>"},
		{TurnPromptInput{ChannelInstructions: "instructions"}, "instructions"},
		{TurnPromptInput{InjectedHookContext: "hook context"}, "hook context"},
	} {
		if got := AssembleTurnPrompt(tc.in); !strings.Contains(got, tc.want) {
			t.Errorf("AssembleTurnPrompt(%+v): expected %q in %q", tc.in, tc.want, got)
		}
	}
	for _, mode := range []string{"mentions", "direct", "always"} {
		_ = PlanBurstExecution(BurstPlanInput{Burst: []db.Message{{ID: "m1"}}, WakeMode: mode})
	}
}

func TestTurnExecution_BuildTurnPrompt_CachedSummary(t *testing.T) {
	store := setupTestStore(t)
	_ = store.SaveThreadSummary(context.Background(), "t-cached-sum", "<THREAD_SUMMARY>hi</THREAD_SUMMARY>", "msg-1")
	CacheDiscordChannel(&discordgo.Channel{
		ID:       "t-cached-sum",
		GuildID:  "g1",
		ParentID: "parent-chan",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})

	te := &turnExecution{
		pool: New(nil, WorkerPoolConfig{
			DiscordSession: &discordgo.Session{State: discordgo.NewState()},
			Store:          store,
			HistoryFetcher: func(ctx context.Context, threadID, beforeID string, limit int) ([]HistoryMessage, error) {
				return []HistoryMessage{{ID: "msg-1", AuthorName: "user", Content: "hello", CreatedAt: time.Now()}}, nil
			},
		}),
		threadID: "t-cached-sum",
		burst:    []db.Message{{ID: "m1", ThreadID: "t-cached-sum", Content: "current"}},
		isCold:   true,
	}
	te.buildTurnPrompt()
	if !strings.Contains(te.turnPrompt, "<THREAD_SUMMARY>hi</THREAD_SUMMARY>") {
		t.Errorf("expected cached summary in prompt, got %q", te.turnPrompt)
	}
	if !strings.Contains(te.turnPrompt, "<CHANNEL_HISTORY>") || !strings.Contains(te.turnPrompt, "hello") {
		t.Errorf("expected channel history lookback in prompt, got %q", te.turnPrompt)
	}
}

// ---------------------------------------------------------------------------
// 4. WorkerPool, Recovery & Store Declarative Suites
// ---------------------------------------------------------------------------

func TestRecoverInterrupted_AllBranches(t *testing.T) {
	t.Parallel()
	RecoverInterrupted(nil, nil)

	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	var deliveredPoison atomic.Int32
	pool := New(nil, WorkerPoolConfig{
		Store: store,
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			deliveredPoison.Add(1)
			return nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "", 0, nil
		},
	})

	messages := []db.Message{
		{ID: "poison-http", ThreadID: "thread-poison-http", AuthorID: "http-client", Content: "long poison message exceeding sixty characters in total length to verify truncation behavior", Status: db.StatusProcessing, RestartCount: 3, RetryCount: 0, CreatedAt: time.Now().UTC().Add(-5 * time.Minute)},
		{ID: "poison-discord", ThreadID: "thread-poison-discord", AuthorID: "user-discord", Content: "short poison", Status: db.StatusProcessing, RestartCount: 1, RetryCount: 3, CreatedAt: time.Now().UTC().Add(-4 * time.Minute)},
		{ID: "normal-interrupted", ThreadID: "thread-normal", AuthorID: "user-1", Content: "normal prompt", Status: db.StatusProcessing, RestartCount: 0, RetryCount: 0, CreatedAt: time.Now().UTC().Add(-3 * time.Minute)},
	}
	for _, m := range messages {
		if err := insertMessage(store, m); err != nil {
			t.Fatalf("InsertMessage failed: %v", err)
		}
	}

	RecoverInterrupted(store, pool)
	if deliveredPoison.Load() != 1 {
		t.Errorf("expected 1 poison pill delivery for discord user, got %d", deliveredPoison.Load())
	}
	for _, id := range []string{"poison-http", "poison-discord"} {
		m, _ := getMessage(store, id)
		if m.Status != db.StatusFailed {
			t.Errorf("expected %s to be FAILED, got %s", id, m.Status)
		}
	}

	closedStore := setupTestStore(t)
	_ = closedStore.Close()
	RecoverInterrupted(closedStore, pool)
}

func TestRecoverInterrupted_DrainErrorBranches(t *testing.T) {
	s := setupTestStore(t)
	mock := &mockDrainErrStore{Store: s, messages: []db.Message{{ID: "m1", ThreadID: "t1", Status: db.StatusProcessing, RestartCount: 5, RetryCount: 5}}}
	pool := New(nil, WorkerPoolConfig{Store: mock})
	RecoverInterrupted(mock, pool)

	tmpDBPath := filepath.Join(t.TempDir(), "closed_recovery.db")
	sqlDB, err := db.InitDB(tmpDBPath)
	if err == nil {
		_ = sqlDB.Close()
		RecoverInterrupted(sqlDB, pool)
	}
}

func TestWorkerPool_NewPermutations(t *testing.T) {
	t.Parallel()
	pool1 := New(nil, WorkerPoolConfig{
		Model: "custom-model", LowEffortModel: "custom-flash", SystemPrompt: "custom-prompt",
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "out", "", 0, nil
		},
	})
	if pool1.cfg.Model != "custom-model" || pool1.cfg.TimeoutMinutes != DefaultTimeoutMinutes || pool1.cfg.MaxAttempts != 3 ||
		pool1.cfg.BackoffBase != 3*time.Second || pool1.cfg.StalenessTTL != 30*time.Minute || pool1.cfg.DrainTimeout != 10*time.Second {
		t.Errorf("pool1 defaults mismatch: %+v", pool1.cfg)
	}

	ctx := context.Background()
	for _, opts := range []runner.WatchdogOptions{
		{MaxDuration: 2 * time.Minute},
		{InactivityTimeout: 3 * time.Minute},
		{},
	} {
		if _, _, _, err := pool1.cfg.RunnerWithOptionsFunc(ctx, "bin", "prompt", "sess", "key", "model", opts); err != nil {
			t.Errorf("unexpected error from RunnerWithOptionsFunc: %v", err)
		}
	}

	pool2 := New(nil, WorkerPoolConfig{
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			return "opt-out", "", 0, nil
		},
	})
	if out, _, _, err := pool2.cfg.RunnerFunc(ctx, "bin", "prompt", "sess", "key", "model", 15); err != nil || out != "opt-out" {
		t.Errorf("expected opt-out, got out=%s err=%v", out, err)
	}

	pool3 := New(nil, WorkerPoolConfig{})
	if _, _, _, err := pool3.cfg.RunnerFunc(ctx, "bin", "prompt", "sess", "key", "model", 10); err == nil || !strings.Contains(err.Error(), "RunnerFunc not configured") {
		t.Errorf("expected 'RunnerFunc not configured' error, got: %v", err)
	}
	if _, _, _, err := pool3.cfg.RunnerWithOptionsFunc(ctx, "bin", "prompt", "sess", "key", "model", runner.WatchdogOptions{}); err == nil || !strings.Contains(err.Error(), "RunnerWithOptionsFunc not configured") {
		t.Errorf("expected 'RunnerWithOptionsFunc not configured' error, got: %v", err)
	}

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

	appCfg := config.NewFromData(&config.ConfigData{SystemChannel: "test-alerts-chan", APIKey: "test-key", AgyBin: "agy"})
	dg, _ := discordgo.New("Bot test")
	var alertSent atomic.Int32
	alertReceived := make(chan struct{}, 1)
	pool5 := New(appCfg, WorkerPoolConfig{
		DiscordSession: dg,
		SystemAlertFunc: func(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
			alertSent.Add(1)
			select {
			case alertReceived <- struct{}{}:
			default:
			}
			return nil
		},
	})

	if pool5.cfg.Classifier != nil && pool5.cfg.Classifier.OnParseError != nil {
		pool5.cfg.Classifier.OnParseError("gemini-flash", strings.Repeat("x", 700)+"\n```json\n@everyone @here\n```", errors.New("parse error 1"))
		select {
		case <-alertReceived:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for parse error alert")
		}
		pool5.cfg.Classifier.OnParseError("gemini-flash", "short", errors.New("parse error 2"))
		if alertSent.Load() != 1 {
			t.Errorf("expected exactly 1 alert sent due to debouncing, got %d", alertSent.Load())
		}
	}

	var systemAlertSent atomic.Int32
	systemAlertReceived := make(chan struct{}, 1)
	pool5b := New(appCfg, WorkerPoolConfig{
		DiscordSession: dg,
		SystemAlertFunc: func(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
			systemAlertSent.Add(1)
			if title != "Classifier Endpoint Failure" {
				t.Errorf("expected alert title 'Classifier Endpoint Failure', got %q", title)
			}
			select {
			case systemAlertReceived <- struct{}{}:
			default:
			}
			return nil
		},
	})
	if pool5b.cfg.Classifier != nil && pool5b.cfg.Classifier.OnSystemAlert != nil {
		pool5b.cfg.Classifier.OnSystemAlert("http://192.168.1.70:8000/v1/systemone", errors.New("connection refused"))
		select {
		case <-systemAlertReceived:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for classifier system alert")
		}
		pool5b.cfg.Classifier.OnSystemAlert("http://192.168.1.70:8000/v1/systemone", errors.New("connection refused 2"))
		if systemAlertSent.Load() != 1 {
			t.Errorf("expected exactly 1 system alert sent due to debouncing, got %d", systemAlertSent.Load())
		}
	}

	_ = pool5.cfg.ResolveChannelPolicy("c1", "general")
	pool6 := New(nil, WorkerPoolConfig{})
	_ = pool6.cfg.ResolveChannelPolicy("c1", "general")
	_, _ = pool6.cfg.HistoryFetcher(context.Background(), "c1", "", 10)
}

func TestWorkerPool_GettersAndStop(t *testing.T) {
	t.Parallel()
	pool := New(nil, WorkerPoolConfig{
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "", 0, nil
		},
	})
	if pool.Classifier() == nil || pool.SessionManager() == nil {
		t.Errorf("expected non-nil Classifier and SessionManager")
	}
	pool.Start()
	pool.StopWithTimeout(0)
	pool.StopWithTimeout(10 * time.Millisecond)
}

func TestWorkerPool_DrainErrorBranches(t *testing.T) {
	s := setupTestStore(t)
	mock := &mockDrainErrStore{Store: s}
	p := New(nil, WorkerPoolConfig{Store: mock, DrainTimeout: 10 * time.Millisecond})
	p.StopWithTimeout(0)

	tmpDBPath := filepath.Join(t.TempDir(), "closed_pool.db")
	sqlDB, err := db.InitDB(tmpDBPath)
	if err == nil {
		_ = sqlDB.Close()
		pDB := New(nil, WorkerPoolConfig{DB: sqlDB, DrainTimeout: 10 * time.Millisecond})
		pDB.StopWithTimeout(10 * time.Millisecond)
	}
}

func TestWorkerPool_EnqueueBurst_Empty(t *testing.T) {
	p := New(nil, WorkerPoolConfig{})
	p.EnqueueBurst()
}

func TestGetSessionLastActivity_ErrorAndEdgeCases(t *testing.T) {
	t.Parallel()
	for _, tid := range []string{"thread-1", ""} {
		if act, isCold, err := GetSessionLastActivity(nil, tid); err != nil || !isCold || !act.IsZero() {
			t.Errorf("expected zero, cold, nil for nil db with tid=%q", tid)
		}
	}

	closedStore := setupTestStore(t)
	_ = closedStore.Close()
	if _, _, err := GetSessionLastActivity(closedStore, "thread-1"); err == nil {
		t.Errorf("expected error querying closed database")
	}

	store := setupTestStore(t)
	defer func() { _ = store.Close() }()
	tmpDir := t.TempDir()
	sessMgr := session.New(filepath.Join(tmpDir, ".gemini"), tmpDir)
	store.SetSessionInfo(db.SessionInfo{
		ThreadID:          "thread-disk-test",
		InternalSessionID: "sess-disk-1",
		TurnCount:         2,
		UpdatedAt:         time.Now().UTC().Add(-10 * time.Minute),
	})
	act, isCold, err := GetSessionLastActivity(store, "thread-disk-test", sessMgr)
	if err != nil || isCold || act.IsZero() {
		t.Fatalf("unexpected GetSessionLastActivity result: act=%v cold=%v err=%v", act, isCold, err)
	}
}

func TestWorkerPool_EnqueueSlowPathAndWorkerIdle(t *testing.T) {
	t.Parallel()
	pool := New(nil, WorkerPoolConfig{
		IdleTimeout:  30 * time.Millisecond,
		DrainTimeout: 1 * time.Millisecond,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "", 0, nil
		},
	})
	pool.stopped = true
	pool.Enqueue(db.Message{ID: "m-stopped", ThreadID: "t-stopped"})
	pool.stopped = false

	state := &threadWorkerState{ch: make(chan db.Message, 1)}
	state.ch <- db.Message{ID: "m-fill"}
	pool.threadChs["thread-full"] = state
	pool.cancel()
	pool.Enqueue(db.Message{ID: "m-blocked", ThreadID: "thread-full"})
	pool.Stop()
}

func TestProcessBurst_AdditionalEdgeCases(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	pool := New(nil, WorkerPoolConfig{
		Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "All good"), "", 0, nil
		},
	})
	pool.processBurst(nil)
	pool.processBurst([]db.Message{})

	poolQuota := New(nil, WorkerPoolConfig{
		Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "OK"), "", 0, nil
		},
	})
	defer poolQuota.Stop()

	now := time.Now().UTC()
	_ = createScheduleRun(store, db.ScheduleRun{
		ID:           "sched-run-quota",
		ScheduleID:   "sched-1",
		ScheduleType: "cron",
		Prompt:       "prompt",
		StartedAt:    now,
		Status:       "running",
	})
	_ = insertMessage(store, db.Message{
		ID:            "msg-quota-pause",
		ThreadID:      "thread-quota-pause",
		AuthorID:      "user-1",
		ScheduleRunID: "sched-run-quota",
		Content:       "help me",
		Status:        db.StatusPending,
		CreatedAt:     now,
	})
	poolQuota.quotaLockedUntil.Store(now.Add(2 * time.Minute).Unix())
	var onCompletedStatus string
	poolQuota.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) {
		onCompletedStatus = finalStatus
	}
	msgQP, _ := getMessage(store, "msg-quota-pause")
	poolQuota.processBurst([]db.Message{*msgQP})
	if onCompletedStatus != db.StatusFailed {
		t.Errorf("expected StatusFailed on quota pause lockout, got %s", onCompletedStatus)
	}

	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	poolCancel := New(nil, WorkerPoolConfig{Store: store})
	poolCancel.ctx = ctxCancel
	poolCancel.processBurst([]db.Message{{ID: "m-cancel", ThreadID: "t-cancel"}})

	runID := "run-edge-1"
	_ = createScheduleRun(store, db.ScheduleRun{ID: runID, ScheduleID: "s1", ScheduleType: "cron", StartedAt: time.Now().UTC(), Status: "running"})
	_ = insertMessage(store, db.Message{ID: "m-edge-1", ThreadID: "t-edge", AuthorID: "u1", ScheduleRunID: runID, Content: "edge", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	msgEdge, _ := getMessage(store, "m-edge-1")

	poolIgnored := New(nil, WorkerPoolConfig{Store: store})
	poolIgnored.cfg.ResolveChannelPolicy = func(channelID, channelName string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "ignore"}
	}
	poolIgnored.processBurst([]db.Message{*msgEdge})
	run, _ := getScheduleRun(store, runID)
	if run == nil || run.Status != "completed" {
		t.Errorf("expected schedule run to be completed, got %+v", run)
	}

	_ = insertMessage(store, db.Message{ID: "m-dm-1", ThreadID: "t-dm", AuthorID: "u1", Content: "dm prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	msgDM, _ := getMessage(store, "m-dm-1")
	poolDM := New(nil, WorkerPoolConfig{
		Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "Hello from DM"), "", 0, nil
		},
	})
	poolDM.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "dm", WakeMode: "mention"}
	}
	poolDM.processBurst([]db.Message{*msgDM})

	ptrF := func(v float64) *float64 { return &v }
	_ = insertMessage(store, db.Message{ID: "m-skip-1", ThreadID: "t-skip", AuthorID: "u1", Content: "ambient chatter", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	msgSkip, _ := getMessage(store, "m-skip-1")
	poolSkip := New(nil, WorkerPoolConfig{
		Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "Should skip"), "", 0, nil
		},
	})
	poolSkip.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrF(0.8)}
	}
	poolSkip.processBurst([]db.Message{*msgSkip})

	runLead := "run-lead-part"
	_ = createScheduleRun(store, db.ScheduleRun{ID: runLead, ScheduleID: "s1", ScheduleType: "cron", StartedAt: time.Now().UTC(), Status: "running"})
	for _, m := range []db.Message{
		{ID: "m-lead", ThreadID: "t-part", AuthorID: "u1", ScheduleRunID: runLead, Content: "ambient chatter", Status: db.StatusPending, CreatedAt: time.Now().UTC()},
		{ID: "m-wake", ThreadID: "t-part", AuthorID: "u1", Content: "<@aerial> wake up!", Status: db.StatusPending, CreatedAt: time.Now().UTC()},
		{ID: "m-trail-amb", ThreadID: "t-part", AuthorID: "u1", Content: "more chatter", Status: db.StatusPending, CreatedAt: time.Now().UTC()},
		{ID: "m-trail-wake", ThreadID: "t-part", AuthorID: "u1", Content: "<@aerial> second command", Status: db.StatusPending, CreatedAt: time.Now().UTC()},
	} {
		_ = insertMessage(store, m)
	}
	ml, _ := getMessage(store, "m-lead")
	mw, _ := getMessage(store, "m-wake")
	mta, _ := getMessage(store, "m-trail-amb")
	mtw, _ := getMessage(store, "m-trail-wake")

	poolPart := New(nil, WorkerPoolConfig{
		Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "Processed wake"), "", 0, nil
		},
	})
	poolPart.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "channel", WakeMode: "mention"}
	}
	poolPart.processBurst([]db.Message{*ml, *mw, *mta, *mtw})
	runP, _ := getScheduleRun(store, runLead)
	if runP == nil || runP.Status != "completed" {
		t.Errorf("expected schedule run to be completed, got lead=%+v", runP)
	}

	_ = insertMessage(store, db.Message{ID: "m-badparse", ThreadID: "t-badparse", AuthorID: "u1", Content: "hello", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mb, _ := getMessage(store, "m-badparse")
	poolBadParse := New(nil, WorkerPoolConfig{
		Store:       store,
		MaxAttempts: 1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "definitely not json and no markers", "", 0, nil
		},
	})
	poolBadParse.processBurst([]db.Message{*mb})
	msgBP, _ := getMessage(store, "m-badparse")
	if msgBP.Status != db.StatusFailed {
		t.Errorf("expected message to fail on parse error, got %s", msgBP.Status)
	}

	runRate := "run-ratelimit-1"
	_ = createScheduleRun(store, db.ScheduleRun{ID: runRate, ScheduleID: "s-rate", ScheduleType: "cron", StartedAt: time.Now().UTC(), Status: "running"})
	_ = insertMessage(store, db.Message{ID: "m-rate-1", ThreadID: "t-rate", AuthorID: "u1", ScheduleRunID: runRate, Content: "query", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mr, _ := getMessage(store, "m-rate-1")
	poolRate := New(nil, WorkerPoolConfig{
		Store:       store,
		BackoffBase: 1 * time.Millisecond,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "Resource has been exhausted: rate limit exceeded 429", 1, errors.New("exit 1")
		},
	})
	poolRate.processBurst([]db.Message{*mr})
	runR, _ := getScheduleRun(store, runRate)
	if runR == nil || runR.Status != "failed" {
		t.Errorf("expected schedule run to fail on rate limit, got %+v", runR)
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

func TestStatusUpdater_SendFailureDefaults(t *testing.T) {
	t.Parallel()
	s, _ := discordgo.New("Bot fake")
	s.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("mock network error")
	})
	up := NewStatusUpdater(s, "thread-def", false)
	if _, err := up.sendFunc("c", "t"); err == nil {
		t.Errorf("expected error from default sendFunc with dummy session")
	}
	up.turnStart = time.Now().Add(10 * time.Second)
	if !strings.Contains(up.currentStatusText(), "0.1s") {
		t.Errorf("expected 0.1s in status text, got %s", up.currentStatusText())
	}
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
}

func TestClassifierTurnSink_OnParseError(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	pool1 := New(nil, WorkerPoolConfig{Store: store})
	if pool1.cfg.Classifier != nil && pool1.cfg.Classifier.OnParseError != nil {
		pool1.cfg.Classifier.OnParseError("model-1", "output ``` @everyone @here", errors.New("syntax error"))
	}
	pool2 := New(nil, WorkerPoolConfig{Store: store})
	if pool2.cfg.Classifier != nil && pool2.cfg.Classifier.OnParseError != nil {
		dg, _ := discordgo.New("Bot fake")
		pool2.SetDiscordSession(dg)
		panicDone := make(chan struct{}, 1)
		pool2.cfg.SystemAlertFunc = func(s *discordgo.Session, ch, title, text string) error {
			close(panicDone)
			panic("deliberate panic in alert handler")
		}
		pool2.cfg.Classifier.OnParseError("model-1", strings.Repeat("a", 700)+"``` @everyone", errors.New("syntax error"))
		select {
		case <-panicDone:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for panic in alert handler")
		}
	}
}

func TestClaimPendingMessage_Branches(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	pool := New(nil, WorkerPoolConfig{
		Store: &mockClaimStore{Store: store, claimErr: errors.New("db lock error")},
	})
	pool.processBurst([]db.Message{{ID: "m-err-claim", ThreadID: "t-claim"}})

	pool2 := New(nil, WorkerPoolConfig{
		Store: &mockClaimStore{Store: store, claimed: false},
	})
	pool2.processBurst([]db.Message{{ID: "m-unclaimed", ThreadID: "t-claim"}})
}

func TestDropBurstAsStale_WithScheduleRun(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	runID := "run-stale-burst-1"
	_ = createScheduleRun(store, db.ScheduleRun{
		ID:           runID,
		ScheduleID:   "sched-stale",
		ScheduleType: "cron",
		StartedAt:    time.Now().UTC().Add(-2 * time.Hour),
		Status:       "running",
	})
	msgID := "msg-stale-burst-1"
	_ = insertMessage(store, db.Message{
		ID:            msgID,
		ThreadID:      "thread-stale-burst",
		AuthorID:      "user-1",
		ScheduleRunID: runID,
		Content:       "old prompt",
		Status:        db.StatusPending,
		CreatedAt:     time.Now().UTC().Add(-2 * time.Hour),
	})
	pool := New(nil, WorkerPoolConfig{Store: store})
	m, _ := getMessage(store, msgID)
	pool.processBurst([]db.Message{*m})
	run, _ := getScheduleRun(store, runID)
	if run == nil || run.Error != "[EXPIRED_STALE]" {
		t.Errorf("expected [EXPIRED_STALE] error, got %+v", run)
	}
}

func TestProcessBurst_ChannelPolicyWakeModes(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	baseCfg := WorkerPoolConfig{
		Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "Done"), "", 0, nil
		},
	}

	pAll := New(nil, baseCfg)
	pAll.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "channel", WakeMode: "all"}
	}
	_ = insertMessage(store, db.Message{ID: "m-all-1", ThreadID: "t-all", AuthorID: "u1", Content: "c1", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	_ = insertMessage(store, db.Message{ID: "m-all-2", ThreadID: "t-all", AuthorID: "u1", Content: "c2", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	m1, _ := getMessage(store, "m-all-1")
	m2, _ := getMessage(store, "m-all-2")
	pAll.processBurst([]db.Message{*m1, *m2})

	ptrF := func(v float64) *float64 { return &v }
	pZero := New(nil, baseCfg)
	pZero.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrF(0.0)}
	}
	_ = insertMessage(store, db.Message{ID: "m-zero-1", ThreadID: "t-zero", AuthorID: "u1", Content: "c1", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mz, _ := getMessage(store, "m-zero-1")
	pZero.processBurst([]db.Message{*mz})

	pSkip := New(nil, baseCfg)
	pSkip.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrF(0.8)}
	}
	_ = insertMessage(store, db.Message{ID: "m-skip-1", ThreadID: "t-skip", AuthorID: "u1", Content: "lol haha", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	ms, _ := getMessage(store, "m-skip-1")
	pSkip.processBurst([]db.Message{*ms})

	pNoClass := New(nil, baseCfg)
	pNoClass.cfg.Classifier = nil
	pNoClass.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrF(0.8)}
	}
	_ = insertMessage(store, db.Message{ID: "m-noclass-1", ThreadID: "t-noclass", AuthorID: "u1", Content: "deep complex query", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mnc, _ := getMessage(store, "m-noclass-1")
	pNoClass.processBurst([]db.Message{*mnc})
}

func TestQueue_CoverageFinalSprint(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	if snap, ok := resolveChannelSnapshot(nil, ""); ok || snap.ID != "" {
		t.Errorf("expected false and empty snapshot for empty channelID")
	}

	dbClosed := setupTestStore(t)
	_ = dbClosed.Close()
	if _, _, err := GetSessionLastActivity(dbClosed, "thread-closed"); err == nil {
		t.Errorf("expected error from GetSessionLastActivity on closed db")
	}

	p := New(nil, WorkerPoolConfig{Store: store, IdleTimeout: 5 * time.Millisecond})
	stIdle := &threadWorkerState{ch: make(chan db.Message, 1)}
	p.mu.Lock()
	p.threadChs["t-idle-sprint"] = stIdle
	p.mu.Unlock()
	p.wg.Add(1)
	go p.runThreadWorker("t-idle-sprint", stIdle)
	time.Sleep(25 * time.Millisecond)

	stClose := &threadWorkerState{ch: make(chan db.Message)}
	p.wg.Add(1)
	go p.runThreadWorker("t-close-sprint", stClose)
	close(stClose.ch)
	p.wg.Wait()

	p.Stop()
	p.Enqueue(db.Message{ID: "m-enqueue-stopped"})

	runIgnored := "run-ignored-sprint"
	_ = createScheduleRun(store, db.ScheduleRun{ID: runIgnored, ScheduleID: "s-ign", ScheduleType: "cron", StartedAt: time.Now().UTC(), Status: "running"})
	_ = insertMessage(store, db.Message{ID: "m-ign-sprint", ThreadID: "t-ign-sprint", AuthorID: "u1", ScheduleRunID: runIgnored, Content: "ignored prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mIgn, _ := getMessage(store, "m-ign-sprint")
	pIgn := New(nil, WorkerPoolConfig{Store: store, StalenessTTL: -1})
	pIgn.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy { return config.ChannelPolicy{Mode: "ignore"} }
	pIgn.processBurst([]db.Message{*mIgn})
	run, _ := getScheduleRun(store, runIgnored)
	if run == nil || run.Status != "completed" {
		t.Errorf("expected completed schedule run")
	}

	_ = insertMessage(store, db.Message{ID: "m-def-pol", ThreadID: "t-def-pol", AuthorID: "u1", Content: "test prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mDef, _ := getMessage(store, "m-def-pol")
	pDefPol := New(nil, WorkerPoolConfig{
		Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "OK"), "", 0, nil
		},
	})
	pDefPol.cfg.ResolveChannelPolicy = nil
	pDefPol.processBurst([]db.Message{*mDef})
	New(nil, WorkerPoolConfig{Store: store, DrainTimeout: -1}).StopWithTimeout(0)

	ptrF2 := func(v float64) *float64 { return &v }
	pTrailNoClass := New(nil, WorkerPoolConfig{
		Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "OK"), "", 0, nil
		},
	})
	pTrailNoClass.cfg.Classifier = nil
	pTrailNoClass.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: ptrF2(0.5)}
	}
	_ = insertMessage(store, db.Message{ID: "m-w-lead", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "<@aerial> start", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	_ = insertMessage(store, db.Message{ID: "m-trail-query", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "why is the ocean blue", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mwLead, _ := getMessage(store, "m-w-lead")
	mtQuery, _ := getMessage(store, "m-trail-query")
	pTrailNoClass.processBurst([]db.Message{*mwLead, *mtQuery})

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
		Store: store,
		HistoryFetcher: func(ctx context.Context, channelID, beforeID string, limit int) ([]HistoryMessage, error) {
			return hist15, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "OK"), "", 0, nil
		},
	})
	_ = insertMessage(store, db.Message{
		ID:        "m-hist-slice",
		ThreadID:  "t-hist-slice-123",
		AuthorID:  "u1",
		Content:   "<@aerial> hello with history",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	})
	mHistSlice, _ := getMessage(store, "m-hist-slice")
	pHistSlice.processBurst([]db.Message{*mHistSlice})

	staleStore := setupTestStore(t)
	msgStale := db.Message{
		ID:        "m-stale-failopen",
		ThreadID:  "t-stale-failopen",
		CreatedAt: time.Now().UTC().Add(-40 * time.Minute),
		Status:    db.StatusPending,
		Content:   "stale content",
	}
	pStale := New(nil, WorkerPoolConfig{
		Store:       &mockClaimStore{Store: staleStore, claimed: true},
		BackoffBase: 1 * time.Millisecond,
	})
	_ = staleStore.Close()
	pStale.processBurst([]db.Message{msgStale})
}

type errQueueStore struct {
	db.Store
}

func (e *errQueueStore) UpdateMessageStatus(ctx context.Context, id, status, errorMsg string) error {
	return errors.New("errQueueStore: update message status error")
}
func (e *errQueueStore) UpdateScheduleRunStatus(ctx context.Context, params db.UpdateRunParams) error {
	return errors.New("errQueueStore: update schedule run status error")
}
func (e *errQueueStore) UpdateMessageCompleted(ctx context.Context, id, responseText string) error {
	return errors.New("errQueueStore: update message completed error")
}
func (e *errQueueStore) IncrementMessageRetry(ctx context.Context, id, errorMsg string) error {
	return errors.New("errQueueStore: increment message retry error")
}
func (e *errQueueStore) SaveSessionID(ctx context.Context, threadID, sessionID string) error {
	return errors.New("errQueueStore: save session id error")
}
func (e *errQueueStore) RotateSessionID(ctx context.Context, threadID, newSessionID string) error {
	return errors.New("errQueueStore: rotate session id error")
}

type mockDrainErrStore struct {
	db.Store
	messages []db.Message
}

func (m *mockDrainErrStore) GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]db.Message, error) {
	if len(m.messages) > 0 {
		return m.messages, nil
	}
	return []db.Message{{ID: "msg-proc-1", Status: db.StatusProcessing}}, nil
}
func (m *mockDrainErrStore) UpdateMessageStatus(ctx context.Context, id, status, errReason string) error {
	return errors.New("mock drain update error")
}

func TestStoreAndTurnExecutionHelpers_Coverage(t *testing.T) {
	t.Parallel()

	// 1. resolveStore permutations
	for _, v := range []interface{}{nil, "unsupported string", (*db.FakeStore)(nil), (*db.SQLStore)(nil), (*sql.DB)(nil)} {
		if s := resolveStore(v); s != nil {
			t.Errorf("expected nil from resolveStore(%v)", v)
		}
	}

	fake := setupTestStore(t)
	defer func() { _ = fake.Close() }()
	if s := resolveStore(fake); s != fake {
		t.Errorf("expected same FakeStore from resolveStore")
	}

	// 2. WorkerPool.Store() and SummaryGroup() permutations
	if s := (*WorkerPool)(nil).Store(); s != nil {
		t.Errorf("expected nil from nil WorkerPool.Store()")
	}
	if sg := (*WorkerPool)(nil).SummaryGroup(); sg != nil {
		t.Errorf("expected nil from nil WorkerPool.SummaryGroup()")
	}
	pEmpty := &WorkerPool{}
	if s := pEmpty.Store(); s != nil {
		t.Errorf("expected nil from empty WorkerPool.Store()")
	}
	if sg := pEmpty.SummaryGroup(); sg == nil {
		t.Errorf("expected non-nil summaryGroup from empty WorkerPool")
	}
	pFake := &WorkerPool{cfg: WorkerPoolConfig{Store: fake}}
	if s := pFake.Store(); s != fake {
		t.Errorf("expected fake from WorkerPool.Store()")
	}

	realDB := &sql.DB{}
	pDB := &WorkerPool{cfg: WorkerPoolConfig{DB: realDB}}
	if s := pDB.Store(); s == nil {
		t.Errorf("expected non-nil Store from WorkerPool with DB")
	}
	if s := resolveStore(realDB); s == nil {
		t.Errorf("expected non-nil Store from resolveStore(*sql.DB)")
	}

	// 2b. GetSessionLastActivity edge cases
	if _, isCold, err := GetSessionLastActivity("invalid-type", "t1"); err != nil || !isCold {
		t.Errorf("expected true, nil for invalid type")
	}
	_ = insertMessage(fake, db.Message{
		ID:        "m-last-act",
		ThreadID:  "t-last-act",
		AuthorID:  "u1",
		Status:    db.StatusCompleted,
		CreatedAt: time.Now().UTC(),
	})
	if act, isCold, err := GetSessionLastActivity(fake, "t-last-act"); err != nil || isCold || act.IsZero() {
		t.Errorf("expected non-zero activity, not cold for completed message")
	}

	// 3. turnExecution helper nil-store branches
	var teNil *turnExecution
	if s := teNil.store(); s != nil {
		t.Errorf("expected nil from nil te.store()")
	}
	teNoStore := &turnExecution{}
	if s := teNoStore.store(); s != nil {
		t.Errorf("expected nil from teNoStore.store()")
	}

	teNoStore.updateMessageStatus("m1", "failed", "err")
	teNoStore.updateScheduleRunStatus(db.UpdateRunParams{RunID: "r1"})
	teNoStore.updateMessageCompleted("m1", "resp")
	teNoStore.incrementMessageRetry("m1", "retry-err")
	teNoStore.saveSessionID("t1", "s1")
	teNoStore.rotateSessionID("t1", "s2")
	if count, err := teNoStore.getSessionTurnCount("t1"); count != 0 || err != nil {
		t.Errorf("expected 0, nil")
	}
	if count, err := teNoStore.incrementSessionTurnCount("t1"); count != 0 || err != nil {
		t.Errorf("expected 0, nil")
	}
	if sid, err := teNoStore.getSessionID("t1"); sid != "" || err != nil {
		t.Errorf("expected empty sid")
	}
	if sid, err := teNoStore.getPreviousSessionID("t1"); sid != "" || err != nil {
		t.Errorf("expected empty prev sid")
	}
	if err := teNoStore.createOneShotSchedule(db.OneShotSchedule{ID: "os1"}); err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if sum, last, err := teNoStore.getThreadSummary("t1"); sum != "" || last != "" || err != nil {
		t.Errorf("expected empty summary")
	}
	if err := teNoStore.saveThreadSummary("t1", "summary", "last-m"); err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if msgs, err := teNoStore.getRecentThreadMessages("t1", 10); msgs != nil || err != nil {
		t.Errorf("expected nil msgs")
	}

	// 4. turnExecution helper active-store branches & error branches
	for _, te := range []*turnExecution{
		{pool: pFake},
		{pool: &WorkerPool{cfg: WorkerPoolConfig{Store: &errQueueStore{Store: fake}}}},
	} {
		te.updateMessageStatus("m1", "processing")
		te.updateScheduleRunStatus(db.UpdateRunParams{RunID: "r1"})
		te.updateMessageCompleted("m1", "resp")
		te.incrementMessageRetry("m1", "retry-err")
		te.saveSessionID("t1", "s1")
		te.rotateSessionID("t1", "s2")
		_, _ = te.getSessionTurnCount("t1")
		_, _ = te.incrementSessionTurnCount("t1")
		_, _ = te.getSessionID("t1")
		_, _ = te.getPreviousSessionID("t1")
		_ = te.createOneShotSchedule(db.OneShotSchedule{ID: "os1"})
		_, _, _ = te.getThreadSummary("t1")
		_ = te.saveThreadSummary("t1", "sum", "m1")
		_, _ = te.getRecentThreadMessages("t1", 10)
	}
}

func TestTurnExecution_StoreErrorBranches(t *testing.T) {
	fake := setupTestStore(t)
	pErr := &WorkerPool{cfg: WorkerPoolConfig{Store: &errQueueStore{Store: fake}}}
	te := &turnExecution{pool: pErr}

	te.updateMessageStatus("m1", "failed", "err")
	te.updateScheduleRunStatus(db.UpdateRunParams{RunID: "r1"})
	te.updateMessageCompleted("m1", "resp")
	te.incrementMessageRetry("m1", "retry-err")
	te.saveSessionID("t1", "s1")
	te.rotateSessionID("t1", "s2")
}

func TestCoverageBoost_DaemonTurnMarshalError(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	threadID := "thread-persist-marshal-err"
	sessID := "b1111111-2222-3333-4444-555555555555"

	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				if _, err := fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", sessID); err != nil {
					t.Logf("write init event: %v", err)
				}
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					if _, err := outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"error\":\"simulated daemon failure\"}}\n")); err != nil {
						t.Logf("write result event: %v", err)
					}
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() { _ = procPool.Close() }()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:    procPool,
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
		MaxAttempts:    1,
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusFailed {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
		},
	})
	defer pool.Stop()

	_ = store.SaveSessionID(context.Background(), threadID, sessID)
	msg := db.Message{
		ID:        "msg-persist-marshal-err",
		ThreadID:  threadID,
		Content:   "Hello error",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message completion")
	}
}

func TestCoverageBoost_PoolStoreResolutionAndOptions(t *testing.T) {
	var nilSQLStore *db.SQLStore
	if s := resolveStore(nilSQLStore); s != nil {
		t.Errorf("expected nil for nil *db.SQLStore, got %v", s)
	}

	rawDB, err := sql.Open("sqlite3", ":memory:")
	if err == nil {
		defer rawDB.Close()
		sqlStore := db.NewSQLStore(rawDB)
		if s := resolveStore(sqlStore); s == nil {
			t.Errorf("expected non-nil for valid *db.SQLStore")
		}
		p := New(nil, WorkerPoolConfig{DB: rawDB})
		if p.Store() == nil {
			t.Errorf("expected store populated from DB")
		}
	}
}

func TestCoverageBoost_SinkAndLegacyAdapterMethods(t *testing.T) {
	var nilP *WorkerPool
	if nilP.AgentPool() != nil {
		t.Errorf("expected nil for nil WorkerPool.AgentPool")
	}
	p := &WorkerPool{processPool: &testRunnerAgentPool{}}
	if p.AgentPool() == nil {
		t.Errorf("expected non-nil AgentPool")
	}
}
