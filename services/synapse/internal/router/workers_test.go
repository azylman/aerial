package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

type defaultOutboundDispatcherCase struct {
	name               string
	callType           string
	snowflake          string
	statusCode         int
	responseBody       string
	responseHeaderType string
	failCount          int
	maxRetries         int
	cancelDuringRetry  bool
	wantRetryRecorded  bool
	wantErr            bool
	wantSubstr         string
	wantRequests       int
}

func defaultOutboundDispatcherPromptCases() []defaultOutboundDispatcherCase {
	validSnowflake := "100000000000000001"
	return []defaultOutboundDispatcherCase{
		{
			name:       "DispatchPrompt invalid snowflake",
			callType:   "prompt",
			snowflake:  "invalid channel!",
			wantErr:    true,
			wantSubstr: "invalid discord snowflake",
		},
		{
			name:         "DispatchPrompt valid channel name from config.yaml",
			callType:     "prompt",
			snowflake:    "aerial-dev",
			responseBody: `{"status":"queued"}`,
		},
		{
			name:              "DispatchPrompt retry on 500 then success",
			callType:          "prompt",
			snowflake:         validSnowflake,
			failCount:         1,
			maxRetries:        4,
			wantRetryRecorded: true,
			responseBody:      `{"status":"queued"}`,
		},
		{
			name:       "DispatchPrompt exhausting retries on 500",
			callType:   "prompt",
			snowflake:  validSnowflake,
			failCount:  5,
			maxRetries: 2,
			wantErr:    true,
			wantSubstr: "returned status 500",
		},
		{
			name:              "DispatchPrompt context cancellation during retry backoff",
			callType:          "prompt",
			snowflake:         validSnowflake,
			failCount:         5,
			maxRetries:        4,
			cancelDuringRetry: true,
			wantErr:           true,
		},
		{
			name:       "DispatchDirectMessage invalid snowflake",
			callType:   "direct_message",
			snowflake:  "invalid channel!",
			wantErr:    true,
			wantSubstr: "invalid discord snowflake",
		},
		{
			name:              "DispatchDirectMessage retry on 500 then success",
			callType:          "direct_message",
			snowflake:         validSnowflake,
			failCount:         1,
			maxRetries:        4,
			wantRetryRecorded: true,
			responseBody:      `{"status":"sent"}`,
		},
		{
			name:         "DispatchDirectMessage 400 client error no retry",
			callType:     "direct_message",
			snowflake:    validSnowflake,
			statusCode:   http.StatusBadRequest,
			responseBody: `{"error":"bad request"}`,
			maxRetries:   3,
			wantErr:      true,
		},
		{
			name:         "DispatchBackground success 200",
			callType:     "dispatch_background",
			statusCode:   http.StatusOK,
			responseBody: `{"status":"accepted"}`,
		},
		{
			name:              "DispatchBackground retry on 500 then success",
			callType:          "dispatch_background",
			failCount:         1,
			maxRetries:        4,
			wantRetryRecorded: true,
			responseBody:      `{"status":"accepted"}`,
		},
		{
			name:         "DispatchBackground 400 client error no retry",
			callType:     "dispatch_background",
			statusCode:   http.StatusBadRequest,
			responseBody: `{"error":"bad request"}`,
			maxRetries:   3,
			wantErr:      true,
		},
	}
}

func defaultOutboundDispatcherDiscordCases() []defaultOutboundDispatcherCase {
	return []defaultOutboundDispatcherCase{
		{
			name:         "DispatchDiscordMessage success 200",
			callType:     "discord_message",
			statusCode:   http.StatusOK,
			responseBody: `{"status":"processed","message_id":"12345"}`,
		},
		{
			name:         "DispatchDiscordMessage 400 client error",
			callType:     "discord_message",
			statusCode:   http.StatusBadRequest,
			responseBody: `{"error":"invalid payload"}`,
			wantErr:      true,
			wantSubstr:   "status 400",
		},
		{
			name:         "DispatchDiscordMessage 500 server error",
			callType:     "discord_message",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error":"internal error"}`,
			wantErr:      true,
			wantSubstr:   "status 500",
		},
		{
			name:         "DispatchDiscordInteraction success 200",
			callType:     "discord_interaction",
			statusCode:   http.StatusOK,
			responseBody: `{"status":"ok"}`,
		},
		{
			name:         "DispatchDiscordInteraction 400 client error",
			callType:     "discord_interaction",
			statusCode:   http.StatusBadRequest,
			responseBody: `{"error":"invalid payload"}`,
			wantErr:      true,
			wantSubstr:   "status 400",
		},
		{
			name:         "DispatchDiscordInteraction 500 server error",
			callType:     "discord_interaction",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error":"internal error"}`,
			wantErr:      true,
			wantSubstr:   "status 500",
		},
		{
			name:              "DispatchDiscordMessage context cancellation",
			callType:          "discord_message",
			cancelDuringRetry: true,
			failCount:         5,
			wantErr:           true,
		},
		{
			name:              "DispatchDiscordInteraction context cancellation",
			callType:          "discord_interaction",
			cancelDuringRetry: true,
			failCount:         5,
			wantErr:           true,
		},
	}
}

func defaultOutboundDispatcherCases() []defaultOutboundDispatcherCase {
	return append(defaultOutboundDispatcherPromptCases(), defaultOutboundDispatcherDiscordCases()...)
}

func newDispatcherMockHandler(tc defaultOutboundDispatcherCase, attempts *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		att := attempts.Add(1)
		if int(att) <= tc.failCount {
			http.Error(w, "transient error", http.StatusInternalServerError)
			return
		}
		if tc.responseHeaderType != "" {
			w.Header().Set("Content-Type", tc.responseHeaderType)
		} else {
			w.Header().Set("Content-Type", "application/json")
		}
		status := tc.statusCode
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(tc.responseBody))
	}
}

func executeDispatcherCall(testCtx context.Context, disp *DefaultOutboundDispatcher, tc defaultOutboundDispatcherCase) error {
	switch tc.callType {
	case "prompt":
		return disp.DispatchPrompt(testCtx, PromptRequest{ChannelID: tc.snowflake, Prompt: "test prompt message"})
	case "direct_message":
		return disp.DispatchDirectMessage(testCtx, DirectMessageRequest{ChannelID: tc.snowflake, Content: "test content"})
	case "dispatch_background":
		return disp.DispatchBackground(testCtx, DispatchRequest{Source: "webhook", Prompt: "test dispatch", Effort: "medium"})
	case "discord_message":
		return disp.DispatchDiscordMessage(testCtx, DiscordMessageIngressRequest{
			BotUserID: "bot-1",
			RoleNames: map[string]string{"role-1": "admin"},
			Message:   &discordgo.Message{ID: "msg-123", Content: "hello"},
		})
	case "discord_interaction":
		return disp.DispatchDiscordInteraction(testCtx, &discordgo.Interaction{
			ID:   "inter-123",
			Type: discordgo.InteractionApplicationCommand,
		})
	}
	return nil
}

func runDefaultOutboundDispatcherCase(ctx context.Context, t *testing.T, tc defaultOutboundDispatcherCase) {
	t.Helper()
	var attempts atomic.Int32
	ts := httptest.NewServer(newDispatcherMockHandler(tc, &attempts))
	defer ts.Close()

	disp := NewDefaultOutboundDispatcher(ts.URL, nil)
	delay := 1 * time.Millisecond
	if tc.cancelDuringRetry {
		delay = 30 * time.Millisecond
	}
	retries := tc.maxRetries
	if retries <= 0 {
		retries = 2
	}
	disp.SetRetryConfig(delay, retries)

	testCtx := ctx
	if tc.cancelDuringRetry {
		var cancel context.CancelFunc
		testCtx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
	}

	var beforeRetry float64
	if tc.wantRetryRecorded {
		beforeRetry = testutil.ToFloat64(metrics.JobRetriesTotal.WithLabelValues(QueueDefault, "brain_http_dispatch", "brain_http_retry"))
	}

	err := executeDispatcherCall(testCtx, disp, tc)

	if (err != nil) != tc.wantErr {
		t.Fatalf("Dispatch (%s) err = %v, wantErr = %v", tc.callType, err, tc.wantErr)
	}
	if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
		t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
	}
	if tc.wantRetryRecorded {
		afterRetry := testutil.ToFloat64(metrics.JobRetriesTotal.WithLabelValues(QueueDefault, "brain_http_dispatch", "brain_http_retry"))
		if afterRetry <= beforeRetry {
			t.Errorf("expected brain_http_retry to increment, before=%v after=%v", beforeRetry, afterRetry)
		}
	}
	if tc.wantRequests > 0 && int(attempts.Load()) != tc.wantRequests {
		t.Errorf("got %d HTTP requests, want %d", attempts.Load(), tc.wantRequests)
	}
}

func TestDefaultOutboundDispatcher(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, tc := range defaultOutboundDispatcherCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDefaultOutboundDispatcherCase(ctx, t, tc)
		})
	}
}
