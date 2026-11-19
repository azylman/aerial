package ericlient

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
	"github.com/azylman/aerial/services/synapse/internal/model"
)

type dispatchTestCase struct {
	name              string
	callType          string
	snowflake         string
	statusCode        int
	responseBody      string
	failCount         int
	maxRetries        int
	cancelDuringRetry bool
	preCancelled      bool
	transportError    bool
	zeroRetryConfig   bool
	nilSubClient      bool
	noBrainURL        bool
	wantRetryRecorded bool
	wantErr           bool
	wantSubstr        string
}

func ericlientTestCases() []dispatchTestCase {
	return append(ericlientPromptCases(), ericlientNonPromptCases()...)
}

func ericlientPromptCases() []dispatchTestCase {
	validSnowflake := "100000000000000001"
	return []dispatchTestCase{
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
			name:         "DispatchPrompt pre-cancelled context",
			callType:     "prompt",
			snowflake:    validSnowflake,
			preCancelled: true,
			wantErr:      true,
		},
		{
			name:           "DispatchPrompt transport failure",
			callType:       "prompt",
			snowflake:      validSnowflake,
			transportError: true,
			wantErr:        true,
			wantSubstr:     "request failed",
		},
		{
			name:            "DispatchPrompt default zero retry bounds",
			callType:        "prompt",
			snowflake:       validSnowflake,
			zeroRetryConfig: true,
			responseBody:    `{"status":"queued"}`,
		},
		{
			name:       "DispatchPrompt empty brainURL",
			callType:   "prompt",
			snowflake:  validSnowflake,
			noBrainURL: true,
			wantErr:    true,
			wantSubstr: "brain URL is not configured",
		},
	}
}

func ericlientNonPromptCases() []dispatchTestCase {
	validSnowflake := "100000000000000001"
	return []dispatchTestCase{
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
			name:       "DispatchDiscordMessage empty brainURL",
			callType:   "discord_message",
			noBrainURL: true,
			wantErr:    true,
			wantSubstr: "brain URL is not configured",
		},
		{
			name:           "DispatchDiscordMessage transport failure",
			callType:       "discord_message",
			transportError: true,
			wantErr:        true,
			wantSubstr:     "request failed",
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
		{
			name:         "DispatchDiscordMessage nil discordMsgClient uses httpClient",
			callType:     "discord_message",
			statusCode:   http.StatusOK,
			responseBody: `{"status":"processed","message_id":"12345"}`,
			nilSubClient: true,
		},
		{
			name:         "DispatchDiscordInteraction nil discordInteractionClient uses httpClient",
			callType:     "discord_interaction",
			statusCode:   http.StatusOK,
			responseBody: `{"status":"ok"}`,
			nilSubClient: true,
		},
	}
}

func executeClientCall(ctx context.Context, client *Client, tc dispatchTestCase) error {
	switch tc.callType {
	case "prompt":
		return client.DispatchPrompt(ctx, model.PromptRequest{ChannelID: tc.snowflake, Prompt: "test prompt"})
	case "direct_message":
		return client.DispatchDirectMessage(ctx, model.DirectMessageRequest{ChannelID: tc.snowflake, Content: "test message"})
	case "dispatch_background":
		return client.DispatchBackground(ctx, model.DispatchRequest{Source: "test", Prompt: "test dispatch", Effort: "medium"})
	case "discord_message":
		return client.DispatchDiscordMessage(ctx, model.DiscordMessageIngressRequest{
			BotUserID: "bot1",
			RoleNames: map[string]string{"r1": "admin"},
			Message:   &discordgo.Message{ID: "m1", Content: "hello"},
		})
	case "discord_interaction":
		return client.DispatchDiscordInteraction(ctx, &discordgo.Interaction{
			ID:   "ix1",
			Type: discordgo.InteractionApplicationCommand,
		})
	}
	return nil
}

func runClientTestCase(t *testing.T, tc dispatchTestCase) {
	t.Helper()

	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := attempts.Add(1)
		if int(att) <= tc.failCount {
			http.Error(w, "transient error", http.StatusInternalServerError)
			return
		}
		status := tc.statusCode
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(tc.responseBody))
	}))
	if tc.transportError {
		ts.Close()
	} else {
		defer ts.Close()
	}

	brainURL := ts.URL
	if tc.noBrainURL {
		brainURL = ""
	}
	client := NewClient(brainURL, nil)
	if tc.nilSubClient {
		client.SetDiscordMsgClient(nil)
		client.SetDiscordInteractionClient(nil)
	}

	if !tc.zeroRetryConfig {
		delay := 1 * time.Millisecond
		if tc.cancelDuringRetry {
			delay = 30 * time.Millisecond
		}
		retries := tc.maxRetries
		if retries <= 0 {
			retries = 2
		}
		client.SetRetryConfig(delay, retries)
	} else {
		client.SetRetryConfig(0, 0)
	}

	ctx := context.Background()
	if tc.cancelDuringRetry {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
	}
	if tc.preCancelled {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	}

	var beforeRetry float64
	if tc.wantRetryRecorded {
		beforeRetry = testutil.ToFloat64(metrics.JobRetriesTotal.WithLabelValues(metrics.QueueDefault, "brain_http_dispatch", "brain_http_retry"))
	}

	err := executeClientCall(ctx, client, tc)

	if (err != nil) != tc.wantErr {
		t.Fatalf("callType=%s err=%v, wantErr=%v", tc.callType, err, tc.wantErr)
	}
	if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
		t.Errorf("err=%v, want substr %q", err, tc.wantSubstr)
	}
	if tc.wantRetryRecorded {
		afterRetry := testutil.ToFloat64(metrics.JobRetriesTotal.WithLabelValues(metrics.QueueDefault, "brain_http_dispatch", "brain_http_retry"))
		if afterRetry <= beforeRetry {
			t.Errorf("expected retry metric increment: before=%v after=%v", beforeRetry, afterRetry)
		}
	}
}

func TestClient(t *testing.T) {
	t.Parallel()

	for _, tc := range ericlientTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runClientTestCase(t, tc)
		})
	}
}

func TestAdvanceBackoffDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		delay      time.Duration
		expBackoff bool
		want       time.Duration
	}{
		{name: "no exponential backoff", delay: 100 * time.Millisecond, expBackoff: false, want: 100 * time.Millisecond},
		{name: "exponential backoff scale", delay: 100 * time.Millisecond, expBackoff: true, want: 150 * time.Millisecond},
		{name: "exponential backoff cap", delay: 4 * time.Second, expBackoff: true, want: 5 * time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := AdvanceBackoffDelay(tc.delay, tc.expBackoff)
			if got != tc.want {
				t.Errorf("AdvanceBackoffDelay(%v, %v) = %v, want %v", tc.delay, tc.expBackoff, got, tc.want)
			}
		})
	}
}

func TestNewClientWithCustomHTTPClient(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer ts.Close()
	custom := ts.Client()
	custom.Timeout = 5 * time.Second
	c := NewClient("http://localhost:9999", custom)
	if c.brainURL != "http://localhost:9999" {
		t.Errorf("brainURL = %q, want http://localhost:9999", c.brainURL)
	}
	if c.httpClient == nil || c.discordMsgClient == nil || c.discordInteractionClient == nil {
		t.Fatalf("expected non-nil wrapped clients")
	}
}

func TestClientMarshalErrors(t *testing.T) {
	t.Parallel()
	c := NewClient("http://localhost:8080", nil)
	unmarshalable := make(chan int)

	if err := c.PostBrainWithRetry(context.Background(), "/prompt", "prompt", "status", "100000000000000001", 1, 0, false, unmarshalable); err == nil {
		t.Errorf("expected marshal error from postBrainWithRetry")
	}
	if err := c.PostDiscordInternalJSON(context.Background(), nil, time.Second, "/discord", "req", "status", unmarshalable); err == nil {
		t.Errorf("expected marshal error from postDiscordInternalJSON")
	}
}

func TestValidateBrainChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		channelID    string
		endpointPath string
		wantErr      bool
	}{
		{name: "dispatch endpoint empty channel valid", channelID: "", endpointPath: "/dispatch", wantErr: false},
		{name: "prompt endpoint valid snowflake", channelID: "123456789012345678", endpointPath: "/prompt", wantErr: false},
		{name: "prompt endpoint invalid snowflake", channelID: "not a snowflake", endpointPath: "/prompt", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateBrainChannel(tc.channelID, tc.endpointPath)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateBrainChannel(%q, %q) err = %v, wantErr = %v", tc.channelID, tc.endpointPath, err, tc.wantErr)
			}
		})
	}
}
