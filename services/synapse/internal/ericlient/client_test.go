package ericlient

import (
	"cmp"
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

const dispatchSnowflake = "100000000000000001"

// dispatchCase is the row shape shared by the per-method Dispatch* tables.
// The fake eri answers 500 to the first failCount hits, then status/body.
type dispatchCase struct {
	name         string
	channel      string
	failCount    int
	status       int
	body         string
	retryDelay   time.Duration
	maxAttempts  int
	noBrainURL   bool
	closedServer bool
	ctxState     string // "" live, "cancelled" before the call, "cancel_on_hit" when the first request lands
	wantHits     int32
	wantRetry    bool
	wantErr      string
}

// newDispatchClient builds the fake eri, the client and the call context
// for one dispatchCase row.
func newDispatchClient(t *testing.T, tc dispatchCase) (*Client, context.Context, *atomic.Int32) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hit := hits.Add(1); int(hit) <= tc.failCount {
			if tc.ctxState == "cancel_on_hit" {
				cancel()
			}
			http.Error(w, "transient error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(cmp.Or(tc.status, http.StatusOK))
		_, _ = w.Write([]byte(tc.body))
	}))
	t.Cleanup(ts.Close)
	if tc.closedServer {
		ts.Close()
	}
	brainURL := ts.URL
	if tc.noBrainURL {
		brainURL = ""
	}
	c := NewClient(brainURL, nil)
	c.SetRetryConfig(tc.retryDelay, tc.maxAttempts)
	if tc.ctxState == "cancelled" {
		cancel()
	}
	return c, ctx, &hits
}

func retryCount() float64 {
	return testutil.ToFloat64(metrics.JobRetriesTotal.WithLabelValues(metrics.QueueDefault, "brain_http_dispatch", "brain_http_retry"))
}

// assertDispatch checks the error text, the number of requests the fake
// eri saw and, for retrying rows, that the retry metric moved.
func assertDispatch(t *testing.T, tc dispatchCase, err error, hits int32, retriesBefore float64) {
	t.Helper()
	if tc.wantErr == "" && err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
		t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
	}
	if hits != tc.wantHits {
		t.Errorf("eri hits = %d, want %d", hits, tc.wantHits)
	}
	if tc.wantRetry && retryCount() <= retriesBefore {
		t.Errorf("retry metric did not move from %v", retriesBefore)
	}
}

func TestDispatchPrompt(t *testing.T) {
	t.Parallel()
	tests := []dispatchCase{
		{name: "invalid snowflake", channel: "invalid channel!", wantErr: "invalid discord snowflake"},
		{name: "channel name accepted", channel: "aerial-dev", body: `{"status":"queued"}`, maxAttempts: 2, wantHits: 1},
		{name: "retry on 500 then success", channel: dispatchSnowflake, failCount: 1, retryDelay: time.Millisecond, maxAttempts: 4, wantHits: 2, wantRetry: true},
		{name: "exhausting retries on 500", channel: dispatchSnowflake, failCount: 5, retryDelay: time.Millisecond, maxAttempts: 2, wantHits: 2, wantErr: "returned status 500"},
		{name: "cancel during retry backoff", channel: dispatchSnowflake, failCount: 5, retryDelay: time.Hour, maxAttempts: 4, ctxState: "cancel_on_hit", wantHits: 1, wantErr: "context canceled"},
		{name: "pre-cancelled context", channel: dispatchSnowflake, maxAttempts: 2, ctxState: "cancelled", wantErr: "context canceled"},
		{name: "transport failure", channel: dispatchSnowflake, retryDelay: time.Millisecond, maxAttempts: 2, closedServer: true, wantErr: "request failed"},
		{name: "zero retry bounds use defaults", channel: dispatchSnowflake, body: `{"status":"queued"}`, wantHits: 1},
		{name: "empty brain url", channel: dispatchSnowflake, noBrainURL: true, wantErr: "brain URL is not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, ctx, hits := newDispatchClient(t, tc)
			before := retryCount()
			err := c.DispatchPrompt(ctx, model.PromptRequest{ChannelID: tc.channel, Prompt: "test prompt"})
			assertDispatch(t, tc, err, hits.Load(), before)
		})
	}
}

func TestDispatchDirectMessage(t *testing.T) {
	t.Parallel()
	tests := []dispatchCase{
		{name: "invalid snowflake", channel: "invalid channel!", wantErr: "invalid discord snowflake"},
		{name: "retry on 500 then success", channel: dispatchSnowflake, failCount: 1, retryDelay: time.Millisecond, body: `{"status":"sent"}`, wantHits: 2, wantRetry: true},
		{name: "400 client error no retry", channel: dispatchSnowflake, status: http.StatusBadRequest, body: `{"error":"bad request"}`, retryDelay: time.Millisecond, wantHits: 1, wantErr: `brain internal message returned status 400: {"error":"bad request"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, ctx, hits := newDispatchClient(t, tc)
			before := retryCount()
			err := c.DispatchDirectMessage(ctx, model.DirectMessageRequest{ChannelID: tc.channel, Content: "test message"})
			assertDispatch(t, tc, err, hits.Load(), before)
		})
	}
}

func TestDispatchBackground(t *testing.T) {
	t.Parallel()
	tests := []dispatchCase{
		{name: "success 200", body: `{"status":"accepted"}`, maxAttempts: 2, wantHits: 1},
		{name: "retry on 500 then success", failCount: 1, retryDelay: time.Millisecond, maxAttempts: 4, wantHits: 2, wantRetry: true},
		{name: "400 client error no retry", status: http.StatusBadRequest, body: `{"error":"bad request"}`, retryDelay: time.Millisecond, maxAttempts: 3, wantHits: 1, wantErr: "brain dispatch returned status 400"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, ctx, hits := newDispatchClient(t, tc)
			before := retryCount()
			err := c.DispatchBackground(ctx, model.DispatchRequest{Source: "test", Prompt: "test dispatch", Effort: "medium"})
			assertDispatch(t, tc, err, hits.Load(), before)
		})
	}
}

func TestDispatchDiscordMessage(t *testing.T) {
	t.Parallel()
	tests := []dispatchCase{
		{name: "success 200", body: `{"status":"processed","message_id":"12345"}`, wantHits: 1},
		{name: "400 client error", status: http.StatusBadRequest, body: `{"error":"invalid payload"}`, wantHits: 1, wantErr: `discord ingress returned status 400: {"error":"invalid payload"}`},
		{name: "500 server error not retried", status: http.StatusInternalServerError, body: `{"error":"internal error"}`, wantHits: 1, wantErr: "status 500"},
		{name: "empty brain url", noBrainURL: true, wantErr: "brain URL is not configured"},
		{name: "transport failure", closedServer: true, wantErr: "discord message ingress request failed"},
		{name: "cancelled context fails request", ctxState: "cancelled", wantErr: "discord message ingress request failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, ctx, hits := newDispatchClient(t, tc)
			before := retryCount()
			err := c.DispatchDiscordMessage(ctx, model.DiscordMessageIngressRequest{BotUserID: "bot1", RoleNames: map[string]string{"r1": "admin"}, Message: &discordgo.Message{ID: "m1", Content: "hello"}})
			assertDispatch(t, tc, err, hits.Load(), before)
		})
	}
}

func TestDispatchDiscordInteraction(t *testing.T) {
	t.Parallel()
	tests := []dispatchCase{
		{name: "success 200", body: `{"status":"ok"}`, wantHits: 1},
		{name: "400 client error", status: http.StatusBadRequest, body: `{"error":"invalid payload"}`, wantHits: 1, wantErr: "discord interaction returned status 400"},
		{name: "500 server error not retried", status: http.StatusInternalServerError, body: `{"error":"internal error"}`, wantHits: 1, wantErr: "status 500"},
		{name: "cancelled context fails request", ctxState: "cancelled", wantErr: "discord interaction request failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, ctx, hits := newDispatchClient(t, tc)
			before := retryCount()
			err := c.DispatchDiscordInteraction(ctx, &discordgo.Interaction{ID: "ix1", Type: discordgo.InteractionApplicationCommand})
			assertDispatch(t, tc, err, hits.Load(), before)
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

func TestDispatchDiscordToURL_UsesExplicitURL(t *testing.T) {
	t.Parallel()
	var fallbackHits, explicitHits atomic.Int32

	fallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer fallbackServer.Close()

	explicitServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		explicitHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer explicitServer.Close()

	c := NewClient(fallbackServer.URL, nil)

	ctx := context.Background()
	msgReq := model.DiscordMessageIngressRequest{
		BotUserID: "bot-1",
		Message:   &discordgo.Message{ID: "m1", Content: "hi"},
	}
	if err := c.DispatchDiscordMessageToURL(ctx, explicitServer.URL, msgReq); err != nil {
		t.Fatalf("DispatchDiscordMessageToURL err = %v", err)
	}

	interaction := &discordgo.Interaction{ID: "i1", Type: discordgo.InteractionApplicationCommand}
	if err := c.DispatchDiscordInteractionToURL(ctx, explicitServer.URL, interaction); err != nil {
		t.Fatalf("DispatchDiscordInteractionToURL err = %v", err)
	}

	if got := explicitHits.Load(); got != 2 {
		t.Errorf("explicitServer hits = %d, want 2", got)
	}
	if got := fallbackHits.Load(); got != 0 {
		t.Errorf("fallbackServer hits = %d, want 0", got)
	}
}
