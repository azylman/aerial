package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// SetRetryConfig overrides retry delay and attempt bounds for fast hermetic unit tests.
func (d *DefaultOutboundDispatcher) SetRetryConfig(delay time.Duration, maxAttempts int) {
	d.retryDelay = delay
	d.maxAttempts = maxAttempts
}

func TestDefaultOutboundDispatcher(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	validSnowflake := "1555405874565091380"

	tests := []struct {
		name               string
		callType           string
		snowflake          string
		statusCode         int
		responseBody       string
		responseHeaderType string
		failCount          int
		maxRetries         int
		cancelDuringRetry  bool
		nilSubClient       bool
		wantRetryRecorded  bool
		wantErr            bool
		wantSubstr         string
		wantMatchedJobs    int
	}{
		{
			name:         "DispatchGitPush success 200",
			callType:     "git_push",
			statusCode:   http.StatusOK,
			responseBody: `{"status":"accepted"}`,
		},
		{
			name:            "DispatchImageReady success 200",
			callType:        "image_ready",
			statusCode:      http.StatusOK,
			responseBody:    `{"status":"accepted","matched_jobs":["brain"]}`,
			wantMatchedJobs: 1,
		},
		{
			name:         "DispatchImageReady invalid JSON decode warning",
			callType:     "image_ready",
			statusCode:   http.StatusOK,
			responseBody: `{not json`,
		},
		{
			name:         "DispatchGitPush hangar 404 unmanaged tolerated",
			callType:     "git_push",
			statusCode:   http.StatusNotFound,
			responseBody: `{"status":"error","message":"repository not managed by hangar"}`,
		},
		{
			name:         "DispatchGitPush hangar 404 other returns error",
			callType:     "git_push",
			statusCode:   http.StatusNotFound,
			responseBody: "endpoint not found",
			wantErr:      true,
			wantSubstr:   "hangar git push returned 404",
		},
		{
			name:         "DispatchImageReady hangar 404 not found tolerated",
			callType:     "image_ready",
			statusCode:   http.StatusNotFound,
			responseBody: `{"status":"not_found","message":"no nomad jobs found using image"}`,
		},
		{
			name:         "DispatchGitPush 500 error not tolerated",
			callType:     "git_push",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error":"internal"}`,
			wantErr:      true,
		},
		{
			name:         "DispatchImageReady 500 error not tolerated",
			callType:     "image_ready",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error":"internal"}`,
			wantErr:      true,
		},
		{
			name:               "DispatchGitPush HTML 404 rejected",
			callType:           "git_push",
			statusCode:         http.StatusNotFound,
			responseHeaderType: "text/html",
			responseBody:       `<html><title>404 Not Found</title><body>Proxy Error</body></html>`,
			wantErr:            true,
		},
		{
			name:               "DispatchImageReady HTML 404 rejected",
			callType:           "image_ready",
			statusCode:         http.StatusNotFound,
			responseHeaderType: "text/html",
			responseBody:       `<html><title>404 Not Found</title><body>Proxy Error</body></html>`,
			wantErr:            true,
		},
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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			}))
			defer ts.Close()

			disp := NewDefaultOutboundDispatcher(ts.URL, ts.URL, nil)
			if tc.nilSubClient {
				disp.discordMsgClient = nil
				disp.discordInteractionClient = nil
			}
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
				beforeRetry = testutil.ToFloat64(JobRetriesTotal.WithLabelValues(river.QueueDefault, "brain_http_dispatch", "brain_http_retry"))
			}

			var err error
			switch tc.callType {
			case "git_push":
				err = disp.DispatchGitPush(testCtx, GitPushEventRequest{Ref: "refs/heads/main", Commit: "testcommit123"})
			case "image_ready":
				jobs, e := disp.DispatchImageReady(testCtx, ImageReadyEventRequest{Image: "ghcr.io/azylman/aerial-synapse:latest", Digest: "sha256:abcd"})
				err = e
				if tc.wantMatchedJobs > 0 && len(jobs) != tc.wantMatchedJobs {
					t.Errorf("got %d matched jobs, want %d", len(jobs), tc.wantMatchedJobs)
				}
			case "prompt":
				err = disp.DispatchPrompt(testCtx, PromptRequest{ChannelID: tc.snowflake, Prompt: "test prompt message"})
			case "direct_message":
				err = disp.DispatchDirectMessage(testCtx, DirectMessageRequest{ChannelID: tc.snowflake, Content: "test content"})
			case "dispatch_background":
				err = disp.DispatchBackground(testCtx, DispatchRequest{Source: "webhook", Prompt: "test dispatch", Effort: "medium"})
			case "discord_message":
				err = disp.DispatchDiscordMessage(testCtx, DiscordMessageIngressRequest{
					BotUserID: "bot-1",
					RoleNames: map[string]string{"role-1": "admin"},
					Message:   &discordgo.Message{ID: "msg-123", Content: "hello"},
				})
			case "discord_interaction":
				err = disp.DispatchDiscordInteraction(testCtx, &discordgo.Interaction{
					ID:   "inter-123",
					Type: discordgo.InteractionApplicationCommand,
				})
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("Dispatch (%s) err = %v, wantErr = %v", tc.callType, err, tc.wantErr)
			}
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
			}
			if tc.wantRetryRecorded {
				afterRetry := testutil.ToFloat64(JobRetriesTotal.WithLabelValues(river.QueueDefault, "brain_http_dispatch", "brain_http_retry"))
				if afterRetry <= beforeRetry {
					t.Errorf("expected brain_http_retry to increment, before=%v after=%v", beforeRetry, afterRetry)
				}
			}
		})
	}
}

func TestDefaultOutboundDispatcher_DispatchWidget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name       string
		channelID  string
		statusCode int
		body       string
		wantErr    bool
		wantMsgID  string
	}{
		{
			name:      "invalid snowflake returns error",
			channelID: "bad-snowflake",
			wantErr:   true,
		},
		{
			name:       "server 200 OK returns message_id",
			channelID:  "1555405874565091380",
			statusCode: http.StatusOK,
			body:       `{"status":"ok","message_id":"msg-widget-dispatch-ok"}`,
			wantMsgID:  "msg-widget-dispatch-ok",
		},
		{
			name:       "server 400 bad request returns error",
			channelID:  "1555405874565091380",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"bad payload"}`,
			wantErr:    true,
		},
		{
			name:       "server invalid JSON returns error",
			channelID:  "1555405874565091380",
			statusCode: http.StatusOK,
			body:       `{not json`,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/discord/widget" {
					http.NotFound(w, r)
					return
				}
				code := tc.statusCode
				if code == 0 {
					code = http.StatusOK
				}
				w.WriteHeader(code)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()

			disp := NewDefaultOutboundDispatcher("", ts.URL, ts.Client())
			msgID, err := disp.DispatchWidget(ctx, StatusWidgetRequest{
				ChannelID: tc.channelID,
				ID:        "widget-1",
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("DispatchWidget() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if msgID != tc.wantMsgID {
				t.Errorf("got msgID = %q, want %q", msgID, tc.wantMsgID)
			}
		})
	}
}

func TestRiverWorkers(t *testing.T) {
	ctx := context.Background()

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/deployments"):
			_, _ = fmt.Fprint(w, `[{"ID":"dep-1","Status":"successful"}]`)
		case strings.Contains(r.URL.Path, "/allocations"):
			_, _ = w.Write([]byte(`[{"ID":"alloc-1","DesiredStatus":"run","ClientStatus":"running"}]`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(nomadServer.Close)

	tests := []struct {
		name          string
		workerType    string
		nilServer     bool
		downstreamErr bool
		attempt       int
		maxAttempts   int
		wantProcessed string
		wantErr       bool
	}{
		{name: "GitHubWebhookWorker valid job", workerType: "github", wantProcessed: "success"},
		{name: "GitHubWebhookWorker nil job handled gracefully", workerType: "github_nil_job"},
		{name: "GitHubWebhookWorker retry attempt > 1", workerType: "github", attempt: 2, wantProcessed: "success"},
		{name: "GitHubWebhookWorker max attempts exhausted discard", workerType: "github", attempt: 5, maxAttempts: 5, downstreamErr: true, wantErr: true, wantProcessed: "discarded"},
		{name: "GitHubWebhookWorker nil server error", workerType: "github", nilServer: true, wantErr: true, wantProcessed: "error"},
		{name: "GitHubWebhookWorker downstream processing error", workerType: "github", downstreamErr: true, wantErr: true, wantProcessed: "error"},
		{name: "HangarWebhookWorker valid job", workerType: "hangar", wantProcessed: "success"},
		{name: "HangarWebhookWorker nil server error", workerType: "hangar", nilServer: true, wantErr: true, wantProcessed: "error"},
		{name: "HangarWebhookWorker downstream dispatch error", workerType: "hangar", downstreamErr: true, wantErr: true, wantProcessed: "error"},
		{name: "NomadEventWorker valid job", workerType: "nomad", wantProcessed: "success"},
		{name: "NomadEventWorker max attempts exhausted discard", workerType: "nomad", attempt: 5, maxAttempts: 5, downstreamErr: true, wantErr: true, wantProcessed: "discarded"},
		{name: "NomadEventWorker nil server error", workerType: "nomad", nilServer: true, wantErr: true, wantProcessed: "error"},
		{name: "NomadEventWorker downstream prompt error", workerType: "nomad", downstreamErr: true, wantErr: true, wantProcessed: "error"},
		{name: "NomadCrashAlertWorker valid job", workerType: "crash", wantProcessed: "success"},
		{name: "NomadCrashAlertWorker nil server error", workerType: "crash", nilServer: true, wantErr: true, wantProcessed: "error"},
		{name: "NomadCrashAlertWorker downstream prompt error", workerType: "crash", downstreamErr: true, wantErr: true, wantProcessed: "error"},
		{name: "WidgetSyncWorker valid job", workerType: "widget", wantProcessed: "success"},
		{name: "WidgetSyncWorker nil server error", workerType: "widget", nilServer: true, wantErr: true, wantProcessed: "error"},
		{name: "WidgetSyncWorker downstream dispatch error", workerType: "widget", downstreamErr: true, wantErr: true, wantProcessed: "error"},
		{name: "EventTriggerWebhookWorker valid job", workerType: "event_trigger", wantProcessed: "success"},
		{name: "EventTriggerWebhookWorker nil server error", workerType: "event_trigger", nilServer: true, wantErr: true, wantProcessed: "error"},
		{name: "EventTriggerWebhookWorker downstream fire error", workerType: "event_trigger", downstreamErr: true, wantErr: true, wantProcessed: "error"},
		{name: "MergeConflictWorker valid job", workerType: "merge_conflict", wantProcessed: "success"},
		{name: "MergeConflictWorker nil server error", workerType: "merge_conflict", nilServer: true, wantErr: true, wantProcessed: "error"},
		{name: "MergeConflictWorker downstream dispatch error", workerType: "merge_conflict", downstreamErr: true, wantErr: true, wantProcessed: "error"},
		{name: "DiscordIngressWorker valid job", workerType: "discord_ingress", wantProcessed: "success"},
		{name: "DiscordIngressWorker nil server error", workerType: "discord_ingress", nilServer: true, wantErr: true, wantProcessed: "error"},
		{name: "DiscordIngressWorker 4xx cancel", workerType: "discord_ingress_4xx", wantErr: true, wantProcessed: "cancelled"},
		{name: "DiscordIngressWorker retry then success", workerType: "discord_ingress_retry", wantProcessed: "success"},
		{name: "DiscordIngressWorker 30m TTL cancel", workerType: "discord_ingress_ttl", wantErr: true, wantProcessed: "cancelled"},
		{name: "DiscordIngressWorker zero retryDelay", workerType: "discord_ingress_zero_delay", wantErr: true, wantProcessed: "cancelled"},
		{name: "DiscordIngressWorker large retryDelay", workerType: "discord_ingress_large_delay", wantErr: true, wantProcessed: "cancelled"},
		{name: "DiscordIngressWorker context cancelled", workerType: "discord_ingress_ctx_cancel", wantErr: true, wantProcessed: "error"},
		{name: "DiscordInteractionWorker valid job", workerType: "discord_interaction", wantProcessed: "success"},
		{name: "DiscordInteractionWorker nil server error", workerType: "discord_interaction", nilServer: true, wantErr: true, wantProcessed: "error"},
		{name: "DiscordInteractionWorker downstream error", workerType: "discord_interaction", downstreamErr: true, wantErr: true, wantProcessed: "error"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockReg := &mockPRRegistry{
				deployedTargetID: "1555405874565091380",
				deployedPRNum:    552,
				deployedMergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
				deployedRepo:     "azylman/aerial",
				deployedUpdated:  true,
				resolveTargetID:  "1555405874565091380",
				resolveBranch:    "feat/widget",
			}
			mockDisp := &mockOutboundDispatcher{}
			if tc.downstreamErr {
				mockDisp.promptErr = errors.New("503 downstream failure")
				mockDisp.widgetErr = errors.New("503 downstream failure")
				mockDisp.dispatchErr = errors.New("503 downstream failure")
				if tc.workerType == "event_trigger" {
					mockReg.claimTriggersErr = errors.New("502 haro fire failure")
				}
			}
			srv := newTestRouter(Config{
				NomadAddr:       nomadServer.URL,
				SystemChannelID: "1555405874565091380",
			}, nil, mockReg, mockDisp)

			expectedQueue := river.QueueDefault
			expectedKind := ""
			switch {
			case tc.workerType == "github":
				expectedKind = "github_webhook"
			case tc.workerType == "hangar":
				expectedKind = "hangar_webhook"
			case tc.workerType == "nomad":
				expectedKind = "nomad_event"
			case tc.workerType == "crash":
				expectedKind = "nomad_crash_alert"
			case tc.workerType == "widget":
				expectedQueue = QueueWidgetSync
				expectedKind = "widget_sync"
			case tc.workerType == "event_trigger":
				expectedKind = "event_trigger_webhook"
			case tc.workerType == "merge_conflict":
				expectedQueue = QueueMergeConflict
				expectedKind = "merge_conflict"
			case strings.HasPrefix(tc.workerType, "discord_ingress"):
				expectedQueue = QueueDiscordIngress
				expectedKind = "discord_ingress"
			case tc.workerType == "discord_interaction":
				expectedQueue = QueueDiscordInteraction
				expectedKind = "discord_interaction"
			}

			var beforeProcessed, beforeRetry, beforeDropped float64
			if tc.wantProcessed != "" {
				beforeProcessed = testutil.ToFloat64(JobsProcessedTotal.WithLabelValues(expectedQueue, expectedKind, tc.wantProcessed))
			}
			if tc.attempt > 1 {
				beforeRetry = testutil.ToFloat64(JobRetriesTotal.WithLabelValues(expectedQueue, expectedKind, "river_retry"))
			}
			if tc.wantProcessed == "discarded" {
				beforeDropped = testutil.ToFloat64(JobsDroppedTotal.WithLabelValues(expectedQueue, expectedKind, "max_attempts_exhausted"))
			}

			var err error
			switch tc.workerType {
			case "github":
				w := &GitHubWebhookWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[GitHubWebhookArgs]{
					JobRow: &rivertype.JobRow{ID: 101, Attempt: tc.attempt, MaxAttempts: tc.maxAttempts},
					Args: GitHubWebhookArgs{
						Event:    "ping",
						Delivery: "del-worker-1",
						Body:     []byte(`{"zen":"Responsive is better than fast.","hook_id":123,"repository":{"full_name":"azylman/aerial"}}`),
					},
				}
				if tc.downstreamErr {
					job.Args.Event = "pull_request"
					job.Args.Body = []byte(`{malformed`)
				}
				err = w.Work(ctx, job)
			case "github_nil_job":
				err = observeRiverJob[GitHubWebhookArgs](nil, "", func() error { return nil })
			case "hangar":
				w := &HangarWebhookWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[HangarWebhookArgs]{
					JobRow: &rivertype.JobRow{ID: 201, Attempt: tc.attempt, MaxAttempts: tc.maxAttempts},
					Args: HangarWebhookArgs{
						Event: HangarDeployEvent{
							Event: "deploy_success", JobName: "brain",
							PRNumber: 558, TargetID: "1555405874565091380", Status: "success",
						},
					},
				}
				err = w.Work(ctx, job)
			case "nomad":
				w := &NomadEventWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[NomadEventArgs]{
					JobRow: &rivertype.JobRow{ID: 301, Attempt: tc.attempt, MaxAttempts: tc.maxAttempts},
					Args: NomadEventArgs{
						Topic:   "Deployment",
						Type:    "DeploymentStatusUpdate",
						Payload: []byte(`{"Deployment":{"ID":"dep-101","JobID":"brain","Status":"successful"}}`),
					},
				}
				err = w.Work(ctx, job)
			case "crash":
				w := &NomadCrashAlertWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[NomadCrashAlertArgs]{
					JobRow: &rivertype.JobRow{ID: 401, Attempt: tc.attempt, MaxAttempts: tc.maxAttempts},
					Args: NomadCrashAlertArgs{
						JobID:      "openobserve",
						AllocID:    "alloc-oom",
						Status:     "failed",
						StatusDesc: "OOMKilled exit code 137",
					},
				}
				err = w.Work(ctx, job)
			case "widget":
				w := &WidgetSyncWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[WidgetSyncArgs]{
					JobRow: &rivertype.JobRow{ID: 501, Attempt: tc.attempt, MaxAttempts: tc.maxAttempts},
					Args: WidgetSyncArgs{
						Repo:     "azylman/aerial",
						PRNumber: 552,
					},
				}
				err = w.Work(ctx, job)
			case "event_trigger":
				w := &EventTriggerWebhookWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[EventTriggerWebhookArgs]{
					JobRow: &rivertype.JobRow{ID: 601, Attempt: tc.attempt, MaxAttempts: tc.maxAttempts},
					Args: EventTriggerWebhookArgs{
						Source:        "custom_source",
						Targets:       []string{"device.alpha"},
						EventType:     "ready",
						ContextHeader: "External event",
					},
				}
				err = w.Work(ctx, job)
			case "merge_conflict":
				w := &MergeConflictWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[MergeConflictArgs]{
					JobRow: &rivertype.JobRow{ID: 701, Attempt: tc.attempt, MaxAttempts: tc.maxAttempts},
					Args: MergeConflictArgs{
						Repo:     "azylman/aerial",
						PRNumber: 543,
						Branch:   "feat/conflict",
						HeadSHA:  "head543",
						TargetID: "1555405874565091380",
					},
				}
				err = w.Work(ctx, job)
			case "discord_ingress", "discord_ingress_4xx", "discord_ingress_retry", "discord_ingress_ttl", "discord_ingress_zero_delay", "discord_ingress_large_delay", "discord_ingress_ctx_cancel":
				w := &DiscordIngressWorker{server: srv, retryDelay: 1 * time.Millisecond}
				switch tc.workerType {
				case "discord_ingress_zero_delay":
					w.retryDelay = 0
				case "discord_ingress_large_delay":
					w.retryDelay = 10 * time.Second
				}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[DiscordIngressArgs]{
					JobRow: &rivertype.JobRow{
						ID:          801,
						CreatedAt:   time.Now(),
						Attempt:     tc.attempt,
						MaxAttempts: tc.maxAttempts,
					},
					Args: DiscordIngressArgs{
						MessageID: "msg-worker-1",
						BotUserID: "bot-1",
						Message:   &discordgo.Message{ID: "msg-worker-1", Content: "ping"},
					},
				}
				workCtx := ctx
				switch tc.workerType {
				case "discord_ingress_4xx", "discord_ingress_zero_delay", "discord_ingress_large_delay":
					mockDisp.discordMessageErr = errors.New("status 400 bad request")
				case "discord_ingress_retry":
					mockDisp.discordMessageFailCount = 1
					mockDisp.discordMessageErr = errors.New("status 503 unavailable")
				case "discord_ingress_ttl":
					job.CreatedAt = time.Now().Add(-31 * time.Minute)
					mockDisp.discordMessageErr = errors.New("status 500 error")
				case "discord_ingress_ctx_cancel":
					cancCtx, cancel := context.WithCancel(ctx)
					cancel()
					workCtx = cancCtx
				}
				err = w.Work(workCtx, job)
			case "discord_interaction":
				w := &DiscordInteractionWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[DiscordInteractionArgs]{
					JobRow: &rivertype.JobRow{ID: 901, Attempt: tc.attempt, MaxAttempts: tc.maxAttempts},
					Args: DiscordInteractionArgs{
						InteractionID: "inter-worker-1",
						Interaction:   &discordgo.Interaction{ID: "inter-worker-1", Type: discordgo.InteractionApplicationCommand},
					},
				}
				if tc.downstreamErr {
					mockDisp.discordInteractionErr = errors.New("500 dispatch interaction error")
				}
				err = w.Work(ctx, job)
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("Work() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantProcessed != "" {
				afterProcessed := testutil.ToFloat64(JobsProcessedTotal.WithLabelValues(expectedQueue, expectedKind, tc.wantProcessed))
				if afterProcessed <= beforeProcessed {
					t.Errorf("JobsProcessedTotal[%s] = %v, want > %v", tc.wantProcessed, afterProcessed, beforeProcessed)
				}
			}
			if tc.attempt > 1 {
				afterRetry := testutil.ToFloat64(JobRetriesTotal.WithLabelValues(expectedQueue, expectedKind, "river_retry"))
				if afterRetry <= beforeRetry {
					t.Errorf("JobRetriesTotal[river_retry] = %v, want > %v", afterRetry, beforeRetry)
				}
			}
			if tc.wantProcessed == "discarded" {
				afterDropped := testutil.ToFloat64(JobsDroppedTotal.WithLabelValues(expectedQueue, expectedKind, "max_attempts_exhausted"))
				if afterDropped <= beforeDropped {
					t.Errorf("JobsDroppedTotal[max_attempts_exhausted] = %v, want > %v", afterDropped, beforeDropped)
				}
			}
		})
	}
}

func TestRiverJobArgsAndUniqueOpts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		args         river.JobArgs
		wantKind     string
		wantQueue    string
		wantByArgs   bool
		wantByPeriod time.Duration
		wantAttempts int
	}{
		{
			name:     "GitHubWebhookArgs",
			args:     GitHubWebhookArgs{Delivery: "del-1"},
			wantKind: "github_webhook",
		},
		{
			name:         "HangarWebhookArgs",
			args:         HangarWebhookArgs{IdempotencyKey: "key-1"},
			wantKind:     "hangar_webhook",
			wantByArgs:   true,
			wantByPeriod: 15 * time.Minute,
		},
		{
			name:         "NomadEventArgs",
			args:         NomadEventArgs{Topic: "Deployment"},
			wantKind:     "nomad_event",
			wantByArgs:   true,
			wantByPeriod: 15 * time.Minute,
		},
		{
			name:         "NomadCrashAlertArgs",
			args:         NomadCrashAlertArgs{JobID: "brain"},
			wantKind:     "nomad_crash_alert",
			wantByArgs:   true,
			wantByPeriod: 15 * time.Minute,
		},
		{
			name:         "WidgetSyncArgs",
			args:         WidgetSyncArgs{Repo: "azylman/aerial", PRNumber: 101},
			wantKind:     "widget_sync",
			wantQueue:    QueueWidgetSync,
			wantAttempts: 5,
		},
		{
			name:         "EventTriggerWebhookArgs",
			args:         EventTriggerWebhookArgs{Source: "custom_source", Targets: []string{"device.alpha"}, EventType: "ready"},
			wantKind:     "event_trigger_webhook",
			wantAttempts: 5,
		},
		{
			name:         "MergeConflictArgs",
			args:         MergeConflictArgs{Repo: "azylman/aerial", PRNumber: 543},
			wantKind:     "merge_conflict",
			wantQueue:    QueueMergeConflict,
			wantByArgs:   true,
			wantByPeriod: 15 * time.Minute,
			wantAttempts: 5,
		},
		{
			name:         "DiscordIngressArgs",
			args:         DiscordIngressArgs{MessageID: "msg-test-1"},
			wantKind:     "discord_ingress",
			wantQueue:    QueueDiscordIngress,
			wantByArgs:   true,
			wantByPeriod: 30 * time.Minute,
			wantAttempts: 30,
		},
		{
			name:         "DiscordInteractionArgs",
			args:         DiscordInteractionArgs{InteractionID: "inter-test-1"},
			wantKind:     "discord_interaction",
			wantQueue:    QueueDiscordInteraction,
			wantByArgs:   true,
			wantByPeriod: 15 * time.Minute,
			wantAttempts: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.args.Kind(); got != tc.wantKind {
				t.Errorf("Kind() = %q, want %q", got, tc.wantKind)
			}
			optsProvider, ok := tc.args.(interface{ InsertOpts() river.InsertOpts })
			if !ok {
				return
			}
			opts := optsProvider.InsertOpts()
			if tc.wantQueue != "" && opts.Queue != tc.wantQueue {
				t.Errorf("Queue = %q, want %q", opts.Queue, tc.wantQueue)
			}
			if tc.wantByArgs && !opts.UniqueOpts.ByArgs {
				t.Errorf("UniqueOpts.ByArgs = false, want true")
			}
			if tc.wantByPeriod > 0 && opts.UniqueOpts.ByPeriod != tc.wantByPeriod {
				t.Errorf("UniqueOpts.ByPeriod = %v, want %v", opts.UniqueOpts.ByPeriod, tc.wantByPeriod)
			}
			if tc.wantAttempts > 0 && opts.MaxAttempts != tc.wantAttempts {
				t.Errorf("MaxAttempts = %d, want %d", opts.MaxAttempts, tc.wantAttempts)
			}
		})
	}
}

func TestDiscordWorkerTimeouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		timeout     time.Duration
		wantTimeout time.Duration
	}{
		{
			name:        "DiscordIngressWorker timeout",
			timeout:     (&DiscordIngressWorker{}).Timeout(nil),
			wantTimeout: 30 * time.Minute,
		},
		{
			name:        "DiscordInteractionWorker timeout",
			timeout:     (&DiscordInteractionWorker{}).Timeout(nil),
			wantTimeout: 5 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.timeout != tc.wantTimeout {
				t.Errorf("Timeout() = %v, want %v", tc.timeout, tc.wantTimeout)
			}
		})
	}
}

func TestDiscordIngressWorker_NextRetry(t *testing.T) {
	t.Parallel()
	now := time.Now()
	nextRetry := (&DiscordIngressWorker{}).NextRetry(nil)
	if nextRetry.Before(now.Add(4*time.Second)) || nextRetry.After(now.Add(6*time.Second)) {
		t.Errorf("NextRetry() = %v, want ~5s in future", nextRetry)
	}
}

type mockRowData struct {
	queue     string
	state     string
	count     int64
	oldestAge float64
}

type mockPgxRows struct {
	rows    []mockRowData
	idx     int
	scanErr error
	rowsErr error
	closed  bool
}

func (m *mockPgxRows) Close() {
	m.closed = true
}

func (m *mockPgxRows) Err() error {
	return m.rowsErr
}

func (m *mockPgxRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (m *mockPgxRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (m *mockPgxRows) Next() bool {
	if m.scanErr != nil && m.idx == 0 {
		return true
	}
	if m.idx < len(m.rows) {
		m.idx++
		return true
	}
	return false
}

func (m *mockPgxRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if m.idx <= 0 || m.idx > len(m.rows) {
		return errors.New("out of range scan")
	}
	row := m.rows[m.idx-1]
	if len(dest) >= 4 {
		if p, ok := dest[0].(*string); ok {
			*p = row.queue
		}
		if p, ok := dest[1].(*string); ok {
			*p = row.state
		}
		if p, ok := dest[2].(*int64); ok {
			*p = row.count
		}
		if p, ok := dest[3].(*float64); ok {
			*p = row.oldestAge
		}
	}
	return nil
}

func (m *mockPgxRows) Values() ([]any, error) {
	return nil, nil
}

func (m *mockPgxRows) RawValues() [][]byte {
	return nil
}

func (m *mockPgxRows) Conn() *pgx.Conn {
	return nil
}

func (m *mockPgxRows) TypeMap() *pgtype.Map {
	return nil
}

type mockQueueStatsQuerier struct {
	queryErr error
	rows     *mockPgxRows
}

func (m *mockQueueStatsQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	if m.rows != nil {
		return m.rows, nil
	}
	return &mockPgxRows{}, nil
}

func TestCollectRiverQueueMetrics(t *testing.T) {
	if !strings.Contains(riverQueueStatsQuery, "FROM river_job") {
		t.Errorf("riverQueueStatsQuery should query FROM river_job, got: %s", riverQueueStatsQuery)
	}
	if strings.Contains(riverQueueStatsQuery, "sidecars.river_job") {
		t.Errorf("riverQueueStatsQuery should not query sidecars.river_job, got: %s", riverQueueStatsQuery)
	}

	ctx := context.Background()

	tests := []struct {
		name               string
		nilQuerier         bool
		queryErr           error
		scanErr            error
		rowsErr            error
		rows               []mockRowData
		wantErr            bool
		wantSubstr         string
		wantDepthQueue     string
		wantDepthState     string
		wantDepthVal       float64
		wantOldestAgeQueue string
		wantOldestAgeVal   float64
	}{
		{
			name:       "nil querier returns nil without error",
			nilQuerier: true,
			wantErr:    false,
		},
		{
			name:       "query error returns wrapped error",
			queryErr:   errors.New("connection closed"),
			wantErr:    true,
			wantSubstr: "query river queue stats",
		},
		{
			name:       "scan error returns wrapped error",
			scanErr:    errors.New("column count mismatch"),
			wantErr:    true,
			wantSubstr: "scan river queue stats",
		},
		{
			name:       "rows iteration error returns wrapped error",
			rowsErr:    errors.New("network disconnect during iteration"),
			wantErr:    true,
			wantSubstr: "iterate river queue stats",
		},
		{
			name: "multi-queue state and age aggregation populates gauges correctly",
			rows: []mockRowData{
				{queue: "default", state: "available", count: 7, oldestAge: 45.5},
				{queue: "default", state: "running", count: 3, oldestAge: 120.0},
				{queue: "default", state: "retryable", count: 2, oldestAge: 90.2},
				{queue: "discord_ingress", state: "available", count: 12, oldestAge: 250.0},
				{queue: "widget_sync", state: "scheduled", count: 4, oldestAge: 18.0},
			},
			wantDepthQueue:     "default",
			wantDepthState:     "available",
			wantDepthVal:       7,
			wantOldestAgeQueue: "default",
			wantOldestAgeVal:   90.2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var q queueStatsQuerier
			if !tc.nilQuerier {
				q = &mockQueueStatsQuerier{
					queryErr: tc.queryErr,
					rows: &mockPgxRows{
						rows:    tc.rows,
						scanErr: tc.scanErr,
						rowsErr: tc.rowsErr,
					},
				}
			}

			err := collectRiverQueueMetrics(ctx, q)
			if (err != nil) != tc.wantErr {
				t.Fatalf("collectRiverQueueMetrics() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
			}
			if tc.wantDepthQueue != "" {
				gotDepth := testutil.ToFloat64(QueueDepth.WithLabelValues(tc.wantDepthQueue, tc.wantDepthState))
				if gotDepth != tc.wantDepthVal {
					t.Errorf("QueueDepth[%s, %s] = %v, want %v", tc.wantDepthQueue, tc.wantDepthState, gotDepth, tc.wantDepthVal)
				}
			}
			if tc.wantOldestAgeQueue != "" {
				gotAge := testutil.ToFloat64(QueueOldestAgeSeconds.WithLabelValues(tc.wantOldestAgeQueue))
				if gotAge != tc.wantOldestAgeVal {
					t.Errorf("QueueOldestAgeSeconds[%s] = %v, want %v", tc.wantOldestAgeQueue, gotAge, tc.wantOldestAgeVal)
				}
			}
		})
	}
}
