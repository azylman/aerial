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
			snowflake:  "not-a-snowflake",
			wantErr:    true,
			wantSubstr: "invalid discord snowflake",
		},
		{
			name:         "DispatchPrompt retry on 500 then success",
			callType:     "prompt",
			snowflake:    validSnowflake,
			failCount:    1,
			maxRetries:   4,
			responseBody: `{"status":"queued"}`,
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
			snowflake:  "not-a-snowflake",
			wantErr:    true,
			wantSubstr: "invalid discord snowflake",
		},
		{
			name:         "DispatchDirectMessage retry on 500 then success",
			callType:     "direct_message",
			snowflake:    validSnowflake,
			failCount:    1,
			maxRetries:   4,
			responseBody: `{"status":"sent"}`,
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

			var err error
			switch tc.callType {
			case "git_push":
				err = disp.DispatchGitPush(testCtx, GitPushEventRequest{Ref: "refs/heads/main", Commit: "testcommit123"})
			case "image_ready":
				jobs, e := disp.DispatchImageReady(testCtx, ImageReadyEventRequest{Image: "ghcr.io/azylman/aerial-webhooks-router:latest", Digest: "sha256:abcd"})
				err = e
				if tc.wantMatchedJobs > 0 && len(jobs) != tc.wantMatchedJobs {
					t.Errorf("got %d matched jobs, want %d", len(jobs), tc.wantMatchedJobs)
				}
			case "prompt":
				err = disp.DispatchPrompt(testCtx, PromptRequest{ChannelID: tc.snowflake, Prompt: "test prompt message"})
			case "direct_message":
				err = disp.DispatchDirectMessage(testCtx, DirectMessageRequest{ChannelID: tc.snowflake, Content: "test content"})
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("Dispatch (%s) err = %v, wantErr = %v", tc.callType, err, tc.wantErr)
			}
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
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
	t.Parallel()
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
		wantErr       bool
	}{
		{name: "GitHubWebhookWorker valid job", workerType: "github"},
		{name: "GitHubWebhookWorker nil server error", workerType: "github", nilServer: true, wantErr: true},
		{name: "GitHubWebhookWorker downstream processing error", workerType: "github", downstreamErr: true, wantErr: true},
		{name: "HangarWebhookWorker valid job", workerType: "hangar"},
		{name: "HangarWebhookWorker nil server error", workerType: "hangar", nilServer: true, wantErr: true},
		{name: "HangarWebhookWorker downstream dispatch error", workerType: "hangar", downstreamErr: true, wantErr: true},
		{name: "NomadEventWorker valid job", workerType: "nomad"},
		{name: "NomadEventWorker nil server error", workerType: "nomad", nilServer: true, wantErr: true},
		{name: "NomadEventWorker downstream prompt error", workerType: "nomad", downstreamErr: true, wantErr: true},
		{name: "NomadCrashAlertWorker valid job", workerType: "crash"},
		{name: "NomadCrashAlertWorker nil server error", workerType: "crash", nilServer: true, wantErr: true},
		{name: "NomadCrashAlertWorker downstream prompt error", workerType: "crash", downstreamErr: true, wantErr: true},
		{name: "WidgetSyncWorker valid job", workerType: "widget"},
		{name: "WidgetSyncWorker nil server error", workerType: "widget", nilServer: true, wantErr: true},
		{name: "WidgetSyncWorker downstream dispatch error", workerType: "widget", downstreamErr: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
			}
			srv := newTestRouter(Config{
				NomadAddr:       nomadServer.URL,
				SystemChannelID: "1555405874565091380",
			}, nil, mockReg, mockDisp)

			var err error
			switch tc.workerType {
			case "github":
				w := &GitHubWebhookWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[GitHubWebhookArgs]{
					JobRow: &rivertype.JobRow{ID: 101},
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
			case "hangar":
				w := &HangarWebhookWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[HangarWebhookArgs]{
					JobRow: &rivertype.JobRow{ID: 201},
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
					JobRow: &rivertype.JobRow{ID: 301},
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
					JobRow: &rivertype.JobRow{ID: 401},
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
					JobRow: &rivertype.JobRow{ID: 501},
					Args: WidgetSyncArgs{
						Repo:     "azylman/aerial",
						PRNumber: 552,
					},
				}
				err = w.Work(ctx, job)
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("Work() err = %v, wantErr = %v", err, tc.wantErr)
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
			wantAttempts: 5,
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
