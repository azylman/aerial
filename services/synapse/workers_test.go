package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// SetRetryConfig overrides retry delay and attempt bounds for fast hermetic unit tests.
func (d *DefaultOutboundDispatcher) SetRetryConfig(delay time.Duration, maxAttempts int) {
	d.retryDelay = delay
	d.maxAttempts = maxAttempts
}

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
	nilSubClient       bool
	wantRetryRecorded  bool
	wantErr            bool
	wantSubstr         string
	wantWorkload       string
	wantRequests       int
}

func defaultOutboundDispatcherFluxAndPromptCases() []defaultOutboundDispatcherCase {
	validSnowflake := "1555405874565091380"
	return []defaultOutboundDispatcherCase{
		{
			name:         "DispatchGitPush forwards to flux receiver",
			callType:     "git_push_flux",
			statusCode:   http.StatusOK,
			responseBody: `{"status":"accepted"}`,
			wantRequests: 1,
		},
		{
			name:     "DispatchGitPush without flux receiver returns nil",
			callType: "git_push",
		},
		{
			name:         "PromoteImageTag forwards to flux receiver",
			callType:     "image_ready_flux",
			statusCode:   http.StatusOK,
			responseBody: `{"status":"accepted"}`,
			wantWorkload: "aerial-synapse",
			wantRequests: 1,
		},
		{
			name:         "DispatchGitPush flux error returns error",
			callType:     "git_push_flux",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error":"flux down"}`,
			wantErr:      true,
			wantRequests: 1,
		},
		{
			name:         "PromoteImageTag flux error returns error",
			callType:     "image_ready_flux",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error":"flux down"}`,
			wantErr:      true,
			wantRequests: 1,
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
	}
}

func defaultOutboundDispatcherKubeAndDiscordCases() []defaultOutboundDispatcherCase {
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
		{
			name:         "PromoteImageTag via kubeAPIServer and flux receiver success",
			callType:     "image_ready_kube",
			statusCode:   http.StatusOK,
			wantWorkload: "aerial-synapse",
			wantRequests: 2,
		},
		{
			name:         "PromoteImageTag workload none skipped",
			callType:     "image_ready_none",
			wantRequests: 0,
		},
		{
			name:       "PromoteImageTag kube error only returns error",
			callType:   "image_ready_kube_err",
			statusCode: http.StatusInternalServerError,
			wantErr:    true,
		},
		{
			name:       "PromoteImageTag both kube and flux error returns combined error",
			callType:   "image_ready_both_err",
			statusCode: http.StatusInternalServerError,
			wantErr:    true,
		},
		{
			name:       "PromoteImageTag configmap 404 POST error returns error",
			callType:   "image_ready_kube_404_post_err",
			statusCode: http.StatusOK,
			wantErr:    true,
		},
		{
			name:       "PromoteImageTag configmap 404 POST conflict retry patch failure returns error",
			callType:   "image_ready_kube_404_409_retry_err",
			statusCode: http.StatusOK,
			wantErr:    true,
		},
		{
			name:         "PromoteImageTag configmap 404 creates then patches on 409 conflict",
			callType:     "image_ready_kube_404_409",
			statusCode:   http.StatusOK,
			wantWorkload: "aerial-synapse",
		},
		{
			name:         "PromoteImageTag configmap 404 creates successfully on POST",
			callType:     "image_ready_kube_404_post_ok",
			statusCode:   http.StatusOK,
			wantWorkload: "aerial-synapse",
		},
	}
}

func defaultOutboundDispatcherCases() []defaultOutboundDispatcherCase {
	return append(defaultOutboundDispatcherFluxAndPromptCases(), defaultOutboundDispatcherKubeAndDiscordCases()...)
}

func handleDispatcherKube404Cases(w http.ResponseWriter, r *http.Request, tc defaultOutboundDispatcherCase, att int32) bool {
	switch tc.callType {
	case "image_ready_kube_404_409":
		if r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "aerial-image-tags") && att == 1 {
			http.Error(w, "not found", http.StatusNotFound)
			return true
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "configmaps") {
			http.Error(w, "conflict", http.StatusConflict)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
		return true
	case "image_ready_kube_404_post_ok":
		if r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "aerial-image-tags") && att == 1 {
			http.Error(w, "not found", http.StatusNotFound)
			return true
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "configmaps") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
		return true
	}
	return false
}

func handleDispatcherKubeErrCases(w http.ResponseWriter, r *http.Request, tc defaultOutboundDispatcherCase, att int32) bool {
	switch tc.callType {
	case "image_ready_kube_404_post_err":
		if r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "aerial-image-tags") {
			http.Error(w, "not found", http.StatusNotFound)
			return true
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "configmaps") {
			http.Error(w, "post failed", http.StatusBadRequest)
			return true
		}
	case "image_ready_kube_404_409_retry_err":
		if r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "aerial-image-tags") && att == 1 {
			http.Error(w, "not found", http.StatusNotFound)
			return true
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "configmaps") {
			http.Error(w, "conflict", http.StatusConflict)
			return true
		}
		if r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "aerial-image-tags") && att == 3 {
			http.Error(w, "retry patch failed", http.StatusBadRequest)
			return true
		}
	}
	return false
}

func handleDispatcherKubeMock(w http.ResponseWriter, r *http.Request, tc defaultOutboundDispatcherCase, att int32) bool {
	return handleDispatcherKube404Cases(w, r, tc, att) || handleDispatcherKubeErrCases(w, r, tc, att)
}

func newDispatcherMockHandler(tc defaultOutboundDispatcherCase, attempts *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		att := attempts.Add(1)
		if handleDispatcherKubeMock(w, r, tc, att) {
			return
		}
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

func configureDispatcherURLs(disp *DefaultOutboundDispatcher, url string, callType string) {
	switch callType {
	case "git_push_flux", "image_ready_flux":
		disp.SetFluxReceiverURL(url)
	case "image_ready_both_err", "image_ready_kube", "image_ready_none":
		disp.SetKubeConfig(url, "test-token")
		disp.SetFluxReceiverURL(url)
	case "image_ready_kube_err", "image_ready_kube_404_409",
		"image_ready_kube_404_post_err", "image_ready_kube_404_409_retry_err", "image_ready_kube_404_post_ok":
		disp.SetKubeConfig(url, "test-token")
	}
}

func executeDispatcherCall(testCtx context.Context, disp *DefaultOutboundDispatcher, tc defaultOutboundDispatcherCase, attempts *atomic.Int32) (string, error) {
	switch tc.callType {
	case "git_push", "git_push_flux":
		return "", disp.DispatchGitPush(testCtx, GitPushEventRequest{Ref: "refs/heads/main", Commit: "testcommit123"})
	case "image_ready", "image_ready_flux", "image_ready_kube", "image_ready_kube_err", "image_ready_kube_404_409", "image_ready_kube_404_post_ok", "image_ready_both_err", "image_ready_kube_404_post_err", "image_ready_kube_404_409_retry_err":
		return disp.PromoteImageTag(testCtx, ImageReadyEventRequest{
			Repo:      "azylman/aerial",
			Image:     "ghcr.io/azylman/aerial-synapse:latest",
			CommitSHA: "abcdef1234567890abcdef1234567890abcdef12",
			Digest:    "sha256:abcd",
		})
	case "image_ready_none":
		workload, err := disp.PromoteImageTag(testCtx, ImageReadyEventRequest{
			Repo:      "azylman/aerial",
			Image:     "ghcr.io/azylman/none:latest",
			CommitSHA: "abcdef1234567890abcdef1234567890abcdef12",
		})
		if attempts.Load() != 0 {
			return workload, fmt.Errorf("expected 0 HTTP requests for none, got %d", attempts.Load())
		}
		return workload, err
	case "prompt":
		return "", disp.DispatchPrompt(testCtx, PromptRequest{ChannelID: tc.snowflake, Prompt: "test prompt message"})
	case "direct_message":
		return "", disp.DispatchDirectMessage(testCtx, DirectMessageRequest{ChannelID: tc.snowflake, Content: "test content"})
	case "dispatch_background":
		return "", disp.DispatchBackground(testCtx, DispatchRequest{Source: "webhook", Prompt: "test dispatch", Effort: "medium"})
	case "discord_message":
		return "", disp.DispatchDiscordMessage(testCtx, DiscordMessageIngressRequest{
			BotUserID: "bot-1",
			RoleNames: map[string]string{"role-1": "admin"},
			Message:   &discordgo.Message{ID: "msg-123", Content: "hello"},
		})
	case "discord_interaction":
		return "", disp.DispatchDiscordInteraction(testCtx, &discordgo.Interaction{
			ID:   "inter-123",
			Type: discordgo.InteractionApplicationCommand,
		})
	}
	return "", nil
}

func runDefaultOutboundDispatcherCase(ctx context.Context, t *testing.T, tc defaultOutboundDispatcherCase) {
	t.Helper()
	var attempts atomic.Int32
	ts := httptest.NewServer(newDispatcherMockHandler(tc, &attempts))
	defer ts.Close()

	disp := NewDefaultOutboundDispatcher(ts.URL, nil)
	configureDispatcherURLs(disp, ts.URL, tc.callType)
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

	workload, err := executeDispatcherCall(testCtx, disp, tc, &attempts)

	if (err != nil) != tc.wantErr {
		t.Fatalf("Dispatch (%s) err = %v, wantErr = %v", tc.callType, err, tc.wantErr)
	}
	if tc.wantWorkload != "" && workload != tc.wantWorkload {
		t.Errorf("got workload %q, want %q", workload, tc.wantWorkload)
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

func TestResolveImageTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		repo          string
		imageRef      string
		wantTagKey    string
		wantImageName string
		wantWorkload  string
	}{
		{
			name:          "empty image",
			repo:          "azylman/aerial",
			imageRef:      "",
			wantTagKey:    "",
			wantImageName: "",
			wantWorkload:  "",
		},
		{
			name:          "none image",
			repo:          "azylman/aerial",
			imageRef:      "ghcr.io/azylman/none:latest",
			wantTagKey:    "",
			wantImageName: "",
			wantWorkload:  "",
		},
		{
			name:          "aerial repo with aerial- prefix",
			repo:          "azylman/aerial",
			imageRef:      "ghcr.io/azylman/aerial-synapse:sha123",
			wantTagKey:    "TAG_AERIAL_SYNAPSE",
			wantImageName: "aerial-synapse",
			wantWorkload:  "aerial-synapse",
		},
		{
			name:          "aerial repo without prefix",
			repo:          "aerial",
			imageRef:      "ghcr.io/azylman/synapse:sha123",
			wantTagKey:    "TAG_AERIAL_SYNAPSE",
			wantImageName: "aerial-synapse",
			wantWorkload:  "synapse",
		},
		{
			name:          "aerial-sidecars repo with aerial-sidecar- prefix",
			repo:          "azylman/aerial-sidecars",
			imageRef:      "ghcr.io/azylman/aerial-sidecar-prospera:sha123",
			wantTagKey:    "TAG_AERIAL_SIDECAR_PROSPERA",
			wantImageName: "aerial-sidecar-prospera",
			wantWorkload:  "aerial-sidecar-prospera",
		},
		{
			name:          "aerial-sidecars repo without prefix",
			repo:          "aerial-sidecars",
			imageRef:      "ghcr.io/azylman/prospera:sha123",
			wantTagKey:    "TAG_AERIAL_SIDECAR_PROSPERA",
			wantImageName: "aerial-sidecar-prospera",
			wantWorkload:  "prospera",
		},
		{
			name:          "aerial-sidecars migrator gets sidecar image name",
			repo:          "azylman/aerial-sidecars",
			imageRef:      "ghcr.io/azylman/aerial-sidecar-migrator:sha123",
			wantTagKey:    "TAG_AERIAL_SIDECAR_MIGRATOR",
			wantImageName: "aerial-sidecar-migrator",
			wantWorkload:  "aerial-sidecar-migrator",
		},
		{
			name:          "aerial-sidecars bare migrator gets sidecar prefix",
			repo:          "aerial-sidecars",
			imageRef:      "ghcr.io/azylman/migrator:sha123",
			wantTagKey:    "TAG_AERIAL_SIDECAR_MIGRATOR",
			wantImageName: "aerial-sidecar-migrator",
			wantWorkload:  "migrator",
		},
		{
			name:          "mirrormere repo with mirrormere- prefix",
			repo:          "azylman/mirrormere",
			imageRef:      "ghcr.io/azylman/mirrormere-kiosk:sha123",
			wantTagKey:    "TAG_MIRRORMERE_KIOSK",
			wantImageName: "mirrormere-kiosk",
			wantWorkload:  "mirrormere-kiosk",
		},
		{
			name:          "mirrormere repo without prefix",
			repo:          "mirrormere",
			imageRef:      "ghcr.io/azylman/kiosk:sha123",
			wantTagKey:    "TAG_MIRRORMERE_KIOSK",
			wantImageName: "mirrormere-kiosk",
			wantWorkload:  "kiosk",
		},
		{
			name:          "other repo with aerial- prefix",
			repo:          "other-org/other-repo",
			imageRef:      "ghcr.io/other/aerial-extra:sha123",
			wantTagKey:    "TAG_AERIAL_EXTRA",
			wantImageName: "aerial-extra",
			wantWorkload:  "aerial-extra",
		},
		{
			name:          "other repo with sidecar or thirdparty",
			repo:          "other-org/other-repo",
			imageRef:      "ghcr.io/other/custom-service:sha123",
			wantTagKey:    "TAG_CUSTOM_SERVICE",
			wantImageName: "custom-service",
			wantWorkload:  "custom-service",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tagKey, imageName, workload := resolveImageTag(tc.repo, tc.imageRef)
			if tagKey != tc.wantTagKey {
				t.Errorf("tagKey = %q, want %q", tagKey, tc.wantTagKey)
			}
			if imageName != tc.wantImageName {
				t.Errorf("imageName = %q, want %q", imageName, tc.wantImageName)
			}
			if workload != tc.wantWorkload {
				t.Errorf("workload = %q, want %q", workload, tc.wantWorkload)
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

			disp := NewDefaultOutboundDispatcher(ts.URL, ts.Client())
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

type riverWorkerCase struct {
	name          string
	workerType    string
	nilServer     bool
	downstreamErr bool
	attempt       int
	maxAttempts   int
	wantProcessed string
	wantErr       bool
}

func riverWorkerCases() []riverWorkerCase {
	return []riverWorkerCase{
		{name: "GitHubWebhookWorker valid job", workerType: "github", wantProcessed: "success"},
		{name: "GitHubWebhookWorker nil job handled gracefully", workerType: "github_nil_job"},
		{name: "GitHubWebhookWorker retry attempt > 1", workerType: "github", attempt: 2, wantProcessed: "success"},
		{name: "GitHubWebhookWorker max attempts exhausted discard", workerType: "github", attempt: 5, maxAttempts: 5, downstreamErr: true, wantErr: true, wantProcessed: "discarded"},
		{name: "GitHubWebhookWorker nil server error", workerType: "github", nilServer: true, wantErr: true, wantProcessed: "error"},
		{name: "GitHubWebhookWorker downstream processing error", workerType: "github", downstreamErr: true, wantErr: true, wantProcessed: "error"},
		{name: "DeployWebhookWorker valid job", workerType: "deploy", wantProcessed: "success"},
		{name: "DeployWebhookWorker nil server error", workerType: "deploy", nilServer: true, wantErr: true, wantProcessed: "error"},
		{name: "DeployWebhookWorker downstream dispatch error", workerType: "deploy", downstreamErr: true, wantErr: true, wantProcessed: "error"},
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
}

func resolveRiverWorkerQueueAndKind(workerType string) (string, string) {
	switch {
	case workerType == "github":
		return river.QueueDefault, "github_webhook"
	case workerType == "deploy":
		return river.QueueDefault, "deploy_webhook"
	case workerType == "widget":
		return QueueWidgetSync, "widget_sync"
	case workerType == "event_trigger":
		return river.QueueDefault, "event_trigger_webhook"
	case workerType == "merge_conflict":
		return QueueMergeConflict, "merge_conflict"
	case strings.HasPrefix(workerType, "discord_ingress"):
		return QueueDiscordIngress, "discord_ingress"
	case workerType == "discord_interaction":
		return QueueDiscordInteraction, "discord_interaction"
	}
	return river.QueueDefault, ""
}

func executeGitHubOrDeployWorker(ctx context.Context, srv *RouterServer, tc riverWorkerCase) (bool, error) {
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
		return true, w.Work(ctx, job)
	case "github_nil_job":
		return true, observeRiverJob[GitHubWebhookArgs](nil, "", func() error { return nil })
	case "deploy":
		w := &DeployWebhookWorker{server: srv}
		if tc.nilServer {
			w.server = nil
		}
		job := &river.Job[DeployWebhookArgs]{
			JobRow: &rivertype.JobRow{ID: 201, Attempt: tc.attempt, MaxAttempts: tc.maxAttempts},
			Args: DeployWebhookArgs{
				Event: DeployEvent{
					Event: "deploy_success", JobName: "brain",
					PRNumber: 558, TargetID: "1555405874565091380", Status: "success",
				},
			},
		}
		return true, w.Work(ctx, job)
	}
	return false, nil
}

func executeWidgetOrTriggerWorker(ctx context.Context, srv *RouterServer, tc riverWorkerCase) (bool, error) {
	switch tc.workerType {
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
		return true, w.Work(ctx, job)
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
		return true, w.Work(ctx, job)
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
		return true, w.Work(ctx, job)
	}
	return false, nil
}

func executeDiscordIngressOrInteractionWorker(ctx context.Context, srv *RouterServer, tc riverWorkerCase, mockDisp *mockOutboundDispatcher) error {
	switch tc.workerType {
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
		return w.Work(workCtx, job)
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
		return w.Work(ctx, job)
	}
	return nil
}

func executeRiverWorker(ctx context.Context, srv *RouterServer, tc riverWorkerCase, mockDisp *mockOutboundDispatcher) error {
	if handled, err := executeGitHubOrDeployWorker(ctx, srv, tc); handled {
		return err
	}
	if handled, err := executeWidgetOrTriggerWorker(ctx, srv, tc); handled {
		return err
	}
	return executeDiscordIngressOrInteractionWorker(ctx, srv, tc, mockDisp)
}

func assertRiverWorkerMetrics(t *testing.T, tc riverWorkerCase, expectedQueue, expectedKind string, beforeProcessed, beforeRetry, beforeDropped float64) {
	t.Helper()
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
}

func runRiverWorkerCase(ctx context.Context, t *testing.T, tc riverWorkerCase) {
	t.Helper()
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
		SystemChannelID: "1555405874565091380",
	}, nil, mockReg, mockDisp)

	expectedQueue, expectedKind := resolveRiverWorkerQueueAndKind(tc.workerType)

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

	err := executeRiverWorker(ctx, srv, tc, mockDisp)
	if (err != nil) != tc.wantErr {
		t.Fatalf("Work() err = %v, wantErr = %v", err, tc.wantErr)
	}

	assertRiverWorkerMetrics(t, tc, expectedQueue, expectedKind, beforeProcessed, beforeRetry, beforeDropped)
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

	for _, tc := range riverWorkerCases() {
		t.Run(tc.name, func(t *testing.T) {
			runRiverWorkerCase(ctx, t, tc)
		})
	}
}

type riverJobArgsCase struct {
	name         string
	args         river.JobArgs
	wantKind     string
	wantQueue    string
	wantByArgs   bool
	wantByStates []rivertype.JobState
	wantByPeriod time.Duration
	wantAttempts int
}

func riverJobArgsCases() []riverJobArgsCase {
	return []riverJobArgsCase{
		{
			name:     "GitHubWebhookArgs",
			args:     GitHubWebhookArgs{Delivery: "del-1"},
			wantKind: "github_webhook",
		},
		{
			name:         "DeployWebhookArgs",
			args:         DeployWebhookArgs{IdempotencyKey: "key-1"},
			wantKind:     "deploy_webhook",
			wantByArgs:   true,
			wantByPeriod: 15 * time.Minute,
		},
		{
			name:         "WidgetSyncArgs",
			args:         WidgetSyncArgs{Repo: "azylman/aerial", PRNumber: 101},
			wantKind:     "widget_sync",
			wantQueue:    QueueWidgetSync,
			wantByArgs:   true,
			wantByStates: []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning, rivertype.JobStateRetryable, rivertype.JobStateScheduled},
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
}

func assertRiverJobStates(t *testing.T, states, wantStates []rivertype.JobState) {
	t.Helper()
	if !slices.Equal(states, wantStates) {
		t.Errorf("UniqueOpts.ByState = %v, want %v", states, wantStates)
	}
	requiredV3States := []rivertype.JobState{
		rivertype.JobStateAvailable,
		rivertype.JobStatePending,
		rivertype.JobStateRunning,
		rivertype.JobStateScheduled,
	}
	for _, reqState := range requiredV3States {
		if !slices.Contains(states, reqState) {
			t.Errorf("UniqueOpts.ByState missing required River v3 state %v", reqState)
		}
	}
}

func assertRiverUniqueOpts(t *testing.T, opts river.InsertOpts, tc riverJobArgsCase) {
	t.Helper()
	if tc.wantQueue != "" && opts.Queue != tc.wantQueue {
		t.Errorf("Queue = %q, want %q", opts.Queue, tc.wantQueue)
	}
	if tc.wantByArgs && !opts.UniqueOpts.ByArgs {
		t.Errorf("UniqueOpts.ByArgs = false, want true")
	}
	if len(tc.wantByStates) > 0 {
		assertRiverJobStates(t, opts.UniqueOpts.ByState, tc.wantByStates)
	}
	if tc.wantByPeriod > 0 && opts.UniqueOpts.ByPeriod != tc.wantByPeriod {
		t.Errorf("UniqueOpts.ByPeriod = %v, want %v", opts.UniqueOpts.ByPeriod, tc.wantByPeriod)
	}
	if tc.wantAttempts > 0 && opts.MaxAttempts != tc.wantAttempts {
		t.Errorf("MaxAttempts = %d, want %d", opts.MaxAttempts, tc.wantAttempts)
	}
}

func runRiverJobArgsCase(t *testing.T, tc riverJobArgsCase) {
	t.Helper()
	if got := tc.args.Kind(); got != tc.wantKind {
		t.Errorf("Kind() = %q, want %q", got, tc.wantKind)
	}
	optsProvider, ok := tc.args.(interface{ InsertOpts() river.InsertOpts })
	if !ok {
		return
	}
	assertRiverUniqueOpts(t, optsProvider.InsertOpts(), tc)
}

func TestRiverJobArgsAndUniqueOpts(t *testing.T) {
	t.Parallel()

	for _, tc := range riverJobArgsCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runRiverJobArgsCase(t, tc)
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
