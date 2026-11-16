package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	validSnowflake := "100000000000000001"
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
			name:     "DispatchGitPush ignores notification-controller root event-server URL",
			callType: "git_push_flux_event_server_root",
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
	case "git_push_flux_event_server_root":
		disp.SetFluxReceiverURL("http://notification-controller.flux-system.svc.cluster.local/")
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
	case "git_push", "git_push_flux", "git_push_flux_event_server_root":
		err := disp.DispatchGitPush(testCtx, GitPushEventRequest{Ref: "refs/heads/main", Commit: "testcommit123"})
		if tc.callType == "git_push_flux_event_server_root" && disp.fluxReceiverURL != "" {
			return "", fmt.Errorf("expected empty fluxReceiverURL for notification-controller root URL, got %q", disp.fluxReceiverURL)
		}
		return "", err
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
		beforeRetry = testutil.ToFloat64(JobRetriesTotal.WithLabelValues(QueueDefault, "brain_http_dispatch", "brain_http_retry"))
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
		afterRetry := testutil.ToFloat64(JobRetriesTotal.WithLabelValues(QueueDefault, "brain_http_dispatch", "brain_http_retry"))
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
