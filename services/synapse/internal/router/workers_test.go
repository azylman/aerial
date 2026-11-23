package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
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

// kubeRoute is a canned kube API response keyed by "METHOD path" in a kubeFixture.
func TestDefaultOutboundDispatcher_DiscoverImageKustomizations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	deploymentList := `{"items":[
		{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"aerial-core","kustomize.toolkit.fluxcd.io/namespace":"flux-system"}},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:abc"}]}}}},
		{"metadata":{"labels":{"app":"unmanaged"}},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:abc"}]}}}}
	]}`
	cronJobList := `{"items":[{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"agent-gamma-core"}},"spec":{"jobTemplate":{"spec":{"template":{"spec":{"initContainers":[{"image":"ghcr.io/azylman/aerial-synapse@sha256:beef"}],"containers":[{"image":"busybox"}]}}}}}}]}`
	restateClusterList := `{"items":[{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"restate-system","kustomize.toolkit.fluxcd.io/namespace":"flux-system"}},"spec":{"compute":{"image":"docker.io/restatedev/restate:1.6"}}}]}`
	kustomizationList := `{"items":[
		{"metadata":{"name":"zeta","namespace":"flux-system"},"spec":{"postBuild":{"substituteFrom":[{"kind":"ConfigMap","name":"aerial-image-tags"}]}}},
		{"metadata":{"name":"flux-system","namespace":"flux-system"},"spec":{}},
		{"metadata":{"name":"secrets-only","namespace":"flux-system"},"spec":{"postBuild":{"substituteFrom":[{"kind":"Secret","name":"aerial-image-tags"}]}}},
		{"metadata":{"name":"alpha","namespace":"flux-system"},"spec":{"postBuild":{"substituteFrom":[{"kind":"ConfigMap","name":"other"},{"kind":"ConfigMap","name":"aerial-image-tags"}]}}}
	]}`

	tests := []struct {
		name         string
		noKube       bool
		imageNames   []string
		routes       map[string]kubeRoute
		want         []fluxKustomizationRef
		wantErr      string
		wantRequests int
	}{
		{
			name:         "labeled deployment and cronjob initContainer owners are discovered",
			imageNames:   []string{"aerial-synapse"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: deploymentList}, kubeCronJobsPath: {body: cronJobList}}),
			want:         []fluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-core"}, {Namespace: "flux-system", Name: "agent-gamma-core"}},
			wantRequests: 7,
		},
		{
			name:         "restate cluster compute image owner is discovered",
			imageNames:   []string{"restate"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeRestateClustersPath: {body: restateClusterList}}),
			want:         []fluxKustomizationRef{{Namespace: "flux-system", Name: "restate-system"}},
			wantRequests: 7,
		},
		{
			name:         "missing crd 404 is treated as empty workload list during discovery",
			imageNames:   []string{"aerial-synapse"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: deploymentList}, kubeRestateDeploymentsPath: {status: http.StatusNotFound, body: `{"reason":"NotFound"}`}, kubeRestateClustersPath: {status: http.StatusNotFound, body: `{"reason":"NotFound"}`}}),
			want:         []fluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-core"}},
			wantRequests: 7,
		},
		{
			name:         "no owner falls back to kustomizations substituting the image tag configmap",
			imageNames:   []string{"aerial-haro"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: deploymentList}, kubeKustomizations: {body: kustomizationList}}),
			want:         []fluxKustomizationRef{{Namespace: "flux-system", Name: "alpha"}, {Namespace: "flux-system", Name: "zeta"}},
			wantRequests: 8,
		},
		{
			name:         "workload list failure returns error",
			imageNames:   []string{"aerial-synapse"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeStatefulSetsPath: {status: http.StatusForbidden, body: `{"reason":"Forbidden"}`}}),
			wantErr:      "returned status 403",
			wantRequests: 2,
		},
		{
			name:         "malformed workload list returns error",
			imageNames:   []string{"aerial-synapse"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: `{"items":`}}),
			wantErr:      "decode /apis/apps/v1/deployments",
			wantRequests: 1,
		},
		{
			name:         "fallback list failure returns error",
			imageNames:   []string{"aerial-haro"},
			routes:       kubeWorkloadRoutes(nil),
			wantErr:      "list kustomizations",
			wantRequests: 8,
		},
		{
			name:         "malformed fallback list returns error",
			imageNames:   []string{"aerial-haro"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeKustomizations: {body: `[`}}),
			wantErr:      "decode kustomizations",
			wantRequests: 8,
		},
		{
			name:       "no image names skips the kube API",
			imageNames: nil,
		},
		{
			name:       "unconfigured kube API skips discovery",
			noKube:     true,
			imageNames: []string{"aerial-synapse"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp, fixture := newConfiguredDispatcher(t, tc.routes, !tc.noKube)
			got, err := disp.DiscoverImageKustomizations(ctx, tc.imageNames)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("refs = %v, want %v", got, tc.want)
			}
			if n := len(fixture.Requests()); n != tc.wantRequests {
				t.Errorf("kube requests = %d (%v), want %d", n, fixture.Requests(), tc.wantRequests)
			}
		})
	}
}

func newConfiguredDispatcher(t *testing.T, routes map[string]kubeRoute, configureKube bool) (*DefaultOutboundDispatcher, *kubeFixture) {
	t.Helper()
	fixture, ts := newKubeFixture(t, routes)
	disp := NewDefaultOutboundDispatcher("", ts.Client())
	if configureKube {
		disp.SetKubeConfig(ts.URL, "test-token")
	}
	return disp, fixture
}

func TestDefaultOutboundDispatcher_ReconcileKustomization(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const agentGamma = "PATCH /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/apps/kustomizations/agent-gamma-core"
	const requestedAt = "2026-11-08T10:00:00.123456789Z"

	tests := []struct {
		name    string
		noKube  bool
		routes  map[string]kubeRoute
		wantErr string
	}{
		{name: "patches the given requestedAt annotation in the ref namespace", routes: map[string]kubeRoute{agentGamma: {body: `{}`}}},
		{name: "patch rejection returns error", routes: map[string]kubeRoute{agentGamma: {status: http.StatusForbidden, body: `forbidden`}}, wantErr: "reconcile kustomization apps/agent-gamma-core"},
		{name: "unconfigured kube API returns error", noKube: true, wantErr: "kube api server not configured"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp, fixture := newConfiguredDispatcher(t, tc.routes, !tc.noKube)
			err := disp.ReconcileKustomization(ctx, fluxKustomizationRef{Namespace: "apps", Name: "agent-gamma-core"}, requestedAt)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			wantBody := `{"metadata":{"annotations":{"reconcile.fluxcd.io/requestedAt":"2026-11-08T10:00:00.123456789Z"}}}`
			if got := fixture.Body(agentGamma); got != wantBody {
				t.Errorf("patch body = %s, want %s", got, wantBody)
			}
		})
	}
}

func TestDefaultOutboundDispatcher_DiscoverRepoKustomizations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const (
		configGitGet = "GET /apis/source.toolkit.fluxcd.io/v1/namespaces/flux-system/gitrepositories/aerial-config"
		aerialGitGet = "GET /apis/source.toolkit.fluxcd.io/v1/namespaces/flux-system/gitrepositories/aerial"
		appsGitGet   = "GET /apis/source.toolkit.fluxcd.io/v1/namespaces/apps/gitrepositories/stack"
		kustList     = `{"items":[` +
			`{"metadata":{"name":"zeta-config"},"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial-config"}}},` +
			`{"metadata":{"name":"aerial-core"},"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial"}}},` +
			`{"metadata":{"name":"alpha-config"},"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial-config"}}},` +
			`{"metadata":{"name":"paused-config"},"spec":{"suspend":true,"sourceRef":{"kind":"GitRepository","name":"aerial-config"}}},` +
			`{"metadata":{"name":"stack"},"spec":{"sourceRef":{"kind":"GitRepository","name":"stack","namespace":"apps"}}},` +
			`{"metadata":{"name":"charts"},"spec":{"sourceRef":{"kind":"OCIRepository","name":"charts"}}}]}`
	)
	sourceRoutes := map[string]kubeRoute{
		kubeKustomizations: {body: kustList},
		configGitGet:       {body: `{"spec":{"url":"https://github.com/Azylman/Aerial-Config.git"}}`},
		aerialGitGet:       {body: `{"spec":{"url":"ssh://git@github.com/azylman/aerial"}}`},
		appsGitGet:         {body: `{"spec":{"url":"git@github.com:acme/stack.git"}}`},
	}

	tests := []struct {
		name      string
		apiServer bool
		repo      string
		routes    map[string]kubeRoute
		want      []fluxKustomizationRef
		wantErr   string
	}{
		{name: "matches every unsuspended kustomization of the repo sorted", apiServer: true, repo: "azylman/aerial-config", routes: sourceRoutes, want: []fluxKustomizationRef{{Namespace: "flux-system", Name: "alpha-config"}, {Namespace: "flux-system", Name: "zeta-config"}}},
		{name: "bare repo name matches no owner-qualified source", apiServer: true, repo: "aerial", routes: sourceRoutes},
		{name: "spaces around the repo are trimmed", apiServer: true, repo: " azylman/aerial ", routes: sourceRoutes, want: []fluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-core"}}},
		{name: "cross-namespace source resolves in its own namespace", apiServer: true, repo: "acme/stack", routes: sourceRoutes, want: []fluxKustomizationRef{{Namespace: "flux-system", Name: "stack"}}},
		{name: "repo with no flux source finds nothing", apiServer: true, repo: "azylman/mirrormere", routes: sourceRoutes},
		{name: "kube api not configured finds nothing", repo: "azylman/aerial-config"},
		{name: "empty repo finds nothing", apiServer: true, repo: " ", routes: sourceRoutes},
		{name: "kustomization list failure", apiServer: true, repo: "azylman/aerial-config", wantErr: "list kustomizations"},
		{name: "malformed kustomization list", apiServer: true, repo: "azylman/aerial-config", routes: map[string]kubeRoute{kubeKustomizations: {body: `{`}}, wantErr: "decode kustomization list"},
		{name: "gitrepository get failure", apiServer: true, repo: "azylman/aerial-config", routes: map[string]kubeRoute{kubeKustomizations: {body: kustList}}, wantErr: "get gitrepository flux-system/aerial-config"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp, _ := newConfiguredDispatcher(t, tc.routes, tc.apiServer)
			got, err := disp.DiscoverRepoKustomizations(ctx, tc.repo)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("refs = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDefaultOutboundDispatcher_ResolveFluxSourceRepo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const (
		coreGet    = "GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations/aerial-core"
		repoGet    = "GET /apis/source.toolkit.fluxcd.io/v1/namespaces/flux-system/gitrepositories/aerial"
		appsGitGet = "GET /apis/source.toolkit.fluxcd.io/v1/namespaces/apps/gitrepositories/stack"
	)

	tests := []struct {
		name    string
		obj     FluxInvolvedObject
		routes  map[string]kubeRoute
		want    string
		wantErr string
	}{
		{name: "gitrepository resolves its own url", obj: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"}, routes: map[string]kubeRoute{repoGet: {body: `{"spec":{"url":"https://github.com/acme/widgets.git"}}`}}, want: "acme/widgets"},
		{name: "kustomization follows sourceRef in its namespace", obj: FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core"}, routes: map[string]kubeRoute{coreGet: {body: `{"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial"}}}`}, repoGet: {body: `{"spec":{"url":"ssh://git@github.com/acme/core"}}`}}, want: "acme/core"},
		{name: "kustomization follows cross-namespace sourceRef", obj: FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core", Namespace: "flux-system"}, routes: map[string]kubeRoute{coreGet: {body: `{"spec":{"sourceRef":{"kind":"GitRepository","name":"stack","namespace":"apps"}}}`}, appsGitGet: {body: `{"spec":{"url":"git@github.com:acme/stack.git"}}`}}, want: "acme/stack"},
		{name: "kustomization with bucket source resolves empty", obj: FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core"}, routes: map[string]kubeRoute{coreGet: {body: `{"spec":{"sourceRef":{"kind":"Bucket","name":"b"}}}`}}},
		{name: "unsupported kind resolves empty", obj: FluxInvolvedObject{Kind: "HelmRelease", Name: "x"}},
		{name: "kustomization get failure", obj: FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core"}, wantErr: "get kustomization flux-system/aerial-core"},
		{name: "malformed kustomization", obj: FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core"}, routes: map[string]kubeRoute{coreGet: {body: `{`}}, wantErr: "decode kustomization flux-system/aerial-core"},
		{name: "gitrepository get failure", obj: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial"}, wantErr: "get gitrepository flux-system/aerial"},
		{name: "malformed gitrepository", obj: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial"}, routes: map[string]kubeRoute{repoGet: {body: `[`}}, wantErr: "decode gitrepository flux-system/aerial"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp, _ := newConfiguredDispatcher(t, tc.routes, true)
			got, err := disp.ResolveFluxSourceRepo(ctx, tc.obj)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Errorf("repo = %q, want %q", got, tc.want)
			}
		})
	}
}
