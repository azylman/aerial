package prstate

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// kubeCallLog is one request the hermetic Kubernetes server received.
type kubeCallLog struct {
	route string
	auth  string
	body  string
}

// kubeServer is the declarative hermetic Kubernetes API and Flux receiver:
// each "METHOD path" route answers its statuses in order, repeating the
// last; unknown routes answer 404.
type kubeServer struct {
	mu     sync.Mutex
	routes map[string][]int
	calls  []kubeCallLog
}

func (k *kubeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	route := r.Method + " " + r.URL.Path
	k.mu.Lock()
	k.calls = append(k.calls, kubeCallLog{route: route, auth: r.Header.Get("Authorization"), body: string(body)})
	status := http.StatusNotFound
	if seq := k.routes[route]; len(seq) > 0 {
		status = seq[0]
		if len(seq) > 1 {
			k.routes[route] = seq[1:]
		}
	}
	k.mu.Unlock()
	w.WriteHeader(status)
	_, _ = w.Write([]byte("{}"))
}

func (k *kubeServer) routesCalled() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.calls) == 0 {
		return nil
	}
	out := make([]string, 0, len(k.calls))
	for _, c := range k.calls {
		out = append(out, c.route)
	}
	return out
}

func (k *kubeServer) first() kubeCallLog {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.calls) == 0 {
		return kubeCallLog{}
	}
	return k.calls[0]
}

// newKubeServer starts a plain-HTTP hermetic server for routes.
func newKubeServer(t *testing.T, routes map[string][]int) (*httptest.Server, *kubeServer) {
	t.Helper()
	ks := &kubeServer{routes: routes}
	srv := httptest.NewServer(ks)
	t.Cleanup(srv.Close)
	return srv, ks
}

// writeToken writes a service-account token file and returns its path.
func writeToken(t *testing.T, token string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte(token+"\n"), 0o600))
	return path
}

const (
	cmPath        = "/api/v1/namespaces/flux-system/configmaps/aerial-image-tags"
	cmCreatePath  = "/api/v1/namespaces/flux-system/configmaps"
	coreKustPath  = "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations/core"
	receiverRoute = "POST /hook"
)

func TestKubeClientDispatchGitPush(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		routes      map[string][]int
		noReceiver  bool
		deadServer  bool
		wantStatus  int
		wantErr     bool
		wantRoutes  []string
		wantPayload string
	}{
		{name: "no receiver url is a no-op", noReceiver: true},
		{
			name: "receiver accepts the push", routes: map[string][]int{receiverRoute: {202}}, wantRoutes: []string{receiverRoute},
			wantPayload: `{"repo":"azylman/aerial","ref":"refs/heads/main","commit":"m1","pr_number":7}`,
		},
		{name: "receiver failure keeps its status", routes: map[string][]int{receiverRoute: {500}}, wantErr: true, wantStatus: 500, wantRoutes: []string{receiverRoute}},
		{name: "unreachable receiver is a transport error", deadServer: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, ks := newKubeServer(t, tc.routes)
			receiver := srv.URL + "/hook"
			if tc.noReceiver {
				receiver = ""
			}
			if tc.deadServer {
				srv.Close()
			}
			c := NewKubeClient(srv.Client(), "", "", "", receiver)
			kerr := c.DispatchGitPush(context.Background(), GitPushRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "m1", PRNumber: 7})
			require.Equal(t, tc.wantErr, kerr != nil)
			if kerr != nil {
				require.Equal(t, tc.wantStatus, kerr.Status)
			}
			require.Equal(t, tc.wantRoutes, ks.routesCalled())
			if tc.wantPayload != "" {
				require.Equal(t, tc.wantPayload, ks.first().body)
			}
		})
	}
}

func TestKubeClientPromoteImageTag(t *testing.T) {
	t.Parallel()
	patch, post := "PATCH "+cmPath, "POST "+cmCreatePath
	tests := []struct {
		name         string
		routes       map[string][]int
		image        string
		commit       string
		noAPI        bool
		receiver     bool
		missingToken bool
		wantWorkload string
		wantStatus   int
		wantErr      bool
		wantRoutes   []string
		wantBody     string
		wantAuth     string
	}{
		{name: "none image promotes nothing", image: "ghcr.io/azylman/none:abc", commit: "s1"},
		{
			name: "patch writes the tag", routes: map[string][]int{patch: {200}}, image: "ghcr.io/azylman/aerial-eri:s1", commit: "s1",
			wantWorkload: "aerial-eri", wantRoutes: []string{patch}, wantAuth: "Bearer tok",
			wantBody: `{"apiVersion":"v1","data":{"TAG_AERIAL_ERI":"s1"},"kind":"ConfigMap","metadata":{"name":"aerial-image-tags","namespace":"flux-system"}}`,
		},
		{name: "missing configmap is created", routes: map[string][]int{patch: {404}, post: {201}}, image: "eri", commit: "s1", wantWorkload: "eri", wantRoutes: []string{patch, post}, wantAuth: "Bearer tok"},
		{name: "create race falls back to patch", routes: map[string][]int{patch: {404, 200}, post: {409}}, image: "eri", commit: "s1", wantWorkload: "eri", wantRoutes: []string{patch, post, patch}, wantAuth: "Bearer tok"},
		{name: "create rejection is terminal", routes: map[string][]int{patch: {404}, post: {422}}, image: "eri", commit: "s1", wantErr: true, wantStatus: 422, wantRoutes: []string{patch, post}, wantAuth: "Bearer tok"},
		{name: "forbidden patch without receiver fails", routes: map[string][]int{patch: {403}}, image: "eri", commit: "s1", wantErr: true, wantStatus: 403, wantRoutes: []string{patch}, wantAuth: "Bearer tok"},
		{name: "receiver success covers a failed patch", routes: map[string][]int{patch: {500}, receiverRoute: {200}}, image: "eri", commit: "s1", receiver: true, wantWorkload: "eri", wantRoutes: []string{patch, receiverRoute}, wantAuth: "Bearer tok"},
		{name: "receiver failure without api fails", routes: map[string][]int{receiverRoute: {502}}, image: "eri", commit: "s1", noAPI: true, receiver: true, wantErr: true, wantStatus: 502, wantRoutes: []string{receiverRoute}},
		{name: "zero commit without receiver writes nothing", image: "eri", commit: "0000000000000000000000000000000000000000"},
		{name: "unreadable token sends no auth", routes: map[string][]int{patch: {200}}, image: "eri", commit: "s1", missingToken: true, wantWorkload: "eri", wantRoutes: []string{patch}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, ks := newKubeServer(t, tc.routes)
			api, receiver := srv.URL, ""
			if tc.noAPI {
				api = ""
			}
			if tc.receiver {
				receiver = srv.URL + "/hook"
			}
			tokenPath := writeToken(t, "tok")
			if tc.missingToken {
				tokenPath = filepath.Join(t.TempDir(), "absent")
			}
			c := NewKubeClient(srv.Client(), api, tokenPath, "", receiver)
			workload, kerr := c.PromoteImageTag(context.Background(), ImagePromotionRequest{Image: tc.image, Repo: "azylman/aerial", CommitSHA: tc.commit})
			require.Equal(t, tc.wantWorkload, workload)
			require.Equal(t, tc.wantErr, kerr != nil)
			if kerr != nil {
				require.Equal(t, tc.wantStatus, kerr.Status)
			}
			require.Equal(t, tc.wantRoutes, ks.routesCalled())
			if tc.wantBody != "" {
				require.Equal(t, tc.wantBody, ks.first().body)
			}
			require.Equal(t, tc.wantAuth, ks.first().auth)
		})
	}
}

func TestKubeClientReconcileKustomization(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		routes     map[string][]int
		namespace  string
		noAPI      bool
		wantErr    bool
		wantStatus int
		wantRetry  bool
		wantRoutes []string
		wantBody   string
	}{
		{
			name: "patch requests the reconcile", routes: map[string][]int{"PATCH " + coreKustPath: {200}}, namespace: "flux-system",
			wantRoutes: []string{"PATCH " + coreKustPath}, wantBody: `{"metadata":{"annotations":{"reconcile.fluxcd.io/requestedAt":"2026-11-20T00:00:00Z"}}}`,
		},
		{name: "empty namespace defaults to flux-system", routes: map[string][]int{"PATCH " + coreKustPath: {200}}, wantRoutes: []string{"PATCH " + coreKustPath}},
		{name: "missing kustomization is terminal", namespace: "flux-system", wantErr: true, wantStatus: 404, wantRoutes: []string{"PATCH " + coreKustPath}},
		{name: "throttled patch is retryable", routes: map[string][]int{"PATCH " + coreKustPath: {429}}, namespace: "flux-system", wantErr: true, wantStatus: 429, wantRetry: true, wantRoutes: []string{"PATCH " + coreKustPath}},
		{name: "no api server is a retryable configuration error", noAPI: true, wantErr: true, wantRetry: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, ks := newKubeServer(t, tc.routes)
			api := srv.URL
			if tc.noAPI {
				api = ""
			}
			c := NewKubeClient(srv.Client(), api, "", "", "")
			kerr := c.ReconcileKustomization(context.Background(), tc.namespace, "core", "2026-11-20T00:00:00Z")
			require.Equal(t, tc.wantErr, kerr != nil)
			if kerr != nil {
				require.Equal(t, tc.wantStatus, kerr.Status)
				require.Equal(t, tc.wantRetry, kerr.Retryable())
			}
			require.Equal(t, tc.wantRoutes, ks.routesCalled())
			if tc.wantBody != "" {
				require.Equal(t, tc.wantBody, ks.first().body)
			}
		})
	}
}

func TestKubeClientTLS(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		withCA  bool
		wantErr bool
	}{
		{name: "service-account ca is trusted", withCA: true},
		{name: "missing ca keeps the system pool and fails verification", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewTLSServer(&kubeServer{routes: map[string][]int{"PATCH " + coreKustPath: {200}}})
			t.Cleanup(srv.Close)
			caPath := filepath.Join(t.TempDir(), "ca.crt")
			if tc.withCA {
				pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
				require.NoError(t, os.WriteFile(caPath, pemBytes, 0o600))
			}
			c := NewKubeClient(nil, srv.URL, "", caPath, "")
			kerr := c.ReconcileKustomization(context.Background(), "", "core", "t1")
			require.Equal(t, tc.wantErr, kerr != nil)
		})
	}
}

func TestNormalizeReceiverURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "receiver webhook is kept and trimmed", raw: " http://receiver.flux/hook/abc/ ", want: "http://receiver.flux/hook/abc"},
		{name: "notification controller root is the event server", raw: "http://notification-controller.flux-system/", want: ""},
		{name: "notification controller receiver path is kept", raw: "http://notification-controller:9292/hook/abc", want: "http://notification-controller:9292/hook/abc"},
		{name: "unparseable value is kept as is", raw: "not a url", want: "not a url"},
		{name: "empty stays empty", raw: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, normalizeReceiverURL(tc.raw))
		})
	}
}

func TestInClusterAPIServer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		host string
		port string
		want string
	}{
		{name: "ipv4 service host", host: "192.0.2.1", port: "443", want: "https://192.0.2.1:443"},
		{name: "ipv6 service host is bracketed", host: "fd00::1", port: "443", want: "https://[fd00::1]:443"},
		{name: "outside a cluster", host: "", port: "443", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, InClusterAPIServer(tc.host, tc.port))
		})
	}
}

func TestKubeError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		err       *KubeError
		wantRetry bool
		wantText  string
	}{
		{name: "nil is not retryable", err: nil, wantText: ""},
		{name: "transport failure is retryable", err: &KubeError{Message: "dial"}, wantRetry: true, wantText: "dial"},
		{name: "server error is retryable", err: &KubeError{Status: 503, Message: "down"}, wantRetry: true, wantText: "kube status 503: down"},
		{name: "conflict is retryable", err: &KubeError{Status: 409, Message: "conflict"}, wantRetry: true, wantText: "kube status 409: conflict"},
		{name: "forbidden is terminal", err: &KubeError{Status: 403, Message: "rbac"}, wantText: "kube status 403: rbac"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.wantRetry, tc.err.Retryable())
			require.Equal(t, tc.wantText, tc.err.Error())
		})
	}
}

func TestKubeClientTransportErrorRedactsReceiverURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		secret string
	}{
		{name: "tokenized receiver path never reaches the error", secret: "s3cr3t-receiver-token"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, ks := newKubeServer(t, nil)
			require.NotNil(t, ks)
			receiver := srv.URL + "/hook/" + tc.secret
			srv.Close()
			kerr := NewKubeClient(srv.Client(), "", "", "", receiver).DispatchGitPush(context.Background(), GitPushRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "m1"})
			require.NotNil(t, kerr)
			require.True(t, kerr.Retryable())
			require.NotContains(t, kerr.Error(), tc.secret)
		})
	}
}
