package prstate

// Kubernetes and Flux client for restate-owned Flux and image effects:
// git-push dispatch to the Flux notification receiver, image-tag promotion
// into ConfigMap/aerial-image-tags, and Flux Kustomization reconcile
// requests. Ported from services/synapse/internal/kube (client.go, auth.go)
// without importing it: synapse is a separate module and its packages are
// internal. Only the write paths these effects need are ported; discovery
// stays in synapse, which carries the resolved Kustomizations in the event.
//
// Error classification is data, not error types, so a journaled result
// replays identically: KubeError carries the HTTP status. Transport
// failures, 5xx, 409 and 429 are retryable; every other 4xx is terminal.

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// In-cluster service-account paths and the Flux objects the effects write.
const (
	DefaultKubeTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	DefaultKubeCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

	fluxNamespace                      = "flux-system"
	imageTagsConfigMap                 = "aerial-image-tags"
	fluxReconcileRequestedAtAnnotation = "reconcile.fluxcd.io/requestedAt"
	kubeResponseLimit                  = 64 << 10
)

// KubeError is one failed Kubernetes or Flux receiver call. Status is 0 for
// transport and configuration errors.
type KubeError struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

func (e *KubeError) Error() string {
	if e == nil {
		return ""
	}
	if e.Status == 0 {
		return e.Message
	}
	return fmt.Sprintf("kube status %d: %s", e.Status, e.Message)
}

// Retryable reports whether a later attempt can succeed: transport
// failures, 5xx, conflicts and throttling.
func (e *KubeError) Retryable() bool {
	if e == nil {
		return false
	}
	return e.Status == 0 || e.Status >= http.StatusInternalServerError || e.Status == http.StatusConflict || e.Status == http.StatusTooManyRequests
}

// GitPushRequest is the Flux receiver payload for one main push, matching
// synapse model.GitPushEventRequest on the wire.
type GitPushRequest struct {
	Repo     string `json:"repo"`
	Ref      string `json:"ref"`
	Commit   string `json:"commit"`
	TargetID string `json:"target_id,omitempty"`
	PRNumber int    `json:"pr_number,omitempty"`
}

// ImagePromotionRequest promotes one image to a commit SHA, matching synapse
// model.ImageReadyEventRequest on the wire (it is also the receiver payload).
type ImagePromotionRequest struct {
	Image     string `json:"image"`
	Repo      string `json:"repo,omitempty"`
	CommitSHA string `json:"commit_sha,omitempty"`
	PRNumber  int    `json:"pr_number,omitempty"`
	TargetID  string `json:"target_id,omitempty"`
}

// KubeEffector is the Kubernetes/Flux boundary for restate-owned Flux and
// image effects. *KubeClient satisfies it; the Restate binding wraps every
// call in one journaled restate.Run.
type KubeEffector interface {
	DispatchGitPush(ctx context.Context, req GitPushRequest) *KubeError
	PromoteImageTag(ctx context.Context, req ImagePromotionRequest) (string, *KubeError)
	ReconcileKustomization(ctx context.Context, namespace, name, requestedAt string) *KubeError
}

var _ KubeEffector = (*KubeClient)(nil)

// KubeClient writes the Flux objects restate-owned effects touch. An empty
// API server disables Kubernetes writes and an empty receiver URL disables
// receiver forwards, exactly like synapse.
type KubeClient struct {
	http        *http.Client
	apiServer   string
	tokenPath   string
	receiverURL string
}

// NewKubeClient builds a client for apiServer authenticating with the token
// read from tokenPath on every call (projected tokens rotate). client is
// used as-is for plain-HTTP servers; HTTPS servers trust caPath.
func NewKubeClient(client *http.Client, apiServer, tokenPath, caPath, receiverURL string) *KubeClient {
	apiServer = strings.TrimRight(strings.TrimSpace(apiServer), "/")
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	if strings.HasPrefix(apiServer, "https://") {
		client = kubeTLSClient(client, caPath)
	}
	return &KubeClient{http: client, apiServer: apiServer, tokenPath: strings.TrimSpace(tokenPath), receiverURL: normalizeReceiverURL(receiverURL)}
}

// InClusterAPIServer returns the API server URL from the in-cluster service
// host and port, or "" outside a cluster.
func InClusterAPIServer(host, port string) string {
	host, port = strings.TrimSpace(host), strings.TrimSpace(port)
	if host == "" || port == "" {
		return ""
	}
	return "https://" + net.JoinHostPort(host, port)
}

// kubeTLSClient trusts the service-account CA on top of the system pool,
// keeping the caller's timeout. A missing CA keeps the system pool.
func kubeTLSClient(base *http.Client, caPath string) *http.Client {
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, IdleConnTimeout: 90 * time.Second}
	if pool := loadCertPool(caPath); pool != nil {
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &http.Client{Timeout: base.Timeout, Transport: transport}
}

func loadCertPool(caPath string) *x509.CertPool {
	data, err := os.ReadFile(strings.TrimSpace(caPath))
	if err != nil || len(data) == 0 {
		return nil
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(data) {
		return nil
	}
	return pool
}

// normalizeReceiverURL trims the receiver URL and drops a bare
// notification-controller root, which is the Flux event server rather than
// a receiver webhook (synapse isFluxEventServerRootURL).
func normalizeReceiverURL(raw string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(raw), "/")
	u, err := url.Parse(trimmed)
	if err != nil || u.Host == "" {
		return trimmed
	}
	host := strings.ToLower(u.Hostname())
	if (host == "notification-controller" || strings.HasPrefix(host, "notification-controller.")) && strings.Trim(u.Path, "/") == "" {
		return ""
	}
	return trimmed
}

func (c *KubeClient) token() string {
	if c.tokenPath == "" {
		return ""
	}
	data, err := os.ReadFile(c.tokenPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// do sends one request and returns the status and a bounded body; transport
// failures return status 0.
func (c *KubeClient) do(ctx context.Context, method, endpoint, contentType string, body []byte, auth bool) (int, []byte, *KubeError) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, &KubeError{Message: fmt.Sprintf("create %s request: %v", method, err)}
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")
	if tok := c.token(); auth && tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, &KubeError{Message: fmt.Sprintf("%s request failed: %v", method, redactURLError(err))}
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, kubeResponseLimit))
	if err != nil {
		return resp.StatusCode, nil, &KubeError{Message: fmt.Sprintf("read %s response: %v", method, err)}
	}
	return resp.StatusCode, respBody, nil
}

// redactURLError drops the request URL a transport *url.Error embeds: the
// Flux receiver URL is a tokenized secret and KubeError messages reach logs
// and deploy-failure facts.
func redactURLError(err error) error {
	if uerr, ok := errors.AsType[*url.Error](err); ok {
		return fmt.Errorf("%s: %w", uerr.Op, uerr.Err)
	}
	return err
}

func statusError(op string, status int, body []byte) *KubeError {
	if status >= 200 && status < 300 {
		return nil
	}
	return &KubeError{Status: status, Message: fmt.Sprintf("%s returned status %d: %s", op, status, truncate(string(body), 512))}
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// forwardReceiver posts payload to the Flux receiver; no URL is a no-op.
func (c *KubeClient) forwardReceiver(ctx context.Context, payload any) *KubeError {
	if c.receiverURL == "" {
		return nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return &KubeError{Message: fmt.Sprintf("marshal flux receiver payload: %v", err)}
	}
	status, respBody, kerr := c.do(ctx, http.MethodPost, c.receiverURL, "application/json", body, false)
	if kerr != nil {
		return kerr
	}
	return statusError("flux receiver", status, respBody)
}

// DispatchGitPush forwards one main push to the Flux receiver, mirroring
// synapse kube.Client.DispatchGitPush.
func (c *KubeClient) DispatchGitPush(ctx context.Context, req GitPushRequest) *KubeError {
	return c.forwardReceiver(ctx, req)
}

// ResolveImageTag resolves the ConfigMap tag key and workload name for one
// image of repo, mirroring synapse kube.ResolveImageTag.
func ResolveImageTag(repo, imageRef string) (tagKey, workloadName string) {
	ref := strings.TrimSpace(imageRef)
	if i := strings.LastIndex(ref, "/"); i != -1 {
		ref = ref[i+1:]
	}
	if i := strings.Index(ref, ":"); i != -1 {
		ref = ref[:i]
	}
	workloadName = strings.TrimSpace(ref)
	if workloadName == "" || strings.EqualFold(workloadName, "none") {
		return "", ""
	}
	var imageName string
	switch repo[strings.LastIndex(repo, "/")+1:] {
	case "aerial":
		imageName = "aerial-" + strings.TrimPrefix(workloadName, "aerial-")
	case "aerial-sidecars":
		imageName = "aerial-sidecar-" + strings.TrimPrefix(workloadName, "aerial-sidecar-")
	case "mirrormere":
		imageName = workloadName
		if !strings.HasPrefix(workloadName, "mirrormere") {
			imageName = "mirrormere-" + workloadName
		}
	default:
		imageName = workloadName
	}
	return "TAG_" + strings.ToUpper(strings.ReplaceAll(imageName, "-", "_")), workloadName
}

// PromoteImageTag writes the image tag into ConfigMap/aerial-image-tags and
// forwards the promotion to the Flux receiver, mirroring synapse
// kube.Client.PromoteImageTag: the promotion succeeds when either write
// succeeds, and returns the workload name ("" when nothing was promoted).
func (c *KubeClient) PromoteImageTag(ctx context.Context, req ImagePromotionRequest) (string, *KubeError) {
	tagKey, workloadName := ResolveImageTag(req.Repo, req.Image)
	if workloadName == "" {
		return "", nil
	}
	var kubeOK, fluxOK bool
	var kubeErr, fluxErr *KubeError
	if commit := strings.TrimSpace(req.CommitSHA); c.apiServer != "" && commit != "" && !IsZeroSHA(commit) {
		if kubeErr = c.upsertImageTag(ctx, tagKey, commit); kubeErr == nil {
			kubeOK = true
		}
	}
	if c.receiverURL != "" {
		if fluxErr = c.forwardReceiver(ctx, req); fluxErr == nil {
			fluxOK = true
		}
	}
	switch {
	case kubeErr != nil && !fluxOK:
		return "", kubeErr
	case fluxErr != nil && !kubeOK:
		return "", fluxErr
	case kubeOK || fluxOK:
		return workloadName, nil
	default:
		return "", nil
	}
}

// upsertImageTag strategic-merge-patches the tag key; a missing ConfigMap is
// created, and a create race (409) falls back to one more patch.
func (c *KubeClient) upsertImageTag(ctx context.Context, tagKey, commitSHA string) *KubeError {
	body, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]string{"name": imageTagsConfigMap, "namespace": fluxNamespace},
		"data":       map[string]string{tagKey: commitSHA},
	})
	if err != nil {
		return &KubeError{Message: fmt.Sprintf("marshal configmap body: %v", err)}
	}
	base := c.apiServer + "/api/v1/namespaces/" + fluxNamespace + "/configmaps"
	patch := func() *KubeError {
		status, respBody, kerr := c.do(ctx, http.MethodPatch, base+"/"+imageTagsConfigMap, "application/strategic-merge-patch+json", body, true)
		if kerr != nil {
			return kerr
		}
		return statusError("configmap patch", status, respBody)
	}
	kerr := patch()
	if kerr == nil || kerr.Status != http.StatusNotFound {
		return kerr
	}
	status, respBody, kerr := c.do(ctx, http.MethodPost, base, "application/json", body, true)
	if kerr != nil {
		return kerr
	}
	if status == http.StatusConflict {
		return patch()
	}
	return statusError("configmap post", status, respBody)
}

// ReconcileKustomization requests a Flux reconcile by merge-patching the
// requestedAt annotation, mirroring synapse kube.Client.ReconcileKustomization.
// Re-patching the same value is a no-op for Flux, so a retry is safe.
func (c *KubeClient) ReconcileKustomization(ctx context.Context, namespace, name, requestedAt string) *KubeError {
	if c.apiServer == "" {
		return &KubeError{Message: "kube api server not configured"}
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = fluxNamespace
	}
	body, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": map[string]string{fluxReconcileRequestedAtAnnotation: requestedAt}},
	})
	if err != nil {
		return &KubeError{Message: fmt.Sprintf("marshal kustomization patch: %v", err)}
	}
	endpoint := c.apiServer + "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/" + url.PathEscape(namespace) + "/kustomizations/" + url.PathEscape(strings.TrimSpace(name))
	status, respBody, kerr := c.do(ctx, http.MethodPatch, endpoint, "application/merge-patch+json", body, true)
	if kerr != nil {
		return kerr
	}
	return statusError("reconcile kustomization "+namespace+"/"+name, status, respBody)
}
