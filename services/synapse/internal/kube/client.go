package kube

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	fieldMetadata                      = "metadata"
	fieldKind                          = "kind"
	queryLabelSelector                 = "labelSelector"
	fluxSystemNamespace                = "flux-system"
	fluxKustomizationNameLabel         = "kustomize.toolkit.fluxcd.io/name"
	fluxKustomizationNamespaceLabel    = "kustomize.toolkit.fluxcd.io/namespace"
	fluxReconcileRequestedAtAnnotation = "reconcile.fluxcd.io/requestedAt"
	fluxManagedSelector                = fluxKustomizationNameLabel

	kindConfigMap      = "ConfigMap"
	kindStatefulSet    = "StatefulSet"
	kindRestateCluster = "RestateCluster"
	kindKustomization  = "Kustomization"

	configMapAerialImageTags = "aerial-image-tags"
)

var (
	// ErrWatchExpired signals that the watch stream has expired and must be re-listed.
	ErrWatchExpired = errors.New("kube watch resourceVersion expired")

	rolloutKustomizationNamespaces = []string{fluxSystemNamespace}

	rolloutWorkloadKinds = []struct{ Kind, Path string }{
		{Kind: "Deployment", Path: "/apis/apps/v1/deployments"},
		{Kind: kindStatefulSet, Path: "/apis/apps/v1/statefulsets"},
		{Kind: "DaemonSet", Path: "/apis/apps/v1/daemonsets"},
		{Kind: kindRestateDeployment, Path: "/apis/restate.dev/v1beta1/restatedeployments"},
		{Kind: kindRestateCluster, Path: "/apis/restate.dev/v1/restateclusters"},
	}

	workloadListPaths = []string{
		"/apis/apps/v1/deployments",
		"/apis/apps/v1/statefulsets",
		"/apis/apps/v1/daemonsets",
		"/apis/batch/v1/jobs",
		"/apis/batch/v1/cronjobs",
		"/apis/restate.dev/v1beta1/restatedeployments",
		"/apis/restate.dev/v1/restateclusters",
	}
)

// Client lists and watches Flux Kustomizations, workloads, ConfigMaps and
// GitRepositories. Writes are limited to Kustomization reconcile requests.
type Client struct {
	kubeAPIServer string
	kubeToken     string
	httpClient    *http.Client
	apiClient     *http.Client
	tokenSource   *k8sauth.TokenSource
	retryDelay    time.Duration
	maxAttempts   int
}

// NewClient constructs a new Client with optional custom http.Client.
func NewClient(client *http.Client) *Client {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	httpClient := tracing.Client(client)
	return &Client{
		httpClient:  httpClient,
		apiClient:   httpClient,
		tokenSource: k8sauth.NewTokenSource(k8sauth.DefaultTokenPath, nil),
		retryDelay:  1500 * time.Millisecond,
		maxAttempts: 6,
	}
}

// SetRetryConfig configures retry backoff delay and max attempts.
func (c *Client) SetRetryConfig(delay time.Duration, maxAttempts int) {
	c.retryDelay = delay
	c.maxAttempts = maxAttempts
}

// RetryDelay returns the current retry backoff duration.
func (c *Client) RetryDelay() time.Duration {
	return c.retryDelay
}

// APIServer returns the configured Kubernetes API server base URL.
func (c *Client) APIServer() string {
	return c.kubeAPIServer
}

// SetKubeConfig sets the Kubernetes API server endpoint and an explicit
// bearer token, and builds the API server HTTP client once for every request.
// An empty token falls back to the mounted service account token.
func (c *Client) SetKubeConfig(apiServer, token string) {
	c.kubeAPIServer = strings.TrimRight(apiServer, "/")
	c.kubeToken = strings.TrimSpace(token)
	c.apiClient = k8sauth.NewAPIServerClient(c.kubeAPIServer, c.httpClient, k8sauth.DefaultCAPath)
}

func (c *Client) getKubeToken() string {
	if c.kubeToken != "" {
		return c.kubeToken
	}
	token, err := c.tokenSource.Token()
	if err != nil {
		return ""
	}
	return token
}

// KubeJSON performs a raw kube API request and returns the response body, failing on non-2xx statuses.
func (c *Client) KubeJSON(ctx context.Context, method, path, contentType string, body []byte) ([]byte, error) {
	if c.kubeAPIServer == "" {
		return nil, errors.New("kube api server not configured")
	}
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.kubeAPIServer+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("create kube %s %s request: %w", method, path, err)
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if token := c.getKubeToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.apiClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kube %s %s request failed: %w", method, path, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("read kube %s %s response: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("kube %s %s returned status %d: %s", method, path, resp.StatusCode, truncateKubeBody(respBody))
	}
	return respBody, nil
}

func truncateKubeBody(body []byte) string {
	const maxLen = 512
	if len(body) > maxLen {
		return string(body[:maxLen]) + "..."
	}
	return string(body)
}

func isKubeNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "returned status 404:")
}

func kustomizationSelector(ref model.FluxKustomizationRef) url.Values {
	return url.Values{queryLabelSelector: {fluxKustomizationNameLabel + "=" + ref.Name + "," + fluxKustomizationNamespaceLabel + "=" + ref.Namespace}}
}

func kustomizationPath(ref model.FluxKustomizationRef) string {
	path := "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/" + url.PathEscape(ref.Namespace) + "/kustomizations"
	if ref.Name != "" {
		path += "/" + url.PathEscape(ref.Name)
	}
	return path
}

func gitRepositoryPath(namespace, name string) string {
	return "/apis/source.toolkit.fluxcd.io/v1/namespaces/" + url.PathEscape(namespace) + "/gitrepositories/" + url.PathEscape(name)
}

func compareKustomizationRefs(a, b model.FluxKustomizationRef) int {
	if c := strings.Compare(a.Namespace, b.Namespace); c != 0 {
		return c
	}
	return strings.Compare(a.Name, b.Name)
}

// ListKustomizationConfigMaps lists when each ConfigMap applied by ref last changed.
func (c *Client) ListKustomizationConfigMaps(ctx context.Context, ref model.FluxKustomizationRef) ([]model.ConfigMapChange, error) {
	body, err := c.KubeJSON(ctx, http.MethodGet, "/api/v1/configmaps?"+kustomizationSelector(ref).Encode(), "", nil)
	if err != nil {
		return nil, fmt.Errorf("list %s configmaps: %w", ref.Name, err)
	}
	changes, err := DecodeConfigMapChanges(body)
	if err != nil {
		return nil, fmt.Errorf("decode %s configmaps: %w", ref.Name, err)
	}
	return changes, nil
}

// ListKustomizationWorkloads lists the Deployments, StatefulSets, DaemonSets and RestateDeployments applied by ref.
func (c *Client) ListKustomizationWorkloads(ctx context.Context, ref model.FluxKustomizationRef) ([]model.WorkloadRollout, error) {
	query := kustomizationSelector(ref)
	var workloads []model.WorkloadRollout
	for _, k := range rolloutWorkloadKinds {
		body, err := c.KubeJSON(ctx, http.MethodGet, k.Path+"?"+query.Encode(), "", nil)
		if err != nil {
			if isKubeNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("list %s workloads: %w", ref.Name, err)
		}
		decoded, err := DecodeWorkloadRollouts(k.Kind, body)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", k.Path, err)
		}
		for i := range decoded {
			if decoded[i].Kind == kindRestateDeployment && decoded[i].Replicas == 0 {
				observation, err := c.CollectStoppedChildren(ctx, decoded[i])
				if err != nil {
					return nil, err
				}
				decoded[i].StoppedChildren = observation
			}
		}
		workloads = append(workloads, decoded...)
	}
	return workloads, nil
}

// ListFluxWorkloadsVersion lists workloads at path and returns list resourceVersion for watching.
func (c *Client) ListFluxWorkloadsVersion(ctx context.Context, path string) (string, error) {
	body, err := c.KubeJSON(ctx, http.MethodGet, path+"?"+url.Values{queryLabelSelector: {fluxManagedSelector}}.Encode(), "", nil)
	if err != nil {
		return "", fmt.Errorf("list flux workloads: %w", err)
	}
	var list struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return "", fmt.Errorf("decode %s: %w", path, err)
	}
	return list.Metadata.ResourceVersion, nil
}

// WatchFluxWorkloads opens a kube watch stream of the Flux-managed workloads at path from resourceVersion.
func (c *Client) WatchFluxWorkloads(ctx context.Context, path, resourceVersion string) (io.ReadCloser, error) {
	return c.watchKube(ctx, path, url.Values{queryLabelSelector: {fluxManagedSelector}}, resourceVersion)
}

// ListWorkloadImages returns container images referenced by workloads across the cluster.
func (c *Client) ListWorkloadImages(ctx context.Context) ([]model.WorkloadImageRef, error) {
	var workloads []model.WorkloadImageRef
	for _, path := range workloadListPaths {
		body, err := c.KubeJSON(ctx, http.MethodGet, path, "", nil)
		if err != nil {
			if isKubeNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("list workloads: %w", err)
		}
		refs, err := DecodeWorkloadImageRefs(body)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", path, err)
		}
		workloads = append(workloads, refs...)
	}
	return workloads, nil
}

func (c *Client) listWorkloadImages(ctx context.Context) ([]model.WorkloadImageRef, error) {
	return c.ListWorkloadImages(ctx)
}

// DiscoverImageKustomizations returns every Flux Kustomization owning a workload that runs one of imageNames.
func (c *Client) DiscoverImageKustomizations(ctx context.Context, imageNames []string) ([]model.FluxKustomizationRef, error) {
	if c.kubeAPIServer == "" || len(imageNames) == 0 {
		return nil, nil
	}
	workloads, err := c.listWorkloadImages(ctx)
	if err != nil {
		return nil, err
	}
	if refs := KustomizationsForImages(workloads, imageNames); len(refs) > 0 {
		return refs, nil
	}
	body, err := c.KubeJSON(ctx, http.MethodGet, kustomizationPath(model.FluxKustomizationRef{Namespace: fluxSystemNamespace}), "", nil)
	if err != nil {
		return nil, fmt.Errorf("list kustomizations: %w", err)
	}
	refs, err := DecodeImageTagKustomizations(body)
	if err != nil {
		return nil, fmt.Errorf("decode kustomizations: %w", err)
	}
	return refs, nil
}

// ReconcileKustomization requests a Flux reconcile by patching the requestedAt annotation.
func (c *Client) ReconcileKustomization(ctx context.Context, ref model.FluxKustomizationRef, requestedAt string) error {
	patchBody, err := json.Marshal(map[string]any{
		fieldMetadata: map[string]any{
			"annotations": map[string]string{fluxReconcileRequestedAtAnnotation: requestedAt},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal kustomization patch body: %w", err)
	}
	if _, err := c.KubeJSON(ctx, http.MethodPatch, kustomizationPath(ref), "application/merge-patch+json", patchBody); err != nil {
		return fmt.Errorf("reconcile kustomization %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	return nil
}

// ResolveFluxSourceRepo returns the owner/repo of the GitRepository behind a Flux object.
func (c *Client) ResolveFluxSourceRepo(ctx context.Context, obj model.FluxInvolvedObject) (string, error) {
	namespace := cmp.Or(strings.TrimSpace(obj.Namespace), fluxSystemNamespace)
	source := model.FluxSourceRef{Kind: obj.Kind, Name: strings.TrimSpace(obj.Name), Namespace: namespace}
	if obj.Kind == kindKustomization {
		body, err := c.KubeJSON(ctx, http.MethodGet, kustomizationPath(model.FluxKustomizationRef{Namespace: namespace, Name: source.Name}), "", nil)
		if err != nil {
			return "", fmt.Errorf("get kustomization %s/%s: %w", namespace, source.Name, err)
		}
		var k struct {
			Spec struct {
				SourceRef model.FluxSourceRef `json:"sourceRef"`
			} `json:"spec"`
		}
		if err := json.Unmarshal(body, &k); err != nil {
			return "", fmt.Errorf("decode kustomization %s/%s: %w", namespace, source.Name, err)
		}
		source = k.Spec.SourceRef
		source.Namespace = cmp.Or(source.Namespace, namespace)
	}
	return c.gitRepositorySlug(ctx, source)
}

func (c *Client) gitRepositorySlug(ctx context.Context, source model.FluxSourceRef) (string, error) {
	if source.Kind != "GitRepository" || source.Name == "" {
		return "", nil
	}
	body, err := c.KubeJSON(ctx, http.MethodGet, gitRepositoryPath(source.Namespace, source.Name), "", nil)
	if err != nil {
		return "", fmt.Errorf("get gitrepository %s/%s: %w", source.Namespace, source.Name, err)
	}
	var repo struct {
		Spec struct {
			URL string `json:"url"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(body, &repo); err != nil {
		return "", fmt.Errorf("decode gitrepository %s/%s: %w", source.Namespace, source.Name, err)
	}
	return GitRepoSlug(repo.Spec.URL), nil
}

// DiscoverRepoKustomizations returns the sorted Kustomizations whose GitRepository source is repo.
func (c *Client) DiscoverRepoKustomizations(ctx context.Context, repo string) ([]model.FluxKustomizationRef, error) {
	repo = model.NormalizeRepo(repo)
	if c.kubeAPIServer == "" || repo == "" {
		return nil, nil
	}
	slugs := make(map[model.FluxSourceRef]string)
	var refs []model.FluxKustomizationRef
	for _, namespace := range rolloutKustomizationNamespaces {
		observations, _, err := c.ListKustomizations(ctx, namespace)
		if err != nil {
			return nil, err
		}
		for _, obs := range observations {
			if obs.Suspended {
				continue
			}
			slug, ok := slugs[obs.SourceRef]
			if !ok {
				if slug, err = c.gitRepositorySlug(ctx, obs.SourceRef); err != nil {
					return nil, err
				}
				slugs[obs.SourceRef] = slug
			}
			if slug != "" && strings.EqualFold(model.NormalizeRepo(slug), repo) {
				refs = append(refs, obs.Ref)
			}
		}
	}
	slices.SortFunc(refs, compareKustomizationRefs)
	return refs, nil
}

// ListKustomizations lists the Kustomizations of namespace and returns them with the list resourceVersion.
func (c *Client) ListKustomizations(ctx context.Context, namespace string) ([]model.KustomizationObservation, string, error) {
	body, err := c.KubeJSON(ctx, http.MethodGet, kustomizationPath(model.FluxKustomizationRef{Namespace: namespace}), "", nil)
	if err != nil {
		return nil, "", fmt.Errorf("list kustomizations: %w", err)
	}
	return DecodeKustomizationList(body, namespace)
}

// WatchKustomizations opens a kube watch stream of namespace's Kustomizations from resourceVersion.
func (c *Client) WatchKustomizations(ctx context.Context, namespace, resourceVersion string) (io.ReadCloser, error) {
	return c.watchKube(ctx, kustomizationPath(model.FluxKustomizationRef{Namespace: namespace}), url.Values{}, resourceVersion)
}

func (c *Client) watchKube(ctx context.Context, path string, query url.Values, resourceVersion string) (io.ReadCloser, error) {
	if c.kubeAPIServer == "" {
		return nil, errors.New("kube api server not configured")
	}
	query.Set("watch", "1")
	query.Set("allowWatchBookmarks", "true")
	query.Set("resourceVersion", resourceVersion)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.kubeAPIServer+path+"?"+query.Encode(), http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create kube watch %s request: %w", path, err)
	}
	req.Header.Set("Accept", "application/json")
	if token := c.getKubeToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	streamClient := *c.apiClient
	streamClient.Timeout = 0
	resp, err := streamClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kube watch %s request failed: %w", path, err)
	}
	if resp.StatusCode == http.StatusGone {
		resp.Body.Close()
		return nil, ErrWatchExpired
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return nil, fmt.Errorf("kube watch %s returned status %d (read error: %w)", path, resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("kube watch %s returned status %d: %s", path, resp.StatusCode, truncateKubeBody(body))
	}
	return resp.Body, nil
}

// GitRepoSlug extracts "owner/repo" from a git remote URL.
func GitRepoSlug(rawURL string) string {
	ref := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(rawURL), "/"), ".git")
	if i := strings.Index(ref, "://"); i != -1 {
		ref = ref[i+3:]
		j := strings.Index(ref, "/")
		if j == -1 {
			return ""
		}
		ref = ref[j+1:]
	} else if i := strings.Index(ref, ":"); i != -1 {
		ref = ref[i+1:]
	}
	parts := strings.Split(strings.Trim(ref, "/"), "/")
	if len(parts) < 2 || parts[len(parts)-2] == "" || parts[len(parts)-1] == "" {
		return ""
	}
	return parts[len(parts)-2] + "/" + parts[len(parts)-1]
}

// CleanRevisionSHA extracts the commit SHA from a Flux revision metadata string.
// Formats observed from Flux: "branch@sha1:1234abcd...", "sha1:1234abcd...", "branch@1234abcd...", "tag:v1.0.0", or bare "1234abcd...".
func CleanRevisionSHA(raw string) string {
	raw = strings.TrimSpace(raw)
	if idx := strings.Index(raw, "@sha1:"); idx != -1 {
		return strings.TrimSpace(raw[idx+len("@sha1:"):])
	}
	if idx := strings.Index(raw, "@"); idx != -1 {
		return strings.TrimSpace(raw[idx+1:])
	}
	if idx := strings.Index(raw, ":"); idx != -1 {
		return strings.TrimSpace(raw[idx+1:])
	}
	return raw
}

// ImageRepositoryBasename extracts the repository basename from an image reference.
func ImageRepositoryBasename(image string) string {
	ref := strings.TrimSpace(image)
	if i := strings.Index(ref, "@"); i != -1 {
		ref = ref[:i]
	}
	if i := strings.LastIndex(ref, "/"); i != -1 {
		ref = ref[i+1:]
	}
	if i := strings.Index(ref, ":"); i != -1 {
		ref = ref[:i]
	}
	return ref
}

// KustomizationsForImages returns the sorted Flux Kustomizations owning workloads running imageNames.
func KustomizationsForImages(workloads []model.WorkloadImageRef, imageNames []string) []model.FluxKustomizationRef {
	if len(workloads) == 0 || len(imageNames) == 0 {
		return nil
	}
	targets := make(map[string]struct{}, len(imageNames))
	for _, n := range imageNames {
		if strings.TrimSpace(n) != "" {
			targets[strings.TrimSpace(n)] = struct{}{}
		}
	}
	seen := make(map[model.FluxKustomizationRef]struct{})
	var refs []model.FluxKustomizationRef
	for _, w := range workloads {
		if strings.TrimSpace(w.DeletionTimestamp) != "" {
			continue
		}
		name := w.Labels[fluxKustomizationNameLabel]
		if name == "" {
			continue
		}
		namespace := cmp.Or(w.Labels[fluxKustomizationNamespaceLabel], fluxSystemNamespace)
		for _, img := range w.Images {
			if _, ok := targets[ImageRepositoryBasename(img)]; ok {
				ref := model.FluxKustomizationRef{Namespace: namespace, Name: name}
				if _, dup := seen[ref]; !dup {
					seen[ref] = struct{}{}
					refs = append(refs, ref)
				}
				break
			}
		}
	}
	slices.SortFunc(refs, compareKustomizationRefs)
	return refs
}

// DecodeImageTagKustomizations decodes Kustomizations that substitute from the aerial-image-tags ConfigMap.
func DecodeImageTagKustomizations(body []byte) ([]model.FluxKustomizationRef, error) {
	var list struct {
		Items []struct {
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
			Spec struct {
				PostBuild struct {
					SubstituteFrom []struct {
						Kind string `json:"kind"`
						Name string `json:"name"`
					} `json:"substituteFrom"`
				} `json:"postBuild"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, err
	}
	var refs []model.FluxKustomizationRef
	for _, item := range list.Items {
		for _, s := range item.Spec.PostBuild.SubstituteFrom {
			if s.Kind == kindConfigMap && s.Name == configMapAerialImageTags {
				refs = append(refs, model.FluxKustomizationRef{
					Namespace: item.Metadata.Namespace,
					Name:      item.Metadata.Name,
				})
				break
			}
		}
	}
	slices.SortFunc(refs, compareKustomizationRefs)
	return refs, nil
}

// DecodeWorkloadImageRefs decodes container and initContainer images from workloads.
func DecodeWorkloadImageRefs(body []byte) ([]model.WorkloadImageRef, error) {
	var list struct {
		Items []struct {
			Metadata struct {
				Labels            map[string]string `json:"labels"`
				DeletionTimestamp string            `json:"deletionTimestamp"`
			} `json:"metadata"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Image string `json:"image"`
						} `json:"containers"`
						InitContainers []struct {
							Image string `json:"image"`
						} `json:"initContainers"`
					} `json:"spec"`
				} `json:"template"`
				JobTemplate struct {
					Spec struct {
						Template struct {
							Spec struct {
								Containers []struct {
									Image string `json:"image"`
								} `json:"containers"`
								InitContainers []struct {
									Image string `json:"image"`
								} `json:"initContainers"`
							} `json:"spec"`
						} `json:"template"`
					} `json:"spec"`
				} `json:"jobTemplate"`
				Compute struct {
					Image string `json:"image"`
				} `json:"compute"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, err
	}
	var refs []model.WorkloadImageRef
	for _, item := range list.Items {
		var images []string
		for _, c := range item.Spec.Template.Spec.Containers {
			images = append(images, c.Image)
		}
		for _, c := range item.Spec.Template.Spec.InitContainers {
			images = append(images, c.Image)
		}
		for _, c := range item.Spec.JobTemplate.Spec.Template.Spec.Containers {
			images = append(images, c.Image)
		}
		for _, c := range item.Spec.JobTemplate.Spec.Template.Spec.InitContainers {
			images = append(images, c.Image)
		}
		if item.Spec.Compute.Image != "" {
			images = append(images, item.Spec.Compute.Image)
		}
		refs = append(refs, model.WorkloadImageRef{
			Labels:            item.Metadata.Labels,
			Images:            images,
			DeletionTimestamp: strings.TrimSpace(item.Metadata.DeletionTimestamp),
		})
	}
	return refs, nil
}

// DecodeConfigMapChanges decodes change timestamps on Flux-managed ConfigMaps.
func DecodeConfigMapChanges(body []byte) ([]model.ConfigMapChange, error) {
	var list struct {
		Items []struct {
			Metadata model.KubeObjectMeta `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, err
	}
	changes := make([]model.ConfigMapChange, 0, len(list.Items))
	for _, item := range list.Items {
		var latest time.Time
		for _, f := range item.Metadata.ManagedFields {
			if f.Subresource == "" && f.Time.After(latest) {
				latest = f.Time
			}
		}
		changes = append(changes, model.ConfigMapChange{
			Namespace: item.Metadata.Namespace,
			Name:      item.Metadata.Name,
			ChangedAt: latest,
		})
	}
	return changes, nil
}

// DecodeKustomizationList decodes a Kustomization LIST into observations and resourceVersion.
func DecodeKustomizationList(body []byte, namespace string) ([]model.KustomizationObservation, string, error) {
	var list struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
		Items []struct {
			Metadata struct {
				Name        string            `json:"name"`
				Namespace   string            `json:"namespace"`
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
			Spec struct {
				Suspend   bool                `json:"suspend"`
				SourceRef model.FluxSourceRef `json:"sourceRef"`
			} `json:"spec"`
			Status struct {
				LastHandledReconcileAt string                `json:"lastHandledReconcileAt"`
				LastAppliedRevision    string                `json:"lastAppliedRevision"`
				Conditions             []model.FluxCondition `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, "", fmt.Errorf("decode kustomization list: %w", err)
	}
	observations := make([]model.KustomizationObservation, 0, len(list.Items))
	for _, item := range list.Items {
		itemNS := item.Metadata.Namespace
		if itemNS == "" {
			itemNS = namespace
		}
		sourceRef := item.Spec.SourceRef
		if sourceRef.Namespace == "" {
			sourceRef.Namespace = itemNS
		}
		observations = append(observations, model.KustomizationObservation{
			Ref:                    model.FluxKustomizationRef{Namespace: itemNS, Name: item.Metadata.Name},
			Suspended:              item.Spec.Suspend,
			RequestedAt:            item.Metadata.Annotations[fluxReconcileRequestedAtAnnotation],
			LastHandledReconcileAt: item.Status.LastHandledReconcileAt,
			LastAppliedRevision:    item.Status.LastAppliedRevision,
			SourceRef:              sourceRef,
			Conditions:             item.Status.Conditions,
		})
	}
	return observations, list.Metadata.ResourceVersion, nil
}

// DecodeWorkloadRollouts decodes a workload list of kind.
func DecodeWorkloadRollouts(kind string, body []byte) ([]model.WorkloadRollout, error) {
	var list struct {
		Items []struct {
			Metadata model.KubeObjectMeta `json:"metadata"`
			Spec     struct {
				Replicas *int32                   `json:"replicas"`
				Selector *model.KubeLabelSelector `json:"selector"`
				Template struct {
					Spec struct {
						Containers []struct {
							Image string `json:"image"`
						} `json:"containers"`
						InitContainers []struct {
							Image string `json:"image"`
						} `json:"initContainers"`
					} `json:"spec"`
				} `json:"template"`
				Compute struct {
					Image    string `json:"image"`
					Replicas *int32 `json:"replicas"`
				} `json:"compute"`
			} `json:"spec"`
			Status struct {
				ObservedGeneration     int64                 `json:"observedGeneration"`
				Replicas               int32                 `json:"replicas"`
				UpdatedReplicas        int32                 `json:"updatedReplicas"`
				AvailableReplicas      int32                 `json:"availableReplicas"`
				ReadyReplicas          int32                 `json:"readyReplicas"`
				DeploymentID           string                `json:"deploymentId"`
				UpdateRevision         string                `json:"updateRevision"`
				CurrentRevision        string                `json:"currentRevision"`
				DesiredNumberScheduled int32                 `json:"desiredNumberScheduled"`
				UpdatedNumberScheduled int32                 `json:"updatedNumberScheduled"`
				NumberAvailable        int32                 `json:"numberAvailable"`
				Conditions             []model.FluxCondition `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, err
	}
	workloads := make([]model.WorkloadRollout, 0, len(list.Items))
	for _, item := range list.Items {
		var images []string
		for _, c := range item.Spec.Template.Spec.Containers {
			images = append(images, c.Image)
		}
		for _, c := range item.Spec.Template.Spec.InitContainers {
			images = append(images, c.Image)
		}
		if item.Spec.Compute.Image != "" {
			images = append(images, item.Spec.Compute.Image)
		}
		replicas := item.Spec.Replicas
		if replicas == nil && item.Spec.Compute.Replicas != nil {
			replicas = item.Spec.Compute.Replicas
		}
		var latest time.Time
		for _, f := range item.Metadata.ManagedFields {
			if f.Subresource == "" && f.Time.After(latest) {
				latest = f.Time
			}
		}
		st := item.Status
		workloads = append(workloads, model.WorkloadRollout{
			Kind:                   kind,
			UID:                    item.Metadata.UID,
			Selector:               item.Spec.Selector,
			Namespace:              item.Metadata.Namespace,
			Name:                   item.Metadata.Name,
			Images:                 images,
			ChangedAt:              latest,
			Annotations:            item.Metadata.Annotations,
			Generation:             item.Metadata.Generation,
			ObservedGeneration:     st.ObservedGeneration,
			Replicas:               *cmp.Or(replicas, new(int32(1))),
			UpdatedReplicas:        st.UpdatedReplicas,
			AvailableReplicas:      st.AvailableReplicas,
			StatusReplicas:         st.Replicas,
			ReadyReplicas:          st.ReadyReplicas,
			DeploymentID:           st.DeploymentID,
			UpdateRevision:         st.UpdateRevision,
			CurrentRevision:        st.CurrentRevision,
			DesiredNumberScheduled: st.DesiredNumberScheduled,
			UpdatedNumberScheduled: st.UpdatedNumberScheduled,
			NumberAvailable:        st.NumberAvailable,
			ReadyConditionTrue: slices.ContainsFunc(st.Conditions, func(c model.FluxCondition) bool {
				return c.Type == "Ready" && c.Status == "True"
			}),
			DeletionTimestamp: strings.TrimSpace(item.Metadata.DeletionTimestamp),
			Finalizers:        slices.Clone(item.Metadata.Finalizers),
		})
	}
	return workloads, nil
}
