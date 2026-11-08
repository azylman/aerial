package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

var (
	kubeTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	kubeCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	k8sHTTPClient = buildK8sHTTPClient()
)

func loadK8sCertPool(caPath string) *x509.CertPool {
	if caPath == "" {
		return nil
	}
	caData, err := os.ReadFile(caPath)
	if err != nil || len(caData) == 0 {
		return nil
	}
	certPool, err := x509.SystemCertPool()
	if err != nil || certPool == nil {
		certPool = x509.NewCertPool()
	} else {
		certPool = certPool.Clone()
	}
	if certPool.AppendCertsFromPEM(caData) {
		return certPool
	}
	return nil
}

func buildK8sHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		IdleConnTimeout: 90 * time.Second,
	}
	if rootCAs := loadK8sCertPool(kubeCAPath); rootCAs != nil {
		transport.TLSClientConfig = &tls.Config{
			RootCAs: rootCAs,
		}
	}
	return &http.Client{
		Timeout:   3 * time.Second,
		Transport: transport,
	}
}

type k8sDeploymentList struct {
	Items []k8sDeployment `json:"items"`
}

type k8sDeployment struct {
	Metadata k8sObjectMeta       `json:"metadata"`
	Spec     k8sDeploymentSpec   `json:"spec"`
	Status   k8sDeploymentStatus `json:"status"`
}

type k8sObjectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	Labels            map[string]string `json:"labels"`
	CreationTimestamp time.Time         `json:"creationTimestamp"`
}

type k8sDeploymentSpec struct {
	Replicas *int32 `json:"replicas"`
}

type k8sDeploymentStatus struct {
	Replicas          int32 `json:"replicas"`
	ReadyReplicas     int32 `json:"readyReplicas"`
	AvailableReplicas int32 `json:"availableReplicas"`
	UpdatedReplicas   int32 `json:"updatedReplicas"`
}

type k8sPodList struct {
	Items []k8sPod `json:"items"`
}

type k8sPod struct {
	Metadata k8sObjectMeta `json:"metadata"`
	Status   k8sPodStatus  `json:"status"`
}

type k8sPodStatus struct {
	Phase                 string               `json:"phase"`
	StartTime             *time.Time           `json:"startTime"`
	ContainerStatuses     []k8sContainerStatus `json:"containerStatuses"`
	InitContainerStatuses []k8sContainerStatus `json:"initContainerStatuses"`
}

type k8sContainerStatus struct {
	Name  string            `json:"name"`
	Ready bool              `json:"ready"`
	State k8sContainerState `json:"state"`
}

type k8sContainerState struct {
	Waiting    *k8sContainerStateWaiting    `json:"waiting"`
	Running    *k8sContainerStateRunning    `json:"running"`
	Terminated *k8sContainerStateTerminated `json:"terminated"`
}

type k8sContainerStateWaiting struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type k8sContainerStateRunning struct {
	StartedAt time.Time `json:"startedAt"`
}

type k8sContainerStateTerminated struct {
	Reason   string `json:"reason"`
	ExitCode int    `json:"exitCode"`
}

func isCrashloopReason(reason string) bool {
	return reason == "CrashLoopBackOff" || reason == "ImagePullBackOff" || reason == "ErrImagePull"
}

func checkContainerCrashloop(statuses []k8sContainerStatus) bool {
	for _, cs := range statuses {
		if cs.State.Waiting != nil && isCrashloopReason(cs.State.Waiting.Reason) {
			return true
		}
	}
	return false
}

func inspectPodHealth(pod k8sPod) (bool, bool, time.Time) {
	if pod.Status.Phase == "Failed" {
		return true, false, time.Time{}
	}
	if checkContainerCrashloop(pod.Status.ContainerStatuses) || checkContainerCrashloop(pod.Status.InitContainerStatuses) {
		return true, false, time.Time{}
	}
	var st time.Time
	if pod.Status.StartTime != nil && !pod.Status.StartTime.IsZero() {
		st = *pod.Status.StartTime
	}
	isPending := pod.Status.Phase == "Pending"
	return false, isPending, st
}

func matchPodToDeployment(pod k8sPod, depNames map[string]struct{}) string {
	for _, labelKey := range []string{"app.kubernetes.io/name", "app", "aerial.service"} {
		if name, ok := pod.Metadata.Labels[labelKey]; ok {
			if _, exists := depNames[name]; exists {
				return name
			}
		}
	}
	if _, exists := depNames[pod.Metadata.Name]; exists {
		return pod.Metadata.Name
	}
	var bestMatch string
	for depName := range depNames {
		if strings.HasPrefix(pod.Metadata.Name, depName+"-") {
			if len(depName) > len(bestMatch) {
				bestMatch = depName
			}
		}
	}
	return bestMatch
}

func indexPodsByDeployment(pods []k8sPod, depNames map[string]struct{}) map[string][]k8sPod {
	res := make(map[string][]k8sPod, len(depNames))
	for _, pod := range pods {
		if depName := matchPodToDeployment(pod, depNames); depName != "" {
			res[depName] = append(res[depName], pod)
		}
	}
	return res
}

func evaluateDeploymentStatus(dep k8sDeployment, pods []k8sPod, now time.Time) (string, int64) {
	hasCrashloop := false
	hasPending := false
	var earliestStartTime time.Time

	for _, pod := range pods {
		crash, pending, st := inspectPodHealth(pod)
		if crash {
			hasCrashloop = true
		}
		if pending {
			hasPending = true
		}
		if !st.IsZero() && (earliestStartTime.IsZero() || st.Before(earliestStartTime)) {
			earliestStartTime = st
		}
	}

	replicas := int32(1)
	if dep.Spec.Replicas != nil {
		replicas = *dep.Spec.Replicas
	}
	readyReplicas := dep.Status.ReadyReplicas

	var uptimeSec int64
	if !earliestStartTime.IsZero() {
		uptimeSec = max(int64(now.Sub(earliestStartTime).Seconds()), 0)
	} else if !dep.Metadata.CreationTimestamp.IsZero() {
		uptimeSec = max(int64(now.Sub(dep.Metadata.CreationTimestamp).Seconds()), 0)
	}

	switch {
	case hasCrashloop:
		return "unhealthy", uptimeSec
	case hasPending || (readyReplicas < replicas):
		return "starting", uptimeSec
	case readyReplicas >= replicas && replicas > 0:
		return "healthy", uptimeSec
	default:
		return "unhealthy", uptimeSec
	}
}

// ParseK8sClusterState parses Kubernetes DeploymentList and PodList JSON into unified ServiceStatus items.
func ParseK8sClusterState(deploymentsData, podsData []byte, now time.Time) ([]ServiceStatus, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if len(deploymentsData) == 0 {
		return nil, fmt.Errorf("empty deployments data")
	}
	var depList k8sDeploymentList
	if err := json.Unmarshal(deploymentsData, &depList); err != nil {
		return nil, fmt.Errorf("failed to parse k8s deployments: %w", err)
	}

	var podList k8sPodList
	if len(podsData) > 0 {
		if err := json.Unmarshal(podsData, &podList); err != nil {
			return nil, fmt.Errorf("failed to parse k8s pods: %w", err)
		}
	}

	depNames := make(map[string]struct{}, len(depList.Items))
	for _, dep := range depList.Items {
		if dep.Metadata.Name != "" {
			depNames[dep.Metadata.Name] = struct{}{}
		}
	}

	podsByDep := indexPodsByDeployment(podList.Items, depNames)

	services := make([]ServiceStatus, 0, len(depList.Items))
	for _, dep := range depList.Items {
		name := dep.Metadata.Name
		if name == "" {
			continue
		}
		status, uptimeSec := evaluateDeploymentStatus(dep, podsByDep[name], now)
		services = append(services, ServiceStatus{
			Name:          name,
			Status:        status,
			UptimeSeconds: uptimeSec,
			LastCheckTime: now,
		})
	}

	sort.Slice(services, func(i, j int) bool {
		return services[i].Name < services[j].Name
	})

	return services, nil
}

func fetchK8sJSON(ctx context.Context, targetURL, bearerToken, desc string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}

	resp, err := k8sHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, desc)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s returned HTTP %d", desc, resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func fetchK8sClusterState(ctx context.Context, kubeAPIServer string) ([]ServiceStatus, error) {
	cleanServer := strings.TrimRight(strings.TrimSpace(kubeAPIServer), "/")
	if cleanServer == "" {
		return nil, fmt.Errorf("empty kubernetes API server address")
	}

	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var bearerToken string
	if data, err := os.ReadFile(kubeTokenPath); err == nil {
		bearerToken = strings.TrimSpace(string(data))
	}

	depsData, err := fetchK8sJSON(reqCtx, cleanServer+"/apis/apps/v1/namespaces/default/deployments", bearerToken, "k8s deployments")
	if err != nil {
		return nil, err
	}

	podsData, err := fetchK8sJSON(reqCtx, cleanServer+"/api/v1/namespaces/default/pods", bearerToken, "k8s pods")
	if err != nil {
		return nil, err
	}

	return ParseK8sClusterState(depsData, podsData, time.Now().UTC())
}
