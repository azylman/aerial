package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
)

// TargetK8sSecret is the hardcoded destination Kubernetes secret for all synced secrets.
const TargetK8sSecret = "default/shared-secrets"

// TargetNomadVariable is retained for backward compatibility with existing clients/tests.
const TargetNomadVariable = TargetK8sSecret

// InfisicalWebhookPayload captures common payload variants from Infisical webhooks.
type InfisicalWebhookPayload struct {
	Event       string `json:"event"`
	WorkspaceID string `json:"workspaceId"`
	ProjectID   string `json:"projectId"`
	Environment string `json:"environment"`
	Data        *struct {
		WorkspaceID string `json:"workspaceId"`
		ProjectID   string `json:"projectId"`
		Environment string `json:"environment"`
	} `json:"data"`
}

func (p *InfisicalWebhookPayload) GetWorkspaceID(fallback string) string {
	var dataWS, dataProj string
	if p.Data != nil {
		dataWS, dataProj = p.Data.WorkspaceID, p.Data.ProjectID
	}
	return firstNonEmpty(p.WorkspaceID, p.ProjectID, dataWS, dataProj, fallback)
}

func (p *InfisicalWebhookPayload) GetEnvironment(fallback string) string {
	var dataEnv string
	if p.Data != nil {
		dataEnv = p.Data.Environment
	}
	return firstNonEmpty(p.Environment, dataEnv, fallback)
}

type UniversalAuthLoginResponse struct {
	AccessToken string `json:"accessToken"`
	Token       string `json:"token"`
}

type InfisicalSecretItem struct {
	SecretKey   string `json:"secretKey"`
	SecretValue string `json:"secretValue"`
}

type InfisicalSecretsResponse struct {
	Secrets []InfisicalSecretItem `json:"secrets"`
}

type K8sSecret struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   map[string]string `json:"metadata"`
	Type       string            `json:"type"`
	StringData map[string]string `json:"stringData"`
}

type GenericWebhookPayload struct {
	WorkspaceID string            `json:"workspaceId"`
	Environment string            `json:"environment"`
	Secrets     map[string]string `json:"secrets"`
}

func (s *RouterServer) handleInfisicalWebhook(w http.ResponseWriter, r *http.Request) {
	var payload InfisicalWebhookPayload
	if !readWebhookJSON(w, r, 1<<20, true, &payload) {
		return
	}
	workspaceID := payload.GetWorkspaceID(s.cfg.InfisicalProjectID)
	envName := payload.GetEnvironment(s.cfg.InfisicalEnvironment)

	httpkit.WriteJSON(w, http.StatusAccepted, map[string]any{
		"status":         "accepted",
		"message":        "Secret sync scheduled",
		"targetSecret":   TargetK8sSecret,
		"targetVariable": TargetK8sSecret,
		"environment":    envName,
	})
	runBackgroundSync("infisical webhook sync", func(ctx context.Context) error {
		return s.SyncSecrets(ctx, workspaceID, envName)
	})
}

func (s *RouterServer) handleGenericWebhook(w http.ResponseWriter, r *http.Request) {
	var payload GenericWebhookPayload
	if !readWebhookJSON(w, r, 1<<20, true, &payload) {
		return
	}

	if len(payload.Secrets) > 0 {
		httpkit.WriteJSON(w, http.StatusAccepted, map[string]any{
			"status":         "accepted",
			"message":        "Direct secret sync scheduled",
			"targetSecret":   TargetK8sSecret,
			"targetVariable": TargetK8sSecret,
			"keyCount":       len(payload.Secrets),
		})
		runBackgroundSync("direct generic sync", func(ctx context.Context) error {
			return s.upsertK8sSecret(ctx, payload.Secrets)
		})
		return
	}

	workspaceID := firstNonEmpty(payload.WorkspaceID, s.cfg.InfisicalProjectID)
	envName := firstNonEmpty(payload.Environment, s.cfg.InfisicalEnvironment)

	httpkit.WriteJSON(w, http.StatusAccepted, map[string]any{
		"status":         "accepted",
		"message":        "Generic secret sync scheduled",
		"targetSecret":   TargetK8sSecret,
		"targetVariable": TargetK8sSecret,
		"environment":    envName,
	})
	runBackgroundSync("generic webhook sync", func(ctx context.Context) error {
		return s.SyncSecrets(ctx, workspaceID, envName)
	})
}

func (s *RouterServer) getInfisicalToken(ctx context.Context) (string, error) {
	if s.cfg.InfisicalClientID == "" || s.cfg.InfisicalClientSecret == "" {
		return "", fmt.Errorf("infisical client ID or client secret not configured")
	}
	payload, err := json.Marshal(map[string]string{
		"clientId":     s.cfg.InfisicalClientID,
		"clientSecret": s.cfg.InfisicalClientSecret,
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.InfisicalURL+"/api/v1/auth/universal-auth/login", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("infisical login request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("infisical login returned status %d: %s", resp.StatusCode, readBodyString(resp.Body))
	}
	var loginResp UniversalAuthLoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return "", fmt.Errorf("decoding infisical login response: %w", err)
	}
	token := firstNonEmpty(loginResp.AccessToken, loginResp.Token)
	if token == "" {
		return "", fmt.Errorf("empty access token in infisical login response")
	}
	return token, nil
}

func (s *RouterServer) fetchInfisicalSecrets(ctx context.Context, token, workspaceID, envName string) (map[string]string, error) {
	params := url.Values{
		"workspaceId": {firstNonEmpty(workspaceID, s.cfg.InfisicalProjectID)},
		"environment": {firstNonEmpty(envName, s.cfg.InfisicalEnvironment)},
		"secretPath":  {"/"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.InfisicalURL+"/api/v3/secrets/raw?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("infisical fetch request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("infisical fetch returned status %d: %s", resp.StatusCode, readBodyString(resp.Body))
	}
	var secretsResp InfisicalSecretsResponse
	if err := json.NewDecoder(resp.Body).Decode(&secretsResp); err != nil {
		return nil, fmt.Errorf("decoding infisical secrets response: %w", err)
	}
	items := make(map[string]string, len(secretsResp.Secrets))
	for _, item := range secretsResp.Secrets {
		items[item.SecretKey] = item.SecretValue
	}
	return items, nil
}

var (
	kubeCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	kubeTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
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

func (s *RouterServer) getKubeToken() string {
	if s.cfg.KubeToken != "" {
		return s.cfg.KubeToken
	}
	if data, err := os.ReadFile(kubeTokenPath); err == nil {
		return strings.TrimSpace(string(data))
	}
	return ""
}

func (s *RouterServer) getK8sHTTPClient() *http.Client {
	if s.httpClient != nil && !strings.HasPrefix(s.cfg.KubeAPIServer, "https://") {
		return s.httpClient
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
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
		Timeout:   15 * time.Second,
		Transport: transport,
	}
}

func (s *RouterServer) upsertK8sSecret(ctx context.Context, items map[string]string) error {
	secretObj := K8sSecret{
		APIVersion: "v1",
		Kind:       "Secret",
		Metadata: map[string]string{
			"name":      "shared-secrets",
			"namespace": "default",
		},
		Type:       "Opaque",
		StringData: items,
	}
	data, err := json.Marshal(secretObj)
	if err != nil {
		return err
	}

	apiServer := strings.TrimRight(s.cfg.KubeAPIServer, "/")
	if apiServer == "" {
		return fmt.Errorf("kubernetes API server not configured")
	}

	putURL := apiServer + "/api/v1/namespaces/default/secrets/shared-secrets"
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	putReq.Header.Set("Content-Type", "application/json")
	token := s.getKubeToken()
	if token != "" {
		putReq.Header.Set("Authorization", "Bearer "+token)
	}

	client := s.getK8sHTTPClient()

	resp, err := client.Do(putReq)
	if err != nil {
		return fmt.Errorf("k8s secret PUT request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return nil
	}

	if resp.StatusCode == http.StatusNotFound {
		postURL := apiServer + "/api/v1/namespaces/default/secrets"
		postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, bytes.NewReader(data))
		if err != nil {
			return err
		}
		postReq.Header.Set("Content-Type", "application/json")
		if token != "" {
			postReq.Header.Set("Authorization", "Bearer "+token)
		}

		postResp, err := client.Do(postReq)
		if err != nil {
			return fmt.Errorf("k8s secret POST request failed: %w", err)
		}
		defer postResp.Body.Close()

		if postResp.StatusCode == http.StatusOK || postResp.StatusCode == http.StatusCreated {
			return nil
		}
		return fmt.Errorf("k8s secret POST returned status %d: %s", postResp.StatusCode, readBodyString(postResp.Body))
	}

	return fmt.Errorf("k8s secret PUT returned status %d: %s", resp.StatusCode, readBodyString(resp.Body))
}

// SyncSecrets coordinates fetching secrets from Infisical and synchronizing directly to Kubernetes Secrets.
func (s *RouterServer) SyncSecrets(ctx context.Context, workspaceID, envName string) error {
	log.Printf("[synapse] Initiating secret sync to %s (env: %s)", TargetK8sSecret, envName)
	token, err := s.getInfisicalToken(ctx)
	if err != nil {
		return err
	}
	newItems, err := s.fetchInfisicalSecrets(ctx, token, workspaceID, envName)
	if err != nil {
		return err
	}
	if err := s.upsertK8sSecret(ctx, newItems); err != nil {
		return err
	}
	log.Printf("[synapse] Successfully synchronized %d secrets to %s", len(newItems), TargetK8sSecret)
	return nil
}
