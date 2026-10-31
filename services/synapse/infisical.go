package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"github.com/azylman/aerial/pkg/httpkit"
)

// TargetNomadVariable is the hardcoded destination Nomad variable for all synced secrets.
const TargetNomadVariable = "nomad/jobs/shared"

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

type NomadVariable struct {
	Path        string            `json:"Path"`
	Items       map[string]string `json:"Items"`
	CreateIndex uint64            `json:"CreateIndex,omitempty"`
	ModifyIndex uint64            `json:"ModifyIndex,omitempty"`
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

	httpkit.WriteJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":         "accepted",
		"message":        "Secret sync scheduled",
		"targetVariable": TargetNomadVariable,
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
		httpkit.WriteJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":         "accepted",
			"message":        "Direct secret sync scheduled",
			"targetVariable": TargetNomadVariable,
			"keyCount":       len(payload.Secrets),
		})
		runBackgroundSync("direct generic sync", func(ctx context.Context) error {
			return s.putNomadVariable(ctx, payload.Secrets)
		})
		return
	}

	workspaceID := firstNonEmpty(payload.WorkspaceID, s.cfg.InfisicalProjectID)
	envName := firstNonEmpty(payload.Environment, s.cfg.InfisicalEnvironment)

	httpkit.WriteJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":         "accepted",
		"message":        "Generic secret sync scheduled",
		"targetVariable": TargetNomadVariable,
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

func (s *RouterServer) putNomadVariable(ctx context.Context, items map[string]string) error {
	data, err := json.Marshal(NomadVariable{Path: TargetNomadVariable, Items: items})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("%s/v1/var/%s", s.cfg.NomadAddr, TargetNomadVariable), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.NomadToken != "" {
		req.Header.Set("X-Nomad-Token", s.cfg.NomadToken)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("nomad put variable request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("nomad put variable returned status %d: %s", resp.StatusCode, readBodyString(resp.Body))
	}
	return nil
}

// SyncSecrets coordinates fetching secrets from Infisical and synchronizing directly to Nomad.
func (s *RouterServer) SyncSecrets(ctx context.Context, workspaceID, envName string) error {
	log.Printf("[synapse] Initiating secret sync to %s (env: %s)", TargetNomadVariable, envName)
	token, err := s.getInfisicalToken(ctx)
	if err != nil {
		return err
	}
	newItems, err := s.fetchInfisicalSecrets(ctx, token, workspaceID, envName)
	if err != nil {
		return err
	}
	if err := s.putNomadVariable(ctx, newItems); err != nil {
		return err
	}
	log.Printf("[synapse] Successfully synchronized %d secrets to %s", len(newItems), TargetNomadVariable)
	return nil
}
