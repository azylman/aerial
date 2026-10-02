package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"time"
)

// Config holds runtime configuration for webhooks-router.
type Config struct {
	Port                  string
	InfisicalURL          string
	InfisicalClientID     string
	InfisicalClientSecret string
	InfisicalProjectID    string
	InfisicalEnvironment  string
	NomadAddr             string
	NomadToken            string
	DefaultTargetJob      string
}

// LoadConfigFromEnv initializes configuration from environment variables.
func LoadConfigFromEnv() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "4020"
	}

	infURL := os.Getenv("INFISICAL_URL")
	if infURL == "" {
		infURL = os.Getenv("INFISICAL_HOST_URL")
	}
	if infURL == "" {
		infURL = "http://127.0.0.1:8085"
	}

	clientID := os.Getenv("INFISICAL_CLIENT_ID")
	if clientID == "" {
		clientID = os.Getenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_ID")
	}

	clientSecret := os.Getenv("INFISICAL_CLIENT_SECRET")
	if clientSecret == "" {
		clientSecret = os.Getenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET")
	}

	projectID := os.Getenv("INFISICAL_PROJECT_ID")
	if projectID == "" {
		projectID = os.Getenv("INFISICAL_WORKSPACE_ID")
	}

	envName := os.Getenv("INFISICAL_ENVIRONMENT")
	if envName == "" {
		envName = "prod"
	}

	nomadAddr := os.Getenv("NOMAD_ADDR")
	if nomadAddr == "" {
		nomadAddr = "http://127.0.0.1:4646"
	}

	defaultJob := os.Getenv("DEFAULT_TARGET_JOB")
	if defaultJob == "" {
		defaultJob = "mirrormere-core"
	}

	return Config{
		Port:                  port,
		InfisicalURL:          strings.TrimRight(infURL, "/"),
		InfisicalClientID:     clientID,
		InfisicalClientSecret: clientSecret,
		InfisicalProjectID:    projectID,
		InfisicalEnvironment:  envName,
		NomadAddr:             strings.TrimRight(nomadAddr, "/"),
		NomadToken:            os.Getenv("NOMAD_TOKEN"),
		DefaultTargetJob:      defaultJob,
	}
}

// ResolveTargetJob extracts target Nomad job name from secretPath or falls back.
func ResolveTargetJob(secretPath, fallbackJob string) string {
	clean := strings.Trim(secretPath, "/")
	if clean == "" {
		return fallbackJob
	}
	parts := strings.Split(clean, "/")
	return parts[0]
}

// InfisicalWebhookPayload captures common payload variants from Infisical webhooks.
type InfisicalWebhookPayload struct {
	Event       string `json:"event"`
	WorkspaceID string `json:"workspaceId"`
	ProjectID   string `json:"projectId"`
	Environment string `json:"environment"`
	SecretPath  string `json:"secretPath"`
	Data        *struct {
		WorkspaceID string `json:"workspaceId"`
		ProjectID   string `json:"projectId"`
		Environment string `json:"environment"`
		SecretPath  string `json:"secretPath"`
	} `json:"data"`
}

func (p *InfisicalWebhookPayload) GetWorkspaceID(fallback string) string {
	if p.WorkspaceID != "" {
		return p.WorkspaceID
	}
	if p.ProjectID != "" {
		return p.ProjectID
	}
	if p.Data != nil {
		if p.Data.WorkspaceID != "" {
			return p.Data.WorkspaceID
		}
		if p.Data.ProjectID != "" {
			return p.Data.ProjectID
		}
	}
	return fallback
}

func (p *InfisicalWebhookPayload) GetEnvironment(fallback string) string {
	if p.Environment != "" {
		return p.Environment
	}
	if p.Data != nil && p.Data.Environment != "" {
		return p.Data.Environment
	}
	return fallback
}

func (p *InfisicalWebhookPayload) GetSecretPath() string {
	if p.SecretPath != "" {
		return p.SecretPath
	}
	if p.Data != nil && p.Data.SecretPath != "" {
		return p.Data.SecretPath
	}
	return "/"
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

// RouterServer handles webhook requests and orchestrates secret synchronization.
type RouterServer struct {
	cfg        Config
	httpClient *http.Client
}

// NewRouterServer constructs a new RouterServer instance.
func NewRouterServer(cfg Config, client *http.Client) *RouterServer {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &RouterServer{
		cfg:        cfg,
		httpClient: client,
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[webhooks-router] JSON encode error: %v", err)
	}
}

// Routes initializes and returns the HTTP mux for the service.
func (s *RouterServer) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /health", s.handleHealthz)
	mux.HandleFunc("POST /webhooks/infisical", s.handleInfisicalWebhook)
	mux.HandleFunc("POST /webhooks/generic", s.handleGenericWebhook)
	return mux
}

func (s *RouterServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "ok",
		"service": "webhooks-router",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *RouterServer) handleInfisicalWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	var payload InfisicalWebhookPayload
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload"})
			return
		}
	}

	secretPath := payload.GetSecretPath()
	workspaceID := payload.GetWorkspaceID(s.cfg.InfisicalProjectID)
	envName := payload.GetEnvironment(s.cfg.InfisicalEnvironment)

	targetJob := r.URL.Query().Get("job")
	if targetJob == "" {
		targetJob = ResolveTargetJob(secretPath, s.cfg.DefaultTargetJob)
	}

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":      "accepted",
		"message":     "Secret sync scheduled",
		"targetJob":   targetJob,
		"secretPath":  secretPath,
		"environment": envName,
	})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[webhooks-router] PANIC recovered in infisical webhook sync: %v", r)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := s.SyncSecrets(ctx, workspaceID, envName, secretPath, targetJob); err != nil {
			log.Printf("[webhooks-router] Background sync failed: %v", err)
		}
	}()
}

type GenericWebhookPayload struct {
	TargetJob   string            `json:"targetJob"`
	Job         string            `json:"job"`
	SecretPath  string            `json:"secretPath"`
	Path        string            `json:"path"`
	WorkspaceID string            `json:"workspaceId"`
	Environment string            `json:"environment"`
	Secrets     map[string]string `json:"secrets"`
}

func (s *RouterServer) handleGenericWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	var payload GenericWebhookPayload
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload"})
			return
		}
	}

	targetJob := r.URL.Query().Get("job")
	if targetJob == "" {
		targetJob = payload.TargetJob
	}
	if targetJob == "" {
		targetJob = payload.Job
	}

	secretPath := r.URL.Query().Get("path")
	if secretPath == "" {
		secretPath = payload.SecretPath
	}
	if secretPath == "" {
		secretPath = payload.Path
	}
	if secretPath == "" {
		secretPath = "/"
	}

	if targetJob == "" {
		targetJob = ResolveTargetJob(secretPath, s.cfg.DefaultTargetJob)
	}

	// If direct secrets map was supplied in payload, write directly to Nomad variables
	if len(payload.Secrets) > 0 {
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":    "accepted",
			"message":   "Direct secret sync scheduled",
			"targetJob": targetJob,
			"keyCount":  len(payload.Secrets),
		})

		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[webhooks-router] PANIC recovered in direct generic sync: %v", r)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := s.putNomadVariable(ctx, targetJob, payload.Secrets); err != nil {
				log.Printf("[webhooks-router] Background direct sync failed: %v", err)
			}
		}()
		return
	}

	workspaceID := payload.WorkspaceID
	if workspaceID == "" {
		workspaceID = s.cfg.InfisicalProjectID
	}

	envName := payload.Environment
	if envName == "" {
		envName = s.cfg.InfisicalEnvironment
	}

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":      "accepted",
		"message":     "Generic secret sync scheduled",
		"targetJob":   targetJob,
		"secretPath":  secretPath,
		"environment": envName,
	})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[webhooks-router] PANIC recovered in generic webhook sync: %v", r)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := s.SyncSecrets(ctx, workspaceID, envName, secretPath, targetJob); err != nil {
			log.Printf("[webhooks-router] Background sync failed: %v", err)
		}
	}()
}

func (s *RouterServer) getInfisicalToken(ctx context.Context) (string, error) {
	if s.cfg.InfisicalClientID == "" || s.cfg.InfisicalClientSecret == "" {
		return "", fmt.Errorf("infisical client ID or client secret not configured")
	}

	loginURL := fmt.Sprintf("%s/api/v1/auth/universal-auth/login", s.cfg.InfisicalURL)
	payload, err := json.Marshal(map[string]string{
		"clientId":     s.cfg.InfisicalClientID,
		"clientSecret": s.cfg.InfisicalClientSecret,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, loginURL, strings.NewReader(string(payload)))
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
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return "", fmt.Errorf("reading infisical login response: %w", readErr)
		}
		return "", fmt.Errorf("infisical login returned status %d: %s", resp.StatusCode, string(body))
	}

	var loginResp UniversalAuthLoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return "", fmt.Errorf("decoding infisical login response: %w", err)
	}

	token := loginResp.AccessToken
	if token == "" {
		token = loginResp.Token
	}
	if token == "" {
		return "", fmt.Errorf("empty access token in infisical login response")
	}

	return token, nil
}

func (s *RouterServer) fetchInfisicalSecrets(ctx context.Context, token, workspaceID, envName, secretPath string) (map[string]string, error) {
	if workspaceID == "" {
		workspaceID = s.cfg.InfisicalProjectID
	}
	if envName == "" {
		envName = s.cfg.InfisicalEnvironment
	}
	if secretPath == "" {
		secretPath = "/"
	}
	if !strings.HasPrefix(secretPath, "/") {
		secretPath = "/" + secretPath
	}

	params := url.Values{}
	params.Set("workspaceId", workspaceID)
	params.Set("environment", envName)
	params.Set("secretPath", secretPath)

	fetchURL := fmt.Sprintf("%s/api/v3/secrets/raw?%s", s.cfg.InfisicalURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fetchURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("infisical fetch request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("reading infisical fetch response: %w", readErr)
		}
		return nil, fmt.Errorf("infisical fetch returned status %d: %s", resp.StatusCode, string(body))
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

func (s *RouterServer) getNomadVariable(ctx context.Context, targetJob string) (map[string]string, bool, error) {
	nomadPath := fmt.Sprintf("nomad/jobs/%s", targetJob)
	reqURL := fmt.Sprintf("%s/v1/var/%s", s.cfg.NomadAddr, nomadPath)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, false, err
	}
	if s.cfg.NomadToken != "" {
		req.Header.Set("X-Nomad-Token", s.cfg.NomadToken)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("nomad get variable request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return make(map[string]string), false, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, false, fmt.Errorf("reading nomad var response: %w", readErr)
		}
		return nil, false, fmt.Errorf("nomad get variable returned status %d: %s", resp.StatusCode, string(body))
	}

	var nomadVar NomadVariable
	if err := json.NewDecoder(resp.Body).Decode(&nomadVar); err != nil {
		return nil, false, fmt.Errorf("decoding nomad variable: %w", err)
	}
	if nomadVar.Items == nil {
		nomadVar.Items = make(map[string]string)
	}
	return nomadVar.Items, true, nil
}

func (s *RouterServer) putNomadVariable(ctx context.Context, targetJob string, items map[string]string) error {
	nomadPath := fmt.Sprintf("nomad/jobs/%s", targetJob)
	reqURL := fmt.Sprintf("%s/v1/var/%s", s.cfg.NomadAddr, nomadPath)

	nomadVar := NomadVariable{
		Path:  nomadPath,
		Items: items,
	}
	data, err := json.Marshal(nomadVar)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, strings.NewReader(string(data)))
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
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("reading nomad put var response: %w", readErr)
		}
		return fmt.Errorf("nomad put variable returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// SyncSecrets coordinates fetching secrets from Infisical and synchronizing to Nomad with deep idempotency checking.
func (s *RouterServer) SyncSecrets(ctx context.Context, workspaceID, envName, secretPath, targetJob string) error {
	if targetJob == "" {
		targetJob = s.cfg.DefaultTargetJob
	}
	log.Printf("[webhooks-router] Initiating secret sync for job %q (path: %s, env: %s)", targetJob, secretPath, envName)

	token, err := s.getInfisicalToken(ctx)
	if err != nil {
		log.Printf("[webhooks-router] ERROR authenticating with infisical: %v", err)
		return err
	}

	newItems, err := s.fetchInfisicalSecrets(ctx, token, workspaceID, envName, secretPath)
	if err != nil {
		log.Printf("[webhooks-router] ERROR fetching secrets from infisical: %v", err)
		return err
	}

	existingItems, exists, err := s.getNomadVariable(ctx, targetJob)
	if err != nil {
		log.Printf("[webhooks-router] ERROR querying existing nomad variable for %q: %v", targetJob, err)
		return err
	}

	// Semantic idempotency check: Avoid PUT if items are unchanged to prevent container restarts in Nomad
	if exists && reflect.DeepEqual(existingItems, newItems) {
		log.Printf("[webhooks-router] Secrets for nomad/jobs/%s are identical (%d keys); skipping Nomad variable PUT to prevent container restarts", targetJob, len(newItems))
		return nil
	}

	if err := s.putNomadVariable(ctx, targetJob, newItems); err != nil {
		log.Printf("[webhooks-router] ERROR updating nomad variable for %q: %v", targetJob, err)
		return err
	}

	log.Printf("[webhooks-router] Successfully synchronized %d secrets to nomad/jobs/%s", len(newItems), targetJob)
	return nil
}

// runServer runs the HTTP server with graceful shutdown and optional ready callback.
func runServer(ctx context.Context, cfg Config, onReady func(addr string)) error {
	server := NewRouterServer(cfg, nil)

	ln, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		return fmt.Errorf("listening on port %s: %w", cfg.Port, err)
	}

	httpServer := &http.Server{
		Handler:      server.Routes(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	if onReady != nil {
		onReady(ln.Addr().String())
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("[webhooks-router] Server listening on %s (Infisical: %s, Nomad: %s, DefaultJob: %s)",
			ln.Addr().String(), cfg.InfisicalURL, cfg.NomadAddr, cfg.DefaultTargetJob)
		if err := httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
		close(errChan)
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errChan:
		runErr = err
	}

	log.Println("[webhooks-router] Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if shutErr := httpServer.Shutdown(shutdownCtx); shutErr != nil && !errors.Is(shutErr, http.ErrServerClosed) {
		log.Printf("[webhooks-router] Shutdown error: %v", shutErr)
	}

	log.Println("[webhooks-router] Server stopped")
	return runErr
}

// runApp runs the webhooks-router application.
func runApp(ctx context.Context, cfg Config) error {
	return runServer(ctx, cfg, nil)
}

func main() {
	cfg := LoadConfigFromEnv()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runApp(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[webhooks-router] Error: %v", err)
	}
}
