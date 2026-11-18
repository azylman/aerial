package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// RestateConfig holds Restate durable workflow runtime configuration for Vanadis.
type RestateConfig struct {
	Enabled           bool   `yaml:"enabled"`
	RestateIngressURL string `yaml:"restate_ingress_url"`
	AgentName         string `yaml:"agent_name"`
}

// NormalizeAgentName trims whitespace for per-agent scoped Restate names.
func NormalizeAgentName(agent string) string {
	return strings.ToLower(strings.TrimSpace(agent))
}

// BuildServiceName derives the Restate Build Virtual Object name for an agent.
// Empty and aerial resolve to the legacy Build name so Phase 1 stays dark.
func BuildServiceName(agent string) string {
	name := NormalizeAgentName(agent)
	if name == "" || name == "aerial" {
		return "Build"
	}
	return "Build-" + name
}

// BuildRouterServiceName derives the Restate BuildRouter service name for an agent.
// Empty and aerial resolve to the legacy BuildRouter name so Phase 1 stays dark.
func BuildRouterServiceName(agent string) string {
	name := NormalizeAgentName(agent)
	if name == "" || name == "aerial" {
		return "BuildRouter"
	}
	return "BuildRouter-" + name
}

// LoadRestateConfig loads Restate configuration from an optional YAML file with environment variable overrides.
func LoadRestateConfig(configPath string) RestateConfig {
	var fileCfg RestateConfig
	target := strings.TrimSpace(configPath)
	if fi, err := os.Stat(target); err != nil || fi.Size() == 0 {
		target = ""
		if fi, err := os.Stat(vanadisFallbackConfigPath); err == nil && fi.Size() > 0 {
			target = vanadisFallbackConfigPath
		}
	}
	if target != "" {
		if data, err := os.ReadFile(target); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err != nil {
				log.Printf("[vanadis] Warning: failed to parse restate yaml config at %s: %v", target, err)
			}
		}
	}
	enabled := fileCfg.Enabled || strings.EqualFold(os.Getenv("VANADIS_RESTATE_ENABLED"), "true") || strings.EqualFold(os.Getenv("RESTATE_ENABLED"), "true")
	ingressURL := strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("RESTATE_INGRESS_URL"), fileCfg.RestateIngressURL)), "/")
	if ingressURL != "" && !strings.EqualFold(os.Getenv("VANADIS_RESTATE_ENABLED"), "false") && !strings.EqualFold(os.Getenv("RESTATE_ENABLED"), "false") {
		enabled = true
	}
	return RestateConfig{
		Enabled:           enabled,
		RestateIngressURL: ingressURL,
		AgentName:         strings.TrimSpace(firstNonEmpty(os.Getenv("AGENT_NAME"), fileCfg.AgentName, "aerial")),
	}
}

func (s *VanadisServer) buildServiceName() string {
	if s == nil {
		return "Build"
	}
	return BuildServiceName(s.restateCfg.AgentName)
}

func (s *VanadisServer) buildRouterServiceName() string {
	if s == nil {
		return "BuildRouter"
	}
	return BuildRouterServiceName(s.restateCfg.AgentName)
}

func (s *VanadisServer) restateObjectPath(buildID, handler string) string {
	return "/" + s.buildServiceName() + "/" + buildID + "/" + handler
}

func (s *VanadisServer) restateRouterPath(handler string) string {
	return "/" + s.buildRouterServiceName() + "/" + handler
}

// CancelBuildRequest represents a cancel build mutation payload.
type CancelBuildRequest struct{}

// RemoveChunkRequest represents a request to remove a chunk from a build.
type RemoveChunkRequest struct {
	ChunkRef string `json:"chunk_ref"`
}

// FailChunkRequest carries the chunk and reason for failing a chunk.
type FailChunkRequest struct {
	DispatchID string `json:"dispatch_id,omitempty"`
	BuildID    string `json:"build_id,omitempty"`
	ChunkID    string `json:"chunk_id"`
	Reason     string `json:"reason"`
}

// FailChunkResponse represents the result of failing a chunk.
type FailChunkResponse struct {
	Failed      bool   `json:"failed"`
	BuildID     string `json:"build_id"`
	ChunkID     string `json:"chunk_id"`
	BuildStatus string `json:"build_status"`
	ChunkStatus string `json:"chunk_status"`
}

// AttachChunkPRResponse represents the result of attaching a PR to a chunk or standalone build.
type AttachChunkPRResponse struct {
	Status   string `json:"status"`
	BuildID  string `json:"build_id"`
	ChunkID  string `json:"chunk_id"`
	ThreadID string `json:"thread_id"`
}

type restateErrorResponse struct {
	Message string `json:"message"`
	Error   string `json:"error"`
	Code    int    `json:"code"`
}

func mapRestateError(statusCode int, msg string) error {
	cleanMsg := strings.TrimSpace(msg)
	if cleanMsg == "" {
		cleanMsg = http.StatusText(statusCode)
	}
	switch statusCode {
	case http.StatusBadRequest:
		return fmt.Errorf("%w: %s", ErrInvalidBuildInput, cleanMsg)
	case http.StatusNotFound:
		if strings.Contains(strings.ToLower(cleanMsg), "chunk") {
			return fmt.Errorf("%w: %s", ErrChunkNotFound, cleanMsg)
		}
		return fmt.Errorf("%w: %s", ErrBuildNotFound, cleanMsg)
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", ErrBuildConflict, cleanMsg)
	default:
		return fmt.Errorf("restate ingress error (%d): %s", statusCode, cleanMsg)
	}
}

func (s *VanadisServer) restateIngressURL() string {
	if s == nil {
		return strings.TrimRight(strings.TrimSpace(os.Getenv("RESTATE_INGRESS_URL")), "/")
	}
	url := strings.TrimRight(strings.TrimSpace(s.restateCfg.RestateIngressURL), "/")
	if url == "" {
		url = strings.TrimRight(strings.TrimSpace(os.Getenv("RESTATE_INGRESS_URL")), "/")
	}
	return url
}

var ErrRestateTimeout = errors.New("upstream response timed out; acceptance unknown; check build status before resubmitting")

func restateCallError(parent context.Context, err error) error {
	var timeout net.Error
	if parent.Err() == nil && (errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout())) {
		return ErrRestateTimeout
	}
	return err
}

// callRestateIngress proxies a synchronous command to Restate ingress.
func (s *VanadisServer) callRestateIngress(ctx context.Context, path string, payload any, out any) error {
	ingressURL := s.restateIngressURL()
	if ingressURL == "" {
		return errors.New("restate ingress URL not configured")
	}
	var reqBody io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal restate payload: %w", err)
		}
		reqBody = bytes.NewReader(b)
	} else {
		reqBody = http.NoBody
	}

	// Leave five seconds within the server write budget to return an actionable response.
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	url := ingressURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reqBody)
	if err != nil {
		return fmt.Errorf("create restate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := http.DefaultClient
	if s != nil && s.httpClient != nil {
		client = s.httpClient
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("call restate ingress %s: %w", path, restateCallError(parent, err))
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("read restate response: %w", restateCallError(parent, err))
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp restateErrorResponse
		msg := string(bodyBytes)
		if jsonErr := json.Unmarshal(bodyBytes, &errResp); jsonErr == nil {
			if errResp.Message != "" {
				msg = errResp.Message
			} else if errResp.Error != "" {
				msg = errResp.Error
			}
		}
		return mapRestateError(resp.StatusCode, msg)
	}

	if out != nil && len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, out); err != nil {
			return fmt.Errorf("unmarshal restate response: %w", err)
		}
	}
	return nil
}
