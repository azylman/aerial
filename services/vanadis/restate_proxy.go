package main

import (
	"context"

	"github.com/azylman/aerial/services/vanadis/internal/config"
	"github.com/azylman/aerial/services/vanadis/internal/restate"
)

// RestateConfig holds Restate durable workflow runtime configuration for Vanadis.
type RestateConfig = config.RestateConfig

var (
	// NormalizeAgentName trims whitespace for per-agent scoped Restate names.
	NormalizeAgentName = config.NormalizeAgentName
	// BuildServiceName derives the Restate Build Virtual Object name for an agent.
	// Empty and aerial resolve to the legacy Build name so Phase 1 stays dark.
	BuildServiceName = config.BuildServiceName
	// BuildRouterServiceName derives the Restate BuildRouter service name for an agent.
	// Empty and aerial resolve to the legacy BuildRouter name so Phase 1 stays dark.
	BuildRouterServiceName = config.BuildRouterServiceName
	// LoadRestateConfig loads Restate configuration from an optional YAML file with environment variable overrides.
	LoadRestateConfig = config.LoadRestateConfig
)

// Client proxies synchronous commands to Restate ingress.
type Client = restate.Client

var (
	// NewClient constructs a Restate ingress Client.
	NewClient = restate.NewClient
	// MapRestateError maps a Restate ingress error status onto a Vanadis sentinel error.
	MapRestateError = restate.MapRestateError
	// ErrRestateTimeout reports an upstream timeout whose acceptance is unknown.
	ErrRestateTimeout = restate.ErrRestateTimeout
	// CallIngress proxies a synchronous command to Restate ingress.
	CallIngress = (*Client).CallIngress
	// CallRetryAction posts a durable action-retry request for a build.
	CallRetryAction = (*Client).CallRetryAction
)

func (s *VanadisServer) restateClient() *Client {
	if s == nil {
		return NewClient(RestateConfig{}, nil)
	}
	return NewClient(s.restateCfg, s.httpClient)
}

func (s *VanadisServer) restateObjectPath(buildID, handler string) string {
	return s.restateClient().ObjectPath(buildID, handler)
}

func (s *VanadisServer) restateRouterPath(handler string) string {
	return s.restateClient().RouterPath(handler)
}

// callRestateIngress proxies a synchronous command to Restate ingress.
func (s *VanadisServer) callRestateIngress(ctx context.Context, path string, payload any, out any) error {
	return s.restateClient().CallIngress(ctx, path, payload, out)
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
