// Package restate holds the Vanadis Restate ingress client used to proxy
// synchronous commands to Restate durable workflow ingress.
package restate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/azylman/aerial/services/vanadis/internal/config"
	"github.com/azylman/aerial/services/vanadis/internal/model"
)

// Client proxies synchronous commands to Restate ingress.
type Client struct {
	cfg        config.RestateConfig
	httpClient *http.Client
}

// NewClient constructs a Client with the given Restate configuration and HTTP client.
func NewClient(cfg config.RestateConfig, httpClient *http.Client) *Client {
	return &Client{cfg: cfg, httpClient: httpClient}
}

// BuildRouterServiceName returns the single shared Restate BuildRouter
// service name, owned by the configured (default) agent's vanadis-restate.
func (c *Client) BuildRouterServiceName() string {
	return config.BuildRouterServiceName(c.cfg.AgentName)
}

// ObjectPath returns the Restate ingress path for a handler on the Build
// Virtual Object named buildService. Callers derive buildService from the
// build's runner agent via config.BuildServiceName.
func (*Client) ObjectPath(buildService, buildID, handler string) string {
	return "/" + buildService + "/" + url.PathEscape(buildID) + "/" + handler
}

// RouterPath returns the Restate ingress path for a BuildRouter service handler.
func (c *Client) RouterPath(handler string) string {
	return "/" + c.BuildRouterServiceName() + "/" + handler
}

// IngressURL returns the configured Restate ingress base URL, or "" when
// none is configured. It never consults the process environment: app wiring
// resolves env overrides once via config.LoadRestateConfig.
func (c *Client) IngressURL() string {
	return strings.TrimRight(strings.TrimSpace(c.cfg.RestateIngressURL), "/")
}

type restateErrorResponse struct {
	Message string `json:"message"`
	Error   string `json:"error"`
	Code    int    `json:"code"`
}

// MapRestateError maps a Restate ingress error status onto a Vanadis sentinel error.
func MapRestateError(statusCode int, msg string) error {
	cleanMsg := strings.TrimSpace(msg)
	if cleanMsg == "" {
		cleanMsg = http.StatusText(statusCode)
	}
	switch statusCode {
	case http.StatusBadRequest:
		return fmt.Errorf("%w: %s", model.ErrInvalidBuildInput, cleanMsg)
	case http.StatusNotFound:
		if strings.Contains(strings.ToLower(cleanMsg), "chunk") {
			return fmt.Errorf("%w: %s", model.ErrChunkNotFound, cleanMsg)
		}
		return fmt.Errorf("%w: %s", model.ErrBuildNotFound, cleanMsg)
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", model.ErrBuildConflict, cleanMsg)
	default:
		return fmt.Errorf("restate ingress error (%d): %s", statusCode, cleanMsg)
	}
}

// ErrRestateTimeout reports an upstream timeout whose acceptance is unknown.
var ErrRestateTimeout = errors.New("upstream response timed out; acceptance unknown; check build status before resubmitting")

func restateCallError(parent context.Context, err error) error {
	var timeout net.Error
	if parent.Err() == nil && (errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout())) {
		return ErrRestateTimeout
	}
	return err
}

// CallIngress proxies a synchronous command to Restate ingress.
func (c *Client) CallIngress(ctx context.Context, path string, payload any, out any) error {
	return c.CallIngressKeyed(ctx, path, "", payload, out)
}

// CallIngressKeyed is CallIngress with an Idempotency-Key header when
// idempotencyKey is non-empty.
func (c *Client) CallIngressKeyed(ctx context.Context, path, idempotencyKey string, payload any, out any) error {
	ingressURL := c.IngressURL()
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
	fullURL := ingressURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, reqBody)
	if err != nil {
		return fmt.Errorf("create restate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	client := http.DefaultClient
	if c.httpClient != nil {
		client = c.httpClient
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
		return MapRestateError(resp.StatusCode, msg)
	}

	if out != nil && len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, out); err != nil {
			return fmt.Errorf("unmarshal restate response: %w", err)
		}
	}
	return nil
}

// CallRetryAction posts a durable action-retry request to the build's Build
// object named buildService and decodes the acknowledgement into out.
func (c *Client) CallRetryAction(ctx context.Context, buildService, buildID string, body []byte, idempotencyKey string, out any) error {
	base := c.IngressURL()
	if base == "" {
		return errors.New("restate ingress URL not configured")
	}
	target := base + c.ObjectPath(buildService, buildID, "RetryActionChunk")
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	client := c.httpClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("durable retry request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		switch response.StatusCode {
		case http.StatusBadRequest:
			return fmt.Errorf("%w: retry rejected", model.ErrInvalidBuildInput)
		case http.StatusNotFound:
			return model.ErrBuildNotFound
		case http.StatusConflict:
			return model.ErrBuildConflict
		}
		return fmt.Errorf("durable retry returned HTTP %d", response.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(out); err != nil {
		return fmt.Errorf("decode durable retry: %w", err)
	}
	return nil
}
