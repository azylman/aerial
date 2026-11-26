// Package restateingress posts JSON requests to the Restate ingress and parses
// its error envelopes the same way for every service.
package restateingress

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
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// HTTPError is a non-2xx response from the Restate ingress.
type HTTPError struct {
	// Status is the HTTP status code.
	Status int
	// Message is the trimmed JSON "message" field, else the trimmed "error" field, else "".
	Message string
	// Body is the raw response body.
	Body string
}

// Error implements error.
func (e *HTTPError) Error() string {
	detail := e.Message
	if detail == "" {
		detail = e.Body
	}
	return fmt.Sprintf("restate ingress returned HTTP %d: %s", e.Status, detail)
}

// ParseHTTPError builds an HTTPError from a non-2xx status and body.
func ParseHTTPError(status int, body []byte) *HTTPError {
	out := &HTTPError{Status: status, Body: string(body)}
	var envelope struct {
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	if json.Unmarshal(body, &envelope) == nil {
		out.Message = strings.TrimSpace(envelope.Message)
		if out.Message == "" {
			out.Message = strings.TrimSpace(envelope.Error)
		}
	}
	return out
}

// Post sends payload as JSON to endpoint and returns the response body.
// A nil client uses http.DefaultClient; a nil payload sends an empty body.
// idempotencyKey is sent as Idempotency-Key when non-blank. maxBody <= 0 reads
// the body unbounded. Marshal, transport and read errors are returned
// unwrapped; a non-2xx status returns *HTTPError.
func Post(ctx context.Context, client *http.Client, endpoint, idempotencyKey string, payload any, maxBody int64) ([]byte, error) {
	var body io.Reader = http.NoBody
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key := strings.TrimSpace(idempotencyKey); key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var reader io.Reader = resp.Body
	if maxBody > 0 {
		reader = io.LimitReader(resp.Body, maxBody)
	}
	respBody, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ParseHTTPError(resp.StatusCode, respBody)
	}
	return respBody, nil
}

// RegisterDeployment registers advertiseURL with the Restate admin deployments endpoint.
func RegisterDeployment(ctx context.Context, client *http.Client, adminURL, advertiseURL string) error {
	cleanAdmin := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	cleanAdv := strings.TrimSpace(advertiseURL)
	if cleanAdmin == "" || cleanAdv == "" {
		return nil
	}
	endpoint := cleanAdmin + "/deployments"
	payload := map[string]any{
		"uri":   cleanAdv,
		"force": true,
	}
	_, err := Post(ctx, client, endpoint, "", payload, 1024)
	if err == nil {
		return nil
	}
	if httpErr, ok := errors.AsType[*HTTPError](err); ok {
		detail := httpErr.Message
		if detail == "" {
			detail = httpErr.Body
		}
		if detail != "" {
			detail = ": " + detail
		}
		return fmt.Errorf("restate admin /deployments returned %d%s", httpErr.Status, detail)
	}
	return fmt.Errorf("register restate deployment: %w", err)
}

// StartServer initializes and starts an unencrypted HTTP/2 (h2c) listener for Restate services.
// It sets up graceful shutdown on ctx cancellation and serves requests in the background.
func StartServer(ctx context.Context, addr string, handler http.Handler) (net.Listener, *http.Server, <-chan error, error) {
	cleanAddr := strings.TrimSpace(addr)
	if cleanAddr == "" {
		cleanAddr = ":9080"
	}
	if !strings.Contains(cleanAddr, ":") {
		cleanAddr = ":" + cleanAddr
	}

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Addr:              cleanAddr,
		Handler:           handler,
		Protocols:         &protocols,
		ReadHeaderTimeout: 10 * time.Second,
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cleanAddr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("listen on %s: %w", cleanAddr, err)
	}

	errChan := make(chan error, 1)
	serveDone := make(chan struct{})

	if ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer cancel()
				if shutErr := srv.Shutdown(shutdownCtx); shutErr != nil && !errors.Is(shutErr, http.ErrServerClosed) {
					log.Printf("[restateingress] server shutdown notice: %v", shutErr)
				}
			case <-serveDone:
			}
		}()
	}

	go func() {
		defer close(serveDone)
		if sErr := srv.Serve(ln); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
			errChan <- sErr
		}
		close(errChan)
	}()

	return ln, srv, errChan, nil
}
