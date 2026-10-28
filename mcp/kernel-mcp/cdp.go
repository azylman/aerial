package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// TargetInfo represents a CDP target returned from /json/list.
type TargetInfo struct {
	ID                   string `json:"id"`
	Type                 string `json:"type"`
	URL                  string `json:"url"`
	WebSocketDebuggerURL string `json:"webSocketDebuggerUrl"`
}

// cdpResponse encapsulates the result or error of a CDP command.
type cdpResponse struct {
	Result json.RawMessage
	Error  *cdpError
	Err    error
}

type cdpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// CDPClient manages a WebSocket connection to Chromium CDP with stealth injection.
type CDPClient struct {
	cfg        *Config
	httpClient *http.Client
	dialer     *websocket.Dialer

	initMu  sync.Mutex
	writeMu sync.Mutex
	stateMu sync.Mutex

	activeConn      *websocket.Conn
	activeSessionID string
	stealthReady    bool
	lastNavHost     string
	pending         map[int64]chan cdpResponse
	msgID           int64
}

// NewCDPClient creates a new CDP client.
func NewCDPClient(cfg *Config) *CDPClient {
	return &CDPClient{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		dialer: &websocket.Dialer{
			HandshakeTimeout: 5 * time.Second,
		},
		pending: make(map[int64]chan cdpResponse),
	}
}

// getActivePageTarget queries /json/list on the CDP host and returns target ID and WS URL.
func (c *CDPClient) getActivePageTarget(ctx context.Context) (string, string, error) {
	resolvedHost := ResolveCDPHost(ctx, c.cfg.CDPHost)
	listURL := fmt.Sprintf("http://%s/json/list", net.JoinHostPort(resolvedHost, c.cfg.CDPPort))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, listURL, nil)
	if err != nil {
		return "", "", fmt.Errorf("create cdp list request error: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("cdp target query failed for %s: %w", listURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("cdp target query returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("read cdp target body: %w", err)
	}

	var targets []TargetInfo
	if err := json.Unmarshal(body, &targets); err != nil {
		return "", "", fmt.Errorf("parse cdp target response: %w", err)
	}

	var selected *TargetInfo
	for i := range targets {
		if targets[i].Type == "page" && targets[i].URL != "" && !strings.HasPrefix(targets[i].URL, "chrome://") {
			selected = &targets[i]
			break
		}
	}
	if selected == nil {
		for i := range targets {
			if targets[i].Type == "page" {
				selected = &targets[i]
				break
			}
		}
	}
	if selected == nil {
		return "", "", errors.New("no open page target found in browser")
	}

	wsURL := RewriteWSUrl(selected.WebSocketDebuggerURL, resolvedHost, c.cfg.CDPPort)
	return selected.ID, wsURL, nil
}

// readLoop listens for incoming CDP messages and routes them to pending channels.
func (c *CDPClient) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			c.handleDisconnect(conn, err)
			return
		}

		var msg struct {
			ID     int64           `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *cdpError       `json:"error"`
			Method string          `json:"method"`
			Params struct {
				SessionID string `json:"sessionId"`
			} `json:"params"`
		}

		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		if msg.Method == "Target.detachedFromTarget" {
			c.handleDisconnect(conn, errors.New("target detached"))
			return
		}

		if msg.ID != 0 {
			c.stateMu.Lock()
			ch, ok := c.pending[msg.ID]
			if ok {
				delete(c.pending, msg.ID)
			}
			c.stateMu.Unlock()

			if ok {
				ch <- cdpResponse{
					Result: msg.Result,
					Error:  msg.Error,
				}
			}
		}
	}
}

// handleDisconnect resets active connection state and fails fast all in-flight requests.
func (c *CDPClient) handleDisconnect(conn *websocket.Conn, err error) {
	c.stateMu.Lock()
	if c.activeConn == conn {
		c.activeConn = nil
		c.activeSessionID = ""
		c.stealthReady = false
		c.lastNavHost = ""
	}
	for id, ch := range c.pending {
		delete(c.pending, id)
		ch <- cdpResponse{
			Err: fmt.Errorf("cdp connection closed: %w", err),
		}
	}
	c.stateMu.Unlock()
	if cErr := conn.Close(); cErr != nil {
		return
	}
}

// EnsureConnection verifies or establishes the active CDP connection.
func (c *CDPClient) EnsureConnection(ctx context.Context) error {
	c.initMu.Lock()
	defer c.initMu.Unlock()

	c.stateMu.Lock()
	conn := c.activeConn
	stealth := c.stealthReady
	c.stateMu.Unlock()

	if conn != nil {
		if !stealth {
			return c.ensureStealthSessionLocked(ctx)
		}
		return nil
	}

	targetID, wsURL, err := c.getActivePageTarget(ctx)
	if err != nil {
		return err
	}

	newConn, resp, err := c.dialer.DialContext(ctx, wsURL, nil)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("dial cdp ws %s: %w", wsURL, err)
	}

	c.stateMu.Lock()
	c.activeConn = newConn
	c.stealthReady = false
	c.stateMu.Unlock()

	go c.readLoop(newConn)

	// Optionally try Target.attachToTarget; ignore if already attached or direct page ws
	attachCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	attachResp, attachErr := c.sendCDPDirect(attachCtx, newConn, "Target.attachToTarget", map[string]any{
		"targetId": targetID,
		"flatten":  true,
	}, "")
	cancel()

	if attachErr == nil {
		var attachResult struct {
			SessionID string `json:"sessionId"`
		}
		if json.Unmarshal(attachResp, &attachResult) == nil && attachResult.SessionID != "" {
			c.stateMu.Lock()
			c.activeSessionID = attachResult.SessionID
			c.stateMu.Unlock()
		}
	}

	return c.ensureStealthSessionLocked(ctx)
}

// sendCDPDirect issues a raw CDP command over a specific connection.
func (c *CDPClient) sendCDPDirect(ctx context.Context, conn *websocket.Conn, method string, params any, sessionID string) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	id := atomic.AddInt64(&c.msgID, 1)
	respCh := make(chan cdpResponse, 1)

	c.stateMu.Lock()
	c.pending[id] = respCh
	c.stateMu.Unlock()

	payload := map[string]any{
		"id":     id,
		"method": method,
	}
	if params != nil {
		payload["params"] = params
	}
	if sessionID != "" {
		payload["sessionId"] = sessionID
	}

	data, err := json.Marshal(payload)
	if err != nil {
		c.stateMu.Lock()
		delete(c.pending, id)
		c.stateMu.Unlock()
		return nil, fmt.Errorf("marshal cdp payload: %w", err)
	}

	c.writeMu.Lock()
	err = conn.WriteMessage(websocket.TextMessage, data)
	c.writeMu.Unlock()

	if err != nil {
		c.stateMu.Lock()
		delete(c.pending, id)
		c.stateMu.Unlock()
		return nil, fmt.Errorf("write cdp payload: %w", err)
	}

	select {
	case <-ctx.Done():
		c.stateMu.Lock()
		delete(c.pending, id)
		c.stateMu.Unlock()
		return nil, ctx.Err()
	case resp := <-respCh:
		if resp.Err != nil {
			return nil, resp.Err
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("cdp error %d: %s", resp.Error.Code, resp.Error.Message)
		}
		return resp.Result, nil
	}
}

// SendCDP executes a CDP command on the active connection.
func (c *CDPClient) SendCDP(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if err := c.EnsureConnection(ctx); err != nil {
		return nil, err
	}

	c.stateMu.Lock()
	conn := c.activeConn
	sessID := c.activeSessionID
	c.stateMu.Unlock()

	if conn == nil {
		return nil, errors.New("no active cdp connection")
	}

	return c.sendCDPDirect(ctx, conn, method, params, sessID)
}

func (c *CDPClient) ensureStealthSessionLocked(ctx context.Context) error {
	c.stateMu.Lock()
	if c.stealthReady || c.activeConn == nil {
		c.stateMu.Unlock()
		return nil
	}
	conn := c.activeConn
	sessID := c.activeSessionID
	c.stateMu.Unlock()

	if _, err := c.sendCDPDirect(ctx, conn, "Page.enable", map[string]any{}, sessID); err != nil {
		return fmt.Errorf("Page.enable: %w", err)
	}

	if _, err := c.sendCDPDirect(ctx, conn, "Emulation.setUserAgentOverride", map[string]any{
		"userAgent":         StealthUserAgent,
		"acceptLanguage":    "en-US,en;q=0.9",
		"platform":          "Linux x86_64",
		"userAgentMetadata": StealthUAMetadata,
	}, sessID); err != nil {
		return fmt.Errorf("Emulation.setUserAgentOverride: %w", err)
	}

	if _, err := c.sendCDPDirect(ctx, conn, "Page.addScriptToEvaluateOnNewDocument", map[string]any{
		"source": StealthInitScript,
	}, sessID); err != nil {
		return fmt.Errorf("Page.addScriptToEvaluateOnNewDocument: %w", err)
	}

	if _, err := c.sendCDPDirect(ctx, conn, "Runtime.evaluate", map[string]any{
		"expression":    StealthInitScript,
		"returnByValue": true,
	}, sessID); err != nil {
		return fmt.Errorf("Runtime.evaluate stealth script: %w", err)
	}

	c.stateMu.Lock()
	c.stealthReady = true
	c.stateMu.Unlock()

	return nil
}

// DispatchTrustedClick performs a realistic 3-step mouse click sequence via CDP.
func (c *CDPClient) DispatchTrustedClick(ctx context.Context, x, y int) error {
	if _, err := c.SendCDP(ctx, "Input.dispatchMouseEvent", map[string]any{
		"type":   "mouseMoved",
		"x":      x,
		"y":      y,
		"button": "none",
	}); err != nil {
		return fmt.Errorf("mouseMoved: %w", err)
	}

	if _, err := c.SendCDP(ctx, "Input.dispatchMouseEvent", map[string]any{
		"type":       "mousePressed",
		"x":          x,
		"y":          y,
		"button":     "left",
		"buttons":    1,
		"clickCount": 1,
	}); err != nil {
		return fmt.Errorf("mousePressed: %w", err)
	}

	c.waitSettleBestEffort(ctx)

	if _, err := c.SendCDP(ctx, "Input.dispatchMouseEvent", map[string]any{
		"type":       "mouseReleased",
		"x":          x,
		"y":          y,
		"button":     "left",
		"buttons":    0,
		"clickCount": 1,
	}); err != nil {
		return fmt.Errorf("mouseReleased: %w", err)
	}

	return nil
}

// EvaluateJS evaluates a JS expression in the active page context.
func (c *CDPClient) EvaluateJS(ctx context.Context, expr string, awaitPromise, returnByValue bool) (json.RawMessage, error) {
	resBytes, err := c.SendCDP(ctx, "Runtime.evaluate", map[string]any{
		"expression":    expr,
		"awaitPromise":  awaitPromise,
		"returnByValue": returnByValue,
	})
	if err != nil {
		return nil, err
	}

	var evalRes struct {
		Result struct {
			Type  string          `json:"type"`
			Value json.RawMessage `json:"value"`
		} `json:"result"`
		ExceptionDetails *struct {
			Text      string `json:"text"`
			Exception *struct {
				Description string `json:"description"`
			} `json:"exception"`
		} `json:"exceptionDetails"`
	}

	if err := json.Unmarshal(resBytes, &evalRes); err != nil {
		return nil, fmt.Errorf("unmarshal evaluate result: %w", err)
	}

	if evalRes.ExceptionDetails != nil {
		desc := evalRes.ExceptionDetails.Text
		if evalRes.ExceptionDetails.Exception != nil && evalRes.ExceptionDetails.Exception.Description != "" {
			desc = evalRes.ExceptionDetails.Exception.Description
		}
		return nil, fmt.Errorf("javascript evaluation error: %s", desc)
	}

	return evalRes.Result.Value, nil
}

// Navigate tells CDP to navigate to url with per-host pacing and waits for settle delay.
func (c *CDPClient) Navigate(ctx context.Context, destURL string) error {
	host, _ := parseURLHostPort(destURL)
	c.stateMu.Lock()
	sameHost := host != "" && c.lastNavHost == host
	c.stateMu.Unlock()
	if sameHost {
		c.waitSettleBestEffort(ctx)
	}

	if _, err := c.SendCDP(ctx, "Page.navigate", map[string]any{
		"url": destURL,
	}); err != nil {
		return fmt.Errorf("Page.navigate: %w", err)
	}

	c.stateMu.Lock()
	c.lastNavHost = host
	c.stateMu.Unlock()

	return c.WaitSettle(ctx)
}

// WaitSettle waits for SettleDelay without using arbitrary sleeps.
func (c *CDPClient) waitSettleBestEffort(ctx context.Context) {
	if err := c.WaitSettle(ctx); err != nil {
		return
	}
}

func (c *CDPClient) WaitSettle(ctx context.Context) error {
	delay := c.cfg.SettleDelay
	if delay <= 0 {
		return nil
	}
	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// InsertText sends Input.insertText via CDP.
func (c *CDPClient) InsertText(ctx context.Context, text string) error {
	_, err := c.SendCDP(ctx, "Input.insertText", map[string]any{
		"text": text,
	})
	return err
}

// CaptureScreenshot captures screenshot in format (png, jpeg, webp) returning base64 bytes.
func (c *CDPClient) CaptureScreenshot(ctx context.Context, format string) (string, error) {
	if format == "" {
		format = "png"
	}
	resBytes, err := c.SendCDP(ctx, "Page.captureScreenshot", map[string]any{
		"format": format,
	})
	if err != nil {
		return "", err
	}

	var res struct {
		Data string `json:"data"`
	}
	if err := json.Unmarshal(resBytes, &res); err != nil {
		return "", fmt.Errorf("unmarshal screenshot result: %w", err)
	}
	return res.Data, nil
}

// Close closes the active connection cleanly.
func (c *CDPClient) Close() error {
	c.stateMu.Lock()
	conn := c.activeConn
	c.activeConn = nil
	c.stealthReady = false
	c.activeSessionID = ""
	c.lastNavHost = ""
	c.stateMu.Unlock()

	if conn != nil {
		return conn.Close()
	}
	return nil
}
