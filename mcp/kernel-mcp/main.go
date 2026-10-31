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
	"strings"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
)

var (
	runServerFn = runServer
	exitFn      = log.Fatalf
)

// JSONRPCRequest represents an incoming JSON-RPC 2.0 request.
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// JSONRPCResponse represents an outgoing JSON-RPC 2.0 response.
type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
}

// JSONRPCError represents a JSON-RPC 2.0 error.
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// ToolCallParams defines params for "tools/call".
type ToolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// ContentItem represents MCP content payload.
type ContentItem struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	Data     string `json:"data,omitempty"`
	MIMEType string `json:"mimeType,omitempty"`
}

// ToolCallResult defines the MCP result payload.
type ToolCallResult struct {
	Content []ContentItem `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

// Server holds server dependencies and handlers.
func unmarshalOptional(raw []byte, dst any) {
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, dst); err != nil {
			return
		}
	}
}

type Server struct {
	cfg    *Config
	client *CDPClient
}

// NewServer creates a new Server.
func NewServer(cfg *Config, client *CDPClient) *Server {
	return &Server{
		cfg:    cfg,
		client: client,
	}
}

// HandleHealth handles the GET /health readiness endpoint.
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// HandleMCPStreamable processes MCP JSON-RPC 2.0 over HTTP.
func (s *Server) HandleMCPStreamable(w http.ResponseWriter, r *http.Request) {
	streamHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			s.writeRPCError(w, nil, -32700, "Failed to read request body")
			return
		}

		var req JSONRPCRequest
		if err := json.Unmarshal(bodyBytes, &req); err != nil {
			s.writeRPCError(w, nil, -32700, "Parse error")
			return
		}

		reqID := req.ID
		if len(reqID) == 0 {
			reqID = []byte("null")
		}

		switch req.Method {
		case "initialize":
			resp := map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities": map[string]any{
					"tools": map[string]any{},
				},
				"serverInfo": map[string]any{
					"name":    "kernel-mcp",
					"version": "1.0.0",
				},
			}
			s.writeRPCResult(w, reqID, resp)

		case "notifications/initialized":
			w.WriteHeader(http.StatusNoContent)

		case "tools/list":
			s.writeRPCResult(w, reqID, map[string]any{
				"tools": Tools,
			})

		case "tools/call":
			var p ToolCallParams
			if err := json.Unmarshal(req.Params, &p); err != nil {
				s.writeRPCError(w, reqID, -32602, "Invalid params")
				return
			}
			result, isErr := s.executeTool(r.Context(), p.Name, p.Arguments)
			if isErr {
				s.writeRPCResult(w, reqID, ToolCallResult{
					Content: []ContentItem{{Type: "text", Text: result}},
					IsError: true,
				})
				return
			}
			if p.Name == "browser_screenshot" || strings.TrimPrefix(strings.TrimPrefix(p.Name, "kernel_"), "steel_") == "browser_screenshot" {
				var sc struct {
					Data   string `json:"data"`
					Format string `json:"format"`
				}
				if err := json.Unmarshal([]byte(result), &sc); err == nil && sc.Data != "" {
					s.writeRPCResult(w, reqID, ToolCallResult{
						Content: []ContentItem{
							{
								Type:     "image",
								MIMEType: "image/" + sc.Format,
								Data:     sc.Data,
							},
							{
								Type: "text",
								Text: fmt.Sprintf("Screenshot captured (%s)", sc.Format),
							},
						},
					})
					return
				}
			}
			s.writeRPCResult(w, reqID, ToolCallResult{
				Content: []ContentItem{{Type: "text", Text: result}},
			})

		default:
			s.writeRPCError(w, reqID, -32601, "Method not found: "+req.Method)
		}
	})

	httpkit.ServeMCPStream(w, r, streamHandler, "kernel-mcp ready\n", "steel_", "kernel_")
}

func (s *Server) writeRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	}
	httpkit.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, msg string) {
	if len(id) == 0 {
		id = []byte("null")
	}
	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &JSONRPCError{
			Code:    code,
			Message: msg,
		},
	}
	httpkit.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) toolBrowserNavigate(ctx context.Context, argsRaw json.RawMessage) (string, bool) {
	var args struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(argsRaw, &args); err != nil || strings.TrimSpace(args.URL) == "" {
		return "Argument 'url' is required and must be a valid URL string", true
	}
	if err := s.client.Navigate(ctx, args.URL); err != nil {
		return fmt.Sprintf("Navigation failed: %v", err), true
	}
	return "Navigated to " + args.URL, false
}

func (s *Server) toolBrowserEvaluate(ctx context.Context, argsRaw json.RawMessage) (string, bool) {
	var args struct {
		Expression   string `json:"expression"`
		AwaitPromise *bool  `json:"awaitPromise"`
		ReturnByVal  *bool  `json:"returnByValue"`
	}
	if err := json.Unmarshal(argsRaw, &args); err != nil || strings.TrimSpace(args.Expression) == "" {
		return "Argument 'expression' is required", true
	}
	if err := ValidateEvaluateExpression(args.Expression); err != nil {
		return err.Error(), true
	}
	awaitPromise := true
	if args.AwaitPromise != nil {
		awaitPromise = *args.AwaitPromise
	}
	returnByValue := true
	if args.ReturnByVal != nil {
		returnByValue = *args.ReturnByVal
	}

	raw, err := s.client.EvaluateJS(ctx, args.Expression, awaitPromise, returnByValue)
	if err != nil {
		return SanitizePII(err.Error()), true
	}
	return SanitizePII(string(raw)), false
}

func (s *Server) toolBrowserGetContent(ctx context.Context, argsRaw json.RawMessage) (string, bool) {
	var args struct {
		Selector  string `json:"selector"`
		MaxLength int    `json:"maxLength"`
		MaxChars  int    `json:"max_chars"`
	}
	unmarshalOptional(argsRaw, &args)
	limit := args.MaxLength
	if limit <= 0 {
		limit = args.MaxChars
	}
	if limit <= 0 {
		limit = 15000
	}

	script := BuildGetContentScript(args.Selector)
	raw, err := s.client.EvaluateJS(ctx, script, true, true)
	if err != nil {
		return fmt.Sprintf("Content extraction failed: %v", err), true
	}

	var res struct {
		Title string `json:"title"`
		URL   string `json:"url"`
		Text  string `json:"text"`
	}
	unmarshalOptional(raw, &res)
	truncatedText, _ := TruncateContent(res.Text, limit)
	res.Text = truncatedText

	outBytes, err := json.Marshal(res)
	if err != nil {
		return fmt.Sprintf("Failed to encode content result: %v", err), true
	}
	return SanitizePII(string(outBytes)), false
}

func (s *Server) toolBrowserClick(ctx context.Context, argsRaw json.RawMessage) (string, bool) {
	var args struct {
		Selector string `json:"selector"`
	}
	if err := json.Unmarshal(argsRaw, &args); err != nil || strings.TrimSpace(args.Selector) == "" {
		return "Argument 'selector' is required", true
	}

	script := BuildClickScript(args.Selector)
	raw, err := s.client.EvaluateJS(ctx, script, true, true)
	if err != nil {
		return fmt.Sprintf("Click script evaluation failed: %v", err), true
	}

	var cr ClickResult
	if err := json.Unmarshal(raw, &cr); err != nil || !cr.Found {
		return "Element not found for selector: " + args.Selector, true
	}

	if cr.HasRect {
		jx, jy := JitterCoord(cr.X, cr.Y, cr.Width, cr.Height)
		if err := s.client.DispatchTrustedClick(ctx, jx, jy); err != nil {
			return fmt.Sprintf("Dispatch trusted click failed: %v", err), true
		}
	}

	out, err := json.Marshal(map[string]any{
		"clicked": true,
		"trusted": cr.HasRect,
		"tag":     cr.Tag,
		"text":    cr.Text,
	})
	if err != nil {
		return fmt.Sprintf("Failed to encode click result: %v", err), true
	}
	return string(out), false
}

func (s *Server) toolBrowserType(ctx context.Context, argsRaw json.RawMessage) (string, bool) {
	var args struct {
		Selector string `json:"selector"`
		Text     string `json:"text"`
		Clear    *bool  `json:"clear"`
	}
	if err := json.Unmarshal(argsRaw, &args); err != nil || strings.TrimSpace(args.Selector) == "" {
		return "Argument 'selector' is required", true
	}
	shouldClear := true
	if args.Clear != nil {
		shouldClear = *args.Clear
	}

	focusScript := BuildFocusAndClearScript(args.Selector, shouldClear)
	raw, err := s.client.EvaluateJS(ctx, focusScript, true, true)
	if err != nil {
		return fmt.Sprintf("Focus evaluation failed: %v", err), true
	}

	var fr FocusResult
	if err := json.Unmarshal(raw, &fr); err != nil || !fr.Found {
		return "Element not found for selector: " + args.Selector, true
	}

	if fr.HasRect {
		if err := s.client.DispatchTrustedClick(ctx, fr.X, fr.Y); err != nil {
			// best effort trusted click
			_ = err
		}
	}

	if err := s.client.InsertText(ctx, args.Text); err != nil {
		return fmt.Sprintf("Input.insertText failed: %v", err), true
	}

	dispatchScript := BuildDispatchInputEventsScript(args.Selector)
	if _, err := s.client.EvaluateJS(ctx, dispatchScript, true, true); err != nil {
		// best effort event dispatch
		_ = err
	}

	out, err := json.Marshal(map[string]any{
		"typed":   true,
		"trusted": true,
	})
	if err != nil {
		return fmt.Sprintf("Failed to encode type result: %v", err), true
	}
	return string(out), false
}

func (s *Server) toolBrowserScreenshot(ctx context.Context, argsRaw json.RawMessage) (string, bool) {
	var args struct {
		Format string `json:"format"`
	}
	unmarshalOptional(argsRaw, &args)
	format := strings.ToLower(strings.TrimSpace(args.Format))
	if format == "" {
		format = "png"
	}
	data, err := s.client.CaptureScreenshot(ctx, format)
	if err != nil {
		return fmt.Sprintf("Screenshot capture failed: %v", err), true
	}
	out, err := json.Marshal(map[string]string{
		"data":   data,
		"format": format,
	})
	if err != nil {
		return fmt.Sprintf("Failed to encode screenshot result: %v", err), true
	}
	return string(out), false
}

func (s *Server) toolAmazonSearch(ctx context.Context, argsRaw json.RawMessage) (string, bool) {
	var args struct {
		Query      string `json:"query"`
		MaxResults int    `json:"maxResults"`
	}
	if err := json.Unmarshal(argsRaw, &args); err != nil || strings.TrimSpace(args.Query) == "" {
		return "Argument 'query' is required", true
	}
	limit := args.MaxResults
	if limit <= 0 {
		limit = 5
	}

	searchURL := "https://www.amazon.com/s?k=" + url.QueryEscape(args.Query)
	if err := s.client.Navigate(ctx, searchURL); err != nil {
		return fmt.Sprintf("Amazon search navigation failed: %v", err), true
	}

	raw, err := s.client.EvaluateJS(ctx, BuildAmazonSearchScript(limit), true, true)
	if err != nil {
		return fmt.Sprintf("Amazon search extraction failed: %v", err), true
	}
	return SanitizePII(string(raw)), false
}

func (s *Server) toolAmazonGetCart(ctx context.Context) (string, bool) {
	cartURL := "https://www.amazon.com/gp/cart/view.html"
	if err := s.client.Navigate(ctx, cartURL); err != nil {
		return fmt.Sprintf("Amazon cart navigation failed: %v", err), true
	}

	raw, err := s.client.EvaluateJS(ctx, BuildAmazonCartScript(), true, true)
	if err != nil {
		return fmt.Sprintf("Amazon cart extraction failed: %v", err), true
	}
	return SanitizePII(string(raw)), false
}

func (s *Server) toolAmazonAddToCart(ctx context.Context, argsRaw json.RawMessage) (string, bool) {
	var args struct {
		ASIN string `json:"asin"`
	}
	if err := json.Unmarshal(argsRaw, &args); err != nil || strings.TrimSpace(args.ASIN) == "" {
		return "Argument 'asin' is required", true
	}
	asin := strings.TrimSpace(args.ASIN)
	if !ValidateASIN(asin) {
		return "Invalid ASIN format: " + asin, true
	}

	productURL := "https://www.amazon.com/dp/" + asin
	if err := s.client.Navigate(ctx, productURL); err != nil {
		return fmt.Sprintf("Product navigation failed: %v", err), true
	}

	raw, err := s.client.EvaluateJS(ctx, BuildAmazonAddToCartScript(), true, true)
	if err != nil {
		return fmt.Sprintf("Locate add-to-cart failed: %v", err), true
	}

	var btnRes struct {
		Found        bool   `json:"found"`
		HasRect      bool   `json:"hasRect"`
		X            int    `json:"x"`
		Y            int    `json:"y"`
		Width        int    `json:"width"`
		Height       int    `json:"height"`
		ProductTitle string `json:"productTitle"`
	}
	if err := json.Unmarshal(raw, &btnRes); err != nil || !btnRes.Found {
		return "Add to cart button not found for ASIN: " + asin, true
	}

	if btnRes.HasRect {
		jx, jy := JitterCoord(btnRes.X, btnRes.Y, btnRes.Width, btnRes.Height)
		if err := s.client.DispatchTrustedClick(ctx, jx, jy); err != nil {
			return fmt.Sprintf("Click add to cart failed: %v", err), true
		}
	}

	s.client.waitSettleBestEffort(ctx)

	countRaw, err := s.client.EvaluateJS(ctx, BuildAmazonCartCountScript(), true, true)
	if err != nil {
		countRaw = nil
	}
	var cartCount string
	unmarshalOptional(countRaw, &cartCount)

	out, err := json.Marshal(map[string]any{
		"added":        true,
		"trusted":      btnRes.HasRect,
		"productTitle": btnRes.ProductTitle,
		"cartCount":    cartCount,
	})
	if err != nil {
		return fmt.Sprintf("Failed to encode add to cart result: %v", err), true
	}
	return string(out), false
}

func (s *Server) executeTool(ctx context.Context, toolName string, argsRaw json.RawMessage) (string, bool) {
	name := strings.TrimPrefix(toolName, "kernel_")
	name = strings.TrimPrefix(name, "steel_")

	switch name {
	case "browser_navigate":
		return s.toolBrowserNavigate(ctx, argsRaw)
	case "browser_evaluate":
		return s.toolBrowserEvaluate(ctx, argsRaw)
	case "browser_get_content":
		return s.toolBrowserGetContent(ctx, argsRaw)
	case "browser_click":
		return s.toolBrowserClick(ctx, argsRaw)
	case "browser_type":
		return s.toolBrowserType(ctx, argsRaw)
	case "browser_screenshot":
		return s.toolBrowserScreenshot(ctx, argsRaw)
	case "amazon_search":
		return s.toolAmazonSearch(ctx, argsRaw)
	case "amazon_get_cart":
		return s.toolAmazonGetCart(ctx)
	case "amazon_add_to_cart":
		return s.toolAmazonAddToCart(ctx, argsRaw)
	default:
		return "Unknown tool: " + toolName, true
	}
}

// NewHTTPMux builds and registers HTTP routes for kernel-mcp.
func NewHTTPMux(srv *Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.HandleHealth)
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("kernel-mcp"))
	mux.HandleFunc("/mcp", srv.HandleMCPStreamable)
	return mux
}

func runServer(ctx context.Context, cfg *Config) error {
	client := NewCDPClient(cfg)
	defer func() {
		if err := client.Close(); err != nil {
			return
		}
	}()

	srv := NewServer(cfg, client)
	mux := NewHTTPMux(srv)

	addr := net.JoinHostPort("0.0.0.0", cfg.Port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("[kernel-mcp] Listening on %s (CDP: %s:%s)", addr, cfg.CDPHost, cfg.CDPPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case <-ctx.Done():
	case err := <-errChan:
		return err
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	return httpServer.Shutdown(shutdownCtx)
}

func main() {
	cfg, err := LoadConfig("")
	if err != nil {
		exitFn("[kernel-mcp] Configuration error: %v", err)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runServerFn(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("[kernel-mcp] Server error: %v", err)
	}
}
