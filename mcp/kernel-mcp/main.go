package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var (
	runServerFn = runServer
	exitFn      = log.Fatalf
)

// unmarshalOptional decodes raw into dst when present, leaving dst untouched on empty or invalid input.
func unmarshalOptional(raw []byte, dst any) {
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, dst); err != nil {
			return
		}
	}
}

// serverInstructions is sent on initialize so clients that defer tool schemas
// still learn how the browser tools fit together.
const serverInstructions = `All tools drive one shared browser page; amazon_* tools navigate it away.
- Flow: browser_navigate, then browser_get_content (cheap text), then browser_click / browser_type. Use browser_screenshot (large image) only when layout matters.
- browser_navigate only waits a short fixed settle delay, not for page load; confirm with browser_get_content before acting.
- browser_evaluate rejects fetch(, XMLHttpRequest and .click(; use browser_click for clicks.
- amazon_add_to_cart changes a real Amazon cart; no tool checks out.`

// Server holds server dependencies and the MCP tool handler.
type Server struct {
	cfg           *Config
	client        *CDPClient
	streamHandler http.Handler
}

// NewServer creates a new Server and its MCP tool handler.
func NewServer(cfg *Config, client *CDPClient) *Server {
	s := &Server{
		cfg:    cfg,
		client: client,
	}
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "kernel-mcp", Version: "1.0.0"}, &mcp.ServerOptions{Instructions: serverInstructions})
	s.registerTools(mcpServer)
	s.streamHandler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	return s
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

// HandleMCPStreamable serves MCP over streamable HTTP, accepting legacy kernel_-prefixed tool names.
func (s *Server) HandleMCPStreamable(w http.ResponseWriter, r *http.Request) {
	httpkit.ServeMCPStream(w, r, s.streamHandler, "kernel-mcp ready\n", "kernel_", "")
}

// NavigateArgs are the browser_navigate arguments.
type NavigateArgs struct {
	URL string `json:"url" jsonschema:"Absolute URL to navigate to (e.g. https://www.amazon.com)."`
}

// EvaluateArgs are the browser_evaluate arguments.
type EvaluateArgs struct {
	Expression    string `json:"expression" jsonschema:"JavaScript code to execute."`
	AwaitPromise  *bool  `json:"awaitPromise,omitempty" jsonschema:"Await a returned promise (default true)."`
	ReturnByValue *bool  `json:"returnByValue,omitempty" jsonschema:"Return the result by value (default true)."`
}

// GetContentArgs are the browser_get_content arguments.
type GetContentArgs struct {
	Selector  string `json:"selector,omitempty" jsonschema:"CSS selector (default body; falls back to body when nothing matches)."`
	MaxLength int    `json:"maxLength,omitempty" jsonschema:"Maximum text characters to return (default 15000)."`
	MaxChars  int    `json:"max_chars,omitempty" jsonschema:"Alias for maxLength, used only when maxLength is unset."`
}

// ClickArgs are the browser_click arguments.
type ClickArgs struct {
	Selector string `json:"selector" jsonschema:"CSS selector of the element to click."`
}

// TypeArgs are the browser_type arguments.
type TypeArgs struct {
	Selector string `json:"selector" jsonschema:"CSS selector of the input field."`
	Text     string `json:"text" jsonschema:"Text to input."`
	Clear    *bool  `json:"clear,omitempty" jsonschema:"Clear existing text before typing (default true)."`
}

// ScreenshotArgs are the browser_screenshot arguments.
type ScreenshotArgs struct {
	Format string `json:"format,omitempty" jsonschema:"Image format: png (default), jpeg or webp."`
}

// AmazonSearchArgs are the amazon_search arguments.
type AmazonSearchArgs struct {
	Query      string `json:"query" jsonschema:"Search keywords or product description."`
	MaxResults int    `json:"maxResults,omitempty" jsonschema:"Maximum number of results to extract (default 5)."`
}

// AmazonGetCartArgs are the (empty) amazon_get_cart arguments.
type AmazonGetCartArgs struct{}

// AmazonAddToCartArgs are the amazon_add_to_cart arguments.
type AmazonAddToCartArgs struct {
	ASIN string `json:"asin" jsonschema:"Amazon ASIN: exactly 10 alphanumeric characters (e.g. B08N5WRWNW), not a URL."`
}

func (s *Server) registerTools(srv *mcp.Server) {
	addTextTool(srv, &mcp.Tool{
		Name:        "browser_navigate",
		Description: "Navigate the active browser page to a URL.",
		InputSchema: inputSchema[NavigateArgs](),
	}, s.toolBrowserNavigate)
	addTextTool(srv, &mcp.Tool{
		Name:        "browser_evaluate",
		Description: "Evaluate a JavaScript expression in the active page and return the result as JSON. Card-like 13-16 digit numbers in the output are redacted.",
		InputSchema: inputSchema[EvaluateArgs](),
	}, s.toolBrowserEvaluate)
	addTextTool(srv, &mcp.Tool{
		Name:        "browser_get_content",
		Description: `Return the active page's visible text (innerText without scripts/styles) as JSON {title,url,text}, with card-like numbers redacted. Text over the limit is cut and ends with "...[truncated]".`,
		InputSchema: inputSchema[GetContentArgs](),
	}, s.toolBrowserGetContent)
	addTextTool(srv, &mcp.Tool{
		Name:        "browser_click",
		Description: "Click the element matching a CSS selector, using a trusted mouse event when it has a visible box.",
		InputSchema: inputSchema[ClickArgs](),
	}, s.toolBrowserClick)
	addTextTool(srv, &mcp.Tool{
		Name:        "browser_type",
		Description: "Type text into the input matching a CSS selector, then dispatch input/change events.",
		InputSchema: inputSchema[TypeArgs](),
	}, s.toolBrowserType)
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "browser_screenshot",
		Description: "Capture the current viewport (not the full page) as an image.",
		InputSchema: inputSchema[ScreenshotArgs](),
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args ScreenshotArgs) (*mcp.CallToolResult, any, error) {
		return s.toolBrowserScreenshot(ctx, args), nil, nil
	})
	addTextTool(srv, &mcp.Tool{
		Name:        "amazon_search",
		Description: "Search Amazon and return structured product results (title, ASIN, price, prime eligibility, rating, URL).",
		InputSchema: inputSchema[AmazonSearchArgs](),
	}, s.toolAmazonSearch)
	addTextTool(srv, &mcp.Tool{
		Name:        "amazon_get_cart",
		Description: "Read the Amazon cart without changing it: item count, subtotal, and items with title, price, quantity, and ASIN.",
		InputSchema: inputSchema[AmazonGetCartArgs](),
	}, s.toolAmazonGetCart)
	addTextTool(srv, &mcp.Tool{
		Name:        "amazon_add_to_cart",
		Description: "Add a product to the Amazon cart by ASIN, returning the product title and resulting cart count.",
		InputSchema: inputSchema[AmazonAddToCartArgs](),
	}, s.toolAmazonAddToCart)
}

// addTextTool registers a tool whose logic returns its result text and whether it is an error.
func addTextTool[In any](srv *mcp.Server, tool *mcp.Tool, run func(context.Context, In) (string, bool)) {
	mcp.AddTool(srv, tool, func(ctx context.Context, _ *mcp.CallToolRequest, args In) (*mcp.CallToolResult, any, error) {
		text, isErr := run(ctx, args)
		return textResult(text, isErr), nil, nil
	})
}

func textResult(text string, isErr bool) *mcp.CallToolResult {
	return &mcp.CallToolResult{IsError: isErr, Content: []mcp.Content{&mcp.TextContent{Text: text}}}
}

// inputSchema infers T's input schema and collapses the ["null", X] unions inferred
// for pointer fields to plain X, matching the types clients have always seen.
func inputSchema[T any]() *jsonschema.Schema {
	schema, err := jsonschema.For[T](nil)
	if err != nil {
		panic(fmt.Sprintf("kernel-mcp: infer input schema: %v", err))
	}
	for _, prop := range schema.Properties {
		if len(prop.Types) == 2 && prop.Types[0] == "null" {
			prop.Type, prop.Types = prop.Types[1], nil
		}
	}
	return schema
}

func (s *Server) toolBrowserNavigate(ctx context.Context, args NavigateArgs) (string, bool) {
	if strings.TrimSpace(args.URL) == "" {
		return "Argument 'url' is required and must be a valid URL string", true
	}
	if err := s.client.Navigate(ctx, args.URL); err != nil {
		return fmt.Sprintf("Navigation failed: %v", err), true
	}
	return "Navigated to " + args.URL, false
}

func (s *Server) toolBrowserEvaluate(ctx context.Context, args EvaluateArgs) (string, bool) {
	if strings.TrimSpace(args.Expression) == "" {
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
	if args.ReturnByValue != nil {
		returnByValue = *args.ReturnByValue
	}

	raw, err := s.client.EvaluateJS(ctx, args.Expression, awaitPromise, returnByValue)
	if err != nil {
		return SanitizePII(err.Error()), true
	}
	return SanitizePII(string(raw)), false
}

func (s *Server) toolBrowserGetContent(ctx context.Context, args GetContentArgs) (string, bool) {
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

func (s *Server) toolBrowserClick(ctx context.Context, args ClickArgs) (string, bool) {
	if strings.TrimSpace(args.Selector) == "" {
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

func (s *Server) toolBrowserType(ctx context.Context, args TypeArgs) (string, bool) {
	if strings.TrimSpace(args.Selector) == "" {
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

// toolBrowserScreenshot returns the capture as an image item plus a caption,
// falling back to the raw {data,format} JSON text when the data is not decodable.
func (s *Server) toolBrowserScreenshot(ctx context.Context, args ScreenshotArgs) *mcp.CallToolResult {
	format := strings.ToLower(strings.TrimSpace(args.Format))
	if format == "" {
		format = "png"
	}
	data, err := s.client.CaptureScreenshot(ctx, format)
	if err != nil {
		return textResult(fmt.Sprintf("Screenshot capture failed: %v", err), true)
	}
	if img, err := base64.StdEncoding.DecodeString(data); err == nil && len(img) > 0 {
		return &mcp.CallToolResult{Content: []mcp.Content{
			&mcp.ImageContent{MIMEType: "image/" + format, Data: img},
			&mcp.TextContent{Text: fmt.Sprintf("Screenshot captured (%s)", format)},
		}}
	}
	out, err := json.Marshal(map[string]string{
		"data":   data,
		"format": format,
	})
	if err != nil {
		return textResult(fmt.Sprintf("Failed to encode screenshot result: %v", err), true)
	}
	return textResult(string(out), false)
}

func (s *Server) toolAmazonSearch(ctx context.Context, args AmazonSearchArgs) (string, bool) {
	if strings.TrimSpace(args.Query) == "" {
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

func (s *Server) toolAmazonGetCart(ctx context.Context, _ AmazonGetCartArgs) (string, bool) {
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

func (s *Server) toolAmazonAddToCart(ctx context.Context, args AmazonAddToCartArgs) (string, bool) {
	if strings.TrimSpace(args.ASIN) == "" {
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

// NewHTTPMux builds and registers HTTP routes for kernel-mcp.
func NewHTTPMux(srv *Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.HandleHealth)
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("kernel-mcp"))
	mux.Handle("/mcp", tracing.Middleware("kernel-mcp", http.HandlerFunc(srv.HandleMCPStreamable)))
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
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
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
	_, flushTracing := tracing.Init(ctx, "kernel-mcp", tracing.DefaultConfigPath)

	err = runServerFn(ctx, cfg)
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("[kernel-mcp] Server error: %v", err)
	}
}
