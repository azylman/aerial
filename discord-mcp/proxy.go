package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// ProxyHandler proxies MCP HTTP requests to the upstream server while filtering blocked tools.
type ProxyHandler struct {
	upstreamURL  *url.URL
	reverseProxy *httputil.ReverseProxy
	httpClient   *http.Client
	blockedTools map[string]bool
}

// NewProxyHandler creates a new ProxyHandler targeting the given upstream URL.
func NewProxyHandler(target string, blocked map[string]bool) (*ProxyHandler, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("invalid upstream URL %q: %w", target, err)
	}

	rp := httputil.NewSingleHostReverseProxy(u)

	return &ProxyHandler{
		upstreamURL:  u,
		reverseProxy: rp,
		httpClient:   &http.Client{},
		blockedTools: blocked,
	}, nil
}

func (p *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// If not POST or not an MCP JSON-RPC endpoint, let the reverse proxy handle it (e.g. SSE streaming or GET).
	if r.Method != http.MethodPost {
		p.reverseProxy.ServeHTTP(w, r)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	// 1. Check if the request is attempting to call a blocked tool
	isBlocked, reqID, toolName, err := IsBlockedToolCall(bodyBytes, p.blockedTools)
	if err == nil && isBlocked {
		log.Printf("[Proxy] Blocked tool call attempted for %q. Returning JSON-RPC error.", toolName)
		errBytes := BuildJSONRPCErrorResponse(reqID, -32601, fmt.Sprintf("Tool %q is disabled. Outbound replies to the user are automatically delivered by Aerial Brain at the end of the turn.", toolName))
		w.Header().Set("Content-Type", "application/json")
		w.Header().Del("Transfer-Encoding")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(errBytes)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(errBytes)
		return
	}

	// 2. Determine if this request is a tools/list request
	isToolsList := IsToolsListRequest(bodyBytes)

	// Forward request to upstream
	targetURL := p.upstreamURL.ResolveReference(r.URL)
	upstreamReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to construct upstream request: %v", err), http.StatusInternalServerError)
		return
	}

	// Copy headers
	for key, values := range r.Header {
		for _, v := range values {
			upstreamReq.Header.Add(key, v)
		}
	}
	upstreamReq.Host = p.upstreamURL.Host
	upstreamReq.Header.Set("Host", p.upstreamURL.Host)

	resp, err := p.httpClient.Do(upstreamReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("upstream error: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read upstream response: %v", err), http.StatusBadGateway)
		return
	}

	// If this was tools/list, filter the response
	if isToolsList && resp.StatusCode == http.StatusOK {
		filtered, err := FilterToolsResponse(respBytes, p.blockedTools)
		if err == nil {
			respBytes = filtered
		} else {
			log.Printf("[Proxy] Warning: failed to filter tools/list response: %v", err)
		}
	}

	// Copy response headers
	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}
	w.Header().Del("Transfer-Encoding")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(respBytes)))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBytes)
}
