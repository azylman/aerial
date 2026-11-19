package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

func TestTranscriptHandlers_ProxyToIndexer(t *testing.T) {
	var mu sync.Mutex
	var receivedPaths []string
	var receivedQueries []string
	mockIndexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedPaths = append(receivedPaths, r.URL.Path)
		receivedQueries = append(receivedQueries, r.URL.RawQuery)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"proxied":true}`))
	}))
	defer mockIndexer.Close()

	cfg := config.NewFromData(&config.ConfigData{
		Transcripts: config.TranscriptsConfig{
			IndexerURL: mockIndexer.URL,
		},
	})

	searchHandler := handleTranscriptSearch(nil, nil, cfg)
	statsHandler := handleTranscriptStats(nil, cfg)
	stepsHandler := handleTranscriptSteps(nil, cfg)

	tests := []struct {
		name      string
		path      string
		wantPath  string
		wantQuery string
	}{
		{
			name:      "proxy search preserving query and empty stack",
			path:      "/transcripts/search?q=foo&stack=&mode=steps",
			wantPath:  "/transcripts/search",
			wantQuery: "q=foo&stack=&mode=steps",
		},
		{
			name:      "proxy api search",
			path:      "/api/transcripts/search?q=bar",
			wantPath:  "/api/transcripts/search",
			wantQuery: "q=bar",
		},
		{
			name:      "proxy stats preserving query",
			path:      "/transcripts/stats?stack=",
			wantPath:  "/transcripts/stats",
			wantQuery: "stack=",
		},
		{
			name:      "proxy api stats",
			path:      "/api/transcripts/stats",
			wantPath:  "/api/transcripts/stats",
			wantQuery: "",
		},
		{
			name:      "proxy steps preserving query",
			path:      "/transcripts/steps?session=s1&step=2&context=1",
			wantPath:  "/transcripts/steps",
			wantQuery: "session=s1&step=2&context=1",
		},
		{
			name:      "proxy api steps",
			path:      "/api/transcripts/steps?session=s2&step=0",
			wantPath:  "/api/transcripts/steps",
			wantQuery: "session=s2&step=0",
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var h http.HandlerFunc
			switch {
			case strings.Contains(tc.path, "/search"):
				h = searchHandler
			case strings.Contains(tc.path, "/stats"):
				h = statsHandler
			default:
				h = stepsHandler
			}
			w := runHTTP(h, http.MethodGet, tc.path, nil)
			if w.Code != http.StatusOK {
				t.Fatalf("got status %d, want 200", w.Code)
			}
			if !strings.Contains(w.Body.String(), `"proxied":true`) {
				t.Fatalf("unexpected body: %s", w.Body.String())
			}
			mu.Lock()
			defer mu.Unlock()
			if len(receivedPaths) <= i || receivedPaths[i] != tc.wantPath {
				t.Errorf("receivedPath[%d] = %q, want %q", i, receivedPaths[i], tc.wantPath)
			}
			if len(receivedQueries) <= i || receivedQueries[i] != tc.wantQuery {
				t.Errorf("receivedQuery[%d] = %q, want %q", i, receivedQueries[i], tc.wantQuery)
			}
		})
	}
}

func TestProxyTranscriptRequest_Errors(t *testing.T) {
	hangingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer hangingServer.Close()

	tests := []struct {
		name       string
		indexerURL string
		wantCode   int
		cancelFast bool
	}{
		{
			name:       "invalid url returns 500",
			indexerURL: "://invalid-url",
			wantCode:   http.StatusInternalServerError,
		},
		{
			name:       "unreachable url returns 502",
			indexerURL: "http://127.0.0.1:0",
			wantCode:   http.StatusBadGateway,
		},
		{
			name:       "hanging indexer returns 502 promptly on cancellation",
			indexerURL: hangingServer.URL,
			wantCode:   http.StatusBadGateway,
			cancelFast: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/transcripts/search", http.NoBody)
			if tc.cancelFast {
				ctx, cancel := context.WithTimeout(r.Context(), 50*time.Millisecond)
				defer cancel()
				r = r.WithContext(ctx)
			}
			proxyTranscriptRequest(w, r, tc.indexerURL)
			if w.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d", w.Code, tc.wantCode)
			}
		})
	}
}
