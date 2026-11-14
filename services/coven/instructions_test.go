package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestServerInstructions(t *testing.T) {
	handler := NewToolHandler(&Config{}, newTestMemStore(), &MockOllamaClient{embedding: testEmbedding(1.0)})
	ts := httptest.NewServer(NewServer(handler).Routes())
	defer ts.Close()

	tests := []struct {
		name     string
		path     string
		wantTool string
	}{
		{"initialize advertises workflow instructions", "/mcp", "supersedes_id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := mcp.NewClient(&mcp.Implementation{Name: "instructions-test", Version: "1.0.0"}, nil)
			session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
				Endpoint:             ts.URL + tc.path,
				HTTPClient:           ts.Client(),
				DisableStandaloneSSE: true,
			}, nil)
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			defer session.Close()

			got := session.InitializeResult().Instructions
			if got == "" || len(got) > 1024 {
				t.Fatalf("instructions length = %d, want 1..1024", len(got))
			}
			if !strings.Contains(got, tc.wantTool) {
				t.Errorf("instructions missing tool name %q: %q", tc.wantTool, got)
			}
		})
	}
}
