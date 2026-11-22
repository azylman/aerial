package store

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIndexerClient_GetTrajectoryContext(t *testing.T) {
	tests := []struct {
		name         string
		threadID     string
		agent        string
		statusCode   int
		body         string
		wantErr      bool
		wantFacts    int
		wantTools    int
		wantStackHit bool
		wantAgent    string
	}{
		{
			name:         "returns facts and tool calls",
			threadID:     "th-1",
			statusCode:   http.StatusOK,
			body:         `{"retrieved_facts":["fact one"],"tool_calls":["tool one","tool two"]}`,
			wantFacts:    1,
			wantTools:    2,
			wantStackHit: true,
		},
		{
			name:         "empty trajectory is not an error",
			threadID:     "th-empty",
			statusCode:   http.StatusOK,
			body:         `{"retrieved_facts":[],"tool_calls":[]}`,
			wantFacts:    0,
			wantTools:    0,
			wantStackHit: true,
		},
		{
			name:       "server error returns error",
			threadID:   "th-1",
			statusCode: http.StatusInternalServerError,
			body:       `oops`,
			wantErr:    true,
		},
		{
			name:       "malformed json returns error",
			threadID:   "th-1",
			statusCode: http.StatusOK,
			body:       `{not json`,
			wantErr:    true,
		},
		{
			name:       "empty thread returns zero value without request",
			threadID:   "   ",
			statusCode: http.StatusOK,
			body:       `{"retrieved_facts":["x"],"tool_calls":["y"]}`,
		},
		{
			name:         "explicit agent overrides default stack in query",
			threadID:     "th-1",
			agent:        "agent-alpha",
			statusCode:   http.StatusOK,
			body:         `{"retrieved_facts":[],"tool_calls":[]}`,
			wantStackHit: true,
			wantAgent:    "agent-alpha",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var hits int
			var gotStack, gotAgent, gotThread string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits++
				gotStack = r.URL.Query().Get("stack")
				gotAgent = r.URL.Query().Get("agent")
				gotThread = r.URL.Path
				w.WriteHeader(tc.statusCode)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer srv.Close()

			client := NewIndexerClient(srv.URL, "canary", srv.Client())
			got, err := client.GetTrajectoryContext(context.Background(), tc.agent, tc.threadID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetTrajectoryContext() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if len(got.RetrievedFacts) != tc.wantFacts {
				t.Errorf("RetrievedFacts count = %d, want %d", len(got.RetrievedFacts), tc.wantFacts)
			}
			if len(got.ToolCalls) != tc.wantTools {
				t.Errorf("ToolCalls count = %d, want %d", len(got.ToolCalls), tc.wantTools)
			}
			expectedAgent := "canary"
			if tc.wantAgent != "" {
				expectedAgent = tc.wantAgent
			}
			assertTrajectoryHits(t, tc.wantStackHit, tc.threadID, hits, gotStack, gotAgent, gotThread, expectedAgent)
		})
	}
}

func assertTrajectoryHits(t *testing.T, wantStackHit bool, threadID string, hits int, gotStack, gotAgent, gotThread, wantAgent string) {
	t.Helper()
	if wantStackHit {
		if hits != 1 {
			t.Fatalf("server hits = %d, want 1", hits)
		}
		if gotStack != wantAgent {
			t.Errorf("stack query param = %q, want %q (Roles N5 stack filter)", gotStack, wantAgent)
		}
		if gotAgent != wantAgent {
			t.Errorf("agent query param = %q, want %q", gotAgent, wantAgent)
		}
		if !strings.Contains(gotThread, "th-") {
			t.Errorf("request path = %q, want thread id in path", gotThread)
		}
		return
	}
	if strings.TrimSpace(threadID) == "" && hits != 0 {
		t.Errorf("empty thread issued %d requests, want 0", hits)
	}
}

func TestIndexerClient_SetMaxSteps(t *testing.T) {
	tests := []struct {
		name string
		set  int
		want string
	}{
		{name: "positive updates limit", set: 7, want: "limit=7"},
		{name: "zero keeps default", set: 0, want: "limit=50"},
		{name: "negative keeps default", set: -3, want: "limit=50"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotQuery string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotQuery = r.URL.RawQuery
				_, _ = fmt.Fprint(w, `{"retrieved_facts":[],"tool_calls":[]}`)
			}))
			defer srv.Close()
			client := NewIndexerClient(srv.URL, "aerial", srv.Client())
			client.SetMaxSteps(tc.set)
			if _, err := client.GetTrajectoryContext(context.Background(), "aerial", "th-1"); err != nil {
				t.Fatalf("GetTrajectoryContext() err = %v", err)
			}
			if !strings.Contains(gotQuery, tc.want) {
				t.Errorf("query = %q, want substring %q", gotQuery, tc.want)
			}
		})
	}
	var nilClient *IndexerClient
	nilClient.SetMaxSteps(7)
}

func TestNewIndexerClient_EmptyBaseURL(t *testing.T) {
	client := NewIndexerClient("  ", "aerial", nil)
	if _, err := client.GetTrajectoryContext(context.Background(), "aerial", "th-1"); err == nil {
		t.Error("GetTrajectoryContext() with empty base URL = nil error, want error")
	}
}
