package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

func TestNormalizeAgent_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "clean lowercase slug", input: "agent-alpha", want: "agent-alpha"},
		{name: "uppercase and whitespace", input: "  AGENT-Alpha  ", want: "agent-alpha"},
		{name: "disallowed symbols stripped", input: "agent_alpha!@#$", want: "agentalpha"},
		{name: "numbers and hyphens kept", input: "agent-007-beta", want: "agent-007-beta"},
		{name: "empty input", input: "", want: ""},
		{name: "whitespace only", input: "   ", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeAgent(tc.input)
			if got != tc.want {
				t.Errorf("NormalizeAgent(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestMatchAgent_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		itemAgent   string
		filterAgent string
		want        bool
	}{
		{name: "empty filter matches any", itemAgent: "agent-alpha", filterAgent: "", want: true},
		{name: "all filter matches any", itemAgent: "agent-alpha", filterAgent: "all", want: true},
		{name: "ALL uppercase matches any", itemAgent: "agent-alpha", filterAgent: "ALL", want: true},
		{name: "exact match", itemAgent: "agent-alpha", filterAgent: "agent-alpha", want: true},
		{name: "normalized case match", itemAgent: "Agent-Alpha", filterAgent: "agent-alpha", want: true},
		{name: "mismatch", itemAgent: "agent-alpha", filterAgent: "agent-beta", want: false},
		{name: "empty item defaults to aerial - aerial filter matches", itemAgent: "", filterAgent: "aerial", want: true},
		{name: "empty item defaults to aerial - non-aerial filter mismatches", itemAgent: "", filterAgent: "agent-alpha", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := MatchAgent(tc.itemAgent, tc.filterAgent)
			if got != tc.want {
				t.Errorf("MatchAgent(%q, %q) = %v, want %v", tc.itemAgent, tc.filterAgent, got, tc.want)
			}
		})
	}
}

func TestSortConfiguredAgents_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		agents []string
		want   []string
	}{
		{name: "nil returns aerial", agents: nil, want: []string{"aerial"}},
		{name: "empty returns aerial", agents: []string{}, want: []string{"aerial"}},
		{name: "aerial only", agents: []string{"aerial"}, want: []string{"aerial"}},
		{name: "aerial preserved first and others sorted alphabetically", agents: []string{"agent-beta", "aerial", "agent-alpha"}, want: []string{"aerial", "agent-alpha", "agent-beta"}},
		{name: "duplicates and all and empty skipped", agents: []string{"", "all", "ALL", "agent-alpha", "agent-alpha", "agent-beta"}, want: []string{"aerial", "agent-alpha", "agent-beta"}},
		{name: "missing aerial automatically prepended", agents: []string{"agent-beta", "agent-alpha"}, want: []string{"aerial", "agent-alpha", "agent-beta"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SortConfiguredAgents(tc.agents)
			if len(got) != len(tc.want) {
				t.Fatalf("SortConfiguredAgents(%v) len = %d, want %d: %v", tc.agents, len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("SortConfiguredAgents(%v)[%d] = %q, want %q", tc.agents, i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestFilterByAgent_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		filter    string
		itemAgent string
		wantKept  bool
	}{
		{name: "matching agent kept by every wrapper", filter: "agent-alpha", itemAgent: "agent-alpha", wantKept: true},
		{name: "other agent dropped by every wrapper", filter: "agent-alpha", itemAgent: "aerial", wantKept: false},
		{name: "all filter short-circuits every wrapper", filter: "all", itemAgent: "agent-alpha", wantKept: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wantLen := 0
			if tc.wantKept {
				wantLen = 1
			}

			if got := len(FilterFactsByAgent([]FactItem{{ID: 1, Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterFactsByAgent len = %d, want %d", got, wantLen)
			}
			if got := len(FilterCronsByAgent([]CronSchedule{{ID: "c", Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterCronsByAgent len = %d, want %d", got, wantLen)
			}
			if got := len(FilterOneShotsByAgent([]OneShotSchedule{{ID: "o", Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterOneShotsByAgent len = %d, want %d", got, wantLen)
			}
			if got := len(FilterScheduleRunsByAgent([]ScheduleRun{{ID: "r", Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterScheduleRunsByAgent len = %d, want %d", got, wantLen)
			}
			if got := len(FilterBuildsByAgent([]VanadisBuildDTO{{ID: "b", Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterBuildsByAgent len = %d, want %d", got, wantLen)
			}
			if got := len(FilterActiveTasksByAgent([]ActiveTaskStatus{{ID: "t", Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterActiveTasksByAgent len = %d, want %d", got, wantLen)
			}
		})
	}
}

func TestFactItemUnmarshalJSON_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		rawJSON   string
		wantAgent string
		wantText  string
	}{
		{name: "agent field present", rawJSON: `{"id":1,"fact_text":"dark mode","agent":"agent-alpha"}`, wantAgent: "agent-alpha", wantText: "dark mode"},
		{name: "source_agent fallback", rawJSON: `{"id":2,"fact_text":"voice setting","source_agent":"agent-beta"}`, wantAgent: "agent-beta", wantText: "voice setting"},
		{name: "agent wins", rawJSON: `{"id":3,"fact_text":"both","agent":"agent-alpha","source_agent":"agent-beta"}`, wantAgent: "agent-alpha", wantText: "both"},
		{name: "neither present leaves agent empty", rawJSON: `{"id":4,"fact_text":"neither"}`, wantText: "neither"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var item FactItem
			if err := json.Unmarshal([]byte(tc.rawJSON), &item); err != nil {
				t.Fatalf("json.Unmarshal error = %v", err)
			}
			if item.Agent != tc.wantAgent {
				t.Errorf("item.Agent = %q, want %q", item.Agent, tc.wantAgent)
			}
			if item.FactText != tc.wantText {
				t.Errorf("item.FactText = %q, want %q", item.FactText, tc.wantText)
			}
		})
	}
}

func TestCachedDiscoveredAgents_TableDriven(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		initial   []string
		queryTime time.Time
		wantHit   bool
		wantCount int
	}{
		{name: "empty cache misses", queryTime: now},
		{name: "fresh cache hits", initial: []string{"aerial", "agent-alpha"}, queryTime: now.Add(10 * time.Second), wantHit: true, wantCount: 2},
		{name: "expired cache misses", initial: []string{"aerial", "agent-alpha"}, queryTime: now.Add(31 * time.Second)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := &cachedDiscoveredAgents{ttl: 30 * time.Second}
			if len(tc.initial) > 0 {
				c.set(tc.initial, now)
			}
			got, ok := c.get(tc.queryTime)
			if ok != tc.wantHit {
				t.Fatalf("c.get() ok = %v, want %v", ok, tc.wantHit)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("c.get() len = %d, want %d", len(got), tc.wantCount)
			}
		})
	}
}

func (c *cachedDiscoveredAgents) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.agents = nil
	c.fetchedAt = time.Time{}
}

func TestFetchUpstreamDiscoveredAgents_TableDriven(t *testing.T) {
	t.Parallel()
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schedules":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
				Crons:    []CronSchedule{{ID: "c1", Agent: "agent-haro"}},
				OneShots: []OneShotSchedule{{ID: "o1", Agent: "agent-oneshot"}},
			})
		case "/builds":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(BuildsAPIResponse{
				Builds: []VanadisBuildDTO{{ID: "b1", Agent: "agent-vanadis"}},
			})
		case "/error/schedules", "/error/builds":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(mockServer.Close)

	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		haroURL    string
		vanadisURL string
		wantCount  int
		wantAgents []string
	}{
		{name: "empty urls return empty"},
		{name: "haro only discovers schedule agents", haroURL: mockServer.URL, wantCount: 2, wantAgents: []string{"agent-haro", "agent-oneshot"}},
		{name: "vanadis only discovers build agents", vanadisURL: mockServer.URL, wantCount: 1, wantAgents: []string{"agent-vanadis"}},
		{
			name:       "both upstream urls query concurrently",
			haroURL:    mockServer.URL,
			vanadisURL: mockServer.URL,
			wantCount:  3,
			wantAgents: []string{"agent-haro", "agent-oneshot", "agent-vanadis"},
		},
		{name: "upstream errors return empty gracefully", haroURL: mockServer.URL + "/error", vanadisURL: mockServer.URL + "/error"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cache := &cachedDiscoveredAgents{ttl: 30 * time.Second}
			got := fetchUpstreamDiscoveredAgentsWithCache(context.Background(), cache, tc.haroURL, tc.vanadisURL, now)
			if len(got) != tc.wantCount {
				t.Fatalf("fetchUpstreamDiscoveredAgentsWithCache() count = %d, want %d: %v", len(got), tc.wantCount, got)
			}
			for _, want := range tc.wantAgents {
				if !slices.Contains(got, want) {
					t.Errorf("expected agent %q in results %v", want, got)
				}
			}

			if tc.wantCount > 0 {
				cachedGot := fetchUpstreamDiscoveredAgentsWithCache(context.Background(), cache, "http://invalid-server-that-should-not-be-called:9999", "http://invalid-server-that-should-not-be-called:9999", now.Add(5*time.Second))
				if len(cachedGot) != tc.wantCount {
					t.Fatalf("cached call count = %d, want %d", len(cachedGot), tc.wantCount)
				}
			}
		})
	}
}

func TestDiscoverConfiguredAgents_TableDriven(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	discoveredAgentsCache.reset()
	tests := []struct {
		name       string
		tasks      []ActiveTaskStatus
		haroURL    string
		vanadisURL string
		want       []string
	}{
		{
			name:  "tasks with agents discovered",
			tasks: []ActiveTaskStatus{{ID: "t1", Agent: "agent-alpha"}, {ID: "t2", Agent: "agent-beta"}},
			want:  []string{"aerial", "agent-alpha", "agent-beta"},
		},
		{
			name:  "empty tasks discovers aerial",
			tasks: nil,
			want:  []string{"aerial"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := discoverConfiguredAgents(context.Background(), tc.tasks, tc.haroURL, tc.vanadisURL, now)
			if len(got) != len(tc.want) {
				t.Fatalf("discoverConfiguredAgents() len = %d, want %d: %v", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("discoverConfiguredAgents()[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestAgentBrainURL_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		defaultURL   string
		defaultAgent string
		agent        string
		want         string
	}{
		{name: "default agent keeps configured host", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agent: "aerial", want: "http://eri.aerial"},
		{name: "default agent match is normalized", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agent: " Aerial ", want: "http://eri.aerial"},
		{name: "sibling replaces first label", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agent: "agent-alpha", want: "http://eri-agent-alpha.aerial"},
		{name: "sibling agent normalized", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agent: "  Agent-Beta! ", want: "http://eri-agent-beta.aerial"},
		{name: "port and scheme preserved", defaultURL: "https://eri.default.svc.cluster.local:8080", defaultAgent: "aerial", agent: "agent-alpha", want: "https://eri-agent-alpha.default.svc.cluster.local:8080"},
		{name: "bare host with port", defaultURL: "http://eri:80", defaultAgent: "aerial", agent: "agent-alpha", want: "http://eri-agent-alpha:80"},
		{name: "default stack not named aerial", defaultURL: "http://eri.aerial", defaultAgent: "agent-alpha", agent: "aerial", want: "http://eri-aerial.aerial"},
		{name: "first label not eri is underivable", defaultURL: "http://brain.aerial", defaultAgent: "aerial", agent: "agent-alpha", want: ""},
		{name: "eri only in later label is underivable", defaultURL: "http://x.eri.aerial", defaultAgent: "aerial", agent: "agent-alpha", want: ""},
		{name: "empty configured url", defaultURL: "", defaultAgent: "aerial", agent: "agent-alpha", want: ""},
		{name: "empty agent", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agent: "  ", want: ""},
		{name: "unparseable url", defaultURL: "http://eri.aerial:bad port", defaultAgent: "aerial", agent: "agent-alpha", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := AgentBrainURL(tc.defaultURL, tc.defaultAgent, tc.agent); got != tc.want {
				t.Errorf("AgentBrainURL(%q, %q, %q) = %q, want %q", tc.defaultURL, tc.defaultAgent, tc.agent, got, tc.want)
			}
		})
	}
}

func TestSiblingBrainTargets_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		defaultURL   string
		defaultAgent string
		agents       []string
		want         []AgentBrainTarget
	}{
		{
			name: "default excluded, duplicates collapsed", defaultURL: "http://eri.aerial", defaultAgent: "aerial",
			agents: []string{"aerial", "agent-alpha", "Agent-Alpha", "agent-beta", ""},
			want:   []AgentBrainTarget{{Agent: "agent-alpha", URL: "http://eri-agent-alpha.aerial"}, {Agent: "agent-beta", URL: "http://eri-agent-beta.aerial"}},
		},
		{name: "underivable host yields none", defaultURL: "http://127.0.0.1:9999", defaultAgent: "aerial", agents: []string{"agent-alpha"}, want: []AgentBrainTarget{}},
		{name: "no agents", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agents: nil, want: []AgentBrainTarget{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := SiblingBrainTargets(tc.defaultURL, tc.defaultAgent, tc.agents); !slices.Equal(got, tc.want) {
				t.Errorf("SiblingBrainTargets() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestDefaultAgentFromTasks_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		tasks []ActiveTaskStatus
		want  string
	}{
		{name: "no tasks falls back to default tenant", tasks: nil, want: "aerial"},
		{name: "rows without agent fall back", tasks: []ActiveTaskStatus{{ID: "1"}}, want: "aerial"},
		{name: "first reported agent wins", tasks: []ActiveTaskStatus{{ID: "1"}, {ID: "2", Agent: " Agent-Alpha "}}, want: "agent-alpha"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := DefaultAgentFromTasks(tc.tasks); got != tc.want {
				t.Errorf("DefaultAgentFromTasks() = %q, want %q", got, tc.want)
			}
		})
	}
}

func newEriTasksServer(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tasks" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func downServerURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	u := srv.URL
	srv.Close()
	return u
}

func TestFetchActiveTasksFromAgents_TableDriven(t *testing.T) {
	t.Parallel()
	healthy := newEriTasksServer(t, `{"status":"ok","total":2,"tasks":[{"id":"m1","status":"PROCESSING"},{"id":"m2","status":"PENDING","agent":"agent-alpha"}]}`)
	other := newEriTasksServer(t, `{"status":"ok","total":1,"tasks":[{"id":"s1","status":"PROCESSING"}]}`)
	down := downServerURL(t)
	broken := newEriTasksServer(t, `not json`)

	tests := []struct {
		name      string
		targets   []AgentBrainTarget
		wantIDs   []string
		wantAgent []string
	}{
		{
			name:      "healthy rows stamped, down agent dropped",
			targets:   []AgentBrainTarget{{Agent: "agent-alpha", URL: healthy}, {Agent: "agent-beta", URL: down}},
			wantIDs:   []string{"m1", "m2"},
			wantAgent: []string{"agent-alpha", "agent-alpha"},
		},
		{
			name:      "merge preserves target order",
			targets:   []AgentBrainTarget{{Agent: "agent-beta", URL: other}, {Agent: "agent-alpha", URL: healthy}},
			wantIDs:   []string{"s1", "m1", "m2"},
			wantAgent: []string{"agent-beta", "agent-alpha", "agent-alpha"},
		},
		{
			name:      "undecodable body dropped",
			targets:   []AgentBrainTarget{{Agent: "agent-beta", URL: broken}, {Agent: "agent-alpha", URL: healthy}},
			wantIDs:   []string{"m1", "m2"},
			wantAgent: []string{"agent-alpha", "agent-alpha"},
		},
		{name: "all down yields empty list", targets: []AgentBrainTarget{{Agent: "agent-beta", URL: down}}, wantIDs: []string{}, wantAgent: []string{}},
		{name: "no targets", targets: nil, wantIDs: []string{}, wantAgent: []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := fetchActiveTasksFromAgents(context.Background(), tc.targets)
			if got == nil {
				t.Fatal("expected non-nil slice")
			}
			ids := make([]string, 0, len(got))
			agents := make([]string, 0, len(got))
			for _, task := range got {
				ids = append(ids, task.ID)
				agents = append(agents, task.Agent)
			}
			if !slices.Equal(ids, tc.wantIDs) || !slices.Equal(agents, tc.wantAgent) {
				t.Errorf("got ids=%v agents=%v, want ids=%v agents=%v", ids, agents, tc.wantIDs, tc.wantAgent)
			}
		})
	}
}

func TestStampTaskAgent_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		tasks []ActiveTaskStatus
		agent string
		want  []ActiveTaskStatus
	}{
		{name: "nil yields empty", tasks: nil, agent: "aerial", want: []ActiveTaskStatus{}},
		{name: "fills empty, keeps reported", tasks: []ActiveTaskStatus{{ID: "1"}, {ID: "2", Agent: "agent-alpha"}}, agent: "aerial", want: []ActiveTaskStatus{{ID: "1", Agent: "aerial"}, {ID: "2", Agent: "agent-alpha"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := StampTaskAgent(tc.tasks, tc.agent)
			if got == nil || !slices.Equal(got, tc.want) {
				t.Errorf("StampTaskAgent() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
