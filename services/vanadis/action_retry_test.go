package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type retryIngressFixture struct {
	status    int
	body      string
	request   RetryActionChunkRequest
	key, path string
}

func (f *retryIngressFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.key = r.Header.Get("Idempotency-Key")
	f.path = r.URL.Path
	_ = json.NewDecoder(r.Body).Decode(&f.request)
	status := f.status
	if status == 0 {
		status = 500
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(f.body))
}

func TestRetryActionIntake(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body                string
		ingressStatus, wantStatus int
		ingressBody               string
		buildStatus               string
		generation                int
		prNumber                  int
		noIngress                 bool
		disabled                  bool
	}{
		{name: "accepted", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "completed replay", buildStatus: "completed", generation: 1, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "failed replay", buildStatus: "failed", generation: 1, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "cancelled replay", buildStatus: "cancelled", generation: 1, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "older replay", buildStatus: "failed", generation: 2, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "attached PR retained acceptance", buildStatus: "failed", generation: 1, prNumber: 123, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "attached PR new retry rejected", buildStatus: "failed", generation: 1, prNumber: 123, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-1","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-2","accepted":true}`, wantStatus: 409},
		{name: "missing ingress", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, noIngress: true, wantStatus: 503},
		{name: "disabled ingress", disabled: true, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, noIngress: false, wantStatus: 503},
		{name: "missing identity", body: `{"build_id":"b1","chunk_id":"c1","reason":"retry"}`, wantStatus: 400},
		{name: "wrong build", body: `{"build_id":"b2","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, wantStatus: 400},
		{name: "invalid json", body: `{`, wantStatus: 400},
		{name: "stale", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 409, wantStatus: 409},
		{name: "transient", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 503, wantStatus: 500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &retryIngressFixture{status: tc.ingressStatus, body: tc.ingressBody}
			ts := httptest.NewServer(f)
			defer ts.Close()
			status := tc.buildStatus
			if status == "" {
				status = "failed"
			}
			s := &VanadisServer{store: &retryReadFixture{status: status, generation: tc.generation, prNumber: tc.prNumber}, httpClient: ts.Client(), restateCfg: RestateConfig{Enabled: !tc.disabled}}
			if !tc.noIngress {
				s.restateCfg.RestateIngressURL = ts.URL
			}
			r := httptest.NewRequest(http.MethodPost, "/builds/b1/retry-action", strings.NewReader(tc.body))
			r.SetPathValue("id", "b1")
			w := httptest.NewRecorder()
			s.handleRetryActionChunk(w, r)
			if w.Code != tc.wantStatus {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if tc.wantStatus == 200 {
				if f.path != "/Build/b1/RetryActionChunk" || f.key == "" {
					t.Fatalf("durable path=%s key=%s", f.path, f.key)
				}
				var result RetryActionChunkResult
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || !result.Accepted || result.DispatchID != "vanadis-c1-turn-1" {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			}
		})
	}
}

func TestRetryActionIdempotencyKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		req   RetryActionChunkRequest
		equal bool
	}{
		{"different reason", RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "different"}, true},
		{"different build", RetryActionChunkRequest{BuildID: "b2", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0"}, false},
		{"different chunk", RetryActionChunkRequest{BuildID: "b1", ChunkID: "c2", ExpectedDispatchID: "vanadis-c1-turn-0"}, false},
		{"different generation", RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-1"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := retryActionIdempotencyKey(RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"})
			if (a == retryActionIdempotencyKey(tc.req)) != tc.equal {
				t.Fatal("idempotency identity mismatch")
			}
		})
	}
}

func TestRetryActionHTTPAndMCP(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, path, body, want string }{
		{"http", "/builds/b1/retry-action", `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, `"dispatch_id":"vanadis-c1-turn-1"`},
		{"mcp", "/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"retry_action_chunk","arguments":{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}}}`, `"accepted":true`},
		{"mcp invalid", "/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"retry_action_chunk","arguments":{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"","reason":"retry"}}}`, `"isError":true`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &retryIngressFixture{status: 200, body: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`}
			ts := httptest.NewServer(f)
			defer ts.Close()
			s := &VanadisServer{store: &retryReadFixture{status: "failed"}, httpClient: ts.Client(), restateCfg: RestateConfig{Enabled: true, RestateIngressURL: ts.URL}}
			r := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Accept", "application/json, text/event-stream")
			w := httptest.NewRecorder()
			SetupMux(s).ServeHTTP(w, r)
			if w.Code != 200 || !strings.Contains(w.Body.String(), tc.want) {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

type retryReadFixture struct {
	VanadisStore

	status     string
	generation int
	prNumber   int
}

func (f *retryReadFixture) GetBuild(context.Context, string) (*VanadisBuild, []VanadisChunk, error) {
	return &VanadisBuild{ID: "b1", Status: f.status}, []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: f.status, StallNudges: f.generation, PRNumber: f.prNumber}}, nil
}

func TestRetryPrematureRequest(t *testing.T) {
	t.Parallel()
	for _, initial := range []string{"running", "pending"} {
		t.Run(initial, func(t *testing.T) {
			t.Parallel()
			f := &retryIngressFixture{status: 200, body: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`}
			ts := httptest.NewServer(f)
			defer ts.Close()
			store := &retryReadFixture{status: initial}
			s := &VanadisServer{store: store, httpClient: ts.Client(), restateCfg: RestateConfig{Enabled: true, RestateIngressURL: ts.URL}}
			req := RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"}
			if _, err := s.retryActionChunk(context.Background(), req); !errors.Is(err, ErrBuildConflict) {
				t.Fatalf("premature error=%v", err)
			}
			if f.key != "" {
				t.Fatal("premature request consumed durable key")
			}
			store.status = "failed"
			got, err := s.retryActionChunk(context.Background(), req)
			if err != nil || !got.Accepted {
				t.Fatalf("eligible retry=%+v error=%v", got, err)
			}
		})
	}
}

type retryEligibilityFixture struct {
	VanadisStore

	build  *VanadisBuild
	chunks []VanadisChunk
	err    error
}

func (f retryEligibilityFixture) GetBuild(context.Context, string) (*VanadisBuild, []VanadisChunk, error) {
	return f.build, f.chunks, f.err
}

func TestRetryEligibility(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, buildStatus, chunkStatus, identity string
		generation                               int
		sibling                                  *VanadisChunk
		wantErr                                  bool
	}{
		{name: "uncertain accepted replay", buildStatus: "running", chunkStatus: "running", identity: "vanadis-c1-turn-0", generation: 1},
		{name: "negative identity", buildStatus: "completed", chunkStatus: "completed", identity: "vanadis-c1-turn--1", generation: 1, wantErr: true},
		{name: "noncanonical identity", buildStatus: "completed", chunkStatus: "completed", identity: "vanadis-c1-turn-00", generation: 1, wantErr: true},
		{name: "foreign identity", buildStatus: "completed", chunkStatus: "completed", identity: "vanadis-c2-turn-0", generation: 1, wantErr: true},
		{name: "future identity", buildStatus: "failed", chunkStatus: "failed", identity: "vanadis-c1-turn-1", wantErr: true},
		{name: "cancelled", buildStatus: "cancelled", chunkStatus: "failed", identity: "vanadis-c1-turn-0", wantErr: true},
		{name: "other failure", buildStatus: "failed", chunkStatus: "failed", identity: "vanadis-c1-turn-0", sibling: &VanadisChunk{ID: "c2", Status: "failed"}, wantErr: true},
		{name: "completed descendant", buildStatus: "failed", chunkStatus: "failed", identity: "vanadis-c1-turn-0", sibling: &VanadisChunk{ID: "c2", Status: "completed", DependsOn: []string{"c1"}}, wantErr: true},
		{name: "pending descendant", buildStatus: "failed", chunkStatus: "failed", identity: "vanadis-c1-turn-0", sibling: &VanadisChunk{ID: "c2", Status: "pending", DependsOn: []string{"c1"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunks := []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: tc.chunkStatus, StallNudges: tc.generation}}
			if tc.sibling != nil {
				chunks = append(chunks, *tc.sibling)
			}
			s := &VanadisServer{store: retryEligibilityFixture{build: &VanadisBuild{ID: "b1", Status: tc.buildStatus}, chunks: chunks}}
			err := s.validateRetryEligibility(context.Background(), RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: tc.identity})
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
