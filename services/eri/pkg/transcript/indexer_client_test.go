package transcript

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestIndexerClientWatermarkDisabled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		baseURL string
	}{
		{name: "empty base url disabled", baseURL: ""},
		{name: "whitespace base url disabled", baseURL: "   "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewIndexerClient(tc.baseURL, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if got, err := c.Watermark(ctx, "aerial", "s1"); err != nil || got != -1 {
				t.Errorf("Watermark disabled = %d, %v; want -1, nil", got, err)
			}
			wms, err := c.Watermarks(ctx, "aerial")
			if err != nil || len(wms) != 0 {
				t.Errorf("Watermarks disabled = %v, %v; want empty map, nil", wms, err)
			}
		})
	}
}

func TestIndexerClientWatermarkRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		status  int
		body    string
		stack   string
		session string
		want    int
		wantErr string
	}{
		{name: "indexed step returned", status: http.StatusOK, body: `{"last_indexed_step":7}`, stack: "aerial", session: "s1", want: 7},
		{name: "zero step", status: http.StatusOK, body: `{"last_indexed_step":0}`, stack: "aerial", session: "s1", want: 0},
		{name: "server error surfaces", status: http.StatusInternalServerError, body: `boom`, stack: "aerial", session: "s1", wantErr: "indexer watermark status 500"},
		{name: "bad json surfaces", status: http.StatusOK, body: `{bad`, stack: "aerial", session: "s1", wantErr: "decode indexer watermark"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotStack, gotSession string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotStack = r.URL.Query().Get("stack")
				gotSession = r.URL.Query().Get("session_id")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c := NewIndexerClient(srv.URL, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := c.Watermark(ctx, tc.stack, tc.session)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotStack != tc.stack {
				t.Errorf("stack query = %q, want %q", gotStack, tc.stack)
			}
			if gotSession != tc.session {
				t.Errorf("session_id query = %q, want %q", gotSession, tc.session)
			}
			if got != tc.want {
				t.Errorf("Watermark = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestIndexerClientWatermarksRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     int
		body       string
		stack      string
		wantMap    map[string]int
		wantErrSub string
	}{
		{
			name:    "sessions and watermarks combined",
			status:  http.StatusOK,
			body:    `{"watermarks":[{"session_id":"s1","last_indexed_step":3},{"session_id":"s2","last_indexed_step":5}],"sessions":{"s3":10}}`,
			stack:   "aerial",
			wantMap: map[string]int{"s1": 3, "s2": 5, "s3": 10},
		},
		{
			name:    "negative steps omitted",
			status:  http.StatusOK,
			body:    `{"watermarks":[{"session_id":"s1","last_indexed_step":-1},{"session_id":"s2","last_indexed_step":4}]}`,
			stack:   "miorine",
			wantMap: map[string]int{"s2": 4},
		},
		{
			name:       "server error surfaces",
			status:     http.StatusInternalServerError,
			body:       `down`,
			stack:      "aerial",
			wantErrSub: "indexer watermarks status 500",
		},
		{
			name:       "bad json surfaces",
			status:     http.StatusOK,
			body:       `{bad`,
			stack:      "aerial",
			wantErrSub: "decode indexer watermarks",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if qStack := r.URL.Query().Get("stack"); qStack != tc.stack {
					t.Errorf("query stack = %q, want %q", qStack, tc.stack)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c := NewIndexerClient(srv.URL, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := c.Watermarks(ctx, tc.stack)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("Watermarks() err = %v, want substring %q", err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("Watermarks() unexpected err = %v", err)
			}
			if len(got) != len(tc.wantMap) {
				t.Fatalf("Watermarks() len = %d, want %d: %v", len(got), len(tc.wantMap), got)
			}
			for k, wantV := range tc.wantMap {
				if got[k] != wantV {
					t.Errorf("Watermarks()[%q] = %d, want %d", k, got[k], wantV)
				}
			}
		})
	}
}
