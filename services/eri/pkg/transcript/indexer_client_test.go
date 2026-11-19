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
			if got, err := c.Watermark(ctx, "aerial", "s1"); err != nil || got != 0 {
				t.Errorf("Watermark disabled = %d, %v; want 0, nil", got, err)
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
