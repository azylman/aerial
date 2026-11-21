package mcpkit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type schemaArgs struct {
	Name  string   `json:"name"`
	Count *int     `json:"count,omitempty"`
	Tags  []string `json:"tags,omitempty"`
}

func TestInputSchema(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		wantRequired []string
		wantTypes    map[string]string
	}{
		{"required string, optional pointer and slice collapse to plain types", []string{"name"}, map[string]string{"name": "string", "count": "integer", "tags": "array"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := InputSchema[schemaArgs]()
			if !slices.Equal(s.Required, tc.wantRequired) {
				t.Errorf("required = %v, want %v", s.Required, tc.wantRequired)
			}
			for prop, want := range tc.wantTypes {
				p := s.Properties[prop]
				if p == nil || p.Type != want || p.Types != nil {
					t.Errorf("property %s = %+v, want plain type %s", prop, p, want)
				}
			}
		})
	}
}

func TestInputSchemaPanicsOnUninferableType(t *testing.T) {
	t.Parallel()
	tests := []struct{ name string }{{"channel type cannot be inferred"}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Error("expected panic")
				}
			}()
			InputSchema[chan int]()
		})
	}
}

type echoArgs struct {
	Text string `json:"text"`
	Fail bool   `json:"fail,omitempty"`
}

func TestAddTextTool(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		args    map[string]any
		wantErr bool
		want    string
	}{
		{"success", map[string]any{"text": "hi"}, false, "echo:hi"},
		{"error result", map[string]any{"text": "bad", "fail": true}, true, "echo:bad"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			srv := mcp.NewServer(&mcp.Implementation{Name: "t", Version: "1"}, nil)
			AddTextTool(srv, &mcp.Tool{Name: "echo", Description: "d", InputSchema: InputSchema[echoArgs]()},
				func(_ context.Context, a echoArgs) (string, bool) { return "echo:" + a.Text, a.Fail })
			ct, st := mcp.NewInMemoryTransports()
			if _, err := srv.Connect(ctx, st, nil); err != nil {
				t.Fatal(err)
			}
			cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "1"}, nil).Connect(ctx, ct, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer cs.Close()
			res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "echo", Arguments: tc.args})
			if err != nil {
				t.Fatal(err)
			}
			if res.IsError != tc.wantErr {
				t.Errorf("IsError = %v, want %v", res.IsError, tc.wantErr)
			}
			tcnt, ok := res.Content[0].(*mcp.TextContent)
			if !ok || tcnt.Text != tc.want {
				t.Errorf("content = %#v, want text %q", res.Content[0], tc.want)
			}
		})
	}
}

func TestHandleHealth(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, method, wantAllow string
		wantStatus              int
	}{
		{"GET ok", http.MethodGet, "", 200},
		{"HEAD ok", http.MethodHead, "", 200},
		{"POST rejected", http.MethodPost, "GET, HEAD", 405},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := httptest.NewRecorder()
			HandleHealth(w, httptest.NewRequest(tc.method, "/health", http.NoBody))
			if w.Code != tc.wantStatus || w.Header().Get("Allow") != tc.wantAllow {
				t.Errorf("got %d allow %q, want %d %q", w.Code, w.Header().Get("Allow"), tc.wantStatus, tc.wantAllow)
			}
		})
	}
}
