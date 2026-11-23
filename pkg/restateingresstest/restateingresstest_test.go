package restateingresstest_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/restateingresstest"
)

func TestServer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		routes     map[string]restateingresstest.Response
		path       string
		key        string
		body       string
		wantStatus int
		wantBody   string
		wantKeySet bool
	}{
		{name: "unknown path defaults to 200", path: "/Svc/k/Run", body: `{"a":1}`, wantStatus: 200},
		{name: "zero status route is 200", routes: map[string]restateingresstest.Response{"/Svc/k/Run": {Body: "ok"}}, path: "/Svc/k/Run", wantStatus: 200, wantBody: "ok"},
		{name: "route status and key", routes: map[string]restateingresstest.Response{"/Svc/k/Run": {Status: 409, Body: `{"message":"busy"}`}}, path: "/Svc/k/Run", key: "idem-1", body: "x", wantStatus: 409, wantBody: `{"message":"busy"}`, wantKeySet: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := restateingresstest.New(t)
			srv.Routes = tc.routes
			status, got := postIngress(t, srv, tc.path, tc.key, tc.body)
			if status != tc.wantStatus || got != tc.wantBody {
				t.Errorf("response = %d %q, want %d %q", status, got, tc.wantStatus, tc.wantBody)
			}
			if err := srv.Err(); err != nil {
				t.Errorf("Err() = %v, want nil", err)
			}
			calls := srv.Calls()
			if len(calls) != 1 {
				t.Fatalf("Calls() len = %d, want 1", len(calls))
			}
			c := calls[0]
			_, keySet := c.Header["Idempotency-Key"]
			if c.Method != http.MethodPost || c.Path != tc.path || c.IdempotencyKey != tc.key || c.ContentType != "application/json" || string(c.Body) != tc.body || keySet != tc.wantKeySet {
				t.Errorf("call = %+v (key set %v), want POST %s key %q body %q (key set %v)", c, keySet, tc.path, tc.key, tc.body, tc.wantKeySet)
			}
		})
	}
}

// postIngress POSTs body to path (plus a query string the fake must ignore)
// and returns the response status and body.
func postIngress(t *testing.T, srv *restateingresstest.Server, path, key, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL()+path+"?q=1", strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return resp.StatusCode, string(got)
}
