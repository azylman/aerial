package mcp_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/mcp"
)

func TestNewDispatcher(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	d := mcp.NewDispatcher(nil, srv.Client())
	if d == nil {
		t.Fatal("expected non-nil dispatcher")
	}
}
