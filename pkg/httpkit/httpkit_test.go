package httpkit

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
)

type mockCloser struct {
	err error
}

func (m *mockCloser) Close() error {
	return m.err
}

func TestIsClientDisconnect_TableDriven(t *testing.T) {
	t.Parallel()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	reqWithCanceledCtx := httptest.NewRequest(http.MethodGet, "/", http.NoBody).WithContext(canceledCtx)
	reqNormal := httptest.NewRequest(http.MethodGet, "/", http.NoBody)

	tests := []struct {
		name     string
		req      *http.Request
		err      error
		expected bool
	}{
		{"nil error", reqNormal, nil, false},
		{"canceled request context", reqWithCanceledCtx, errors.New("arbitrary error"), true},
		{"broken pipe syscall EPIPE", reqNormal, syscall.EPIPE, true},
		{"connection reset syscall ECONNRESET", reqNormal, syscall.ECONNRESET, true},
		{"net ErrClosed", reqNormal, net.ErrClosed, true},
		{"generic unrelated error", reqNormal, errors.New("unrelated disk error"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := IsClientDisconnect(tc.req, tc.err)
			if got != tc.expected {
				t.Errorf("IsClientDisconnect() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestWriteResponse_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		payload  []byte
		wantBody string
	}{
		{name: "payload written verbatim", payload: []byte("payload"), wantBody: "payload"},
		{name: "empty payload writes empty body", payload: []byte{}, wantBody: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			rec := httptest.NewRecorder()

			WriteResponse(rec, req, "test-svc", tc.payload)
			if rec.Code != http.StatusOK {
				t.Errorf("WriteResponse code = %d, want %d", rec.Code, http.StatusOK)
			}
			if rec.Body.String() != tc.wantBody {
				t.Errorf("WriteResponse body = %q, want %q", rec.Body.String(), tc.wantBody)
			}
		})
	}
}

func TestWriteJSON_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		status         int
		data           any
		existingCT     string
		wantStatus     int
		wantBodySubstr string
	}{
		{
			name: "valid map with default content-type", status: http.StatusOK, data: map[string]string{"foo": "bar"}, wantStatus: http.StatusOK,
			wantBodySubstr: `{"foo":"bar"}`,
		},
		{
			name: "valid map with custom content-type preserved", status: http.StatusCreated, data: map[string]int{"count": 42},
			existingCT: "application/vnd.api+json", wantStatus: http.StatusCreated, wantBodySubstr: `{"count":42}`,
		},
		{
			name: "unmarshalable type returns 500 internal error", status: http.StatusOK, data: make(chan int), wantStatus: http.StatusInternalServerError,
			wantBodySubstr: "Internal error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			if tc.existingCT != "" {
				rec.Header().Set("Content-Type", tc.existingCT)
			}

			WriteJSON(rec, tc.status, tc.data)
			if rec.Code != tc.wantStatus {
				t.Errorf("WriteJSON code = %d, want %d", rec.Code, tc.wantStatus)
			}
			if !strings.Contains(rec.Body.String(), tc.wantBodySubstr) {
				t.Errorf("WriteJSON body = %q, want substring %q", rec.Body.String(), tc.wantBodySubstr)
			}
		})
	}
}

func TestCloseWarn_TableDriven(t *testing.T) {
	// Not t.Parallel(): CloseWarn logs through the standard logger, which this test redirects.
	var logBuf bytes.Buffer
	origOut, origFlags := log.Writer(), log.Flags()
	log.SetOutput(&logBuf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(origOut)
		log.SetFlags(origFlags)
	})

	tests := []struct {
		name    string
		closer  io.Closer
		wantLog string
	}{
		{"nil closer", nil, ""},
		{"closer without error", &mockCloser{err: nil}, ""},
		{"closer with error", &mockCloser{err: errors.New("close failure")}, "Warning closing test-resource: close failure\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logBuf.Reset()
			CloseWarn(tc.closer, "test-resource")
			if got := logBuf.String(); got != tc.wantLog {
				t.Errorf("CloseWarn log = %q, want %q", got, tc.wantLog)
			}
		})
	}
}

func TestSetupHealthMux(t *testing.T) {
	// Not t.Parallel() on the flush cases: they mutate WriteCountersDirFn/WriteMetaDirFn.
	origCounters := WriteCountersDirFn
	origMeta := WriteMetaDirFn
	t.Cleanup(func() {
		WriteCountersDirFn = origCounters
		WriteMetaDirFn = origMeta
	})
	WriteMetaDirFn = func(string) error { return nil }
	WriteCountersDirFn = func(string) error { return nil }

	stubMetrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("stub-metrics")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	tests := []struct {
		name        string
		metrics     http.Handler
		method      string
		path        string
		wantStatus  int
		wantBody    string
		wantBodySub string
		wantCT      string
	}{
		{name: "GET /health returns ok JSON", metrics: stubMetrics, method: http.MethodGet, path: "/health", wantStatus: http.StatusOK, wantBody: `{"status":"ok"}`},
		{name: "GET /healthz returns ok JSON", metrics: stubMetrics, method: http.MethodGet, path: "/healthz", wantStatus: http.StatusOK, wantBody: `{"status":"ok"}`},
		{
			name: "GET /metrics served when handler non-nil", metrics: stubMetrics, method: http.MethodGet, path: "/metrics", wantStatus: http.StatusOK,
			wantBodySub: "stub-metrics",
		},
		{name: "GET /metrics missing when handler nil", method: http.MethodGet, path: "/metrics", wantStatus: http.StatusNotFound},
		{
			name: "POST /debug/coverage/flush streams tar", metrics: stubMetrics, method: http.MethodPost, path: "/debug/coverage/flush", wantStatus: http.StatusOK,
			wantCT: "application/x-tar",
		},
		{name: "unknown route returns 404", metrics: stubMetrics, method: http.MethodGet, path: "/nope", wantStatus: http.StatusNotFound},
		{name: "POST /health is not registered", metrics: stubMetrics, method: http.MethodPost, path: "/health", wantStatus: http.StatusMethodNotAllowed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mux := SetupHealthMux("test-svc", tc.metrics)
			req := httptest.NewRequest(tc.method, tc.path, http.NoBody)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("SetupHealthMux %s %s status = %d, want %d", tc.method, tc.path, rec.Code, tc.wantStatus)
			}
			if body := strings.TrimSpace(rec.Body.String()); tc.wantBody != "" && body != tc.wantBody {
				t.Errorf("SetupHealthMux %s %s body = %q, want %q", tc.method, tc.path, body, tc.wantBody)
			}
			if !strings.Contains(rec.Body.String(), tc.wantBodySub) {
				t.Errorf("SetupHealthMux %s %s body = %q, want substring %q", tc.method, tc.path, rec.Body.String(), tc.wantBodySub)
			}
			if tc.wantCT != "" && rec.Header().Get("Content-Type") != tc.wantCT {
				t.Errorf("SetupHealthMux %s %s Content-Type = %q, want %q", tc.method, tc.path, rec.Header().Get("Content-Type"), tc.wantCT)
			}
		})
	}
}
