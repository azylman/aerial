package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newMockDeploymentServer(statusCode int, body string, expectedAdvertiseURL string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/deployments" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req["force"] != true || req["uri"] != expectedAdvertiseURL {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		status := statusCode
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		if body != "" {
			_, _ = w.Write([]byte(body))
		}
	}))
}

func setupAdminTestEndpoint(unreachable, skipServer bool, statusCode int, responseBody, advertiseURL string) (string, *http.Client, func()) {
	if unreachable {
		return "http://127.0.0.1:1", nil, func() {}
	}
	if skipServer {
		return "", nil, func() {}
	}
	srv := newMockDeploymentServer(statusCode, responseBody, advertiseURL)
	return srv.URL, srv.Client(), srv.Close
}

func TestRegisterRestateDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		statusCode   int
		responseBody string
		advertiseURL string
		unreachable  bool
		skipServer   bool
		wantErr      bool
		wantSubstr   string
	}{
		{
			name:         "empty admin url returns nil",
			advertiseURL: "http://coven:9080",
			skipServer:   true,
			wantErr:      false,
		},
		{
			name:         "empty advertise url returns nil",
			statusCode:   http.StatusOK,
			advertiseURL: "",
			wantErr:      false,
		},
		{
			name:         "successful registration",
			statusCode:   http.StatusOK,
			responseBody: `{"id":"dep-123"}`,
			advertiseURL: "http://coven.aerial:9080",
			wantErr:      false,
		},
		{
			name:         "server error with bounded error body",
			statusCode:   http.StatusInternalServerError,
			responseBody: "deployment failed: internal error",
			advertiseURL: "http://coven.aerial:9080",
			wantErr:      true,
			wantSubstr:   "restate admin /deployments returned 500: deployment failed",
		},
		{
			name:         "server error with json envelope",
			statusCode:   http.StatusForbidden,
			responseBody: `{"message":"unauthorized"}`,
			advertiseURL: "http://coven.aerial:9080",
			wantErr:      true,
			wantSubstr:   "restate admin /deployments returned 403: unauthorized",
		},
		{
			name:         "connection error returns formatted error",
			advertiseURL: "http://coven.aerial:9080",
			unreachable:  true,
			wantErr:      true,
			wantSubstr:   "register coven restate deployment:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			adminURL, client, cleanup := setupAdminTestEndpoint(tc.unreachable, tc.skipServer, tc.statusCode, tc.responseBody, tc.advertiseURL)
			defer cleanup()

			err := registerRestateDeployment(context.Background(), client, adminURL, tc.advertiseURL)
			if tc.wantErr {
				require.Error(t, err)
				if tc.wantSubstr != "" {
					require.Contains(t, err.Error(), tc.wantSubstr)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestWorker_ArmDreamer_RegistersAndArms(t *testing.T) {
	t.Parallel()

	var deploymentRegistered atomic.Bool
	var dreamerArmed atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/deployments":
			deploymentRegistered.Store(true)
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{}`)); err != nil {
				return
			}
		case r.Method == http.MethodPost && r.URL.Path == "/CovenDreamer/aerial/Arm":
			if !deploymentRegistered.Load() {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			dreamerArmed.Store(true)
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{}`)); err != nil {
				return
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	w := NewWorker(WorkerConfig{
		Stack:               "aerial",
		RestateIngressURL:   server.URL,
		RestateAdminURL:     server.URL,
		RestateAdvertiseURL: "http://coven.aerial:9080",
		HTTPClient:          server.Client(),
		ArmRetryInterval:    5 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := w.armDreamer(ctx)
	require.NoError(t, err)
	require.True(t, deploymentRegistered.Load(), "deployment should have been registered")
	require.True(t, dreamerArmed.Load(), "dreamer should have been armed")
}

func TestWorker_ArmDreamer_RegistrationRetry(t *testing.T) {
	t.Parallel()

	var registrationAttempts atomic.Int32
	var dreamerArmed atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/deployments":
			if registrationAttempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{}`)); err != nil {
				return
			}
		case r.Method == http.MethodPost && r.URL.Path == "/CovenDreamer/aerial/Arm":
			dreamerArmed.Store(true)
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{}`)); err != nil {
				return
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	w := NewWorker(WorkerConfig{
		Stack:               "aerial",
		RestateIngressURL:   server.URL,
		RestateAdminURL:     server.URL,
		RestateAdvertiseURL: "http://coven.aerial:9080",
		HTTPClient:          server.Client(),
		ArmRetryInterval:    5 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := w.armDreamer(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, registrationAttempts.Load(), int32(2))
	require.True(t, dreamerArmed.Load())
}

func TestWorker_ArmDreamer_RegistrationContextCancel(t *testing.T) {
	t.Parallel()

	canceled := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cancel()
		select {
		case <-canceled:
		default:
			close(canceled)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	w := NewWorker(WorkerConfig{
		Stack:               "aerial",
		RestateIngressURL:   server.URL,
		RestateAdminURL:     server.URL,
		RestateAdvertiseURL: "http://coven.aerial:9080",
		HTTPClient:          server.Client(),
		ArmRetryInterval:    50 * time.Millisecond,
	})

	err := w.armDreamer(ctx)
	require.ErrorIs(t, err, context.Canceled)
	<-canceled
}

func TestWorker_ArmDreamer_PostContextCancel(t *testing.T) {
	t.Parallel()

	canceled := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/deployments":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/CovenDreamer/aerial/Arm":
			cancel()
			select {
			case <-canceled:
			default:
				close(canceled)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	w := NewWorker(WorkerConfig{
		Stack:               "aerial",
		RestateIngressURL:   server.URL,
		RestateAdminURL:     server.URL,
		RestateAdvertiseURL: "http://coven.aerial:9080",
		HTTPClient:          server.Client(),
		ArmRetryInterval:    50 * time.Millisecond,
	})

	err := w.armDreamer(ctx)
	require.ErrorIs(t, err, context.Canceled)
	<-canceled
}
