package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/prospera/internal/fakes"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/store"
)

func TestSetupProsperaBus(t *testing.T) {
	tests := []struct {
		name    string
		natsURL string
		busMode string
		wantNil bool
	}{
		{
			name:    "empty url returns nil",
			natsURL: "   ",
			wantNil: true,
		},
		{
			name:    "bus factory error returns nil",
			natsURL: "nats://127.0.0.1:4222",
			busMode: "dial_err",
			wantNil: true,
		},
		{
			name:    "successful bus sets event bus and consumers",
			natsURL: "nats://127.0.0.1:4222",
			busMode: "memory",
			wantNil: false,
		},
		{
			name:    "consumer start failure logs and returns bus",
			natsURL: "nats://127.0.0.1:4222",
			busMode: "fail_consumer",
			wantNil: false,
		},
		{
			name:    "ensure streams failure logs and returns bus",
			natsURL: "nats://127.0.0.1:4222",
			busMode: "fail_streams",
			wantNil: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origBusFn := newProsperaBusFn
			defer func() { newProsperaBusFn = origBusFn }()
			switch tc.busMode {
			case "":
				// keep default newProsperaBusFn
			case "dial_err":
				newProsperaBusFn = func(context.Context, string) (pubsub.Bus, error) {
					return nil, errors.New("connection failed")
				}
			case "memory":
				newProsperaBusFn = func(context.Context, string) (pubsub.Bus, error) {
					return pubsub.NewMemoryBus(), nil
				}
			case "fail_consumer":
				newProsperaBusFn = func(context.Context, string) (pubsub.Bus, error) {
					return &pubsubtest.FaultBus{Bus: pubsub.NewMemoryBus(), ConsumeErr: errors.New("consume queue failed")}, nil
				}
			case "fail_streams":
				newProsperaBusFn = func(context.Context, string) (pubsub.Bus, error) {
					return &pubsubtest.FaultBus{Bus: pubsub.NewMemoryBus(), EnsureErr: errors.New("ensure streams failed")}, nil
				}
			default:
				t.Fatalf("unknown busMode %q", tc.busMode)
			}

			server, _, _ := setupTestServer()
			bus := setupProsperaBus(context.Background(), server, tc.natsURL)
			if (bus == nil) != tc.wantNil {
				t.Fatalf("setupProsperaBus() nil = %v, wantNil = %v", bus == nil, tc.wantNil)
			}
			if !tc.wantNil {
				if server.EventBus() != bus {
					t.Fatalf("server.EventBus() = %v, want %v", server.EventBus(), bus)
				}
			}
		})
	}
}

func TestRegisterRestateDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		adminURL     string
		advertiseURL string
		statusCode   int
		expectBody   string
		closeServer  bool
		nilClient    bool
		wantErr      bool
	}{
		{
			name:         "empty admin url is skipped",
			adminURL:     "",
			advertiseURL: "http://prospera:9080",
			wantErr:      false,
		},
		{
			name:         "empty advertise url is skipped",
			adminURL:     "http://restate:9070",
			advertiseURL: "",
			wantErr:      false,
		},
		{
			name:       "successful registration",
			statusCode: http.StatusOK,
			expectBody: `{"uri":"http://prospera.default.svc.cluster.local:9080","force":true}`,
			wantErr:    false,
		},
		{
			name:       "successful registration with nil client defaults",
			statusCode: http.StatusOK,
			expectBody: `{"uri":"http://prospera.default.svc.cluster.local:9080","force":true}`,
			nilClient:  true,
			wantErr:    false,
		},
		{
			name:       "admin api error returns error",
			statusCode: http.StatusInternalServerError,
			wantErr:    true,
		},
		{
			name:        "network error returns error",
			closeServer: true,
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var receivedBody string
			var receivedPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedPath = r.URL.Path
				b, _ := io.ReadAll(r.Body)
				receivedBody = string(b)
				w.WriteHeader(tc.statusCode)
			}))
			defer srv.Close()

			adminURL := tc.adminURL
			if adminURL == "" && tc.name != "empty admin url is skipped" {
				adminURL = srv.URL
			}
			advURL := tc.advertiseURL
			if advURL == "" && tc.name != "empty advertise url is skipped" {
				advURL = "http://prospera.default.svc.cluster.local:9080"
			}

			var client *http.Client
			if !tc.nilClient {
				client = srv.Client()
			}
			if tc.closeServer {
				srv.Close()
			}

			err := RegisterRestateDeployment(context.Background(), client, adminURL, advURL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RegisterRestateDeployment() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.expectBody != "" {
				if receivedPath != "/deployments" {
					t.Errorf("path = %q, want /deployments", receivedPath)
				}
				if !strings.Contains(receivedBody, `"force":true`) {
					t.Errorf("body missing force=true: %q", receivedBody)
				}
			}
		})
	}
}

type sweepTestCase struct {
	name       string
	ingressURL string
	nilStore   bool
	nilClient  bool
	storeErr   error
	calls      []model.VoiceCall
	postStatus int
	wantErr    bool
	wantCount  int
}

func runSweepUnfinishedReportsTest(t *testing.T, tc sweepTestCase) {
	t.Helper()
	var postCount int
	var receivedPaths []string
	var receivedKeys []string
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		postCount++
		receivedPaths = append(receivedPaths, r.URL.Path)
		receivedKeys = append(receivedKeys, r.Header.Get("Idempotency-Key"))
		mu.Unlock()
		w.WriteHeader(tc.postStatus)
	}))
	defer srv.Close()

	ingressURL := tc.ingressURL
	if ingressURL == "test-server" {
		ingressURL = srv.URL
	}

	var st *fakes.FakeStore
	if !tc.nilStore {
		st = fakes.NewFakeStore()
		st.OpErr = tc.storeErr
		for _, c := range tc.calls {
			_ = st.CreateOutboundCall(context.Background(), c, 5)
		}
	}

	var storeArg store.Store
	if st != nil {
		storeArg = st
	}

	var client *http.Client
	if !tc.nilClient {
		client = srv.Client()
	}

	err := SweepUnfinishedReports(context.Background(), client, storeArg, ingressURL)
	if (err != nil) != tc.wantErr {
		t.Fatalf("SweepUnfinishedReports() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantCount > 0 {
		mu.Lock()
		defer mu.Unlock()
		if postCount != tc.wantCount {
			t.Fatalf("postCount = %d, want %d", postCount, tc.wantCount)
		}
		for _, p := range receivedPaths {
			if !strings.HasPrefix(p, "/ProsperaReport/") || !strings.HasSuffix(p, "/DeliverReport/send") {
				t.Errorf("unexpected path: %s", p)
			}
		}
		for _, k := range receivedKeys {
			if !strings.HasPrefix(k, "prospera:report:") {
				t.Errorf("unexpected idempotency key: %s", k)
			}
		}
	}
}

func TestSweepUnfinishedReports(t *testing.T) {
	t.Parallel()

	tests := []sweepTestCase{
		{
			name:       "empty ingress url is skipped",
			ingressURL: "",
			wantErr:    false,
		},
		{
			name:       "nil store is skipped",
			ingressURL: "http://restate:8080",
			nilStore:   true,
			wantErr:    false,
		},
		{
			name:       "store error returns error",
			ingressURL: "http://restate:8080",
			storeErr:   errors.New("db error"),
			wantErr:    true,
		},
		{
			name:       "successful sweep enqueues pending calls",
			ingressURL: "test-server",
			calls: []model.VoiceCall{
				{CallID: "call-sweep-1", ReportStatus: "pending"},
				{CallID: "call-sweep-2", ReportStatus: "processing"},
			},
			postStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "nil client defaults to http.DefaultClient",
			ingressURL: "test-server",
			nilClient:  true,
			calls: []model.VoiceCall{
				{CallID: "call-sweep-nilclient", ReportStatus: "pending"},
			},
			postStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "post failure logs and returns joined error",
			ingressURL: "test-server",
			calls: []model.VoiceCall{
				{CallID: "call-sweep-fail", ReportStatus: "pending"},
			},
			postStatus: http.StatusInternalServerError,
			wantErr:    true,
			wantCount:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSweepUnfinishedReportsTest(t, tc)
		})
	}
}
