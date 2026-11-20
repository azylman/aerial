package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/k8sauth"
)

func TestHandlePRRegister(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfgBotID   string
		useNilReg  bool
		regErr     error
		body       string
		wantStatus int
	}{
		{
			name:       "nil registry returns 500",
			useNilReg:  true,
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "invalid json returns 400",
			body:       `{malformed`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty repo returns 400",
			body:       `{"repo":"","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "zero pr_number returns 400",
			body:       `{"repo":"azylman/aerial","pr_number":0,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty branch returns 400",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty head_sha returns 400",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"","target_id":"100000000000000001"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid target_id returns 400",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"bad"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "headless session key resolves system channel fallback and succeeds",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"dispatch-uuid-1"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid request succeeds",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "registry register error returns 500",
			regErr:     errors.New("db insert fail"),
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "foreign bot_id returns 409 conflict",
			cfgBotID:   "aerial",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","bot_id":"agent-beta"}`,
			wantStatus: http.StatusConflict,
		},
		{
			name:       "matching bot_id succeeds",
			cfgBotID:   "aerial",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","bot_id":"aerial"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "case-insensitive matching bot_id succeeds",
			cfgBotID:   "aerial",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","bot_id":"AERIAL"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty bot_id in request succeeds when config has bot_id",
			cfgBotID:   "aerial",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","bot_id":""}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "bot_id in request succeeds when config has empty bot_id",
			cfgBotID:   "",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","bot_id":"agent-beta"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reg PRRegistryUpdater
			var mockReg *mockPRRegistry
			if !tc.useNilReg {
				mockReg = &mockPRRegistry{registerErr: tc.regErr}
				reg = mockReg
			}
			srv := newTestRouter(Config{SystemChannelID: "100000000000000001", BotID: tc.cfgBotID}, nil, reg, &mockOutboundDispatcher{})
			rec := doHTTPRequest(srv.Routes(), http.MethodPost, "/internal/pr/register", strings.NewReader(tc.body), nil)
			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d; body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus == http.StatusConflict {
				if !strings.Contains(rec.Body.String(), "bot_id mismatch for local pr_registry") {
					t.Errorf("body = %s, want substr 'bot_id mismatch for local pr_registry'", rec.Body.String())
				}
				if mockReg != nil && len(mockReg.registerCalls) > 0 {
					t.Errorf("registerCalls = %d, want 0 when bot_id mismatches", len(mockReg.registerCalls))
				}
			}
		})
	}
}

type sarRule struct {
	Resource string
	Verb     string
	Name     string
}

type staticAuthRoundTripper struct {
	authStatus int
	authBody   string
	sarStatus  int
	sarBody    string
	sarRules   []sarRule
	err        error
}

func (s *staticAuthRoundTripper) matchSARRules(resource, verb, name string) bool {
	for _, r := range s.sarRules {
		if (r.Resource == "" || r.Resource == resource) &&
			(r.Verb == "" || r.Verb == verb) &&
			(r.Name == "" || r.Name == name) {
			return true
		}
	}
	return false
}

func (s *staticAuthRoundTripper) handleSAR(req *http.Request) (int, string) {
	statusCode := s.sarStatus
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	if len(s.sarRules) > 0 {
		var sarReq struct {
			Spec struct {
				ResourceAttributes struct {
					Resource string `json:"resource"`
					Verb     string `json:"verb"`
					Name     string `json:"name"`
				} `json:"resourceAttributes"`
			} `json:"spec"`
		}
		if req.Body != nil {
			_ = json.NewDecoder(req.Body).Decode(&sarReq)
		}
		attrs := sarReq.Spec.ResourceAttributes
		allowed := s.matchSARRules(attrs.Resource, attrs.Verb, attrs.Name)
		return statusCode, fmt.Sprintf(`{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":%v}}`, allowed)
	}
	if s.sarBody != "" {
		return statusCode, s.sarBody
	}
	return statusCode, `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":false}}`
}

func (s *staticAuthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if s.err != nil {
		return nil, s.err
	}
	if strings.Contains(req.URL.Path, "tokenreviews") {
		return &http.Response{
			StatusCode: s.authStatus,
			Body:       io.NopCloser(strings.NewReader(s.authBody)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	}
	if strings.Contains(req.URL.Path, "subjectaccessreviews") {
		statusCode, sarRespBody := s.handleSAR(req)
		return &http.Response{
			StatusCode: statusCode,
			Body:       io.NopCloser(strings.NewReader(sarRespBody)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{}`)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func newTestK8sAuthConfig(t *testing.T, tr *staticAuthRoundTripper) k8sauth.Config {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("host-token"), 0o600); err != nil {
		t.Fatalf("failed to write host token: %v", err)
	}
	return k8sauth.Config{
		Enabled:      true,
		TokenPath:    tokenFile,
		APIServerURL: "https://k8s.local",
		HTTPClient:   &http.Client{Transport: tr},
	}
}

type handlePRRegisterK8sAuthCase struct {
	name       string
	token      string
	authStatus int
	authBody   string
	sarStatus  int
	sarRules   []sarRule
	sarBody    string
	rawMeta    map[string]any
	rawMetaErr error
	cfgBots    map[string]BotConfig
	body       string
	wantStatus int
	wantAgent  string
}

func handlePRRegisterK8sAuthCases() []handlePRRegisterK8sAuthCase {
	return []handlePRRegisterK8sAuthCase{
		{
			name:       "missing bearer token returns 401",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "invalid token returns 401",
			token:      "invalid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false}}`,
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "mismatched bot_id without prs/all returns 403",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "aerial"}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"aerial","bot_id":"agent-alpha"}`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "mismatched bot_id with prs/all succeeds",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarRules:   []sarRule{{Resource: "prs/all", Verb: "create", Name: ""}, {Resource: "prs", Verb: "create", Name: "aerial"}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"aerial","bot_id":"agent-alpha"}`,
			wantStatus: http.StatusOK,
			wantAgent:  "aerial",
		},
		{
			name:       "caller forbidden to create prs returns 403",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-agent-alpha"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "agent-alpha"}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"agent-beta"}`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "caller allowed to create prs for target succeeds",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-agent-alpha"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "agent-alpha"}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"agent-alpha"}`,
			wantStatus: http.StatusOK,
			wantAgent:  "agent-alpha",
		},
		{
			name:       "caller with empty agent defaults to caller agent and succeeds",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-agent-alpha"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "agent-alpha"}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusOK,
			wantAgent:  "agent-alpha",
		},
		{
			name:       "service caller with valid target agent and prs/all succeeds",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:ci-runner"}}}`,
			sarRules:   []sarRule{{Resource: "prs/all", Verb: "create", Name: "agent-alpha"}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"agent-alpha"}`,
			wantStatus: http.StatusOK,
			wantAgent:  "agent-alpha",
		},
		{
			name:       "service caller with missing target agent returns 400",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:ci-runner"}}}`,
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "caller with unknown target agent when bots configured returns 400",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			cfgBots:    map[string]BotConfig{"aerial": {}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"unknown-agent"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "existing PR with different agent update forbidden returns 403",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "aerial"}},
			rawMeta:    map[string]any{"agent": "agent-beta"},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"aerial"}`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "existing PR with update allowed succeeds",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "aerial"}, {Resource: "prs", Verb: "update", Name: "aerial"}},
			rawMeta:    map[string]any{"agent": "aerial"},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"aerial"}`,
			wantStatus: http.StatusOK,
			wantAgent:  "aerial",
		},
		{
			name:       "existing PR metadata error rejects request",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "aerial"}},
			rawMetaErr: errors.New("db load error"),
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"aerial"}`,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "existing legacy PR with empty metadata update allowed succeeds",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "aerial"}, {Resource: "prs", Verb: "update", Name: "aerial"}},
			rawMeta:    map[string]any{},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"aerial"}`,
			wantStatus: http.StatusOK,
			wantAgent:  "aerial",
		},
		{
			name:       "existing legacy PR with empty metadata update forbidden returns 403",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-agent-alpha"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "agent-alpha"}, {Resource: "prs", Verb: "update", Name: "agent-alpha"}},
			rawMeta:    map[string]any{},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"agent-alpha"}`,
			wantStatus: http.StatusForbidden,
		},
	}
}

func TestHandlePRRegister_K8sAuth(t *testing.T) {
	t.Parallel()

	for _, tc := range handlePRRegisterK8sAuthCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tr := &staticAuthRoundTripper{
				authStatus: tc.authStatus,
				authBody:   tc.authBody,
				sarStatus:  tc.sarStatus,
				sarRules:   tc.sarRules,
				sarBody:    tc.sarBody,
			}
			authCfg := newTestK8sAuthConfig(t, tr)
			reg := &mockPRRegistry{rawMetadata: tc.rawMeta, rawMetadataErr: tc.rawMetaErr}
			srv := newTestRouter(Config{
				SystemChannelID: "100000000000000001",
				Bots:            tc.cfgBots,
				K8sAuthEnabled:  true,
				K8sAuthConfig:   authCfg,
			}, nil, reg, &mockOutboundDispatcher{})

			headers := make(map[string]string)
			if tc.token != "" {
				headers["Authorization"] = "Bearer " + tc.token
			}
			rec := doHTTPRequest(srv.Routes(), http.MethodPost, "/internal/pr/register", strings.NewReader(tc.body), headers)
			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d; body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantAgent != "" {
				if !strings.Contains(rec.Body.String(), `"agent":"`+tc.wantAgent+`"`) {
					t.Errorf("body = %s, want agent %q", rec.Body.String(), tc.wantAgent)
				}
			}
		})
	}
}

func TestWriteK8sAuthError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{
			name:       "validation error",
			err:        &k8sauth.ValidationError{Message: "bad"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unauthorized error",
			err:        &k8sauth.UnauthorizedError{Message: "unauth"},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "forbidden error",
			err:        &k8sauth.ForbiddenError{Message: "denied"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "generic error",
			err:        errors.New("generic"),
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			writeK8sAuthError(rec, tc.err)
			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if tc.wantStatus == http.StatusInternalServerError {
				if !strings.Contains(rec.Body.String(), `"error":"internal error"`) {
					t.Errorf("body = %s, want error: internal error", rec.Body.String())
				}
			}
		})
	}
}

func TestIsKnownBot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilCfg   bool
		cfgBotID string
		cfgBots  map[string]BotConfig
		botID    string
		want     bool
	}{
		{name: "nil cfgSrc returns false", nilCfg: true, botID: "aerial", want: false},
		{name: "empty botID returns false", botID: "", want: false},
		{name: "no bots configured returns true for any botID", botID: "anyone", want: true},
		{name: "matching cfg.BotID returns true", cfgBotID: "aerial", botID: "aerial", want: true},
		{name: "mismatched cfg.BotID returns false", cfgBotID: "aerial", botID: "agent-beta", want: false},
		{name: "matching cfg.Bots returns true", cfgBots: map[string]BotConfig{"agent-beta": {}}, botID: "agent-beta", want: true},
		{name: "case-insensitive matching cfg.Bots returns true", cfgBots: map[string]BotConfig{"agent-beta": {}}, botID: "AGENT-BETA", want: true},
		{name: "unknown bot in cfg.Bots returns false", cfgBots: map[string]BotConfig{"agent-beta": {}}, botID: "other", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *RouterServer
			if tc.nilCfg {
				srv = &RouterServer{}
			} else {
				srv = newTestRouter(Config{BotID: tc.cfgBotID, Bots: tc.cfgBots}, nil, nil, nil)
			}
			got := srv.isKnownBot(tc.botID)
			if got != tc.want {
				t.Errorf("isKnownBot(%q) = %v, want %v", tc.botID, got, tc.want)
			}
		})
	}
}

func TestAuthorizeExistingPROwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilReg   bool
		repo     string
		prNum    int
		meta     map[string]any
		sarRules []sarRule
		wantErr  bool
	}{
		{
			name:   "nil registry returns nil",
			nilReg: true,
		},
		{
			name:  "empty repo returns nil",
			repo:  "",
			prNum: 42,
		},
		{
			name:  "zero prNum returns nil",
			repo:  "azylman/aerial",
			prNum: 0,
		},
		{
			name:  "non-existent PR returns nil",
			repo:  "azylman/aerial",
			prNum: 42,
			meta:  nil,
		},
		{
			name:     "empty metadata uses default owner and succeeds when update allowed",
			repo:     "azylman/aerial",
			prNum:    42,
			meta:     map[string]any{},
			sarRules: []sarRule{{Resource: "prs", Verb: "update", Name: "aerial"}},
			wantErr:  false,
		},
		{
			name:     "metadata with matching bot_id fallback succeeds",
			repo:     "azylman/aerial",
			prNum:    42,
			meta:     map[string]any{"bot_id": "aerial"},
			sarRules: []sarRule{{Resource: "prs", Verb: "update", Name: "aerial"}},
			wantErr:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reg PRRegistryUpdater
			if !tc.nilReg {
				reg = &mockPRRegistry{rawMetadata: tc.meta}
			}
			srv := newTestRouter(Config{}, nil, reg, nil)
			tr := &staticAuthRoundTripper{sarRules: tc.sarRules}
			authCfg := newTestK8sAuthConfig(t, tr)
			auth := k8sauth.NewAuthenticator(authCfg)
			caller := k8sauth.CallerIdentity{
				Username:    "system:serviceaccount:default:eri-aerial",
				CallerAgent: "aerial",
			}
			err := srv.authorizeExistingPROwner(context.Background(), auth, caller, tc.repo, tc.prNum)
			if (err != nil) != tc.wantErr {
				t.Errorf("authorizeExistingPROwner() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
