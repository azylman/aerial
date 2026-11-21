package router

import (
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

// registryIngress is a hermetic Restate ingress that answers every call
// with one status and body and records request paths and bodies.
type registryIngress struct {
	mu     sync.Mutex
	paths  []string
	bodies []string
}

func newRegistryIngress(t *testing.T, status int, body string) (*registryIngress, string) {
	t.Helper()
	ing := &registryIngress{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		ing.mu.Lock()
		ing.paths = append(ing.paths, r.URL.EscapedPath())
		ing.bodies = append(ing.bodies, string(raw))
		ing.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return ing, srv.URL
}

func (i *registryIngress) snapshot() ([]string, []string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]string(nil), i.paths...), append([]string(nil), i.bodies...)
}

func TestShadowRegisterPR(t *testing.T) {
	t.Parallel()
	const target = "123456789012345678"
	req := PRRegisterRequest{Repo: "azylman/aerial", PRNumber: 7, Branch: "feat", HeadSHA: "h1", TargetID: target, Agent: "aerial", BotID: "aerial", Metadata: `{"build_ref":"ref-1","widget_state":"deployed"}`}
	tests := []struct {
		name            string
		disabled        bool
		k8sAuth         bool
		status          int
		body            string
		req             PRRegisterRequest
		wantCalls       int
		wantSends       uint64
		wantErrors      uint64
		wantDivergences uint64
		wantBodySubs    []string
	}{
		{name: "matching target counts a send", status: 200, body: `{"target_id":"` + target + `","agent":"aerial","status":"open"}`, req: req, wantCalls: 1, wantSends: 1, wantBodySubs: []string{`"build_ref":"ref-1"`, `"widget_state":"deployed"`, `"fallback_target_id":"223456789012345678"`}},
		{name: "target mismatch counts a divergence", status: 200, body: `{"target_id":"999999999999999999","agent":"aerial","status":"open"}`, req: req, wantCalls: 1, wantSends: 1, wantDivergences: 1},
		{name: "k8s auth enforces the registered owner", k8sAuth: true, status: 200, body: `{"target_id":"` + target + `","agent":"aerial","status":"open"}`, req: req, wantCalls: 1, wantSends: 1, wantBodySubs: []string{`"enforce_owner":true`, `"expected_owner":"aerial"`}},
		{name: "ingress rejection fails closed", status: 409, body: "owner", req: req, wantCalls: 1, wantErrors: 1},
		{name: "disabled reporter sends nothing", disabled: true, req: req},
		{name: "zero pr sends nothing", status: 200, req: PRRegisterRequest{Repo: "azylman/aerial"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ing, url := newRegistryIngress(t, tc.status, tc.body)
			if tc.disabled {
				url = ""
			}
			cfg := Config{RestateURL: url, SystemChannelID: "223456789012345678", K8sAuthEnabled: tc.k8sAuth}
			srv := newTestRouter(cfg, nil, &mockPRRegistry{}, nil)
			r := newShadowRestateReporter(cfg, nil)
			srv.SetRestateShadow(r)
			srv.shadowRegisterPR(context.Background(), tc.req, target)
			paths, bodies := ing.snapshot()
			if len(paths) != tc.wantCalls {
				t.Fatalf("calls = %d, want %d", len(paths), tc.wantCalls)
			}
			if tc.wantCalls > 0 && paths[0] != "/PRObject/azylman%2Faerial%237/Register" {
				t.Fatalf("path = %q, want Register on repo#7", paths[0])
			}
			for _, sub := range tc.wantBodySubs {
				if !strings.Contains(bodies[0], sub) {
					t.Fatalf("body %q missing %q", bodies[0], sub)
				}
			}
			if r.Sends() != tc.wantSends || r.SendErrors() != tc.wantErrors || r.Divergences() != tc.wantDivergences {
				t.Fatalf("sends/errors/divergences = %d/%d/%d, want %d/%d/%d", r.Sends(), r.SendErrors(), r.Divergences(), tc.wantSends, tc.wantErrors, tc.wantDivergences)
			}
		})
	}
}

func TestRegisterMetadataStrings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		want map[string]string
	}{
		{name: "string keys forward trimmed", raw: `{"build_ref":" ref-1 ","widget_state":"deployed"}`, want: map[string]string{"build_ref": "ref-1", "widget_state": "deployed"}},
		{name: "non-string and blank values drop", raw: `{"build_ref":7,"widget_state":{"a":1},"agent":" "}`},
		{name: "malformed json drops", raw: `{nope`},
		{name: "empty metadata drops", raw: ``},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := registerMetadataStrings(tc.raw); !maps.Equal(got, tc.want) {
				t.Fatalf("registerMetadataStrings() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRouteDeployFailed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		status        int
		prNumber      int
		acknowledged  bool
		wantErr       bool
		wantCalls     int
		wantBodySub   string
	}{
		{name: "authoritative failure journals", authoritative: true, status: 202, prNumber: 7, wantCalls: 1, wantBodySub: `"merge_sha":"m1"`},
		{name: "authoritative ack journals acknowledged", authoritative: true, status: 202, prNumber: 7, acknowledged: true, wantCalls: 1, wantBodySub: `"acknowledged":true`},
		{name: "authoritative ingress failure surfaces", authoritative: true, status: 500, prNumber: 7, wantErr: true, wantCalls: 1},
		{name: "shadow failure mirrors fail-closed", status: 500, prNumber: 7, wantCalls: 1},
		{name: "pr-less failure journals nothing", authoritative: true, status: 202},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ing, url := newRegistryIngress(t, tc.status, "")
			srv := newTestRouter(Config{RestateAuthoritative: tc.authoritative, RestateURL: url}, nil, &mockPRRegistry{}, nil)
			err := srv.routeDeployFailed(context.Background(), "azylman/aerial", tc.prNumber, "h1", "m1", tc.acknowledged)
			if (err != nil) != tc.wantErr {
				t.Fatalf("routeDeployFailed() err = %v, wantErr %v", err, tc.wantErr)
			}
			paths, bodies := ing.snapshot()
			if len(paths) != tc.wantCalls {
				t.Fatalf("calls = %d, want %d", len(paths), tc.wantCalls)
			}
			if tc.wantCalls > 0 && (!strings.HasSuffix(paths[0], "/HandleDeployFailed/send") || !strings.Contains(bodies[0], tc.wantBodySub)) {
				t.Fatalf("call = %q %q, want HandleDeployFailed send containing %q", paths[0], bodies[0], tc.wantBodySub)
			}
		})
	}
}

func TestBuildMetadataAdapterJournal(t *testing.T) {
	t.Parallel()
	errDB := errors.New("db error")
	tests := []struct {
		name        string
		status      int
		innerErr    error
		wantErr     bool
		wantOK      bool
		wantCalls   int
		wantBodySub string
	}{
		{name: "journal precedes the legacy record", status: 202, wantOK: true, wantCalls: 1, wantBodySub: `"build_metadata":{"version":1,"kind":"final"`},
		{name: "journal failure surfaces before the legacy record", status: 500, wantErr: true, wantCalls: 1},
		{name: "legacy failure surfaces after the journal", status: 202, innerErr: errDB, wantErr: true, wantCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ing, url := newRegistryIngress(t, tc.status, "")
			reg := &mockPRRegistry{recordBuildMetadataErr: tc.innerErr}
			srv := newTestRouter(Config{RestateAuthoritative: true, RestateURL: url}, nil, reg, nil)
			adapter := buildMetadataRegistryAdapter{inner: reg, srv: srv}
			ok, err := adapter.RecordPRBuildMetadata(context.Background(), "azylman/aerial", 7, model.BuildMetadata{Version: 1, Kind: "final", Repository: "azylman/aerial", SourceSHA: "m1"})
			if (err != nil) != tc.wantErr || ok != tc.wantOK {
				t.Fatalf("RecordPRBuildMetadata() = (%v, %v), want ok=%v wantErr=%v", ok, err, tc.wantOK, tc.wantErr)
			}
			paths, bodies := ing.snapshot()
			if len(paths) != tc.wantCalls {
				t.Fatalf("calls = %d, want %d", len(paths), tc.wantCalls)
			}
			if !strings.HasSuffix(paths[0], "/HandleBuildMetadata/send") || !strings.Contains(bodies[0], tc.wantBodySub) || !strings.Contains(bodies[0], `"head_sha":"m1"`) {
				t.Fatalf("call = %q %q, want HandleBuildMetadata send containing %q", paths[0], bodies[0], tc.wantBodySub)
			}
		})
	}
}
