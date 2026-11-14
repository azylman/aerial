package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type prOwnedLocallyTestCase struct {
	name       string
	nilServer  bool
	nilReg     bool
	prNum      int
	targetID   string
	branch     string
	resolveErr error
	wantOwned  bool
}

func runIsPROwnedLocallyCase(t *testing.T, tc prOwnedLocallyTestCase) {
	t.Helper()
	t.Parallel()
	if tc.nilServer {
		var srv *RouterServer
		if got := srv.isPROwnedLocally(context.Background(), "azylman/aerial", tc.prNum); got != tc.wantOwned {
			t.Fatalf("isPROwnedLocally got %v, want %v", got, tc.wantOwned)
		}
		return
	}
	var reg PRRegistryUpdater
	if !tc.nilReg {
		reg = &mockPRRegistry{
			resolveTargetID: tc.targetID,
			resolveBranch:   tc.branch,
			resolveErr:      tc.resolveErr,
		}
	}
	srv := newTestRouter(Config{}, nil, reg, nil)
	if got := srv.isPROwnedLocally(context.Background(), "azylman/aerial", tc.prNum); got != tc.wantOwned {
		t.Fatalf("isPROwnedLocally got %v, want %v", got, tc.wantOwned)
	}
}

func TestIsPROwnedLocally(t *testing.T) {
	t.Parallel()

	tests := []prOwnedLocallyTestCase{
		{name: "nil server", nilServer: true, prNum: 101, wantOwned: false},
		{name: "nil registry", nilReg: true, prNum: 101, wantOwned: false},
		{name: "prNum zero", prNum: 0, targetID: "12345", wantOwned: false},
		{name: "prNum negative", prNum: -1, targetID: "12345", wantOwned: false},
		{name: "resolve error", prNum: 101, resolveErr: errors.New("db error"), wantOwned: false},
		{name: "empty target and branch", prNum: 101, targetID: "", branch: "", wantOwned: false},
		{name: "whitespace target and branch", prNum: 101, targetID: "   ", branch: "\t", wantOwned: false},
		{name: "non-empty targetID", prNum: 101, targetID: "123456789012345678", wantOwned: true},
		{name: "non-empty branch", prNum: 101, branch: "feat/foo", wantOwned: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runIsPROwnedLocallyCase(t, tc)
		})
	}
}

func TestDispatchWorkflowRunImages_Ownership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		prNum          int
		defaultOwner   bool
		forwardURLs    map[string]string
		nilReg         bool
		ownedLocally   bool
		wantDispatched int
	}{
		{
			name:           "prNum > 0, locally owned, not default owner -> promotes",
			prNum:          101,
			defaultOwner:   false,
			ownedLocally:   true,
			wantDispatched: 1,
		},
		{
			name:           "prNum > 0, unowned locally, default owner with empty forward URLs -> promotes",
			prNum:          102,
			defaultOwner:   true,
			forwardURLs:    nil,
			ownedLocally:   false,
			wantDispatched: 1,
		},
		{
			name:           "prNum > 0, unowned locally, default owner with peer forward URLs -> skipped",
			prNum:          103,
			defaultOwner:   true,
			forwardURLs:    map[string]string{"coven": "http://coven.local"},
			ownedLocally:   false,
			wantDispatched: 0,
		},
		{
			name:           "prNum > 0, unowned locally, not default owner -> skipped",
			prNum:          104,
			defaultOwner:   false,
			ownedLocally:   false,
			wantDispatched: 0,
		},
		{
			name:           "prNum == 0, default owner -> promotes",
			prNum:          0,
			defaultOwner:   true,
			wantDispatched: 1,
		},
		{
			name:           "prNum == 0, not default owner with registry -> skipped",
			prNum:          0,
			defaultOwner:   false,
			wantDispatched: 0,
		},
		{
			name:           "prNum == 0, not default owner with nil registry -> promotes",
			prNum:          0,
			defaultOwner:   false,
			nilReg:         true,
			wantDispatched: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/actions/runs/") {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{
						"total_count": 1,
						"jobs": [
							{
								"id": 1,
								"name": "Build & Push Container Images (eri)",
								"status": "completed",
								"conclusion": "success"
							}
						]
					}`))
					return
				}
				w.WriteHeader(http.StatusNotFound)
			}))
			defer mockServer.Close()

			var reg PRRegistryUpdater
			if !tc.nilReg {
				reg = &mockPRRegistry{
					resolveTargetID: "123456789012345678",
				}
				if !tc.ownedLocally {
					reg = &mockPRRegistry{}
				}
			}
			disp := &mockOutboundDispatcher{
				kustomizationsByImage: map[string][]fluxKustomizationRef{"aerial-eri": {{Namespace: "flux-system", Name: "aerial-core"}}},
			}
			cfg := Config{
				GitHubAPIURL:       mockServer.URL,
				DefaultOwner:       tc.defaultOwner,
				WebhookForwardURLs: tc.forwardURLs,
			}
			srv := newTestRouter(cfg, mockServer.Client(), reg, disp)

			err := srv.dispatchWorkflowRunImages(context.Background(), "azylman/aerial", 1, "main", "success", "sha123", "target-123", tc.prNum)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(disp.PromoteCalls()) != tc.wantDispatched {
				t.Fatalf("got %d promote calls, want %d", len(disp.PromoteCalls()), tc.wantDispatched)
			}
		})
	}
}

func TestDispatchGitPush_Ownership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		defaultOwner   bool
		targetID       string
		ref            string
		sha            string
		wantDispatched bool
	}{
		{
			name:           "default owner without targetID -> dispatches",
			defaultOwner:   true,
			targetID:       "",
			ref:            "refs/heads/main",
			sha:            "abc1234",
			wantDispatched: true,
		},
		{
			name:           "not default owner without targetID -> skipped",
			defaultOwner:   false,
			targetID:       "",
			ref:            "refs/heads/main",
			sha:            "abc1234",
			wantDispatched: false,
		},
		{
			name:           "not default owner with targetID -> dispatches",
			defaultOwner:   false,
			targetID:       "123456789012345678",
			ref:            "refs/heads/main",
			sha:            "abc1234",
			wantDispatched: true,
		},
		{
			name:           "non-main ref -> skipped regardless of ownership",
			defaultOwner:   true,
			targetID:       "123456789012345678",
			ref:            "refs/heads/feature",
			sha:            "abc1234",
			wantDispatched: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp := &mockOutboundDispatcher{}
			cfg := Config{
				DefaultOwner: tc.defaultOwner,
			}
			srv := newTestRouter(cfg, nil, nil, disp)

			err := srv.dispatchGitPush(context.Background(), "azylman/aerial", tc.ref, tc.sha, tc.targetID, 0)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if (len(disp.GitPushCalls()) > 0) != tc.wantDispatched {
				t.Fatalf("gitPushCalls = %d, wantDispatched = %v", len(disp.GitPushCalls()), tc.wantDispatched)
			}
		})
	}
}

type checkOpenPROwnershipTestCase struct {
	name            string
	defaultOwner    bool
	ownedPRs        map[int]bool
	wantUpdateCalls int
	wantUpdatedPR   int
}

func newCheckOpenPROwnershipMockServer(updateCalls *[]int, mu *sync.Mutex) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/pulls") && r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[
				{"number": 10, "draft": false, "user": {"login": "azylman"}, "head": {"ref": "branch-10", "sha": "sha10"}},
				{"number": 20, "draft": false, "user": {"login": "azylman"}, "head": {"ref": "branch-20", "sha": "sha20"}}
			]`))
			return
		}
		if strings.Contains(r.URL.Path, "/update-branch") && r.Method == http.MethodPut {
			parts := strings.Split(r.URL.Path, "/")
			for i, p := range parts {
				if p == "pulls" && i+1 < len(parts) {
					var num int
					_, _ = fmt.Sscanf(parts[i+1], "%d", &num)
					mu.Lock()
					*updateCalls = append(*updateCalls, num)
					mu.Unlock()
				}
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
}

func runCheckOpenPROwnershipCase(t *testing.T, tc checkOpenPROwnershipTestCase) {
	t.Helper()
	t.Parallel()
	var mu sync.Mutex
	var updateCalls []int
	mockServer := newCheckOpenPROwnershipMockServer(&updateCalls, &mu)
	defer mockServer.Close()

	reg := &ownedMapRegistry{ownedPRs: tc.ownedPRs}
	cfg := Config{
		GitHubAPIURL:        mockServer.URL,
		DefaultOwner:        tc.defaultOwner,
		AutoUpdatePRAuthors: []string{"azylman"},
	}
	srv := newTestRouter(cfg, mockServer.Client(), reg, nil)

	err := srv.checkOpenPRConflicts(context.Background(), "azylman/aerial")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(updateCalls) != tc.wantUpdateCalls {
		t.Fatalf("updateCalls len = %d, want %d", len(updateCalls), tc.wantUpdateCalls)
	}
	if tc.wantUpdateCalls > 0 && updateCalls[0] != tc.wantUpdatedPR {
		t.Fatalf("updated PR = %d, want %d", updateCalls[0], tc.wantUpdatedPR)
	}
}

func TestCheckOpenPRConflicts_Ownership(t *testing.T) {
	t.Parallel()

	tests := []checkOpenPROwnershipTestCase{
		{
			name:            "default owner checks first eligible PR",
			defaultOwner:    true,
			ownedPRs:        nil,
			wantUpdateCalls: 1,
			wantUpdatedPR:   10,
		},
		{
			name:            "non-default owner skips unowned PR and updates owned PR",
			defaultOwner:    false,
			ownedPRs:        map[int]bool{20: true},
			wantUpdateCalls: 1,
			wantUpdatedPR:   20,
		},
		{
			name:            "non-default owner skips all when none owned",
			defaultOwner:    false,
			ownedPRs:        map[int]bool{},
			wantUpdateCalls: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCheckOpenPROwnershipCase(t, tc)
		})
	}
}
