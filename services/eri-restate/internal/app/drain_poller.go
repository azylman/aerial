package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/agent"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

// Drain poller: edge-triggered handoff for a superseded eri-restate primary.
// Runs outside Restate handlers (started from RunApp on ctx): every drainPollInterval
// it GETs {RestateAdminURL}/deployments with drainPollTimeout, keeps only deployments
// carrying the running agent's full six-service set, and tracks the newest by max
// created_at. The pod's own deployment is identified by exact match between the
// first DNS label of the deployment uri and the pod's ReplicaSet name (hostname minus
// its final pod suffix), because the Restate operator registers each ReplicaSet as
// http://<replicaset>.<ns>.svc.cluster.local:9080/. Two consecutive conclusive
// newer-not-self polls latch BeginDrain on every underlying unified pool. Any error,
// malformed payload, unregistered self, or partial set resets the counter and logs
// fail-open, so an inconclusive poll never drains a healthy primary.
const (
	drainPollInterval = 10 * time.Second
	drainPollTimeout  = 5 * time.Second
)

// drainTarget is anything that can latch pool drain (unified pools directly, or the
// InterchangeablePool/DynamicVoicePool wrappers that delegate to their underlying).
type drainTarget interface {
	BeginDrain()
}

// deploymentInfo is the poller-local view of one Restate admin deployment.
// HostLabel is the first DNS label of the deployment uri host ("" when unknown,
// which means the deployment can never be identified as self).
type deploymentInfo struct {
	ID        string
	HostLabel string
	CreatedAt time.Time
	Services  map[string]bool
}

// drainSelfName derives the pod's ReplicaSet name from its hostname (Kubernetes sets
// the container hostname to the pod name, which is "<replicaset>-<suffix>"). It
// returns "" when the hostname has no '-' or either side of the last '-' is empty.
func drainSelfName(hostname string) string {
	h := strings.TrimSpace(hostname)
	i := strings.LastIndex(h, "-")
	if i <= 0 || i == len(h)-1 {
		return ""
	}
	return h[:i]
}

// drainServiceNames returns the running agent's six-service set via
// agent.ScopedServiceName; matching is always exact.
func drainServiceNames(agentName string) []string {
	bases := []string{"ThreadSession", "DispatchSession", "VoiceSession", "SessionManager", "TurnRunner", "LLM"}
	names := make([]string, 0, len(bases))
	for _, b := range bases {
		names = append(names, agent.ScopedServiceName(b, agentName))
	}
	return names
}

// hasFullServiceSet reports whether services holds every wanted name; callers
// guarantee want is non-empty (decideDrain rejects an empty set up front).
func hasFullServiceSet(services map[string]bool, want []string) bool {
	for _, name := range want {
		if !services[name] {
			return false
		}
	}
	return true
}

func hasPartialServiceSet(services map[string]bool, want []string) bool {
	hits := 0
	for _, name := range want {
		if services[name] {
			hits++
		}
	}
	return hits > 0 && hits < len(want)
}

// drainHostLabel returns the first DNS label of rawURI's host, or "" when the uri
// is empty, unparseable, or has no host.
func drainHostLabel(rawURI string) string {
	raw := strings.TrimSpace(rawURI)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	if i := strings.Index(host, "."); i >= 0 {
		host = host[:i]
	}
	return host
}

// decideDrain is the pure drain decision: given the pod's self name, the agent's
// wanted service set, the listed deployments, and the prior consecutive-confirmation
// count, it returns the updated count and whether to latch drain. Self is the
// deployment whose HostLabel equals selfName exactly. Any inconclusive input (empty
// or unregistered self, self without the full set, malformed timestamps, a partial
// set anywhere, or a newest that is not strictly newer than self) resets to zero
// and never drains.
func decideDrain(selfName string, want []string, deps []deploymentInfo, prior int) (int, bool) {
	if strings.TrimSpace(selfName) == "" || len(want) == 0 {
		return 0, false
	}
	var self *deploymentInfo
	for i := range deps {
		if deps[i].HostLabel == selfName {
			self = &deps[i]
			break
		}
	}
	if self == nil || self.CreatedAt.IsZero() || !hasFullServiceSet(self.Services, want) {
		return 0, false
	}
	var newest *deploymentInfo
	for i := range deps {
		d := &deps[i]
		if d.CreatedAt.IsZero() {
			return 0, false
		}
		if !hasFullServiceSet(d.Services, want) {
			if hasPartialServiceSet(d.Services, want) {
				return 0, false
			}
			continue
		}
		if newest == nil || d.CreatedAt.After(newest.CreatedAt) {
			newest = d
		}
	}
	if newest == nil || newest.ID == self.ID || !newest.CreatedAt.After(self.CreatedAt) {
		return 0, false
	}
	next := prior + 1
	return next, next >= 2
}

type restateDeploymentService struct {
	Name string `json:"name"`
}

type restateDeployment struct {
	ID        string                     `json:"id"`
	URI       string                     `json:"uri"`
	CreatedAt time.Time                  `json:"created_at"`
	Services  []restateDeploymentService `json:"services"`
}

// parseDrainDeployments decodes a Restate admin GET /deployments body, accepting both
// the wrapped {"deployments": [...]} shape and a bare [...] array.
func parseDrainDeployments(body []byte) ([]deploymentInfo, error) {
	var raw []restateDeployment
	var wrapped struct {
		Deployments []restateDeployment `json:"deployments"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		if arrErr := json.Unmarshal(body, &raw); arrErr != nil {
			return nil, fmt.Errorf("decode deployments: %w", err)
		}
	} else if wrapped.Deployments != nil {
		raw = wrapped.Deployments
	} else if arrErr := json.Unmarshal(body, &raw); arrErr != nil {
		return nil, fmt.Errorf("decode deployments: no deployments key")
	}
	deps := make([]deploymentInfo, 0, len(raw))
	for _, d := range raw {
		if strings.TrimSpace(d.ID) == "" {
			return nil, fmt.Errorf("decode deployments: deployment missing id")
		}
		set := make(map[string]bool, len(d.Services))
		for _, s := range d.Services {
			if strings.TrimSpace(s.Name) != "" {
				set[s.Name] = true
			}
		}
		deps = append(deps, deploymentInfo{ID: d.ID, HostLabel: drainHostLabel(d.URI), CreatedAt: d.CreatedAt, Services: set})
	}
	return deps, nil
}

// fetchDrainDeployments GETs {baseURL}/deployments with the given timeout.
func fetchDrainDeployments(ctx context.Context, baseURL string, timeout time.Duration) ([]deploymentInfo, error) {
	client := http.DefaultClient
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/deployments", http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list deployments: unexpected status %d", resp.StatusCode)
	}
	return parseDrainDeployments(body)
}

// drainPollState tracks the self identity, wanted services, and consecutive confirmations.
type drainPollState struct {
	selfName      string
	want          []string
	confirmations int
	latched       bool
}

func newDrainPollState(selfName string, want []string) *drainPollState {
	return &drainPollState{selfName: selfName, want: want}
}

// step performs one poll against adminURL and latches BeginDrain on targets once
// decideDrain confirms a newer primary twice in a row.
func (s *drainPollState) step(ctx context.Context, adminURL string, timeout time.Duration, targets []drainTarget) {
	if s == nil || s.latched {
		return
	}
	base := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	if base == "" {
		return
	}
	deps, err := fetchDrainDeployments(ctx, base, timeout)
	if err != nil {
		log.Printf("[eri-restate] drain poller: listing failed (fail-open): %v", err)
		s.confirmations = 0
		return
	}
	var drain bool
	s.confirmations, drain = decideDrain(s.selfName, s.want, deps, s.confirmations)
	if drain {
		s.latched = true
		for _, t := range targets {
			if t != nil {
				t.BeginDrain()
			}
		}
		log.Printf("[eri-restate] drain poller: superseded by newer deployment, pools draining")
	}
}

// startDrainPoller launches the edge-triggered drain loop outside Restate handlers.
// It resolves the pod's self deployment name from os.Hostname once (disabled when
// that fails) and reads RestateAdminURL JIT from cfg on every tick; an empty URL is
// a no-op.
func startDrainPoller(ctx context.Context, appCfg *config.Config, agentName string, targets []drainTarget) {
	hostname, err := os.Hostname()
	selfName := ""
	if err == nil {
		selfName = drainSelfName(hostname)
	}
	if selfName == "" {
		log.Printf("[eri-restate] drain poller: cannot resolve self deployment name (disabled)")
		return
	}
	startDrainPollerWithInterval(ctx, appCfg, selfName, drainServiceNames(agentName), targets, drainPollInterval, drainPollTimeout)
}

// startDrainPollerWithInterval is startDrainPoller with explicit identity and
// injectable timing for tests.
func startDrainPollerWithInterval(ctx context.Context, appCfg *config.Config, selfName string, want []string, targets []drainTarget, interval, timeout time.Duration) {
	if appCfg == nil {
		return
	}
	if interval <= 0 {
		interval = drainPollInterval
	}
	if timeout <= 0 {
		timeout = drainPollTimeout
	}
	state := newDrainPollState(selfName, want)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		poll := func(pollCtx context.Context) {
			state.step(pollCtx, appCfg.Current().RestateAdminURL, timeout, targets)
		}
		poll(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				poll(ctx)
				if state.latched {
					return
				}
			}
		}
	}()
}
