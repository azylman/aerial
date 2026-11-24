package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/agent"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

// Drain poller: edge-triggered handoff for the superseded eri-suletta-restate primary.
// Runs outside Restate handlers (started from RunApp on ctx): every drainPollInterval
// it GETs {RestateAdminURL}/deployments with drainPollTimeout, keeps only deployments
// carrying the full six-service Suletta set, and tracks the newest by max created_at.
// Two consecutive conclusive newer-not-self polls latch BeginDrain on every underlying
// unified pool. Any error, malformed payload, missing self, or partial set resets the
// counter and logs fail-open, so an inconclusive poll never drains a healthy primary.
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
type deploymentInfo struct {
	ID        string
	CreatedAt time.Time
	Services  map[string]bool
}

// sulettaDrainServiceNames returns the full six-service Suletta set via
// agent.ScopedServiceName; matching is always exact, never unscoped.
func sulettaDrainServiceNames() []string {
	return []string{
		agent.ScopedServiceName("ThreadSession", "suletta"),
		agent.ScopedServiceName("DispatchSession", "suletta"),
		agent.ScopedServiceName("VoiceSession", "suletta"),
		agent.ScopedServiceName("SessionManager", "suletta"),
		agent.ScopedServiceName("TurnRunner", "suletta"),
		agent.ScopedServiceName("LLM", "suletta"),
	}
}

func hasFullSulettaSet(services map[string]bool) bool {
	for _, want := range sulettaDrainServiceNames() {
		if !services[want] {
			return false
		}
	}
	return true
}

func hasPartialSulettaSet(services map[string]bool) bool {
	hits := 0
	want := sulettaDrainServiceNames()
	for _, name := range want {
		if services[name] {
			hits++
		}
	}
	return hits > 0 && hits < len(want)
}

// decideDrain is the pure drain decision: given the known self deployment identity,
// the listed deployments, and the prior consecutive-confirmation count, it returns
// the updated count and whether to latch drain. Any inconclusive input (empty or
// missing self, self without the full set, malformed timestamps, no full-set listing,
// a partial Suletta set anywhere, or a newest that is not strictly newer than self)
// resets to zero and never drains.
func decideDrain(selfID string, deps []deploymentInfo, prior int) (int, bool) {
	if strings.TrimSpace(selfID) == "" {
		return 0, false
	}
	var self *deploymentInfo
	for i := range deps {
		if deps[i].ID == selfID {
			self = &deps[i]
			break
		}
	}
	if self == nil || self.CreatedAt.IsZero() || !hasFullSulettaSet(self.Services) {
		return 0, false
	}
	var newest *deploymentInfo
	for i := range deps {
		d := &deps[i]
		if d.CreatedAt.IsZero() {
			return 0, false
		}
		if !hasFullSulettaSet(d.Services) {
			if hasPartialSulettaSet(d.Services) {
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

// resolveDrainBaseline picks the newest full-set deployment as the self identity at
// poller startup; "" means no baseline yet (keep retrying, never drain).
func resolveDrainBaseline(deps []deploymentInfo) string {
	var newest *deploymentInfo
	for i := range deps {
		d := &deps[i]
		if d.CreatedAt.IsZero() || !hasFullSulettaSet(d.Services) {
			continue
		}
		if newest == nil || d.CreatedAt.After(newest.CreatedAt) {
			newest = d
		}
	}
	if newest == nil {
		return ""
	}
	return newest.ID
}

type restateDeploymentService struct {
	Name string `json:"name"`
}

type restateDeployment struct {
	ID        string                     `json:"id"`
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
		deps = append(deps, deploymentInfo{ID: d.ID, CreatedAt: d.CreatedAt, Services: set})
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

// drainPollState tracks the baseline identity and consecutive confirmations.
type drainPollState struct {
	selfID        string
	confirmations int
	latched       bool
}

func newDrainPollState() *drainPollState {
	return &drainPollState{}
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
	if s.selfID == "" {
		s.selfID = resolveDrainBaseline(deps)
		if s.selfID == "" {
			s.confirmations = 0
			log.Printf("[eri-restate] drain poller: no baseline deployment yet (fail-open)")
		}
		return
	}
	var drain bool
	s.confirmations, drain = decideDrain(s.selfID, deps, s.confirmations)
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
// It reads RestateAdminURL JIT from cfg on every tick; an empty URL is a no-op.
func startDrainPoller(ctx context.Context, appCfg *config.Config, targets []drainTarget) {
	startDrainPollerWithInterval(ctx, appCfg, targets, drainPollInterval, drainPollTimeout)
}

// startDrainPollerWithInterval is startDrainPoller with injectable timing for tests.
func startDrainPollerWithInterval(ctx context.Context, appCfg *config.Config, targets []drainTarget, interval, timeout time.Duration) {
	if ctx == nil || appCfg == nil {
		return
	}
	if interval <= 0 {
		interval = drainPollInterval
	}
	if timeout <= 0 {
		timeout = drainPollTimeout
	}
	state := newDrainPollState()
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
