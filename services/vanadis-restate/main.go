package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"

	"github.com/azylman/aerial/pkg/httpkit"
)

const (
	statusCompleted = "completed"
	statusFailed    = "failed"
	statusCancelled = "cancelled"
	statusNotFound  = "not_found"
)

// Config holds runtime configuration for the vanadis-restate worker service.
type Config struct {
	RestatePort string
	HealthPort  string
	VanadisURL  string
}

// LoadConfigFromEnv loads configuration from environment variables with defaults.
func LoadConfigFromEnv() Config {
	restatePort := strings.TrimSpace(os.Getenv("RESTATE_PORT"))
	if restatePort == "" {
		restatePort = strings.TrimSpace(os.Getenv("PORT"))
	}
	if restatePort == "" {
		restatePort = "9080"
	}

	healthPort := strings.TrimSpace(os.Getenv("HEALTH_PORT"))
	if healthPort == "" {
		healthPort = "4026"
	}

	vanadisURL := strings.TrimRight(strings.TrimSpace(os.Getenv("VANADIS_URL")), "/")
	if vanadisURL == "" {
		vanadisURL = "http://vanadis.aerial:4025"
	}

	return Config{
		RestatePort: restatePort,
		HealthPort:  healthPort,
		VanadisURL:  vanadisURL,
	}
}

// ReconcileBuildRequest is the input payload for the Restate Build Virtual Object.
type ReconcileBuildRequest struct {
	Reason string `json:"reason,omitempty"`
}

// ReconcileBuildResult is the output of the Restate Build Virtual Object.
type ReconcileBuildResult struct {
	BuildID        string   `json:"build_id"`
	Status         string   `json:"status"`
	LaunchedChunks []string `json:"launched_chunks,omitempty"`
	Error          string   `json:"error,omitempty"`
}

// VanadisBuild represents a multi-chunk engineering build run.
type VanadisBuild struct {
	ID              string    `json:"id"`
	ThreadID        string    `json:"thread_id"`
	Tier            int       `json:"tier"`
	Title           string    `json:"title"`
	Objective       string    `json:"objective"`
	SharedContext   string    `json:"shared_context,omitempty"`
	Status          string    `json:"status"`
	StatusMessageID string    `json:"status_message_id,omitempty"`
	Quiet           bool      `json:"quiet"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// VanadisChunk represents an individual ordered task chunk within a VanadisBuild.
type VanadisChunk struct {
	ID                 string            `json:"id"`
	BuildID            string            `json:"build_id"`
	ChunkIndex         int               `json:"chunk_index"`
	Mode               string            `json:"mode"`
	Repo               string            `json:"repo,omitempty"`
	Title              string            `json:"title"`
	Instructions       string            `json:"instructions"`
	TargetFiles        []string          `json:"target_files"`
	TargetSymbols      []string          `json:"target_symbols"`
	Effort             string            `json:"effort"`
	AcceptanceCriteria []string          `json:"acceptance_criteria"`
	Status             string            `json:"status"`
	PRNumber           int               `json:"pr_number,omitempty"`
	StallNudges        int               `json:"stall_nudges,omitempty"`
	DependsOn          []string          `json:"depends_on"`
	CIFailCount        int               `json:"ci_fail_count"`
	CIChecks           map[string]string `json:"ci_checks,omitempty"`
	Banner             string            `json:"banner,omitempty"`
	MergeAwakeableID   string            `json:"merge_awakeable_id,omitempty"`
	DeployAwakeableID  string            `json:"deploy_awakeable_id,omitempty"`
	StartedAt          time.Time         `json:"started_at,omitempty"`
	CompletedAt        time.Time         `json:"completed_at,omitempty"`
	CreatedAt          time.Time         `json:"created_at"`
	UpdatedAt          time.Time         `json:"updated_at"`
}

// BuildObjectState represents the authoritative K/V state for a Vanadis build managed by BuildObject.
type BuildObjectState struct {
	Build    VanadisBuild    `json:"build"`
	Chunks   []VanadisChunk  `json:"chunks"`
	Launched map[string]bool `json:"launched,omitempty"`
}

// BuildMutationRequest represents a typed mutation payload targeting BuildObject.
type BuildMutationRequest struct {
	Action  string `json:"action"`
	ChunkID string `json:"chunk_id,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// RestateTransitionRequest represents the payload sent to the internal Vanadis transition endpoint.
type RestateTransitionRequest struct {
	Mutation      *BuildMutationRequest `json:"mutation,omitempty"`
	PriorLaunched map[string]bool       `json:"prior_launched,omitempty"`
}

type reconcileStepOutput struct {
	BuildStatus    string            `json:"build_status"`
	LaunchedChunks []string          `json:"launched_chunks,omitempty"`
	LaunchedMap    map[string]bool   `json:"launched_map,omitempty"`
	Error          string            `json:"error,omitempty"`
	State          *BuildObjectState `json:"state,omitempty"`
}

type objectRuntime interface {
	Key() string
	GetLaunched() (map[string]bool, error)
	SetLaunched(launched map[string]bool)
	GetState() (*BuildObjectState, error)
	SetState(state BuildObjectState)
	ClearState()
	RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error)
}

type restateObjectRuntime struct {
	ctx restate.ObjectContext
}

func (r restateObjectRuntime) Key() string {
	return restate.Key(r.ctx)
}

func (r restateObjectRuntime) GetLaunched() (map[string]bool, error) {
	return restate.Get[map[string]bool](r.ctx, "launched")
}

func (r restateObjectRuntime) SetLaunched(launched map[string]bool) {
	if launched == nil {
		launched = make(map[string]bool)
	}
	restate.Set(r.ctx, "launched", launched)
}

func (r restateObjectRuntime) GetState() (*BuildObjectState, error) {
	return restate.Get[*BuildObjectState](r.ctx, "state")
}

func (r restateObjectRuntime) SetState(state BuildObjectState) {
	restate.Set(r.ctx, "state", &state)
}

func (r restateObjectRuntime) ClearState() {
	restate.ClearAll(r.ctx)
}

func (r restateObjectRuntime) RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error) {
	return restate.Run(r.ctx, func(rc restate.RunContext) (reconcileStepOutput, error) {
		return fn(rc)
	}, restate.WithName(name))
}

// BuildObject implements the Restate-authoritative Virtual Object for multi-chunk builds.
type BuildObject struct {
	vanadisURL     string
	httpClient     *http.Client
	runtimeFactory func(restate.ObjectContext) objectRuntime
}

// NewBuildObject creates a new BuildObject bound to vanadisURL with an optional HTTP client.
func NewBuildObject(vanadisURL string, httpClient *http.Client, runtimeFactory ...func(restate.ObjectContext) objectRuntime) *BuildObject {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	var rf func(restate.ObjectContext) objectRuntime
	if len(runtimeFactory) > 0 {
		rf = runtimeFactory[0]
	}
	return &BuildObject{
		vanadisURL:     strings.TrimRight(strings.TrimSpace(vanadisURL), "/"),
		httpClient:     httpClient,
		runtimeFactory: rf,
	}
}

// ServiceName returns the Restate service name for this Virtual Object.
func (*BuildObject) ServiceName() string {
	return "Build"
}

func (b *BuildObject) getRuntime(ctx restate.ObjectContext) objectRuntime {
	if b != nil && b.runtimeFactory != nil {
		return b.runtimeFactory(ctx)
	}
	return restateObjectRuntime{ctx: ctx}
}

// Reconcile reconciles the build state on Restate.
func (b *BuildObject) Reconcile(ctx restate.ObjectContext, req ReconcileBuildRequest) (ReconcileBuildResult, error) {
	_ = req
	if b == nil || strings.TrimSpace(b.vanadisURL) == "" {
		return ReconcileBuildResult{}, restate.TerminalErrorf("vanadis url not configured")
	}
	rt := b.getRuntime(ctx)
	return b.executeObjectTransition(rt, "reconcile-build", nil)
}

// Mutate applies a mutation to the build and reconciles state.
func (b *BuildObject) Mutate(ctx restate.ObjectContext, req BuildMutationRequest) (ReconcileBuildResult, error) {
	if b == nil || strings.TrimSpace(b.vanadisURL) == "" {
		return ReconcileBuildResult{}, restate.TerminalErrorf("vanadis url not configured")
	}
	rt := b.getRuntime(ctx)
	return b.executeObjectTransition(rt, "mutate-build", &req)
}

func (b *BuildObject) executeObjectTransition(rt objectRuntime, stepName string, mutation *BuildMutationRequest) (ReconcileBuildResult, error) {
	buildID := strings.TrimSpace(rt.Key())
	if buildID == "" {
		return ReconcileBuildResult{}, restate.TerminalErrorf("build_id cannot be empty")
	}

	priorState, err := rt.GetState()
	if err != nil {
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}

	priorLaunched, err := rt.GetLaunched()
	if err != nil {
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}

	if priorState != nil && len(priorState.Launched) > 0 {
		if priorLaunched == nil {
			priorLaunched = make(map[string]bool, len(priorState.Launched))
		}
		for k, v := range priorState.Launched {
			if v {
				priorLaunched[k] = true
			}
		}
	}

	stepOut, err := rt.RunReconcileStep(stepName, func(ctx context.Context) (reconcileStepOutput, error) {
		return b.callVanadisTransition(ctx, buildID, mutation, priorLaunched)
	})
	if err != nil {
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}

	if stepOut.BuildStatus == statusCompleted || stepOut.BuildStatus == statusFailed || stepOut.BuildStatus == statusCancelled || stepOut.BuildStatus == statusNotFound {
		rt.ClearState()
	} else {
		if stepOut.State != nil {
			rt.SetState(*stepOut.State)
		}
		rt.SetLaunched(stepOut.LaunchedMap)
	}

	return ReconcileBuildResult{
		BuildID:        buildID,
		Status:         stepOut.BuildStatus,
		LaunchedChunks: stepOut.LaunchedChunks,
		Error:          stepOut.Error,
	}, nil
}

func (b *BuildObject) callVanadisTransition(ctx context.Context, buildID string, mutation *BuildMutationRequest, priorLaunched map[string]bool) (reconcileStepOutput, error) {
	transReq := RestateTransitionRequest{
		Mutation:      mutation,
		PriorLaunched: priorLaunched,
	}
	bodyBytes, err := json.Marshal(transReq)
	if err != nil {
		return reconcileStepOutput{}, restate.TerminalErrorf("marshal transition request: %v", err)
	}

	targetURL := fmt.Sprintf("%s/internal/restate/builds/%s/transition", b.vanadisURL, buildID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return reconcileStepOutput{}, restate.TerminalErrorf("create transition request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := b.httpClient.Do(httpReq)
	if err != nil {
		return reconcileStepOutput{}, fmt.Errorf("transition request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, rErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if rErr != nil {
		return reconcileStepOutput{}, fmt.Errorf("read transition response: %w", rErr)
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return reconcileStepOutput{}, restate.TerminalErrorf("vanadis transition rejected (status %d): %s", resp.StatusCode, string(respBody))
	}
	if resp.StatusCode >= 500 || resp.StatusCode < 200 {
		return reconcileStepOutput{}, fmt.Errorf("vanadis transition server error (status %d): %s", resp.StatusCode, string(respBody))
	}

	var stepOut reconcileStepOutput
	if err := json.Unmarshal(respBody, &stepOut); err != nil {
		return reconcileStepOutput{}, restate.TerminalErrorf("unmarshal transition response: %v", err)
	}

	return stepOut, nil
}

// SetupHealthMux creates the ServeMux for health, metrics, and coverage flush.
func SetupHealthMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /healthz", handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())

	coverageFlush := httpkit.HandleCoverageFlush("vanadis-restate")
	mux.HandleFunc("POST /debug/coverage/flush", coverageFlush)
	mux.HandleFunc("GET /debug/coverage/flush", coverageFlush)
	return mux
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// RunApp runs both the health server and the Restate worker server.
func RunApp(ctx context.Context, cfg Config) error {
	healthMux := SetupHealthMux()
	healthServer := &http.Server{
		Handler:      healthMux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	var lc net.ListenConfig
	healthLn, err := lc.Listen(ctx, "tcp", ":"+cfg.HealthPort)
	if err != nil {
		return fmt.Errorf("listening on health port %s: %w", cfg.HealthPort, err)
	}

	errChan := make(chan error, 2)
	go func() {
		log.Printf("[vanadis-restate] Health server listening on :%s", cfg.HealthPort)
		if sErr := healthServer.Serve(healthLn); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
			errChan <- sErr
		}
	}()

	restateObj := NewBuildObject(cfg.VanadisURL, nil)
	restateServer := server.NewRestate().Bind(restate.Reflect(restateObj))

	go func() {
		log.Printf("[vanadis-restate] Restate HTTP/2 server listening on :%s (VanadisURL: %s)", cfg.RestatePort, cfg.VanadisURL)
		if sErr := restateServer.Start(ctx, ":"+cfg.RestatePort); sErr != nil && !errors.Is(sErr, context.Canceled) {
			errChan <- sErr
		}
	}()

	select {
	case <-ctx.Done():
	case rErr := <-errChan:
		err = rErr
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()
	if sErr := healthServer.Shutdown(shutdownCtx); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) && err == nil {
		err = sErr
	}

	log.Println("[vanadis-restate] Server stopped")
	return err
}

var (
	loadConfigFn = LoadConfigFromEnv
	runAppFn     = RunApp
	exitFn       = log.Fatalf
)

func main() {
	cfg := loadConfigFn()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runAppFn(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("[vanadis-restate] Error: %v", err)
	}
}
