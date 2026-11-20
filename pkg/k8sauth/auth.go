package k8sauth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"
)

// Default Kubernetes service account mount and API server constants.
const (
	DefaultAgentSAPrefix        = "system:serviceaccount:default:eri-"
	DefaultAPIServerURL         = "https://kubernetes.default.svc"
	DefaultServiceAccountCAPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

// CallerIdentity represents the authenticated principal and its agent mapping.
type CallerIdentity struct {
	Username    string              `json:"username"`
	CallerAgent string              `json:"caller_agent"`
	Groups      []string            `json:"groups"`
	Extra       map[string][]string `json:"extra,omitempty"`
	IsService   bool                `json:"is_service"`
}

// Config specifies the runtime settings for the Authenticator.
type Config struct {
	Enabled          bool              `json:"enabled"           yaml:"enabled"`
	TokenPath        string            `json:"token_path"        yaml:"token_path"`
	APIServerURL     string            `json:"api_server_url"    yaml:"api_server_url"`
	AgentSAPrefix    string            `json:"agent_sa_prefix"   yaml:"agent_sa_prefix"`
	SAAgentAliases   map[string]string `json:"sa_agent_aliases"  yaml:"sa_agent_aliases"`
	ConfiguredAgents []string          `json:"configured_agents" yaml:"configured_agents"`
	CacheTTL         time.Duration     `json:"cache_ttl"         yaml:"cache_ttl"`
	CacheMaxEntries  int               `json:"cache_max_entries" yaml:"cache_max_entries"`
	HTTPClient       *http.Client      `json:"-"                 yaml:"-"`
	TokenSource      *TokenSource      `json:"-"                 yaml:"-"`
	Now              func() time.Time  `json:"-"                 yaml:"-"`
}

// ValidationError indicates a client error (e.g. missing or unknown target agent).
type ValidationError struct {
	Message string `json:"error"`
}

func (e *ValidationError) Error() string {
	return e.Message
}

// NewValidationError creates a new ValidationError.
func NewValidationError(msg string) *ValidationError {
	return &ValidationError{Message: msg}
}

// UnauthorizedError indicates an authentication failure (HTTP 401).
type UnauthorizedError struct {
	Message string `json:"error"`
}

func (e *UnauthorizedError) Error() string {
	return e.Message
}

// NewUnauthorizedError creates a new UnauthorizedError.
func NewUnauthorizedError(msg string) *UnauthorizedError {
	return &UnauthorizedError{Message: msg}
}

// ForbiddenError indicates an authorization rejection (HTTP 403).
type ForbiddenError struct {
	Message string `json:"error"`
}

func (e *ForbiddenError) Error() string {
	return e.Message
}

// NewForbiddenError creates a new ForbiddenError.
func NewForbiddenError(msg string) *ForbiddenError {
	return &ForbiddenError{Message: msg}
}

type callerIdentityKey struct{}

// WithCallerIdentity stores CallerIdentity in the context.
func WithCallerIdentity(ctx context.Context, id CallerIdentity) context.Context {
	return context.WithValue(ctx, callerIdentityKey{}, id)
}

// CallerFromContext extracts CallerIdentity from the context if present.
func CallerFromContext(ctx context.Context) (CallerIdentity, bool) {
	if ctx == nil {
		return CallerIdentity{}, false
	}
	id, ok := ctx.Value(callerIdentityKey{}).(CallerIdentity)
	return id, ok
}

// ExtractBearerToken parses the Bearer token from the Authorization header.
func ExtractBearerToken(r *http.Request) (string, error) {
	if r == nil {
		return "", NewUnauthorizedError("nil request")
	}
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", NewUnauthorizedError("missing Authorization header")
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", NewUnauthorizedError("invalid Authorization header format")
	}
	return strings.TrimSpace(parts[1]), nil
}

// Authenticator provides Kubernetes TokenReview and SubjectAccessReview authentication/authorization.
type Authenticator struct {
	cfg              Config
	tokenSource      *TokenSource
	tokenReviewCache *LRUCache[CallerIdentity]
	sarCache         *LRUCache[bool]
	httpClient       *http.Client
}

// NewAuthenticator constructs an Authenticator applying default settings where omitted.
func NewAuthenticator(cfg Config) *Authenticator {
	if cfg.TokenPath == "" {
		cfg.TokenPath = DefaultServiceAccountTokenPath
	}
	if cfg.AgentSAPrefix == "" {
		cfg.AgentSAPrefix = DefaultAgentSAPrefix
	}
	if len(cfg.SAAgentAliases) == 0 {
		cfg.SAAgentAliases = map[string]string{
			"system:serviceaccount:default:eri": "aerial",
		}
	} else {
		cloned := make(map[string]string, len(cfg.SAAgentAliases))
		maps.Copy(cloned, cfg.SAAgentAliases)
		cfg.SAAgentAliases = cloned
	}
	if len(cfg.ConfiguredAgents) == 0 {
		cfg.ConfiguredAgents = []string{"aerial", "miorine", "suletta"}
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 45 * time.Second
	}
	if cfg.CacheMaxEntries <= 0 {
		cfg.CacheMaxEntries = 1024
	}
	if cfg.APIServerURL == "" {
		host := os.Getenv("KUBERNETES_SERVICE_HOST")
		port := os.Getenv("KUBERNETES_SERVICE_PORT")
		if host != "" && port != "" {
			cfg.APIServerURL = "https://" + net.JoinHostPort(host, port)
		} else {
			cfg.APIServerURL = DefaultAPIServerURL
		}
	}
	cfg.APIServerURL = strings.TrimRight(cfg.APIServerURL, "/")

	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		var tlsConfig *tls.Config
		caCert, err := os.ReadFile(DefaultServiceAccountCAPath)
		if err == nil {
			rootCAs := x509.NewCertPool()
			if rootCAs.AppendCertsFromPEM(caCert) {
				tlsConfig = &tls.Config{
					RootCAs:    rootCAs,
					MinVersion: tls.VersionTLS12,
				}
			}
		}
		httpClient = &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		}
	}

	ts := cfg.TokenSource
	if ts == nil {
		ts = NewTokenSource(cfg.TokenPath, WithTokenSourceClock(now))
	}

	return &Authenticator{
		cfg:              cfg,
		tokenSource:      ts,
		tokenReviewCache: NewLRUCache[CallerIdentity](cfg.CacheMaxEntries, cfg.CacheTTL, now),
		sarCache:         NewLRUCache[bool](cfg.CacheMaxEntries, cfg.CacheTTL, now),
		httpClient:       httpClient,
	}
}

// Config returns a copy of the Authenticator configuration.
func (a *Authenticator) Config() Config {
	return a.cfg
}

// TokenReview request/response schemas.
type tokenReviewSpec struct {
	Token string `json:"token"`
}

type tokenReviewRequest struct {
	APIVersion string          `json:"apiVersion"`
	Kind       string          `json:"kind"`
	Spec       tokenReviewSpec `json:"spec"`
}

type tokenReviewUser struct {
	Username string              `json:"username"`
	UID      string              `json:"uid,omitempty"`
	Groups   []string            `json:"groups,omitempty"`
	Extra    map[string][]string `json:"extra,omitempty"`
}

type tokenReviewStatus struct {
	Authenticated bool            `json:"authenticated"`
	User          tokenReviewUser `json:"user,omitempty"`
	Error         string          `json:"error,omitempty"`
}

type tokenReviewResponse struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Status     tokenReviewStatus `json:"status"`
}

func (a *Authenticator) resolveAgentFromSA(username, saName string) (string, bool) {
	if alias, ok := a.cfg.SAAgentAliases[username]; ok {
		return alias, false
	}
	if alias, ok := a.cfg.SAAgentAliases[saName]; ok {
		return alias, false
	}
	if a.cfg.AgentSAPrefix != "" && strings.HasPrefix(username, a.cfg.AgentSAPrefix) {
		return strings.TrimPrefix(username, a.cfg.AgentSAPrefix), false
	}

	cleanPrefix := a.cfg.AgentSAPrefix
	if idx := strings.LastIndex(cleanPrefix, ":"); idx >= 0 {
		cleanPrefix = cleanPrefix[idx+1:]
	}
	if cleanPrefix != "" && strings.HasPrefix(saName, cleanPrefix) {
		return strings.TrimPrefix(saName, cleanPrefix), false
	}

	return saName, true
}

func (a *Authenticator) mapUserToCaller(user tokenReviewUser) CallerIdentity {
	username := user.Username
	saName := username
	if strings.HasPrefix(username, "system:serviceaccount:") {
		parts := strings.Split(username, ":")
		if len(parts) >= 4 {
			saName = parts[3]
		}
	}

	callerAgent, isService := a.resolveAgentFromSA(username, saName)
	return CallerIdentity{
		Username:    username,
		CallerAgent: callerAgent,
		Groups:      user.Groups,
		Extra:       user.Extra,
		IsService:   isService,
	}
}

// Authenticate verifies bearerToken using Kubernetes TokenReview and maps it to CallerIdentity.
func (a *Authenticator) Authenticate(ctx context.Context, bearerToken string) (CallerIdentity, error) {
	trimmedToken := strings.TrimSpace(bearerToken)
	if trimmedToken == "" {
		return CallerIdentity{}, NewUnauthorizedError("empty bearer token")
	}

	h := sha256.Sum256([]byte(trimmedToken))
	cacheKey := hex.EncodeToString(h[:])

	if id, ok := a.tokenReviewCache.Get(cacheKey); ok {
		return id, nil
	}

	trReq := tokenReviewRequest{
		APIVersion: "authentication.k8s.io/v1",
		Kind:       "TokenReview",
		Spec:       tokenReviewSpec{Token: trimmedToken},
	}

	bodyBytes, err := json.Marshal(trReq)
	if err != nil {
		return CallerIdentity{}, fmt.Errorf("marshal TokenReview: %w", err)
	}

	hostToken, err := a.tokenSource.Token()
	if err != nil {
		return CallerIdentity{}, fmt.Errorf("read serviceaccount token: %w", err)
	}

	endpoint := a.cfg.APIServerURL + "/apis/authentication.k8s.io/v1/tokenreviews"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return CallerIdentity{}, fmt.Errorf("create TokenReview request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+hostToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return CallerIdentity{}, fmt.Errorf("execute TokenReview: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return CallerIdentity{}, fmt.Errorf("tokenreview returned status %d", resp.StatusCode)
		}
		return CallerIdentity{}, fmt.Errorf("tokenreview returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var trResp tokenReviewResponse
	if err := json.NewDecoder(resp.Body).Decode(&trResp); err != nil {
		return CallerIdentity{}, fmt.Errorf("decode TokenReview response: %w", err)
	}

	if !trResp.Status.Authenticated {
		return CallerIdentity{}, NewUnauthorizedError("token not authenticated")
	}

	caller := a.mapUserToCaller(trResp.Status.User)
	a.tokenReviewCache.Set(cacheKey, caller)
	return caller, nil
}

// SubjectAccessReview request/response schemas.
type resourceAttributes struct {
	Group    string `json:"group"`
	Resource string `json:"resource"`
	Verb     string `json:"verb"`
	Name     string `json:"name,omitempty"`
}

type sarSpec struct {
	User               string              `json:"user"`
	Groups             []string            `json:"groups,omitempty"`
	Extra              map[string][]string `json:"extra,omitempty"`
	ResourceAttributes resourceAttributes  `json:"resourceAttributes"`
}

type sarRequest struct {
	APIVersion string  `json:"apiVersion"`
	Kind       string  `json:"kind"`
	Spec       sarSpec `json:"spec"`
}

type sarStatus struct {
	Allowed         bool   `json:"allowed"`
	Denied          bool   `json:"denied,omitempty"`
	Reason          string `json:"reason,omitempty"`
	EvaluationError string `json:"evaluationError,omitempty"`
}

type sarResponse struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Status     sarStatus `json:"status"`
}

// Authorize checks whether caller has permission to perform verb on resource for targetAgent.
func (a *Authenticator) Authorize(ctx context.Context, caller CallerIdentity, resource, verb, targetAgent string) error {
	key := fmt.Sprintf("%s:%s:%s:%s:%s", caller.Username, "aerial.io", resource, verb, targetAgent)
	if allowed, ok := a.sarCache.Get(key); ok && allowed {
		return nil
	}

	sarReq := sarRequest{
		APIVersion: "authorization.k8s.io/v1",
		Kind:       "SubjectAccessReview",
		Spec: sarSpec{
			User:   caller.Username,
			Groups: caller.Groups,
			Extra:  caller.Extra,
			ResourceAttributes: resourceAttributes{
				Group:    "aerial.io",
				Resource: resource,
				Verb:     verb,
				Name:     targetAgent,
			},
		},
	}

	bodyBytes, err := json.Marshal(sarReq)
	if err != nil {
		return fmt.Errorf("marshal SubjectAccessReview: %w", err)
	}

	hostToken, err := a.tokenSource.Token()
	if err != nil {
		return fmt.Errorf("read serviceaccount token: %w", err)
	}

	endpoint := a.cfg.APIServerURL + "/apis/authorization.k8s.io/v1/subjectaccessreviews"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create SubjectAccessReview request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+hostToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute SubjectAccessReview: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("subjectaccessreview returned status %d", resp.StatusCode)
		}
		return fmt.Errorf("subjectaccessreview returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var sarResp sarResponse
	if err := json.NewDecoder(resp.Body).Decode(&sarResp); err != nil {
		return fmt.Errorf("decode SubjectAccessReview response: %w", err)
	}

	if !sarResp.Status.Allowed {
		msg := sarResp.Status.Reason
		if msg == "" {
			msg = fmt.Sprintf("forbidden: user %q is not authorized to %s %s", caller.Username, verb, resource)
		}
		return NewForbiddenError(msg)
	}

	a.sarCache.Set(key, true)
	return nil
}

func isReadVerb(verb string) bool {
	switch strings.ToLower(verb) {
	case "get", "list", "watch":
		return true
	default:
		return false
	}
}

func (a *Authenticator) isConfiguredAgent(target string) bool {
	return slices.Contains(a.cfg.ConfiguredAgents, target)
}

// ResolveTargetAgent resolves the owning agent from caller identity, requested target, and operation verb.
func (a *Authenticator) ResolveTargetAgent(caller CallerIdentity, requestedTarget, baseResource, verb string) (string, error) {
	if isReadVerb(verb) {
		if requestedTarget != "" && requestedTarget != "*" {
			if !a.isConfiguredAgent(requestedTarget) {
				return "", NewValidationError(fmt.Sprintf("unknown target agent: %q", requestedTarget))
			}
		}
		return requestedTarget, nil
	}

	// Write verbs (create, update, patch, delete)
	if caller.IsService {
		if requestedTarget == "" || requestedTarget == "*" || !a.isConfiguredAgent(requestedTarget) {
			return "", NewValidationError("missing or unknown target agent for service principal")
		}
		return requestedTarget, nil
	}

	// Agent principal
	if requestedTarget == "" {
		return caller.CallerAgent, nil
	}
	if requestedTarget != "*" && !a.isConfiguredAgent(requestedTarget) {
		return "", NewValidationError(fmt.Sprintf("unknown target agent: %q", requestedTarget))
	}
	return requestedTarget, nil
}

// AuthorizeTarget resolves the target agent, determines the appropriate SAR resource (escalating cross-agent/service writes to <resource>/all), and evaluates authorization.
func (a *Authenticator) AuthorizeTarget(ctx context.Context, caller CallerIdentity, baseResource, verb, requestedTarget string) (string, error) {
	targetAgent, err := a.ResolveTargetAgent(caller, requestedTarget, baseResource, verb)
	if err != nil {
		return "", err
	}

	sarResource := baseResource
	if !isReadVerb(verb) {
		if caller.IsService || caller.CallerAgent != targetAgent || targetAgent == "*" {
			sarResource = baseResource + "/all"
		}
	}

	if err := a.Authorize(ctx, caller, sarResource, verb, targetAgent); err != nil {
		return "", err
	}
	return targetAgent, nil
}

// Middleware returns an HTTP middleware verifying the Kubernetes bearer token on non-exempt paths.
func (a *Authenticator) Middleware(exemptPaths ...string) func(http.Handler) http.Handler {
	exemptMap := make(map[string]struct{}, len(exemptPaths))
	for _, p := range exemptPaths {
		exemptMap[p] = struct{}{}
		if strings.HasSuffix(p, "/") && len(p) > 1 {
			exemptMap[strings.TrimSuffix(p, "/")] = struct{}{}
		} else if !strings.HasSuffix(p, "/") {
			exemptMap[p+"/"] = struct{}{}
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !a.cfg.Enabled {
				next.ServeHTTP(w, r)
				return
			}

			if _, exempt := exemptMap[r.URL.Path]; exempt {
				next.ServeHTTP(w, r)
				return
			}

			token, err := ExtractBearerToken(r)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				if encErr := json.NewEncoder(w).Encode(map[string]string{"error": err.Error()}); encErr != nil {
					return
				}
				return
			}

			caller, err := a.Authenticate(r.Context(), token)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				if encErr := json.NewEncoder(w).Encode(map[string]string{"error": err.Error()}); encErr != nil {
					return
				}
				return
			}

			ctx := WithCallerIdentity(r.Context(), caller)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
