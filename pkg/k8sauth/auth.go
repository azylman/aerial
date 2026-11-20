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
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// CallerIdentity encapsulates the verified identity of an authenticated caller.
type CallerIdentity struct {
	Username    string              `json:"username"`
	CallerAgent string              `json:"caller_agent"`
	Groups      []string            `json:"groups,omitempty"`
	Extra       map[string][]string `json:"extra,omitempty"`
	IsService   bool                `json:"is_service"`
}

// Config specifies the configuration options for the Authenticator.
type Config struct {
	Enabled          bool              `json:"enabled"           yaml:"enabled"`
	TokenPath        string            `json:"token_path"        yaml:"token_path"`
	AgentSAPrefix    string            `json:"agent_sa_prefix"   yaml:"agent_sa_prefix"`
	SAAgentAliases   map[string]string `json:"sa_agent_aliases"  yaml:"sa_agent_aliases"`
	ConfiguredAgents []string          `json:"configured_agents" yaml:"configured_agents"`
	CacheTTL         time.Duration     `json:"cache_ttl"         yaml:"cache_ttl"`
	CacheMaxEntries  int               `json:"cache_max_entries" yaml:"cache_max_entries"`
	APIServerURL     string            `json:"api_server_url"    yaml:"api_server_url"`
	HTTPClient       *http.Client      `json:"-"                 yaml:"-"`
	TokenSource      *TokenSource      `json:"-"                 yaml:"-"`
	Now              func() time.Time  `json:"-"                 yaml:"-"`
}

// ValidationError represents an HTTP 400 Bad Request error.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

func (*ValidationError) StatusCode() int {
	return http.StatusBadRequest
}

// UnauthorizedError represents an HTTP 401 Unauthorized error.
type UnauthorizedError struct {
	Message string
}

func (e *UnauthorizedError) Error() string {
	return e.Message
}

func (*UnauthorizedError) StatusCode() int {
	return http.StatusUnauthorized
}

// ForbiddenError represents an HTTP 403 Forbidden error.
type ForbiddenError struct {
	Message string
}

func (e *ForbiddenError) Error() string {
	return e.Message
}

func (*ForbiddenError) StatusCode() int {
	return http.StatusForbidden
}

type callerContextKey struct{}

// WithCallerIdentity stores the caller identity in the context.
func WithCallerIdentity(ctx context.Context, id CallerIdentity) context.Context {
	return context.WithValue(ctx, callerContextKey{}, id)
}

// CallerFromContext extracts the caller identity from the context if present.
func CallerFromContext(ctx context.Context) (CallerIdentity, bool) {
	id, ok := ctx.Value(callerContextKey{}).(CallerIdentity)
	return id, ok
}

// ExtractBearerToken extracts a bearer token from the HTTP Authorization header.
func ExtractBearerToken(r *http.Request) (string, error) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", &UnauthorizedError{Message: "missing Authorization header"}
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", &UnauthorizedError{Message: "invalid bearer token in Authorization header"}
	}
	return strings.TrimSpace(parts[1]), nil
}

// Authenticator evaluates Kubernetes TokenReview and SubjectAccessReview requests.
type Authenticator struct {
	cfg         Config
	tokenCache  *LRUCache[CallerIdentity]
	sarCache    *LRUCache[struct{}]
	tokenSource *TokenSource
}

// NewAuthenticator constructs an Authenticator with sensible defaults.
func NewAuthenticator(cfg Config) *Authenticator {
	if cfg.TokenPath == "" {
		cfg.TokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	}
	if cfg.AgentSAPrefix == "" {
		cfg.AgentSAPrefix = "system:serviceaccount:default:eri-"
	}
	if cfg.SAAgentAliases == nil {
		cfg.SAAgentAliases = map[string]string{
			"system:serviceaccount:default:eri": "aerial",
		}
	} else {
		aliases := make(map[string]string, len(cfg.SAAgentAliases))
		maps.Copy(aliases, cfg.SAAgentAliases)
		cfg.SAAgentAliases = aliases
	}
	if len(cfg.ConfiguredAgents) == 0 {
		cfg.ConfiguredAgents = []string{"aerial", "miorine", "suletta"}
	} else {
		agents := make([]string, len(cfg.ConfiguredAgents))
		copy(agents, cfg.ConfiguredAgents)
		cfg.ConfiguredAgents = agents
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
			cfg.APIServerURL = "https://kubernetes.default.svc"
		}
	}
	cfg.APIServerURL = strings.TrimRight(cfg.APIServerURL, "/")
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = defaultK8sHTTPClient(cfg.TokenPath)
	}

	ts := cfg.TokenSource
	if ts == nil {
		ts = NewTokenSource(cfg.TokenPath, cfg.Now)
	}

	return &Authenticator{
		cfg:         cfg,
		tokenCache:  NewLRUCache[CallerIdentity](cfg.CacheMaxEntries, cfg.CacheTTL, cfg.Now),
		sarCache:    NewLRUCache[struct{}](cfg.CacheMaxEntries, cfg.CacheTTL, cfg.Now),
		tokenSource: ts,
	}
}

func defaultK8sHTTPClient(tokenPath string) *http.Client {
	certPath := "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	if dir := filepath.Dir(tokenPath); dir != "." && dir != "/" {
		candidate := filepath.Join(dir, "ca.crt")
		if _, err := os.Stat(candidate); err == nil {
			certPath = candidate
		}
	}

	var tlsConfig *tls.Config
	if caCert, err := os.ReadFile(certPath); err == nil {
		rootCAs := x509.NewCertPool()
		if rootCAs.AppendCertsFromPEM(caCert) {
			tlsConfig = &tls.Config{RootCAs: rootCAs}
		}
	}

	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
}

type tokenReviewRequest struct {
	APIVersion string                 `json:"apiVersion"`
	Kind       string                 `json:"kind"`
	Spec       tokenReviewRequestSpec `json:"spec"`
}

type tokenReviewRequestSpec struct {
	Token string `json:"token"`
}

type tokenReviewResponse struct {
	APIVersion string                    `json:"apiVersion"`
	Kind       string                    `json:"kind"`
	Status     tokenReviewResponseStatus `json:"status"`
}

type tokenReviewResponseStatus struct {
	Authenticated bool            `json:"authenticated"`
	User          tokenReviewUser `json:"user"`
	Error         string          `json:"error,omitempty"`
}

type tokenReviewUser struct {
	Username string              `json:"username"`
	UID      string              `json:"uid,omitempty"`
	Groups   []string            `json:"groups,omitempty"`
	Extra    map[string][]string `json:"extra,omitempty"`
}

type subjectAccessReviewRequest struct {
	APIVersion string                         `json:"apiVersion"`
	Kind       string                         `json:"kind"`
	Spec       subjectAccessReviewRequestSpec `json:"spec"`
}

type subjectAccessReviewRequestSpec struct {
	User               string                                `json:"user"`
	Groups             []string                              `json:"groups,omitempty"`
	Extra              map[string][]string                   `json:"extra,omitempty"`
	ResourceAttributes subjectAccessReviewResourceAttributes `json:"resourceAttributes"`
}

type subjectAccessReviewResourceAttributes struct {
	Group    string `json:"group"`
	Resource string `json:"resource"`
	Verb     string `json:"verb"`
	Name     string `json:"name,omitempty"`
}

type subjectAccessReviewResponse struct {
	APIVersion string                            `json:"apiVersion"`
	Kind       string                            `json:"kind"`
	Status     subjectAccessReviewResponseStatus `json:"status"`
}

type subjectAccessReviewResponseStatus struct {
	Allowed bool   `json:"allowed"`
	Denied  bool   `json:"denied,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

func (a *Authenticator) mapUserToCaller(user tokenReviewUser) CallerIdentity {
	identity := CallerIdentity{
		Username: user.Username,
		Groups:   user.Groups,
		Extra:    user.Extra,
	}

	if alias, ok := a.cfg.SAAgentAliases[user.Username]; ok {
		identity.CallerAgent = alias
		identity.IsService = false
		return identity
	}

	saName := user.Username
	parts := strings.Split(user.Username, ":")
	if len(parts) >= 4 && parts[0] == "system" && parts[1] == "serviceaccount" {
		saName = parts[3]
	}

	if alias, ok := a.cfg.SAAgentAliases[saName]; ok {
		identity.CallerAgent = alias
		identity.IsService = false
		return identity
	}

	if a.cfg.AgentSAPrefix != "" && strings.HasPrefix(user.Username, a.cfg.AgentSAPrefix) {
		identity.CallerAgent = strings.TrimPrefix(user.Username, a.cfg.AgentSAPrefix)
		identity.IsService = false
		return identity
	}

	if a.cfg.AgentSAPrefix != "" && strings.HasPrefix(saName, a.cfg.AgentSAPrefix) {
		identity.CallerAgent = strings.TrimPrefix(saName, a.cfg.AgentSAPrefix)
		identity.IsService = false
		return identity
	}

	identity.CallerAgent = saName
	identity.IsService = true
	return identity
}

func (a *Authenticator) postAPI(ctx context.Context, path string, reqBody any, respTarget any) error {
	hostToken, err := a.tokenSource.Token()
	if err != nil {
		return fmt.Errorf("read host token: %w", err)
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.APIServerURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+hostToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.cfg.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("api returned status %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(respTarget); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// Authenticate verifies a bearer token using TokenReview, caching positive outcomes.
func (a *Authenticator) Authenticate(ctx context.Context, bearerToken string) (CallerIdentity, error) {
	if bearerToken == "" {
		return CallerIdentity{}, &UnauthorizedError{Message: "empty bearer token"}
	}

	h := sha256.Sum256([]byte(bearerToken))
	cacheKey := hex.EncodeToString(h[:])

	if id, ok := a.tokenCache.Get(cacheKey); ok {
		return id, nil
	}

	reqBody := tokenReviewRequest{
		APIVersion: "authentication.k8s.io/v1",
		Kind:       "TokenReview",
		Spec: tokenReviewRequestSpec{
			Token: bearerToken,
		},
	}

	var trResp tokenReviewResponse
	if err := a.postAPI(ctx, "/apis/authentication.k8s.io/v1/tokenreviews", reqBody, &trResp); err != nil {
		return CallerIdentity{}, fmt.Errorf("k8sauth: tokenreview: %w", err)
	}

	if !trResp.Status.Authenticated {
		msg := trResp.Status.Error
		if msg == "" {
			msg = "token not authenticated"
		}
		return CallerIdentity{}, &UnauthorizedError{Message: msg}
	}

	identity := a.mapUserToCaller(trResp.Status.User)
	a.tokenCache.Set(cacheKey, identity)
	return identity, nil
}

// Authorize verifies that caller has permissions for the resource, verb, and targetAgent in aerial.io.
func (a *Authenticator) Authorize(ctx context.Context, caller CallerIdentity, resource, verb, targetAgent string) error {
	sarKey := caller.Username + "\x00" + resource + "\x00" + verb + "\x00" + targetAgent
	if _, ok := a.sarCache.Get(sarKey); ok {
		return nil
	}

	reqBody := subjectAccessReviewRequest{
		APIVersion: "authorization.k8s.io/v1",
		Kind:       "SubjectAccessReview",
		Spec: subjectAccessReviewRequestSpec{
			User:   caller.Username,
			Groups: caller.Groups,
			Extra:  caller.Extra,
			ResourceAttributes: subjectAccessReviewResourceAttributes{
				Group:    "aerial.io",
				Resource: resource,
				Verb:     verb,
				Name:     targetAgent,
			},
		},
	}

	var sarResp subjectAccessReviewResponse
	if err := a.postAPI(ctx, "/apis/authorization.k8s.io/v1/subjectaccessreviews", reqBody, &sarResp); err != nil {
		return fmt.Errorf("k8sauth: subjectaccessreview: %w", err)
	}

	if !sarResp.Status.Allowed {
		msg := sarResp.Status.Reason
		if msg == "" {
			msg = fmt.Sprintf("access denied for user %q on %s/%s (target: %q)", caller.Username, resource, verb, targetAgent)
		}
		return &ForbiddenError{Message: msg}
	}

	a.sarCache.Set(sarKey, struct{}{})
	return nil
}

func isReadVerb(verb string) bool {
	v := strings.ToLower(strings.TrimSpace(verb))
	return v == "get" || v == "list"
}

func (a *Authenticator) isConfiguredAgent(agent string) bool {
	return slices.Contains(a.cfg.ConfiguredAgents, agent)
}

// ResolveTargetAgent determines the target agent and the evaluated SAR resource string.
func (a *Authenticator) ResolveTargetAgent(caller CallerIdentity, requestedTarget, baseResource, verb string) (targetAgent string, sarResource string, err error) {
	if isReadVerb(verb) {
		if requestedTarget != "" && requestedTarget != "*" && !a.isConfiguredAgent(requestedTarget) {
			return "", "", &ValidationError{
				Message: fmt.Sprintf("unknown target agent %q", requestedTarget),
			}
		}
		return requestedTarget, baseResource, nil
	}

	// Write verbs (create, update, patch, delete)
	if caller.IsService {
		if requestedTarget == "" || !a.isConfiguredAgent(requestedTarget) {
			return "", "", &ValidationError{
				Message: "missing or unknown target agent for service principal",
			}
		}
		return requestedTarget, baseResource + "/all", nil
	}

	// Agent caller
	if requestedTarget == "" {
		targetAgent = caller.CallerAgent
	} else {
		if requestedTarget != "*" && !a.isConfiguredAgent(requestedTarget) {
			return "", "", &ValidationError{
				Message: fmt.Sprintf("unknown target agent %q", requestedTarget),
			}
		}
		targetAgent = requestedTarget
	}

	if targetAgent == caller.CallerAgent {
		return targetAgent, baseResource, nil
	}
	return targetAgent, baseResource + "/all", nil
}

// AuthorizeTarget combines target resolution with SubjectAccessReview authorization.
func (a *Authenticator) AuthorizeTarget(ctx context.Context, caller CallerIdentity, baseResource, verb, requestedTarget string) (string, error) {
	targetAgent, sarResource, err := a.ResolveTargetAgent(caller, requestedTarget, baseResource, verb)
	if err != nil {
		return "", err
	}
	if err := a.Authorize(ctx, caller, sarResource, verb, targetAgent); err != nil {
		return "", err
	}
	return targetAgent, nil
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSONError(w http.ResponseWriter, statusCode int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(errorResponse{Error: msg}); err != nil {
		return
	}
}

// Middleware creates HTTP middleware that authenticates requests using bearer tokens.
func (a *Authenticator) Middleware(exemptPaths ...string) func(http.Handler) http.Handler {
	exempt := make(map[string]struct{}, len(exemptPaths)*2)
	for _, p := range exemptPaths {
		exempt[p] = struct{}{}
		if trimmed, ok := strings.CutSuffix(p, "/"); ok {
			exempt[trimmed] = struct{}{}
		} else {
			exempt[p+"/"] = struct{}{}
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !a.cfg.Enabled {
				next.ServeHTTP(w, r)
				return
			}

			if _, ok := exempt[r.URL.Path]; ok {
				next.ServeHTTP(w, r)
				return
			}

			token, err := ExtractBearerToken(r)
			if err != nil {
				writeJSONError(w, http.StatusUnauthorized, err.Error())
				return
			}

			caller, err := a.Authenticate(r.Context(), token)
			if err != nil {
				writeJSONError(w, http.StatusUnauthorized, err.Error())
				return
			}

			ctx := WithCallerIdentity(r.Context(), caller)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
