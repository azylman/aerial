// Package k8sauthtest provides a declarative fake Kubernetes API for tests of
// k8sauth callers: TokenReview and SubjectAccessReview responses are set as
// fields, and every request is recorded.
package k8sauthtest

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/pkg/k8sauth"
)

const (
	tokenReviewPath = "tokenreviews"
	sarPath         = "subjectaccessreviews"
)

// Request is one request the fake received.
type Request struct {
	Path string
	Body string
}

// RoundTripper fakes the Kubernetes API server. A zero status means 200.
//
// SARRules, when non-empty, decides SubjectAccessReview answers from the
// decoded resourceAttributes instead of SARBody: keys are SARKey(resource,
// verb, name), an empty part matches anything, and the review is allowed when
// any matching key maps to true.
type RoundTripper struct {
	AuthStatus int
	AuthBody   string
	SARStatus  int
	SARBody    string
	SARRules   map[string]bool
	Err        error

	mu       sync.Mutex
	requests []Request
}

// SARKey builds a SARRules key.
func SARKey(resource, verb, name string) string {
	return resource + "|" + verb + "|" + name
}

// RoundTrip implements http.RoundTripper.
func (rt *RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	var readErr error
	if req.Body != nil {
		body, readErr = io.ReadAll(req.Body)
		_ = req.Body.Close()
	}
	rt.mu.Lock()
	rt.requests = append(rt.requests, Request{Path: req.URL.Path, Body: string(body)})
	rt.mu.Unlock()

	if readErr != nil {
		return nil, readErr
	}
	if rt.Err != nil {
		return nil, rt.Err
	}
	status, respBody := http.StatusOK, `{}`
	switch {
	case strings.Contains(req.URL.Path, tokenReviewPath):
		status, respBody = orOK(rt.AuthStatus), rt.AuthBody
	case strings.Contains(req.URL.Path, sarPath):
		status, respBody = orOK(rt.SARStatus), rt.sarResponse(body)
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewBufferString(respBody)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

// Requests returns a copy of every request received so far.
func (rt *RoundTripper) Requests() []Request {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return append([]Request(nil), rt.requests...)
}

func (rt *RoundTripper) sarResponse(body []byte) string {
	if len(rt.SARRules) == 0 {
		if rt.SARBody != "" {
			return rt.SARBody
		}
		return SAR(false, "")
	}
	var sarReq struct {
		Spec struct {
			ResourceAttributes struct {
				Resource string `json:"resource"`
				Verb     string `json:"verb"`
				Name     string `json:"name"`
			} `json:"resourceAttributes"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(body, &sarReq); err != nil {
		return SAR(false, "")
	}
	attrs := sarReq.Spec.ResourceAttributes
	for key, allowed := range rt.SARRules {
		if allowed && ruleMatches(key, attrs.Resource, attrs.Verb, attrs.Name) {
			return SAR(true, "")
		}
	}
	return SAR(false, "")
}

func ruleMatches(key string, got ...string) bool {
	parts := strings.SplitN(key, "|", len(got))
	if len(parts) != len(got) {
		return false
	}
	for i, p := range parts {
		if p != "" && p != got[i] {
			return false
		}
	}
	return true
}

func orOK(status int) int {
	if status == 0 {
		return http.StatusOK
	}
	return status
}

type tokenReviewUser struct {
	Username string `json:"username"`
}

type tokenReviewStatus struct {
	Authenticated bool             `json:"authenticated"`
	User          *tokenReviewUser `json:"user,omitempty"`
}

type sarStatus struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason,omitempty"`
}

type review[S any] struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Status     S      `json:"status"`
}

// TokenReview returns an authenticated TokenReview response for user.
func TokenReview(user string) string {
	return mustJSON(review[tokenReviewStatus]{"authentication.k8s.io/v1", "TokenReview", tokenReviewStatus{true, &tokenReviewUser{user}}})
}

// TokenReviewDenied returns an unauthenticated TokenReview response.
func TokenReviewDenied() string {
	return mustJSON(review[tokenReviewStatus]{"authentication.k8s.io/v1", "TokenReview", tokenReviewStatus{}})
}

// SAR returns a SubjectAccessReview response.
func SAR(allowed bool, reason string) string {
	return mustJSON(review[sarStatus]{"authorization.k8s.io/v1", "SubjectAccessReview", sarStatus{allowed, reason}})
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // unreachable: the review types are plain strings and bools
	}
	return string(b)
}

// Config returns an enabled k8sauth.Config that talks to rt, with a temporary
// service-account token file.
func Config(t testing.TB, rt *RoundTripper) k8sauth.Config {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("host-sa-token"), 0o600); err != nil {
		t.Fatalf("write service-account token: %v", err)
	}
	return k8sauth.Config{
		Enabled:      true,
		TokenPath:    tokenFile,
		APIServerURL: "https://k8s.example.com",
		HTTPClient:   &http.Client{Transport: rt},
	}
}

// Authenticator returns a k8sauth.Authenticator built from Config(t, rt).
func Authenticator(t testing.TB, rt *RoundTripper) *k8sauth.Authenticator {
	t.Helper()
	return k8sauth.NewAuthenticator(Config(t, rt))
}
