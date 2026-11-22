// Package restateingresstest is a recording fake Restate ingress for tests.
package restateingresstest

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// Response is the canned reply for one path. Status 0 means 200.
type Response struct {
	Status int
	Body   string
}

// Call is one request the fake ingress received.
type Call struct {
	Method         string
	Path           string
	IdempotencyKey string
	ContentType    string
	// Header is a clone of the request headers, for presence checks.
	Header http.Header
	Body   []byte
}

// Server is a fake Restate ingress. Set Routes (keyed by URL path) before
// the first URL call; unknown paths reply 200 with an empty body. The
// httptest server starts on the first URL call and closes via t.Cleanup.
type Server struct {
	Routes map[string]Response

	t     testing.TB
	once  sync.Once
	srv   *httptest.Server
	mu    sync.Mutex
	calls []Call
	ioErr error
}

// New returns an unstarted fake ingress bound to t.
func New(t testing.TB) *Server {
	t.Helper()
	return &Server{t: t}
}

// URL starts the server on first use and returns its base URL.
func (s *Server) URL() string {
	s.once.Do(func() {
		s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
		s.t.Cleanup(s.srv.Close)
	})
	return s.srv.URL
}

// Client starts the server on first use and returns an HTTP client for it.
func (s *Server) Client() *http.Client {
	s.URL()
	return s.srv.Client()
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	body, readErr := io.ReadAll(r.Body)
	s.mu.Lock()
	s.ioErr = errors.Join(s.ioErr, readErr)
	s.calls = append(s.calls, Call{
		Method:         r.Method,
		Path:           r.URL.Path,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		ContentType:    r.Header.Get("Content-Type"),
		Header:         r.Header.Clone(),
		Body:           body,
	})
	s.mu.Unlock()
	resp := s.Routes[r.URL.Path]
	if resp.Status == 0 {
		resp.Status = http.StatusOK
	}
	w.WriteHeader(resp.Status)
	_, writeErr := io.WriteString(w, resp.Body)
	s.mu.Lock()
	s.ioErr = errors.Join(s.ioErr, writeErr)
	s.mu.Unlock()
}

// Err returns every error the fake hit reading request bodies or writing
// responses, joined; nil when every exchange was clean.
func (s *Server) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ioErr
}

// Calls returns a copy of the requests received so far, in order.
func (s *Server) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Call(nil), s.calls...)
}
