// Package githubapitest is a route-scripted fake GitHub REST API for tests.
package githubapitest

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// Response is one canned GitHub response. Status 0 means 200; Remaining and
// RetryAfter set X-Ratelimit-Remaining and Retry-After when non-empty.
type Response struct {
	Status     int
	Body       string
	Remaining  string
	RetryAfter string
}

// Request is one request the fake API saw. Path includes the query string.
type Request struct {
	Method      string
	Path        string
	Auth        string
	Accept      string
	ContentType string
	Body        string
}

// Server serves canned responses per "METHOD path?query" in order; the last
// response of a route repeats. Unknown routes return 404 with GitHub's
// {"message":"Not Found"} body.
type Server struct {
	srv *httptest.Server

	mu        sync.Mutex
	responses map[string][]Response
	requests  []Request
	ioErr     error
}

// New starts a fake API serving routes (copied, so the caller's map is never
// mutated) and closes it via t.Cleanup.
func New(t testing.TB, routes map[string][]Response) *Server {
	t.Helper()
	s := &Server{responses: make(map[string][]Response, len(routes))}
	for route, queue := range routes {
		s.responses[route] = append([]Response(nil), queue...)
	}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.srv.Close)
	return s
}

// URL returns the server's base URL (no trailing slash).
func (s *Server) URL() string { return s.srv.URL }

// Client returns an HTTP client for the server.
func (s *Server) Client() *http.Client { return s.srv.Client() }

// Close shuts the server down early, e.g. to provoke a transport error.
func (s *Server) Close() { s.srv.Close() }

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	body, readErr := io.ReadAll(r.Body)
	resp := s.next(readErr, Request{Method: r.Method, Path: r.URL.RequestURI(), Auth: r.Header.Get("Authorization"), Accept: r.Header.Get("Accept"), ContentType: r.Header.Get("Content-Type"), Body: string(body)})
	if resp.Remaining != "" {
		w.Header().Set("X-Ratelimit-Remaining", resp.Remaining)
	}
	if resp.RetryAfter != "" {
		w.Header().Set("Retry-After", resp.RetryAfter)
	}
	w.WriteHeader(resp.Status)
	_, writeErr := io.WriteString(w, resp.Body)
	s.recordErr(writeErr)
}

// next records req (and any body read error) and pops the route's next
// response (404 when unknown).
func (s *Server) next(readErr error, req Request) Response {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ioErr = errors.Join(s.ioErr, readErr)
	s.requests = append(s.requests, req)
	route := req.Method + " " + req.Path
	queue := s.responses[route]
	if len(queue) == 0 {
		return Response{Status: http.StatusNotFound, Body: `{"message":"Not Found"}`}
	}
	if len(queue) > 1 {
		s.responses[route] = queue[1:]
	}
	resp := queue[0]
	if resp.Status == 0 {
		resp.Status = http.StatusOK
	}
	return resp
}

func (s *Server) recordErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ioErr = errors.Join(s.ioErr, err)
}

// Err returns every error the fake hit reading request bodies or writing
// responses, joined; nil when every exchange was clean.
func (s *Server) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ioErr
}

// Requests returns a copy of the requests seen so far, in order.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}
