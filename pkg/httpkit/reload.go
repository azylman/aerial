package httpkit

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/netip"
	"reflect"
	"sync"
	"sync/atomic"
)

// ReloadStatus reports the outcome of a single Reloadable.Reload call.
type ReloadStatus string

// Reload outcomes reported by Reloadable.Reload and the reload HTTP handler.
const (
	ReloadUnchanged    ReloadStatus = "unchanged"
	ReloadApplied      ReloadStatus = "reloaded"
	ReloadLKGCRetained ReloadStatus = "lkgc_retained"
	ReloadRestarting   ReloadStatus = "restarting"
)

// Reloadable holds an immutable configuration snapshot that can be swapped
// atomically on demand. A snapshot that fails to load is never installed: the
// last known good configuration (LKGC) stays live. When a reload changes a
// field that cannot be applied in-process, the snapshot is left untouched and
// RestartRequested is closed so the owning service can exit gracefully.
type Reloadable[T any] struct {
	current         atomic.Pointer[T]
	load            func() (*T, error)
	restartRequired func(old, next *T) bool
	reloadMu        sync.Mutex
	restartCh       chan struct{}
	restartOnce     sync.Once
}

// NewReloadable returns a Reloadable seeded with initial. load re-reads the
// configuration from its source. restartRequired reports whether the
// difference between old and next can only be applied by a process restart;
// nil means every field can be swapped live. It panics if initial or load is nil.
func NewReloadable[T any](initial *T, load func() (*T, error), restartRequired func(old, next *T) bool) *Reloadable[T] {
	if initial == nil {
		panic("httpkit: NewReloadable initial snapshot cannot be nil")
	}
	if load == nil {
		panic("httpkit: NewReloadable load func cannot be nil")
	}
	r := &Reloadable[T]{
		load:            load,
		restartRequired: restartRequired,
		restartCh:       make(chan struct{}),
	}
	r.current.Store(initial)
	return r
}

// Current returns the live configuration snapshot. Callers must treat it as
// immutable and should read it once per request, call, or worker tick.
func (r *Reloadable[T]) Current() *T {
	return r.current.Load()
}

// RestartRequested is closed once a reload observes a change that requires a
// process restart.
func (r *Reloadable[T]) RestartRequested() <-chan struct{} {
	return r.restartCh
}

// Reload re-reads the configuration and applies it according to the LKGC and
// restart rules documented on Reloadable. Concurrent calls are serialized.
func (r *Reloadable[T]) Reload() (ReloadStatus, error) {
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()

	next, err := r.load()
	if err == nil && next == nil {
		err = errNilSnapshot
	}
	if err != nil {
		return ReloadLKGCRetained, err
	}
	old := r.current.Load()
	if reflect.DeepEqual(old, next) {
		return ReloadUnchanged, nil
	}
	if r.restartRequired != nil && r.restartRequired(old, next) {
		r.restartOnce.Do(func() { close(r.restartCh) })
		return ReloadRestarting, nil
	}
	r.current.Store(next)
	return ReloadApplied, nil
}

var errNilSnapshot = errors.New("reload: load returned a nil snapshot")

type reloadResponse struct {
	Status ReloadStatus `json:"status"`
	Error  string       `json:"error,omitempty"`
}

// Handler serves the loopback-only reload webhook (POST /-/reload) invoked by
// the git-sync sidecar. It always answers 200 once a reload was attempted:
// git-sync retries non-success responses for the same commit forever, and
// retrying an unchanged bad file can never succeed.
func (r *Reloadable[T]) Handler(service string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !isLoopbackRemote(req.RemoteAddr) {
			log.Printf("%s: config reload rejected from non-loopback address %q", service, req.RemoteAddr)
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		status, err := r.Reload()
		resp := reloadResponse{Status: status}
		switch {
		case err != nil:
			resp.Error = err.Error()
			log.Printf("%s: ERROR config reload failed, keeping last known good config: %v", service, err)
		case status == ReloadRestarting:
			log.Printf("%s: config change requires restart, shutting down gracefully", service)
		case status == ReloadApplied:
			log.Printf("%s: config reloaded", service)
		default:
			log.Printf("%s: config reload requested, no changes", service)
		}
		WriteJSON(w, http.StatusOK, resp)
	})
}

func isLoopbackRemote(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return addr.IsLoopback()
}
