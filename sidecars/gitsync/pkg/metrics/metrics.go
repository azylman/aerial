package metrics

import (
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Registry is the dedicated Prometheus registry for Aerial GitSync sidecar.
	Registry = prometheus.NewRegistry()

	// PullsTotal counts git pull/fetch operations per repository.
	PullsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_gitsync_pulls_total",
			Help: "Total number of Git pull and synchronization operations executed.",
		},
		[]string{"repo", "status", "changed"},
	)

	// PullDurationSeconds measures git pull execution duration.
	PullDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aerial_gitsync_pull_duration_seconds",
			Help:    "Execution duration of Git pull operations in seconds.",
			Buckets: []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 45.0, 90.0, 120.0},
		},
		[]string{"repo", "status"},
	)

	// SyncRequestsTotal counts sync triggers (periodic ticker vs HTTP webhook).
	SyncRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_gitsync_sync_requests_total",
			Help: "Total sync requests triggered via periodic ticker or HTTP webhook.",
		},
		[]string{"source", "status"},
	)

	// ReconciliationsTotal counts Docker Compose reconciliation events.
	ReconciliationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_gitsync_reconciliations_total",
			Help: "Total Docker Compose GitOps reconciliation operations executed.",
		},
		[]string{"status"},
	)

	// ComposeDurationSeconds measures Docker Compose reconciliation duration.
	ComposeDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aerial_gitsync_compose_duration_seconds",
			Help:    "Execution duration of Docker Compose reconciliation in seconds.",
			Buckets: []float64{1.0, 5.0, 15.0, 30.0, 60.0, 120.0},
		},
		[]string{"status"},
	)

	// LastSyncTimestampSeconds tracks the unix epoch timestamp of the last successful sync per repo.
	LastSyncTimestampSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aerial_gitsync_last_sync_timestamp_seconds",
			Help: "Unix timestamp in seconds of the last successful Git synchronization.",
		},
		[]string{"repo"},
	)

	// BuildInfo exposes static build metadata for the GitSync sidecar.
	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aerial_gitsync_build_info",
			Help: "Static build metadata for Aerial GitSync sidecar.",
		},
		[]string{"version", "goversion"},
	)
)

func init() {
	// Register Go runtime and process collectors on dedicated registry
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	// Register GitSync application metrics
	Registry.MustRegister(
		PullsTotal,
		PullDurationSeconds,
		SyncRequestsTotal,
		ReconciliationsTotal,
		ComposeDurationSeconds,
		LastSyncTimestampSeconds,
		BuildInfo,
	)

	BuildInfo.WithLabelValues("1.0.0", runtime.Version()).Set(1)
}

// NormalizeRepoName converts absolute paths to stable symbolic repo identifiers (e.g., "aerial", "aerial-config").
func NormalizeRepoName(rawPath string) string {
	cleaned := filepath.Clean(rawPath)
	base := filepath.Base(cleaned)
	if base == "." || base == "/" || base == "" {
		return "unknown"
	}
	return base
}

// Handler returns the HTTP handler for scraping GitSync metrics.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// RecordPull records git pull duration, status, and whether changes were applied.
func RecordPull(rawRepo, status string, changed bool, duration time.Duration) {
	repo := NormalizeRepoName(rawRepo)
	if status == "" {
		status = "unknown"
	}
	changedStr := "false"
	if changed {
		changedStr = "true"
	}
	PullsTotal.WithLabelValues(repo, status, changedStr).Inc()
	PullDurationSeconds.WithLabelValues(repo, status).Observe(duration.Seconds())
}

// RecordSyncRequest records sync triggers from periodic ticker or HTTP webhook.
func RecordSyncRequest(source, status string) {
	if source == "" {
		source = "unknown"
	}
	if status == "" {
		status = "unknown"
	}
	SyncRequestsTotal.WithLabelValues(source, status).Inc()
}

// RecordReconciliation records Docker Compose reconciliation duration and status.
func RecordReconciliation(status string, duration time.Duration) {
	if status == "" {
		status = "unknown"
	}
	ReconciliationsTotal.WithLabelValues(status).Inc()
	ComposeDurationSeconds.WithLabelValues(status).Observe(duration.Seconds())
}

// RecordLastSync sets the timestamp of the last sync for a repository.
func RecordLastSync(rawRepo string, ts time.Time) {
	repo := NormalizeRepoName(rawRepo)
	if !ts.IsZero() {
		LastSyncTimestampSeconds.WithLabelValues(repo).Set(float64(ts.Unix()))
	}
}

// SanitizeStatus normalizes error or status strings into bounded low-cardinality values.
func SanitizeStatus(err error) string {
	if err == nil {
		return "success"
	}
	errStr := strings.ToLower(err.Error())
	switch {
	case strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline exceeded"):
		return "timeout"
	case strings.Contains(errStr, "validation failed"):
		return "validation_failed"
	case strings.Contains(errStr, "index.lock"):
		return "lock_active"
	case strings.Contains(errStr, "git dir not found"):
		return "not_found"
	case strings.Contains(errStr, "reset failed") || strings.Contains(errStr, "fetch failed"):
		return "git_error"
	case strings.Contains(errStr, "compose up failed"):
		return "apply_failed"
	default:
		return "error"
	}
}
