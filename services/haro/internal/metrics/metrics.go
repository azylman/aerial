package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/haro/internal/model"
)

const (
	labelScheduleType = "schedule_type"
)

var (
	SchedulesActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "haro_schedules_active",
			Help: "Number of active persistent schedules by type.",
		},
		[]string{labelScheduleType},
	)

	ScheduleRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "haro_schedule_runs_total",
			Help: "Total number of schedule runs executed by type and status.",
		},
		[]string{labelScheduleType, model.FieldStatus},
	)

	ScheduleDispatchDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "haro_schedule_dispatch_duration_seconds",
			Help:    "Dispatch duration of schedule runs in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{labelScheduleType},
	)
)

var (
	knownScheduleTypes = []string{model.ScheduleTypeCron, model.ScheduleTypeOneShot, model.ScheduleTypeHeartbeat, model.ScheduleTypeEventWatch}
	knownRunStatuses   = []string{model.StatusCompleted, model.StatusFailed, model.StatusStale, model.StatusSkippedActive}
)

// RegisterHaroCollector registers a Haro collector, tolerating duplicate registration.
func RegisterHaroCollector(c prometheus.Collector) {
	httpkit.RegisterCollector(c)
}

func init() {
	RegisterHaroCollector(SchedulesActive)
	RegisterHaroCollector(ScheduleRunsTotal)
	RegisterHaroCollector(ScheduleDispatchDurationSeconds)

	for _, st := range knownScheduleTypes {
		SchedulesActive.WithLabelValues(st).Set(0)
		ScheduleDispatchDurationSeconds.WithLabelValues(st)
		for _, status := range knownRunStatuses {
			ScheduleRunsTotal.WithLabelValues(st, status).Add(0)
		}
	}
}

// RecordScheduleRun records a schedule execution count partitioned by schedule_type and status.
func RecordScheduleRun(scheduleType, status string) {
	ScheduleRunsTotal.WithLabelValues(scheduleType, status).Inc()
}

// RecordScheduleDispatchDuration observes the prompt dispatch duration in seconds for a schedule type.
func RecordScheduleDispatchDuration(scheduleType string, durationSeconds float64) {
	ScheduleDispatchDurationSeconds.WithLabelValues(scheduleType).Observe(durationSeconds)
}
