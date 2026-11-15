package main

import (
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
		[]string{labelScheduleType, fieldStatus},
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
	knownScheduleTypes = []string{scheduleTypeCron, scheduleTypeOneShot, scheduleTypeHeartbeat, scheduleTypeEventWatch}
	knownRunStatuses   = []string{statusCompleted, statusFailed, statusStale, statusSkippedActive}
)

func registerHaroCollector(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			panic(err)
		}
	}
}

func init() {
	registerHaroCollector(SchedulesActive)
	registerHaroCollector(ScheduleRunsTotal)
	registerHaroCollector(ScheduleDispatchDurationSeconds)

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

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.handler != nil && s.handler.db != nil {
		if summary, err := GetScheduleSummaryMetrics(r.Context(), s.handler.db); err == nil {
			SchedulesActive.WithLabelValues(scheduleTypeCron).Set(float64(summary.CronCount))
			SchedulesActive.WithLabelValues(scheduleTypeHeartbeat).Set(float64(summary.HeartbeatCount))
			SchedulesActive.WithLabelValues(scheduleTypeOneShot).Set(float64(summary.OneShotCount))
		}
		if triggers, err := ListEventTriggers(r.Context(), s.handler.db, ""); err == nil {
			SchedulesActive.WithLabelValues(scheduleTypeEventWatch).Set(float64(len(triggers)))
		}
	}
	promhttp.Handler().ServeHTTP(w, r)
}
