package main

import (
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	SchedulesActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "haro_schedules_active",
			Help: "Number of active persistent schedules by type.",
		},
		[]string{"schedule_type"},
	)

	ScheduleRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "haro_schedule_runs_total",
			Help: "Total number of schedule runs executed by type and status.",
		},
		[]string{"schedule_type", "status"},
	)

	ScheduleDispatchDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "haro_schedule_dispatch_duration_seconds",
			Help:    "Dispatch duration of schedule runs in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"schedule_type"},
	)
)

var (
	knownScheduleTypes = []string{"cron", "one_shot", "heartbeat", "event_watch"}
	knownRunStatuses   = []string{"completed", "failed", "stale", "skipped_active"}
)

func registerHaroCollector(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if !errors.As(err, &are) {
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
			SchedulesActive.WithLabelValues("cron").Set(float64(summary.CronCount))
			SchedulesActive.WithLabelValues("heartbeat").Set(float64(summary.HeartbeatCount))
			SchedulesActive.WithLabelValues("one_shot").Set(float64(summary.OneShotCount))
		}
		if triggers, err := ListEventTriggers(r.Context(), s.handler.db, ""); err == nil {
			SchedulesActive.WithLabelValues("event_watch").Set(float64(len(triggers)))
		}
	}
	promhttp.Handler().ServeHTTP(w, r)
}
