package main

// Root compatibility shims for the Layer-1 metrics package.
//
// Every metric vector and record helper formerly defined here now lives in
// internal/metrics (Layer 1, imports model only). This file keeps zero-logic
// aliases only so all not-yet-moved callers (ticker.go, server.go) compile
// unchanged until the final purge (haro-07b). Shims contain no logic and
// carry no coverage-bearing statements. The Server.handleMetrics handler
// stays here; it moves to internal/server in haro-07a.

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/services/haro/internal/metrics"
)

var (
	SchedulesActive                 = metrics.SchedulesActive
	ScheduleRunsTotal               = metrics.ScheduleRunsTotal
	ScheduleDispatchDurationSeconds = metrics.ScheduleDispatchDurationSeconds
)

// RecordScheduleRun records a schedule execution count partitioned by schedule_type and status.
func RecordScheduleRun(scheduleType, status string) {
	metrics.RecordScheduleRun(scheduleType, status)
}

// RecordScheduleDispatchDuration observes the prompt dispatch duration in seconds for a schedule type.
func RecordScheduleDispatchDuration(scheduleType string, durationSeconds float64) {
	metrics.RecordScheduleDispatchDuration(scheduleType, durationSeconds)
}

func registerHaroCollector(c prometheus.Collector) {
	metrics.RegisterHaroCollector(c)
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
