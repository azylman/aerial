package main

// Root compatibility shims for the Layer-1 metrics package.
//
// Every metric vector and record helper formerly defined here now lives in
// internal/metrics (Layer 1, imports model only). Internal consumers
// (ticker, server) import that package directly; no root production code
// references this file anymore. The three collector var aliases below are
// the last remnants, kept only until the final purge (haro-07b) removes
// this file. Shims contain no logic and carry no coverage-bearing
// statements. The Server.handleMetrics handler moved to internal/server
// in haro-07a along with the record helpers.

import (
	"github.com/azylman/aerial/services/haro/internal/metrics"
)

var (
	SchedulesActive                 = metrics.SchedulesActive
	ScheduleRunsTotal               = metrics.ScheduleRunsTotal
	ScheduleDispatchDurationSeconds = metrics.ScheduleDispatchDurationSeconds
)
