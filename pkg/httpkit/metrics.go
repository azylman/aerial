// Package httpkit holds shared HTTP handlers, client helpers, and Prometheus registration utilities.
package httpkit

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// RegisterCollector registers a Prometheus collector, tolerating duplicate registration.
func RegisterCollector(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			panic(err)
		}
	}
}
