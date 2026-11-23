package server

import "github.com/prometheus/client_golang/prometheus"

// Outcomes of a rejected summary LLM answer.
const (
	rejectionRetry     = "retry"
	rejectionExhausted = "exhausted"
)

// summaryRejectionsTotal counts rejected summary LLM answers by outcome:
// "retry" (another keyed attempt follows) or "exhausted" (failed terminally).
var summaryRejectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "transcripts_summary_rejections_total",
	Help: "Rejected transcript summary LLM answers by outcome (retry or exhausted).",
}, []string{"outcome"})

func init() {
	prometheus.MustRegister(summaryRejectionsTotal)
	// Export both outcomes at zero so alerts see the series before the first rejection.
	summaryRejectionsTotal.WithLabelValues(rejectionRetry)
	summaryRejectionsTotal.WithLabelValues(rejectionExhausted)
}
