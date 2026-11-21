package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRecordRestateShadowSend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		handler string
		status  string
	}{
		{name: "blank labels fall back to unknown", handler: "", status: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler, status := tc.handler, tc.status
			if handler == "" {
				handler = unknownLabel
			}
			if status == "" {
				status = unknownLabel
			}
			counter := RestateShadowSendsTotal.WithLabelValues(handler, status)
			before := testutil.ToFloat64(counter)
			RecordRestateShadowSend(tc.handler, tc.status)
			if got, want := testutil.ToFloat64(counter), before+1; got != want {
				t.Errorf("shadow sends = %v, want %v", got, want)
			}
		})
	}
}

func TestRecordRestateShadowDivergence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		reason string
	}{
		{name: "blank reason falls back to unknown", reason: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reason := tc.reason
			if reason == "" {
				reason = unknownLabel
			}
			counter := RestateShadowDivergenceTotal.WithLabelValues(reason)
			before := testutil.ToFloat64(counter)
			RecordRestateShadowDivergence(tc.reason)
			if got, want := testutil.ToFloat64(counter), before+1; got != want {
				t.Errorf("shadow divergences = %v, want %v", got, want)
			}
		})
	}
}

// TestRecordRegistryAndLookupCounters pins the three string-labelled cutover
// counters: each records its label and blank labels fall back to unknown.
func TestRecordRegistryAndLookupCounters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		label string
	}{
		{name: "label is recorded", input: "UpdatePRSync", label: "UpdatePRSync"},
		{name: "blank label falls back to unknown", input: "", label: "unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := RegistryCallsTotal.WithLabelValues(tc.label)
			divergences := RestateLookupDivergenceTotal.WithLabelValues(tc.label)
			recovered := RestateMergeRecoveredTotal.WithLabelValues(tc.label)
			beforeCalls, beforeDivergences, beforeRecovered := testutil.ToFloat64(calls), testutil.ToFloat64(divergences), testutil.ToFloat64(recovered)
			RecordRegistryCall(tc.input)
			RecordRestateLookupDivergence(tc.input)
			RecordRestateMergeRecovered(tc.input)
			if got := testutil.ToFloat64(recovered); got < beforeRecovered+1 {
				t.Errorf("merges recovered = %v, want at least %v", got, beforeRecovered+1)
			}
			if got := testutil.ToFloat64(calls); got < beforeCalls+1 {
				t.Errorf("registry calls = %v, want at least %v", got, beforeCalls+1)
			}
			if got := testutil.ToFloat64(divergences); got < beforeDivergences+1 {
				t.Errorf("lookup divergences = %v, want at least %v", got, beforeDivergences+1)
			}
		})
	}
}
