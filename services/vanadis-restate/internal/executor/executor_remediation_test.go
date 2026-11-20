package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsGateAggregatorCheck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		checkName string
		want      bool
	}{
		{"ci gate", true},
		{"CI GATE", true},
		{"  ci gate  ", true},
		{"build gate", true},
		{"lint gate", true},
		{"test gate", true},
		{"ci", false},
		{"gatekeeper", false},
		{"unit tests", false},
		{"", false},
	}
	for _, tc := range tests {
		t.Run(tc.checkName, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, IsGateAggregatorCheck(tc.checkName))
		})
	}
}

func TestCIRemediationDispatchID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		chunkID        string
		headSHA        string
		failCount      int
		discriminators []string
		want           string
	}{
		{"legacy format with head sha", "c1", "abcdef123456", 1, nil, "vanadis-c1-cifail-abcdef123456-1"},
		{"legacy format normalized head sha", "c1", "  ABCDEF123456  ", 2, nil, "vanadis-c1-cifail-abcdef123456-2"},
		{"legacy format empty sha", "c1", "", 3, nil, "vanadis-c1-cifail-3"},
		{"legacy format whitespace only sha", " c1 ", "   ", 4, nil, "vanadis-c1-cifail-4"},
		{"empty discriminators preserved as legacy", "c1", "abcdef123456", 1, []string{"", "  "}, "vanadis-c1-cifail-abcdef123456-1"},
		{"single discriminator appended with 16-hex digest", "c1", "abcdef123456", 1, []string{"unit-tests"}, "vanadis-c1-cifail-abcdef123456-1-00d0e4e593878fd0"},
		{"multiple discriminators appended with 16-hex digest", "c1", "abcdef123456", 1, []string{"unit-tests", "https://ci/1", "2026-11-20T16:00:00Z"}, "vanadis-c1-cifail-abcdef123456-1-d42062339e12c8bb"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, CIRemediationDispatchID(tc.chunkID, tc.headSHA, tc.failCount, tc.discriminators...))
		})
	}
}

func TestConflictRemediationDispatchID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		chunkID        string
		headSHA        string
		discriminators []string
		want           string
	}{
		{"legacy format with head sha", "c1", "abcdef123456", nil, "vanadis-conflict-c1-abcdef1"},
		{"legacy format normalized head sha", "c1", "  ABCDEF123456  ", nil, "vanadis-conflict-c1-abcdef1"},
		{"legacy format short sha", "c1", "abc", nil, "vanadis-conflict-c1-abc"},
		{"legacy format empty sha", "c1", "", nil, "vanadis-conflict-c1"},
		{"legacy format whitespace sha", " c1 ", "   ", nil, "vanadis-conflict-c1"},
		{"empty discriminators preserved as legacy", "c1", "abcdef123456", []string{"", "  "}, "vanadis-conflict-c1-abcdef1"},
		{"single discriminator appended with 16-hex digest", "c1", "abcdef123456", []string{"2026-11-20T16:00:00Z"}, "vanadis-conflict-c1-abcdef1-a630618a836d7670"},
		{"single discriminator with empty sha", "c1", "", []string{"2026-11-20T16:00:00Z"}, "vanadis-conflict-c1-a630618a836d7670"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ConflictRemediationDispatchID(tc.chunkID, tc.headSHA, tc.discriminators...))
		})
	}
}
