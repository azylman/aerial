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
		name      string
		chunkID   string
		headSHA   string
		failCount int
		want      string
	}{
		{"legacy format with head sha", "c1", "abcdef123456", 1, "vanadis-c1-cifail-abcdef123456-1"},
		{"legacy format normalized head sha", "c1", "  ABCDEF123456  ", 2, "vanadis-c1-cifail-abcdef123456-2"},
		{"legacy format empty sha", "c1", "", 3, "vanadis-c1-cifail-3"},
		{"legacy format whitespace only sha", " c1 ", "   ", 4, "vanadis-c1-cifail-4"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, CIRemediationDispatchID(tc.chunkID, tc.headSHA, tc.failCount))
		})
	}
}

func TestConflictRemediationDispatchID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		chunkID string
		headSHA string
		want    string
	}{
		{"legacy format with head sha", "c1", "abcdef123456", "vanadis-conflict-c1-abcdef1"},
		{"legacy format normalized head sha", "c1", "  ABCDEF123456  ", "vanadis-conflict-c1-abcdef1"},
		{"legacy format short sha", "c1", "abc", "vanadis-conflict-c1-abc"},
		{"legacy format empty sha", "c1", "", "vanadis-conflict-c1"},
		{"legacy format whitespace sha", " c1 ", "   ", "vanadis-conflict-c1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ConflictRemediationDispatchID(tc.chunkID, tc.headSHA))
		})
	}
}
