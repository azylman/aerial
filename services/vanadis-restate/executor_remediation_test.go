package main

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
			assert.Equal(t, tc.want, isGateAggregatorCheck(tc.checkName))
		})
	}
}

func TestCIRemediationDispatchID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		chunkID   string
		headSHA   string
		failCount int
		want      string
	}{
		{"c1", "abcdef123456", 1, "vanadis-c1-cifail-abcdef123456-1"},
		{"c1", "  ABCDEF123456  ", 2, "vanadis-c1-cifail-abcdef123456-2"},
		{"c1", "", 3, "vanadis-c1-cifail-3"},
		{" c1 ", "   ", 4, "vanadis-c1-cifail-4"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ciRemediationDispatchID(tc.chunkID, tc.headSHA, tc.failCount))
		})
	}
}

func TestConflictRemediationDispatchID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		chunkID string
		headSHA string
		want    string
	}{
		{"c1", "abcdef123456", "vanadis-conflict-c1-abcdef1"},
		{"c1", "  ABCDEF123456  ", "vanadis-conflict-c1-abcdef1"},
		{"c1", "abc", "vanadis-conflict-c1-abc"},
		{"c1", "", "vanadis-conflict-c1"},
		{" c1 ", "   ", "vanadis-conflict-c1"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, conflictRemediationDispatchID(tc.chunkID, tc.headSHA))
		})
	}
}
