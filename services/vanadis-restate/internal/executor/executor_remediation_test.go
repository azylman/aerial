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

func TestParseCIRemediationDispatchID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		chunkID     string
		dispatchID  string
		wantSHA     string
		wantAttempt int
		wantOK      bool
	}{
		{"round trips head sha form", "c1", CIRemediationDispatchID("c1", "ABCDEF123456", 2), "abcdef123456", 2, true},
		{"round trips sha-less form", "c1", CIRemediationDispatchID("c1", "", 3), "", 3, true},
		{"round trips hyphenated chunk id", "silent-fixup-backstop", CIRemediationDispatchID("silent-fixup-backstop", "0f45d92", 5), "0f45d92", 5, true},
		{"round trips hyphenated chunk id sha-less", "fix-ci-loop", CIRemediationDispatchID("fix-ci-loop", " ", 1), "", 1, true},
		{"trims chunk and dispatch whitespace", " c1 ", " vanadis-c1-cifail-abc-4 ", "abc", 4, true},
		{"other chunk rejected", "c1", CIRemediationDispatchID("c2", "abc", 1), "", 0, false},
		{"chunk id prefix of another chunk rejected", "fix", CIRemediationDispatchID("fix-ci", "abc", 1), "", 0, false},
		{"conflict dispatch rejected", "c1", ConflictRemediationDispatchID("c1", "abc"), "", 0, false},
		{"launch dispatch rejected", "c1", "vanadis-c1-turn-0", "", 0, false},
		{"non-numeric attempt rejected", "c1", "vanadis-c1-cifail-abc-x", "", 0, false},
		{"zero attempt rejected", "c1", "vanadis-c1-cifail-abc-0", "", 0, false},
		{"padded attempt rejected", "c1", "vanadis-c1-cifail-abc-01", "", 0, false},
		{"empty sha segment rejected", "c1", "vanadis-c1-cifail--1", "", 0, false},
		{"hyphenated sha rejected", "c1", "vanadis-c1-cifail-ab-cd-1", "", 0, false},
		{"empty chunk rejected", "", "vanadis--cifail-abc-1", "", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sha, attempt, ok := ParseCIRemediationDispatchID(tc.chunkID, tc.dispatchID)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantSHA, sha)
			assert.Equal(t, tc.wantAttempt, attempt)
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
