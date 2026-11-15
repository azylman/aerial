package session

import (
	"strings"
	"testing"
)

func TestNormalizeEnvTargetID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		id   string
		want string
	}{
		{
			name: "empty id",
			id:   "",
			want: "",
		},
		{
			name: "whitespace only",
			id:   "   ",
			want: "",
		},
		{
			name: "discord target with whitespace",
			id:   "  discord-9876543210  ",
			want: "9876543210",
		},
		{
			name: "pooled worker target without discord prefix",
			id:   "discord:worker-0",
			want: "",
		},
		{
			name: "ephemeral worker target",
			id:   "ephemeral:worker-0",
			want: "",
		},
		{
			name: "worker target ignored",
			id:   "worker-pool-1",
			want: "",
		},
		{
			name: "raw numeric discord id",
			id:   "1234567890",
			want: "1234567890",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeEnvTargetID(tc.id)
			if got != tc.want {
				t.Fatalf("NormalizeEnvTargetID(%q) = %q, want %q", tc.id, got, tc.want)
			}
		})
	}
}

func TestFormatTurnEnvFileContent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		targetID       string
		vanadisChunkID string
		wantContains   []string
		wantNotContain []string
	}{
		{
			name:           "populated target and chunk with opt-out guard",
			targetID:       "discord-123456",
			vanadisChunkID: "build-1-chunk-2",
			wantContains: []string{
				"if [ \"${AERIAL_NO_VANADIS_CHUNK:-0}\" != \"1\" ]; then\n" +
					"export AERIAL_VANADIS_CHUNK_ID='build-1-chunk-2'\n" +
					"export VANADIS_CHUNK_ID='build-1-chunk-2'\n" +
					"else\n" +
					"unset AERIAL_VANADIS_CHUNK_ID\n" +
					"unset VANADIS_CHUNK_ID\n" +
					"fi\n",
			},
			wantNotContain: nil,
		},
		{
			name:           "empty target and chunk unsets vanadis chunk id",
			targetID:       "",
			vanadisChunkID: "",
			wantContains: []string{
				"unset VANADIS_CHUNK_ID\n",
			},
			wantNotContain: []string{
				"export AERIAL_TARGET_ID",
			},
		},
		{
			name:           "escaping single quotes preserves vanadis chunk id",
			targetID:       "foo'bar",
			vanadisChunkID: "chunk'escaped'test",
			wantContains: []string{
				"export VANADIS_CHUNK_ID='chunk'\\''escaped'\\''test'\n",
			},
			wantNotContain: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatTurnEnvFileContent(tc.targetID, tc.vanadisChunkID)
			for _, exp := range tc.wantContains {
				if !strings.Contains(got, exp) {
					t.Errorf("FormatTurnEnvFileContent(%q, %q) missing %q; got:\n%s", tc.targetID, tc.vanadisChunkID, exp, got)
				}
			}
			for _, unexpected := range tc.wantNotContain {
				if strings.Contains(got, unexpected) {
					t.Errorf("FormatTurnEnvFileContent(%q, %q) unexpectedly contains %q; got:\n%s", tc.targetID, tc.vanadisChunkID, unexpected, got)
				}
			}
		})
	}
}
