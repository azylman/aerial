package session

import (
	"errors"
	"os"
	"path/filepath"
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
		{name: "empty id", id: "", want: ""},
		{name: "whitespace only", id: "   ", want: ""},
		{name: "discord target with whitespace", id: "  discord-9876543210  ", want: "9876543210"},
		{name: "pooled worker target without discord prefix", id: "discord:worker-0", want: ""},
		{name: "colon target with discord suffix", id: "kiosk:discord-123", want: "123"},
		{name: "colon target with invalid suffix", id: "kiosk:foo-123", want: ""},
		{name: "ephemeral worker target", id: "ephemeral:worker-0", want: ""},
		{name: "worker target ignored", id: "worker-pool-1", want: ""},
		{name: "heartbeat target ignored", id: "heartbeat-tick", want: ""},
		{name: "dispatch target ignored", id: "dispatch-job", want: ""},
		{name: "vanadis target ignored", id: "vanadis-chunk", want: ""},
		{name: "raw numeric discord id", id: "1234567890", want: "1234567890"},
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
		saTokenPath    string
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
		},
		{name: "empty target and chunk unsets vanadis chunk id", wantContains: []string{"unset VANADIS_CHUNK_ID\n"}, wantNotContain: []string{"export AERIAL_TARGET_ID"}},
		{
			name: "escaping single quotes preserves vanadis chunk id", targetID: "foo'bar", vanadisChunkID: "chunk'escaped'test",
			wantContains: []string{"export VANADIS_CHUNK_ID='chunk'\\''escaped'\\''test'\n"},
		},
		{
			name: "exports AERIAL_SA_TOKEN_PATH when provided", targetID: "discord-123", saTokenPath: "/data/runtimes/discord/.gemini/turn_env/sa-token",
			wantContains: []string{"export AERIAL_SA_TOKEN_PATH='/data/runtimes/discord/.gemini/turn_env/sa-token'\n"},
		},
		{name: "omits AERIAL_SA_TOKEN_PATH when blank", targetID: "discord-123", saTokenPath: "   ", wantNotContain: []string{"AERIAL_SA_TOKEN_PATH"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatTurnEnvFileContent(tc.targetID, tc.vanadisChunkID, tc.saTokenPath)
			for _, exp := range tc.wantContains {
				if !strings.Contains(got, exp) {
					t.Errorf("FormatTurnEnvFileContent(%q, %q, %q) missing %q; got:\n%s", tc.targetID, tc.vanadisChunkID, tc.saTokenPath, exp, got)
				}
			}
			for _, unexpected := range tc.wantNotContain {
				if strings.Contains(got, unexpected) {
					t.Errorf("FormatTurnEnvFileContent(%q, %q, %q) unexpectedly contains %q; got:\n%s", tc.targetID, tc.vanadisChunkID, tc.saTokenPath, unexpected, got)
				}
			}
		})
	}
}

type syncTurnSATokenCase struct {
	name      string
	srcToken  string
	missing   bool
	emptyDest bool
	blockDest bool
	euid      int
	chownErr  error
	wantToken string
	wantErr   bool
}

func runSyncTurnSATokenCase(t *testing.T, tc syncTurnSATokenCase) {
	t.Helper()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src-token")
	if !tc.missing {
		if err := os.WriteFile(srcPath, []byte(tc.srcToken), 0o600); err != nil {
			t.Fatalf("write src: %v", err)
		}
	}
	destPath := filepath.Join(tmp, "turn_env", "sa-token")
	if tc.emptyDest {
		destPath = "   "
	} else if tc.blockDest {
		blocker := filepath.Join(tmp, "blocker")
		if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
			t.Fatalf("write blocker: %v", err)
		}
		destPath = filepath.Join(blocker, "sa-token")
	}

	var chownCalls int
	chownFn := func(_ string, uid, gid int) error {
		chownCalls++
		if uid != 1000 || gid != 1000 {
			t.Errorf("chown uid:gid = %d:%d, want 1000:1000", uid, gid)
		}
		return tc.chownErr
	}

	gotPath, err := SyncTurnSATokenFile(srcPath, destPath, tc.euid, chownFn)
	if (err != nil) != tc.wantErr {
		t.Fatalf("SyncTurnSATokenFile() error = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr || tc.wantToken == "" {
		if !tc.wantErr && gotPath != "" {
			t.Errorf("gotPath = %q, want empty", gotPath)
		}
		return
	}
	if gotPath != destPath {
		t.Errorf("gotPath = %q, want %q", gotPath, destPath)
	}
	data, readErr := os.ReadFile(destPath)
	if readErr != nil {
		t.Fatalf("read dest: %v", readErr)
	}
	if string(data) != tc.wantToken {
		t.Errorf("dest content = %q, want %q", string(data), tc.wantToken)
	}
	if tc.euid == 0 && chownCalls != 1 {
		t.Errorf("chownCalls = %d, want 1", chownCalls)
	}
}

func TestSyncTurnSATokenFile(t *testing.T) {
	t.Parallel()
	tests := []syncTurnSATokenCase{
		{
			name:      "syncs trimmed token and chowns when root",
			srcToken:  "  eyJhbGciOiJSUzI1NiJ9.token  \n",
			euid:      0,
			wantToken: "eyJhbGciOiJSUzI1NiJ9.token\n",
		},
		{name: "syncs trimmed token without chown when non-root", srcToken: "non-root-token", euid: 1000, wantToken: "non-root-token\n"},
		{name: "returns empty without error when source missing", missing: true, euid: 0},
		{name: "returns empty without error when source blank", srcToken: "   \n\t ", euid: 0},
		{name: "returns empty without error when dest path blank", srcToken: "valid-token", emptyDest: true, euid: 0},
		{
			name:     "returns error when chown fails as root",
			srcToken: "valid-token",
			euid:     0,
			chownErr: errors.New("chown failed"),
			wantErr:  true,
		},
		{
			name:      "returns error when destination parent is blocked by file",
			srcToken:  "valid-token",
			blockDest: true,
			euid:      1000,
			wantErr:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSyncTurnSATokenCase(t, tc)
		})
	}
}
