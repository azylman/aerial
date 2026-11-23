package config

import (
	"archive/tar"
	"bytes"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// withDefaults fills the zero fields of c with the values ParseConfig applies by default.
func withDefaults(c Config) Config {
	if c.FlushTimeout == 0 {
		c.FlushTimeout = 60 * time.Second
	}
	if c.ConnectRetryWindow == 0 {
		c.ConnectRetryWindow = 30 * time.Second
	}
	if c.ConnectRetryInterval == 0 {
		c.ConnectRetryInterval = time.Second
	}
	if c.PullInterval == 0 {
		c.PullInterval = 15 * time.Minute
	}
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.MaxPushBytes == 0 {
		c.MaxPushBytes = 64 << 20
	}
	return c
}

func TestParseConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		yaml    string
		want    Config
		wantErr string
	}{
		{
			name: "explicit timeout and shared service",
			yaml: "targets:\n  - service: eri\n    url: http://eri.aerial/debug/coverage/flush\n  - service: eri\n    url: http://eri-agent-alpha.aerial/debug/coverage/flush\nflush_timeout: 90s\n",
			want: withDefaults(Config{Targets: []Target{
				{Service: "eri", URL: "http://eri.aerial/debug/coverage/flush"},
				{Service: "eri", URL: "http://eri-agent-alpha.aerial/debug/coverage/flush"},
			}, FlushTimeout: 90 * time.Second}),
		},
		{
			name: "explicit connect retry",
			yaml: "targets:\n  - service: eri\n    url: http://x\nconnect_retry_window: 10s\nconnect_retry_interval: 250ms\n",
			want: withDefaults(Config{Targets: []Target{{Service: "eri", URL: "http://x"}}, ConnectRetryWindow: 10 * time.Second, ConnectRetryInterval: 250 * time.Millisecond}),
		},
		{
			name: "default timeout",
			yaml: "targets:\n  - service: haro\n    url: http://haro.aerial/debug/coverage/flush\n",
			want: withDefaults(Config{Targets: []Target{{Service: "haro", URL: "http://haro.aerial/debug/coverage/flush"}}}),
		},
		{
			name: "explicit zero retry durations take the defaults",
			yaml: "targets:\n  - service: eri\n    url: http://x\nconnect_retry_window: 0s\nconnect_retry_interval: 0s\n",
			want: withDefaults(Config{Targets: []Target{{Service: "eri", URL: "http://x"}}}),
		},
		{
			name: "push only with explicit server settings",
			yaml: "push_services: [photos-sync]\npull_interval: 1m\nlisten: ':9090'\nmax_push_bytes: 1024\n",
			want: withDefaults(Config{PushServices: []string{"photos-sync"}, PullInterval: time.Minute, Listen: ":9090", MaxPushBytes: 1024}),
		},
		{
			name: "pull targets and distinct push services",
			yaml: "targets:\n  - service: eri\n    url: http://x\npush_services: [photos-sync, backup]\n",
			want: withDefaults(Config{Targets: []Target{{Service: "eri", URL: "http://x"}}, PushServices: []string{"photos-sync", "backup"}}),
		},
		{name: "no targets", yaml: "flush_timeout: 10s\n", wantErr: "no targets or push_services"},
		{name: "blank push service", yaml: "push_services: ['']\n", wantErr: "push_services 0 is empty"},
		{name: "duplicate push service", yaml: "push_services: [photos-sync, photos-sync]\n", wantErr: `push service "photos-sync" is listed twice`},
		{name: "push service also pulled", yaml: "targets:\n  - service: eri\n    url: http://x\npush_services: [eri]\n", wantErr: `push service "eri" is also a pull target`},
		{name: "negative pull interval", yaml: "push_services: [p]\npull_interval: -1m\n", wantErr: "pull_interval must not be negative"},
		{name: "negative max push bytes", yaml: "push_services: [p]\nmax_push_bytes: -1\n", wantErr: "max_push_bytes must not be negative"},
		{name: "missing url", yaml: "targets:\n  - service: eri\n", wantErr: "target 0 needs service and url"},
		{name: "blank service", yaml: "targets:\n  - service: ' '\n    url: http://x\n", wantErr: "target 0 needs service and url"},
		{name: "negative timeout", yaml: "targets:\n  - service: eri\n    url: http://x\nflush_timeout: -1s\n", wantErr: "flush_timeout must not be negative"},
		{name: "negative retry window", yaml: "targets:\n  - service: eri\n    url: http://x\nconnect_retry_window: -1s\n", wantErr: "connect_retry_window must not be negative"},
		{name: "negative retry interval", yaml: "targets:\n  - service: eri\n    url: http://x\nconnect_retry_interval: -1ms\n", wantErr: "connect_retry_interval must not be negative"},
		{name: "unknown field", yaml: "targetz: []\n", wantErr: "parse config"},
		{name: "invalid yaml", yaml: "targets: [\n", wantErr: "parse config"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseConfig([]byte(tc.yaml))
			assertErrContains(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("config = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseFuncReport(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		out  string
		want []CoverageRow
	}{
		{
			name: "plain and receiver functions",
			out: "github.com/azylman/aerial/services/eri/funnel.go:45:\t\tsyncBotMemberRoles\t\t57.1%\n" +
				"github.com/azylman/aerial/services/eri/funnel.go:90:\t\t(*Funnel).Start\t\t0.0%\n" +
				"total\t\t\t\t\t(statements)\t\t41.3%\n",
			want: []CoverageRow{
				{File: "github.com/azylman/aerial/services/eri/funnel.go", Function: "syncBotMemberRoles", Percent: 57.1},
				{File: "github.com/azylman/aerial/services/eri/funnel.go", Function: "Start", Percent: 0},
			},
		},
		{
			name: "duplicate keys after receiver strip keep max",
			out: "pkg/a.go:10:\tA.Close\t12.5%\n" +
				"pkg/a.go:20:\tB.Close\t80.0%\n" +
				"pkg/a.go:30:\tC.Close\t40.0%\n",
			want: []CoverageRow{{File: "pkg/a.go", Function: "Close", Percent: 80}},
		},
		{
			name: "short, total, and malformed lines skipped",
			out:  "\n  \npkg/a.go:1:\tonlyTwo\ntotal: (statements) 10.0%\npkg/b.go:2:\tf\tNaNx%\npkg/c.go:3:\tg\t100.0%\n",
			want: []CoverageRow{{File: "pkg/c.go", Function: "g", Percent: 100}},
		},
		{
			name: "location without line suffix kept as is",
			out:  "pkg/d.go\th\t5.0%\npkg/e.go:x:\ti\t6.0%\n",
			want: []CoverageRow{{File: "pkg/d.go", Function: "h", Percent: 5}, {File: "pkg/e.go:x", Function: "i", Percent: 6}},
		},
		{name: "trailing-dot function name skipped", out: "pkg/f.go:1:\tT.\t9.0%\n", want: nil},
		{name: "empty", out: "", want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ParseFuncReport([]byte(tc.out)); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("rows = %+v, want %+v", got, tc.want)
			}
		})
	}
}

type tarEntry struct {
	name     string
	typeflag byte
	body     string
	size     int64 // header size override; 0 means len(body)
}

func buildTar(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		size := e.size
		if size == 0 {
			size = int64(len(e.body))
		}
		typ := e.typeflag
		if typ == 0 {
			typ = tar.TypeReg
		}
		if err := tw.WriteHeader(&tar.Header{Name: e.name, Typeflag: typ, Mode: 0o600, Size: size, Linkname: "x"}); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if e.size == 0 && e.body != "" {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("write body: %v", err)
			}
		}
	}
	// Oversize fixtures leave the last entry short, so Close may fail; the header bytes
	// already reached buf, which is all those cases need.
	_ = tw.Close()
	return buf.Bytes()
}

func reg(name, body string) tarEntry { return tarEntry{name: name, body: body} }

func TestExtractFlushArchive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		archive   []byte
		want      int
		wantFiles []string
		wantErr   string
	}{
		{
			name:      "meta and counters",
			archive:   buildTar(t, reg("covmeta.abc", "m"), reg("covcounters.abc.1.2", "c1"), reg("covcounters.abc.3.4", "c2")),
			want:      2,
			wantFiles: []string{"covcounters.abc.1.2", "covcounters.abc.3.4", "covmeta.abc"},
		},
		{name: "empty archive", archive: buildTar(t), want: 0, wantFiles: []string{}},
		{name: "meta only", archive: buildTar(t, reg("covmeta.abc", "m")), want: 0, wantFiles: []string{"covmeta.abc"}},
		{name: "slash in name", archive: buildTar(t, reg("sub/covcounters.x", "c")), wantErr: "unsafe archive entry", wantFiles: []string{}},
		{name: "backslash in name", archive: buildTar(t, reg(`sub\covcounters.x`, "c")), wantErr: "unsafe archive entry", wantFiles: []string{}},
		{name: "dotdot in name", archive: buildTar(t, reg("..covcounters", "c")), wantErr: "unsafe archive entry", wantFiles: []string{}},
		{name: "symlink entry", archive: buildTar(t, tarEntry{name: "covmeta.l", typeflag: tar.TypeSymlink}), wantErr: "not a regular file", wantFiles: []string{}},
		{name: "oversize entry", archive: buildTar(t, tarEntry{name: "covcounters.big", size: MaxArchiveEntryBytes + 1}), wantErr: "exceeds", wantFiles: []string{}},
		{
			name:      "duplicate name",
			archive:   buildTar(t, reg("covcounters.a", "1"), reg("covcounters.a", "2")),
			want:      1,
			wantErr:   "create covcounters.a",
			wantFiles: []string{"covcounters.a"},
		},
		{name: "garbage", archive: []byte(strings.Repeat("x", 600)), wantErr: "read archive", wantFiles: []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			got, err := ExtractFlushArchive(bytes.NewReader(tc.archive), dir)
			assertErrContains(t, err, tc.wantErr)
			if got != tc.want {
				t.Fatalf("counters = %d, want %d", got, tc.want)
			}
			if files := listDir(t, dir); !slices.Equal(files, tc.wantFiles) {
				t.Fatalf("files = %v, want %v", files, tc.wantFiles)
			}
		})
	}
}

func TestPruneKeepSet(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cfg     Config
		want    []string
		wantErr string
	}{
		{
			name: "targets and push services deduped with eri-restate",
			cfg:  Config{Targets: []Target{{Service: "haro"}, {Service: "eri"}, {Service: "eri"}}, PushServices: []string{"photos-sync"}},
			want: []string{"eri", "eri-restate", "haro", "photos-sync"},
		},
		{
			name: "eri-restate target is not duplicated",
			cfg:  Config{Targets: []Target{{Service: "eri-restate"}, {Service: "eri"}}},
			want: []string{"eri", "eri-restate"},
		},
		{name: "push services only", cfg: Config{PushServices: []string{"photos-sync"}}, want: []string{"eri-restate", "photos-sync"}},
		{name: "empty config refused", cfg: Config{}, wantErr: "keep set is empty"},
		{name: "blank names refused", cfg: Config{Targets: []Target{{Service: " "}}, PushServices: []string{""}}, wantErr: "keep set is empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := PruneKeepSet(tc.cfg)
			assertErrContains(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("PruneKeepSet = %v, want %v", got, tc.want)
			}
		})
	}
}

func assertErrContains(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want it to contain %q", err, want)
	}
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}
