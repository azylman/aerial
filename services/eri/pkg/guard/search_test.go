package guard

import (
	"testing"
)

func TestCheckSearch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tool     string
		cmd      string
		cwd      string
		wantDeny bool
	}{
		// Non-search tools / empty / prefix cases
		{"write_to_file allowed", "write_to_file", "find /", "", false},
		{"empty command line allowed", "run_command", "", "", false},
		{"whitespace command line allowed", "run_command", "   ", "", false},
		{"default_api prefix deny", "default_api:run_command", "find . -name '*.go'", "", true},
		{"empty grep_search allowed", "grep_search", "", "", false},
		{"empty find_by_name allowed", "find_by_name", "", "", false},
		{"escaped quote allowed", "run_command", `echo \"escaped\ quote\" && find . -maxdepth 1`, "", false},
		{"repeated delimiters allowed", "run_command", `echo 1 ; ; echo 2 | | echo 3`, "", false},

		// Find commands
		{"find targeting root directory denied", "run_command", "find / -maxdepth 1 -name '*.go'", "", true},
		{"find targeting /proc denied", "run_command", "find /proc -maxdepth 1", "", true},
		{"find targeting /sys denied", "run_command", "find /sys -maxdepth 1", "", true},
		{"find dot when cwd is root denied", "run_command", "find . -maxdepth 1", "/", true},
		{"find without maxdepth denied", "run_command", "find . -name '*.go'", "", true},
		{"find in share without maxdepth denied", "run_command", "find /share/aerial -type f", "", true},
		{"find with -maxdepth allowed", "run_command", "find . -maxdepth 2 -name '*.go'", "", false},
		{"find with --maxdepth allowed", "run_command", "find /share/aerial -maxdepth 3 -type f", "", false},
		{"find with -mtime flag allowed", "run_command", "find . -maxdepth 1 -mtime 1 -type f", "", false},
		{"find --help allowed", "run_command", "find --help", "", false},
		{"find -help allowed", "run_command", "find -help", "", false},
		{"find --version allowed", "run_command", "find --version", "", false},
		{"find targeting .git denied", "run_command", "find .git -maxdepth 1", "", true},
		{"find targeting node_modules denied", "run_command", "find node_modules -maxdepth 1", "", true},

		// Grep commands
		{"grep -r targeting root denied", "run_command", "grep -r 'pattern' /", "", true},
		{"grep -r targeting /proc denied", "run_command", "grep -r 'pattern' /proc", "", true},
		{"rg targeting root denied", "run_command", "rg 'pattern' /", "", true},
		{"grep targeting .git denied", "run_command", "grep -r 'pattern' .git", "", true},
		{"grep targeting node_modules denied", "run_command", "grep -r 'pattern' node_modules", "", true},
		{"recursive grep without exclude-dir denied", "run_command", "grep -r 'pattern' .", "", true},
		{"recursive grep targeting /share without exclude-dir denied", "run_command", "grep -rn 'pattern' /share", "", true},
		{"recursive grep with --exclude-dir allowed", "run_command", "grep -rn --exclude-dir=.git 'pattern' .", "", false},
		{"recursive grep with space-delimited --exclude-dir allowed", "run_command", "grep -rn --exclude-dir node_modules 'pattern' .", "", false},
		{"non-recursive grep with --color allowed", "run_command", "grep --color 'pattern' .", "", false},
		{"non-recursive grep with --ignore-case allowed", "run_command", "grep --ignore-case 'pattern' file.txt", "", false},
		{"ripgrep targeting workspace allowed", "run_command", "rg 'pattern' .", "", false},
		{"grep targeting system binary directory denied", "run_command", "grep 'pattern' /usr/bin/bash", "", true},
		{"strings targeting binary directory denied", "run_command", "strings /usr/local/bin/hook-guard", "", true},
		{"grep targeting .so file denied", "run_command", "grep 'pattern' libfoo.so", "", true},
		{"grep targeting .tar.gz denied", "run_command", "grep 'pattern' archive.tar.gz", "", true},
		{"grep targeting .zip denied", "run_command", "grep 'pattern' file.zip", "", true},
		{"grep for .zip string in source file allowed", "run_command", "grep '.zip' main.go", "", false},
		{"grep in file named bin.go allowed", "run_command", "grep 'pattern' bin.go", "", false},
		{"grep with explicit -e flag allowed", "run_command", "grep -e 'foo' main.go", "", false},

		// Pipelines & Quotes
		{"echo containing find in quotes allowed", "run_command", `echo "find / -name foo"`, "", false},
		{"chained safe commands allowed", "run_command", `ls -la && find . -maxdepth 1 -name '*.go'`, "", false},
		{"pipeline with unconstrained find denied", "run_command", `echo starting && find . -name '*.go'`, "", true},
		{"pipe inside quotes allowed", "run_command", `grep "a | b" file.txt`, "", false},
		{"semicolon inside quotes allowed", "run_command", `git commit -m "fix; update search guard"`, "", false},
		{"or operator pipeline with bad find denied", "run_command", `test -d /foo || find / -maxdepth 1`, "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tool := tc.tool
			if tool == "" {
				tool = "run_command"
			}
			d := CheckSearch(tool, ToolArgs{CommandLine: tc.cmd, Cwd: tc.cwd})
			if tc.wantDeny && d.Decision != DecisionDeny {
				t.Fatalf("cmd %q: expected deny, got %v", tc.cmd, d)
			}
			if !tc.wantDeny && d.Decision != DecisionAllow {
				t.Fatalf("cmd %q: expected allow, got deny: %v", tc.cmd, d)
			}
		})
	}
}

func TestSearchParsers(t *testing.T) {
	t.Parallel()

	pipelineTests := []struct {
		name  string
		input string
		want  []string
	}{
		{"empty", "", nil},
		{"simple single", "echo 1", []string{"echo 1"}},
		{"semicolon", "echo 1; echo 2", []string{"echo 1", "echo 2"}},
		{"and operator", "echo 1 && echo 2", []string{"echo 1", "echo 2"}},
		{"or operator", "echo 1 || echo 2", []string{"echo 1", "echo 2"}},
		{"pipe operator", "echo 1 | grep 1", []string{"echo 1", "grep 1"}},
		{"quoted pipe", `echo "1 | 2"`, []string{`echo "1 | 2"`}},
		{"escaped quotes", `echo \"hello\"`, []string{`echo \"hello\"`}},
	}
	for _, tc := range pipelineTests {
		t.Run("pipeline_"+tc.name, func(t *testing.T) {
			t.Parallel()
			got := splitPipeline(tc.input)
			if len(got) != len(tc.want) {
				t.Fatalf("splitPipeline(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}

	tokenTests := []struct {
		name  string
		input string
		want  int
	}{
		{"empty", "", 0},
		{"simple tokens", "find . -maxdepth 1", 4},
		{"single quotes", "grep 'a b c' file", 3},
		{"double quotes", `grep "hello world" file`, 3},
		{"escaped space", `grep hello\ world file`, 3},
	}
	for _, tc := range tokenTests {
		t.Run("tokens_"+tc.name, func(t *testing.T) {
			t.Parallel()
			got := tokenizeQuoteAware(tc.input)
			if len(got) != tc.want {
				t.Fatalf("tokenizeQuoteAware(%q) length = %d, want %d", tc.input, len(got), tc.want)
			}
		})
	}

	argTests := []struct {
		name     string
		rawJSON  string
		wantFile string
		wantCmd  string
	}{
		{"object format", `{"TargetFile": "/share/aerial", "CommandLine": "find ."}`, "/share/aerial", "find ."},
		{"string format", `"{\"CommandLine\": \"grep foo\"}"`, "", "grep foo"},
	}
	for _, tc := range argTests {
		t.Run("args_"+tc.name, func(t *testing.T) {
			t.Parallel()
			args := ParseArgs([]byte(tc.rawJSON))
			if args.FilePath() != tc.wantFile || args.CommandLine != tc.wantCmd {
				t.Fatalf("ParseArgs(%q) = %+v, want file=%q cmd=%q", tc.rawJSON, args, tc.wantFile, tc.wantCmd)
			}
		})
	}
}
