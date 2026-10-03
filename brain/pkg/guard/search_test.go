package guard

import (
	"testing"
)

func TestSearchGuardFindCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cmd      string
		cwd      string
		wantDeny bool
	}{
		// Rule 1: Root Searches
		{
			name:     "find targeting root directory denied",
			cmd:      "find / -maxdepth 1 -name '*.go'",
			wantDeny: true,
		},
		{
			name:     "find targeting /proc denied",
			cmd:      "find /proc -maxdepth 1",
			wantDeny: true,
		},
		{
			name:     "find targeting /sys denied",
			cmd:      "find /sys -maxdepth 1",
			wantDeny: true,
		},
		{
			name:     "find dot when cwd is root denied",
			cmd:      "find . -maxdepth 1",
			cwd:      "/",
			wantDeny: true,
		},
		// Rule 2: Missing -maxdepth
		{
			name:     "find without maxdepth denied",
			cmd:      "find . -name '*.go'",
			wantDeny: true,
		},
		{
			name:     "find in share without maxdepth denied",
			cmd:      "find /share/aerial -type f",
			wantDeny: true,
		},
		{
			name:     "find with -maxdepth allowed",
			cmd:      "find . -maxdepth 2 -name '*.go'",
			wantDeny: false,
		},
		{
			name:     "find with --maxdepth allowed",
			cmd:      "find /share/aerial -maxdepth 3 -type f",
			wantDeny: false,
		},
		{
			name:     "find with -mtime flag allowed",
			cmd:      "find . -maxdepth 1 -mtime 1 -type f",
			wantDeny: false,
		},
		{
			name:     "find --help allowed",
			cmd:      "find --help",
			wantDeny: false,
		},
		{
			name:     "find -help allowed",
			cmd:      "find -help",
			wantDeny: false,
		},
		{
			name:     "find --version allowed",
			cmd:      "find --version",
			wantDeny: false,
		},
		// Rule 3: Giant internal directories
		{
			name:     "find targeting .git denied",
			cmd:      "find .git -maxdepth 1",
			wantDeny: true,
		},
		{
			name:     "find targeting node_modules denied",
			cmd:      "find node_modules -maxdepth 1",
			wantDeny: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := CheckSearch("run_command", ToolArgs{CommandLine: tt.cmd, Cwd: tt.cwd})
			if tt.wantDeny && d.Decision != DecisionDeny {
				t.Errorf("cmd %q: expected deny, got allow: %v", tt.cmd, d)
			}
			if !tt.wantDeny && d.Decision != DecisionAllow {
				t.Errorf("cmd %q: expected allow, got deny: %v (reason: %s)", tt.cmd, d, d.Reason)
			}
		})
	}
}

func TestSearchGuardGrepCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cmd      string
		wantDeny bool
	}{
		// Rule 1: Root Searches
		{
			name:     "grep -r targeting root denied",
			cmd:      "grep -r 'pattern' /",
			wantDeny: true,
		},
		{
			name:     "grep -r targeting /proc denied",
			cmd:      "grep -r 'pattern' /proc",
			wantDeny: true,
		},
		{
			name:     "rg targeting root denied",
			cmd:      "rg 'pattern' /",
			wantDeny: true,
		},
		// Rule 3: Giant internal directories
		{
			name:     "grep targeting .git denied",
			cmd:      "grep -r 'pattern' .git",
			wantDeny: true,
		},
		{
			name:     "grep targeting node_modules denied",
			cmd:      "grep -r 'pattern' node_modules",
			wantDeny: true,
		},
		{
			name:     "recursive grep without exclude-dir denied",
			cmd:      "grep -r 'pattern' .",
			wantDeny: true,
		},
		{
			name:     "recursive grep targeting /share without exclude-dir denied",
			cmd:      "grep -rn 'pattern' /share",
			wantDeny: true,
		},
		{
			name:     "recursive grep with --exclude-dir allowed",
			cmd:      "grep -rn --exclude-dir=.git 'pattern' .",
			wantDeny: false,
		},
		{
			name:     "recursive grep with space-delimited --exclude-dir allowed",
			cmd:      "grep -rn --exclude-dir node_modules 'pattern' .",
			wantDeny: false,
		},
		{
			name:     "non-recursive grep with --color allowed",
			cmd:      "grep --color 'pattern' .",
			wantDeny: false,
		},
		{
			name:     "non-recursive grep with --ignore-case allowed",
			cmd:      "grep --ignore-case 'pattern' file.txt",
			wantDeny: false,
		},
		{
			name:     "ripgrep targeting workspace allowed (rg excludes git by default)",
			cmd:      "rg 'pattern' .",
			wantDeny: false,
		},
		// Rule 4: Binary Searches
		{
			name:     "grep targeting system binary directory denied",
			cmd:      "grep 'pattern' /usr/bin/bash",
			wantDeny: true,
		},
		{
			name:     "strings targeting binary directory denied",
			cmd:      "strings /usr/local/bin/hook-guard",
			wantDeny: true,
		},
		{
			name:     "grep targeting .so file denied",
			cmd:      "grep 'pattern' libfoo.so",
			wantDeny: true,
		},
		{
			name:     "grep targeting .tar.gz denied",
			cmd:      "grep 'pattern' archive.tar.gz",
			wantDeny: true,
		},
		{
			name:     "grep targeting .zip denied",
			cmd:      "grep 'pattern' file.zip",
			wantDeny: true,
		},
		// Safe false-positive resistance
		{
			name:     "grep for .zip string in source file allowed",
			cmd:      "grep '.zip' main.go",
			wantDeny: false,
		},
		{
			name:     "grep in file named bin.go allowed",
			cmd:      "grep 'pattern' bin.go",
			wantDeny: false,
		},
		{
			name:     "grep with explicit -e flag allowed",
			cmd:      "grep -e 'foo' main.go",
			wantDeny: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := CheckSearch("run_command", ToolArgs{CommandLine: tt.cmd})
			if tt.wantDeny && d.Decision != DecisionDeny {
				t.Errorf("cmd %q: expected deny, got allow: %v", tt.cmd, d)
			}
			if !tt.wantDeny && d.Decision != DecisionAllow {
				t.Errorf("cmd %q: expected allow, got deny: %v (reason: %s)", tt.cmd, d, d.Reason)
			}
		})
	}
}

func TestSearchGuardPipelinesAndQuotes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cmd      string
		wantDeny bool
	}{
		{
			name:     "echo containing find in quotes allowed",
			cmd:      `echo "find / -name foo"`,
			wantDeny: false,
		},
		{
			name:     "chained safe commands allowed",
			cmd:      `ls -la && find . -maxdepth 1 -name '*.go'`,
			wantDeny: false,
		},
		{
			name:     "pipeline with unconstrained find denied",
			cmd:      `echo starting && find . -name '*.go'`,
			wantDeny: true,
		},
		{
			name:     "pipe inside quotes allowed",
			cmd:      `grep "a | b" file.txt`,
			wantDeny: false,
		},
		{
			name:     "semicolon inside quotes allowed",
			cmd:      `git commit -m "fix; update search guard"`,
			wantDeny: false,
		},
		{
			name:     "or operator pipeline with bad find denied",
			cmd:      `test -d /foo || find / -maxdepth 1`,
			wantDeny: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := CheckSearch("run_command", ToolArgs{CommandLine: tt.cmd})
			if tt.wantDeny && d.Decision != DecisionDeny {
				t.Errorf("cmd %q: expected deny, got allow: %v", tt.cmd, d)
			}
			if !tt.wantDeny && d.Decision != DecisionAllow {
				t.Errorf("cmd %q: expected allow, got deny: %v (reason: %s)", tt.cmd, d, d.Reason)
			}
		})
	}
}

func TestSearchGuardEdgeCases(t *testing.T) {
	t.Parallel()

	// Non-search tools allowed
	d1 := CheckSearch("write_to_file", ToolArgs{CommandLine: "find /"})
	if d1.Decision != DecisionAllow {
		t.Errorf("expected allow for write_to_file, got %v", d1)
	}

	// Empty command line allowed
	d2 := CheckSearch("run_command", ToolArgs{CommandLine: ""})
	if d2.Decision != DecisionAllow {
		t.Errorf("expected allow for empty command line, got %v", d2)
	}

	d3 := CheckSearch("run_command", ToolArgs{CommandLine: "   "})
	if d3.Decision != DecisionAllow {
		t.Errorf("expected allow for whitespace command line, got %v", d3)
	}

	// default_api prefix support
	d4 := CheckSearch("default_api:run_command", ToolArgs{CommandLine: "find . -name '*.go'"})
	if d4.Decision != DecisionDeny {
		t.Errorf("expected deny for default_api:run_command with unconstrained find, got %v", d4)
	}

	// grep_search and find_by_name tool calls with empty/clean commands
	d5 := CheckSearch("grep_search", ToolArgs{})
	if d5.Decision != DecisionAllow {
		t.Errorf("expected allow for empty grep_search, got %v", d5)
	}

	d6 := CheckSearch("find_by_name", ToolArgs{})
	if d6.Decision != DecisionAllow {
		t.Errorf("expected allow for empty find_by_name, got %v", d6)
	}

	// Escaped quotes and backslashes in splitPipeline and tokenizeQuoteAware
	d7 := CheckSearch("run_command", ToolArgs{CommandLine: `echo \"escaped\ quote\" && find . -maxdepth 1`})
	if d7.Decision != DecisionAllow {
		t.Errorf("expected allow for escaped quote command, got %v", d7)
	}

	// Repeated delimiters / empty commands in pipeline
	d8 := CheckSearch("run_command", ToolArgs{CommandLine: `echo 1 ; ; echo 2 | | echo 3`})
	if d8.Decision != DecisionAllow {
		t.Errorf("expected allow for empty pipeline parts, got %v", d8)
	}
}
