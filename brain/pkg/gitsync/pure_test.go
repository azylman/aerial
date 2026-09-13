package gitsync

import (
	"encoding/base64"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCleanURL_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "https url with basic auth user and password",
			input:    "https://user:password123@github.com/azylman/aerial.git",
			expected: "https://github.com/azylman/aerial.git",
		},
		{
			name:     "https url with personal access token as username",
			input:    "https://x-access-token:ghp_mocktoken1234567890@github.com/azylman/aerial.git",
			expected: "https://github.com/azylman/aerial.git",
		},
		{
			name:     "http url with basic auth",
			input:    "http://admin:secret@127.0.0.1:8080/repo.git",
			expected: "http://127.0.0.1:8080/repo.git",
		},
		{
			name:     "ssh url with userinfo",
			input:    "ssh://git:token@github.com/azylman/aerial.git",
			expected: "ssh://github.com/azylman/aerial.git",
		},
		{
			name:     "git protocol url with userinfo",
			input:    "git://user:token@github.com/azylman/aerial.git",
			expected: "git://github.com/azylman/aerial.git",
		},
		{
			name:     "clean https url without userinfo",
			input:    "https://github.com/azylman/aerial.git",
			expected: "https://github.com/azylman/aerial.git",
		},
		{
			name:     "url with leading and trailing whitespace",
			input:    "  https://github.com/azylman/aerial.git\n ",
			expected: "https://github.com/azylman/aerial.git",
		},
		{
			name:     "local file path unchanged",
			input:    "/var/git/repo.git",
			expected: "/var/git/repo.git",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "malformed url with scheme returns raw url",
			input:    "http://[::1]:namedport/repo",
			expected: "http://[::1]:namedport/repo",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CleanURL(tc.input)
			if got != tc.expected {
				t.Fatalf("CleanURL(%q) = %q; want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestBuildAuthArgs_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		pat      string
		expected []string
	}{
		{
			name:     "empty pat returns empty slice",
			pat:      "",
			expected: []string{},
		},
		{
			name:     "whitespace pat returns empty slice",
			pat:      "   \t\n  ",
			expected: []string{},
		},
		{
			name: "classic pat returns authorization header",
			pat:  "ghp_testtoken123",
			expected: []string{
				"-c",
				fmt.Sprintf("http.extraHeader=AUTHORIZATION: basic %s",
					base64.StdEncoding.EncodeToString([]byte("x-access-token:ghp_testtoken123"))),
			},
		},
		{
			name: "fine-grained pat returns authorization header",
			pat:  "github_pat_11ABCDEF_longtokenvalue",
			expected: []string{
				"-c",
				fmt.Sprintf("http.extraHeader=AUTHORIZATION: basic %s",
					base64.StdEncoding.EncodeToString([]byte("x-access-token:github_pat_11ABCDEF_longtokenvalue"))),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildAuthArgs(tc.pat)
			if len(got) != len(tc.expected) {
				t.Fatalf("BuildAuthArgs(%q) returned %d args; want %d args: %v", tc.pat, len(got), len(tc.expected), got)
			}
			for i := range got {
				if got[i] != tc.expected[i] {
					t.Fatalf("BuildAuthArgs(%q)[%d] = %q; want %q", tc.pat, i, got[i], tc.expected[i])
				}
			}
		})
	}
}

func TestParseGitDirContent_TableDriven(t *testing.T) {
	baseDir := filepath.Clean("/workspace/repo")
	if runtime.GOOS == "windows" {
		baseDir = `C:\workspace\repo`
	}

	tests := []struct {
		name         string
		data         []byte
		repoPath     string
		expectedPath string
		expectedOK   bool
	}{
		{
			name:         "standard relative gitdir",
			data:         []byte("gitdir: ../worktrees/feature-branch\n"),
			repoPath:     baseDir,
			expectedPath: filepath.Clean(filepath.Join(baseDir, "../worktrees/feature-branch")),
			expectedOK:   true,
		},
		{
			name:         "crlf line ending on windows",
			data:         []byte("gitdir: .git/worktrees/wt1\r\n"),
			repoPath:     baseDir,
			expectedPath: filepath.Clean(filepath.Join(baseDir, ".git/worktrees/wt1")),
			expectedOK:   true,
		},
		{
			name:         "double quoted target path with spaces",
			data:         []byte("gitdir: \"../work trees/feature\"\n"),
			repoPath:     baseDir,
			expectedPath: filepath.Clean(filepath.Join(baseDir, "../work trees/feature")),
			expectedOK:   true,
		},
		{
			name:         "single quoted target path",
			data:         []byte("gitdir: '../wt/test'\n"),
			repoPath:     baseDir,
			expectedPath: filepath.Clean(filepath.Join(baseDir, "../wt/test")),
			expectedOK:   true,
		},
		{
			name:         "missing gitdir prefix",
			data:         []byte("not a gitdir pointer file"),
			repoPath:     baseDir,
			expectedPath: "",
			expectedOK:   false,
		},
		{
			name:         "empty data",
			data:         []byte(""),
			repoPath:     baseDir,
			expectedPath: "",
			expectedOK:   false,
		},
		{
			name:         "prefix only without target path",
			data:         []byte("gitdir:   \r\n"),
			repoPath:     baseDir,
			expectedPath: "",
			expectedOK:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotPath, gotOK := ParseGitDirContent(tc.data, tc.repoPath)
			if gotOK != tc.expectedOK {
				t.Fatalf("ParseGitDirContent() ok = %v; want %v", gotOK, tc.expectedOK)
			}
			if gotPath != tc.expectedPath {
				t.Fatalf("ParseGitDirContent() path = %q; want %q", gotPath, tc.expectedPath)
			}
		})
	}
}

func TestBuildCloneArgs_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		repoPath string
		pat      string
		expected []string
	}{
		{
			name:     "clone without pat cleans url",
			url:      "https://x:token@github.com/azylman/aerial.git",
			repoPath: "/share/aerial",
			pat:      "",
			expected: []string{"clone", "https://github.com/azylman/aerial.git", "/share/aerial"},
		},
		{
			name:     "clone with pat injects auth args before clone",
			url:      "https://github.com/azylman/aerial.git",
			repoPath: "/share/aerial",
			pat:      "ghp_test",
			expected: []string{
				"-c",
				fmt.Sprintf("http.extraHeader=AUTHORIZATION: basic %s",
					base64.StdEncoding.EncodeToString([]byte("x-access-token:ghp_test"))),
				"clone",
				"https://github.com/azylman/aerial.git",
				"/share/aerial",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildCloneArgs(tc.url, tc.repoPath, tc.pat)
			if strings.Join(got, " ") != strings.Join(tc.expected, " ") {
				t.Fatalf("BuildCloneArgs() = %v; want %v", got, tc.expected)
			}
		})
	}
}

func TestBuildFetchArgs_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		pat      string
		remote   string
		branch   string
		expected []string
	}{
		{
			name:     "fetch origin with branch and pat",
			pat:      "ghp_test",
			remote:   "origin",
			branch:   "main",
			expected: []string{"-c", BuildAuthArgs("ghp_test")[1], "fetch", "origin", "main"},
		},
		{
			name:     "fetch origin without branch and without pat",
			pat:      "",
			remote:   "",
			branch:   "",
			expected: []string{"fetch", "origin"},
		},
		{
			name:     "fetch custom remote and custom branch",
			pat:      "",
			remote:   "upstream",
			branch:   "feature",
			expected: []string{"fetch", "upstream", "feature"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildFetchArgs(tc.pat, tc.remote, tc.branch)
			if strings.Join(got, " ") != strings.Join(tc.expected, " ") {
				t.Fatalf("BuildFetchArgs() = %v; want %v", got, tc.expected)
			}
		})
	}
}

func TestBuildPullArgs_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		pat      string
		expected []string
	}{
		{
			name:     "pull without pat",
			pat:      "",
			expected: []string{"pull", "--ff-only"},
		},
		{
			name:     "pull with pat",
			pat:      "ghp_pull_token",
			expected: []string{"-c", BuildAuthArgs("ghp_pull_token")[1], "pull", "--ff-only"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildPullArgs(tc.pat)
			if strings.Join(got, " ") != strings.Join(tc.expected, " ") {
				t.Fatalf("BuildPullArgs() = %v; want %v", got, tc.expected)
			}
		})
	}
}

func TestBuildResetSoftArgs_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		ref      string
		expected []string
	}{
		{
			name:     "reset soft against origin/main",
			ref:      "origin/main",
			expected: []string{"reset", "--soft", "origin/main"},
		},
		{
			name:     "reset soft against FETCH_HEAD",
			ref:      "FETCH_HEAD",
			expected: []string{"reset", "--soft", "FETCH_HEAD"},
		},
		{
			name:     "empty ref defaults to FETCH_HEAD",
			ref:      "",
			expected: []string{"reset", "--soft", "FETCH_HEAD"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildResetSoftArgs(tc.ref)
			if strings.Join(got, " ") != strings.Join(tc.expected, " ") {
				t.Fatalf("BuildResetSoftArgs() = %v; want %v", got, tc.expected)
			}
		})
	}
}

func TestBuildSafeDirectoryArgs_TableDriven(t *testing.T) {
	got := BuildSafeDirectoryArgs()
	expected := []string{"config", "--global", "safe.directory", "*"}
	if strings.Join(got, " ") != strings.Join(expected, " ") {
		t.Fatalf("BuildSafeDirectoryArgs() = %v; want %v", got, expected)
	}
}

func TestBuildHooksPathArgs_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		hooksDir string
		expected []string
	}{
		{
			name:     "empty hooksDir defaults to .githooks",
			hooksDir: "",
			expected: []string{"config", "core.hooksPath", ".githooks"},
		},
		{
			name:     "custom hooksDir",
			hooksDir: "custom/hooks",
			expected: []string{"config", "core.hooksPath", "custom/hooks"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildHooksPathArgs(tc.hooksDir)
			if strings.Join(got, " ") != strings.Join(tc.expected, " ") {
				t.Fatalf("BuildHooksPathArgs() = %v; want %v", got, tc.expected)
			}
		})
	}
}

func TestBuildBranchArgs_TableDriven(t *testing.T) {
	t.Run("branch rename", func(t *testing.T) {
		got := BuildBranchRenameArgs("develop")
		expected := []string{"branch", "-M", "develop"}
		if strings.Join(got, " ") != strings.Join(expected, " ") {
			t.Fatalf("BuildBranchRenameArgs() = %v; want %v", got, expected)
		}

		gotDefault := BuildBranchRenameArgs("")
		expectedDefault := []string{"branch", "-M", "main"}
		if strings.Join(gotDefault, " ") != strings.Join(expectedDefault, " ") {
			t.Fatalf("BuildBranchRenameArgs(\"\") = %v; want %v", gotDefault, expectedDefault)
		}
	})

	t.Run("branch upstream track", func(t *testing.T) {
		got := BuildBranchTrackArgs("upstream", "feature")
		expected := []string{"branch", "-u", "upstream/feature", "feature"}
		if strings.Join(got, " ") != strings.Join(expected, " ") {
			t.Fatalf("BuildBranchTrackArgs() = %v; want %v", got, expected)
		}

		gotDefault := BuildBranchTrackArgs("", "")
		expectedDefault := []string{"branch", "-u", "origin/main", "main"}
		if strings.Join(gotDefault, " ") != strings.Join(expectedDefault, " ") {
			t.Fatalf("BuildBranchTrackArgs(\"\", \"\") = %v; want %v", gotDefault, expectedDefault)
		}
	})
}

func TestBuildRevParseVerifyArgs_TableDriven(t *testing.T) {
	got := BuildRevParseVerifyArgs("origin/main")
	expected := []string{"rev-parse", "--verify", "origin/main"}
	if strings.Join(got, " ") != strings.Join(expected, " ") {
		t.Fatalf("BuildRevParseVerifyArgs() = %v; want %v", got, expected)
	}
}

func TestBuildGitEnv_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		baseEnv     [][]string
		mustContain string
	}{
		{
			name:        "nil base environment returns default terminal prompt",
			baseEnv:     nil,
			mustContain: "GIT_TERMINAL_PROMPT=0",
		},
		{
			name:        "existing base environment preserves existing and appends prompt",
			baseEnv:     [][]string{{"PATH=/usr/bin", "USER=alex"}},
			mustContain: "GIT_TERMINAL_PROMPT=0",
		},
		{
			name:        "base environment already having terminal prompt does not duplicate",
			baseEnv:     [][]string{{"GIT_TERMINAL_PROMPT=1"}},
			mustContain: "GIT_TERMINAL_PROMPT=1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildGitEnv(tc.baseEnv...)
			found := false
			for _, e := range got {
				if e == tc.mustContain {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("BuildGitEnv() = %v; expected to contain %q", got, tc.mustContain)
			}
		})
	}
}

func TestResolveGitBin_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		lookup     func(string) string
		fileExists []func(string) bool
		lookPath   func(string) (string, error)
		goos       string
		expected   string
	}{
		{
			name:   "found in PATH via LookPath",
			lookup: func(k string) string { return "" },
			lookPath: func(file string) (string, error) {
				return "/usr/bin/git", nil
			},
			goos:     "linux",
			expected: "/usr/bin/git",
		},
		{
			name: "windows mingit in LOCALAPPDATA",
			lookup: func(k string) string {
				if k == "LOCALAPPDATA" {
					return `C:\Users\test\AppData\Local`
				}
				return ""
			},
			fileExists: []func(string) bool{
				func(p string) bool {
					return strings.Contains(p, `MinGit\cmd\git.exe`)
				},
			},
			lookPath: func(file string) (string, error) {
				return "", fmt.Errorf("not found")
			},
			goos:     "windows",
			expected: filepath.Clean(`C:\Users\test\AppData\Local\Programs\MinGit\cmd\git.exe`),
		},
		{
			name: "windows git in ProgramFiles",
			lookup: func(k string) string {
				if k == "ProgramFiles" {
					return `C:\Program Files`
				}
				return ""
			},
			fileExists: []func(string) bool{
				func(p string) bool {
					return strings.Contains(p, `Program Files\Git\cmd\git.exe`)
				},
			},
			lookPath: func(file string) (string, error) {
				return "", fmt.Errorf("not found")
			},
			goos:     "windows",
			expected: filepath.Clean(`C:\Program Files\Git\cmd\git.exe`),
		},
		{
			name: "windows git in ProgramFiles(x86)",
			lookup: func(k string) string {
				if k == "ProgramFiles(x86)" {
					return `C:\Program Files (x86)`
				}
				return ""
			},
			fileExists: []func(string) bool{
				func(p string) bool {
					return strings.Contains(p, `Program Files (x86)\Git\cmd\git.exe`)
				},
			},
			lookPath: func(file string) (string, error) {
				return "", fmt.Errorf("not found")
			},
			goos:     "windows",
			expected: filepath.Clean(`C:\Program Files (x86)\Git\cmd\git.exe`),
		},
		{
			name: "windows git in USERPROFILE fallback",
			lookup: func(k string) string {
				if k == "USERPROFILE" {
					return `C:\Users\fallback`
				}
				return ""
			},
			fileExists: []func(string) bool{
				func(p string) bool {
					return strings.Contains(p, `fallback\AppData\Local\Programs\MinGit\cmd\git.exe`)
				},
			},
			lookPath: func(file string) (string, error) {
				return "", fmt.Errorf("not found")
			},
			goos:     "windows",
			expected: filepath.Clean(`C:\Users\fallback\AppData\Local\Programs\MinGit\cmd\git.exe`),
		},
		{
			name: "no git found anywhere returns default git string",
			lookup: func(k string) string {
				return ""
			},
			fileExists: []func(string) bool{
				func(p string) bool { return false },
			},
			lookPath: func(file string) (string, error) {
				return "", fmt.Errorf("not found")
			},
			goos:     "linux",
			expected: "git",
		},
		{
			name:     "nil lookup returns default git string",
			lookup:   nil,
			lookPath: func(file string) (string, error) { return "", fmt.Errorf("not found") },
			goos:     "linux",
			expected: "git",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveGitBinInternal(tc.lookup, tc.fileExists, tc.lookPath, tc.goos)
			if got != tc.expected {
				t.Fatalf("resolveGitBinInternal() = %q; want %q", got, tc.expected)
			}
		})
	}

	// Test public ResolveGitBin wrapper
	t.Run("public ResolveGitBin wrapper", func(t *testing.T) {
		bin := ResolveGitBin(func(string) string { return "" })
		if bin == "" {
			t.Fatalf("expected non-empty git binary string")
		}
	})
}

func TestSanitizeLog_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
		redacted string
	}{
		{
			name:     "clean string without secrets",
			input:    "repository successfully synced to HEAD",
			redacted: "repository successfully synced to HEAD",
		},
		{
			name:     "classic github PAT is redacted",
			input:    "fatal: authentication failed for https://ghp_abcdef1234567890ABCDEF@github.com",
			contains: "[REDACTED_TOKEN]",
		},
		{
			name:     "fine-grained PAT is redacted",
			input:    "using token github_pat_11AAAAAA_longsecretvaluehere in request",
			contains: "[REDACTED_TOKEN]",
		},
		{
			name:     "basic auth header is redacted",
			input:    "-c http.extraHeader=AUTHORIZATION: basic eC1hY2Nlc3MtdG9rZW46Z2hwX3Rlc3Q=",
			contains: "[REDACTED_TOKEN]",
		},
		{
			name:     "embedded x-access-token is redacted",
			input:    "url: https://x-access-token:ghp_mocktoken@github.com",
			contains: "[REDACTED_TOKEN]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SanitizeLog(tc.input)
			if tc.redacted != "" && got != tc.redacted {
				t.Fatalf("SanitizeLog(%q) = %q; want %q", tc.input, got, tc.redacted)
			}
			if tc.contains != "" && !strings.Contains(got, tc.contains) {
				t.Fatalf("SanitizeLog(%q) = %q; expected to contain %q", tc.input, got, tc.contains)
			}
		})
	}
}
