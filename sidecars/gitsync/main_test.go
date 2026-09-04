package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSanitizeLog(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "clean string",
			input:    "repository synced cleanly",
			expected: "repository synced cleanly",
		},
		{
			name:     "github pat",
			input:    "failed with token github_pat_11AAAA_secret_token_123",
			expected: "failed with token [REDACTED_TOKEN]",
		},
		{
			name:     "classic ghp token",
			input:    "cloning with ghp_1234567890abcdef1234567890abcdef12",
			expected: "cloning with [REDACTED_TOKEN]",
		},
		{
			name:     "basic auth header",
			input:    "AUTHORIZATION: basic eC1hY2Nlc3MtdG9rZW46c2VjcmV0",
			expected: "AUTHORIZATION: [REDACTED_TOKEN]",
		},
		{
			name:     "postgres connection string",
			input:    "connecting to postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable",
			expected: "connecting to [REDACTED_TOKEN]?sslmode=disable",
		},
		{
			name:     "anthropic api key",
			input:    "using sk-ant-api03-1234567890abcdef-secret",
			expected: "using [REDACTED_TOKEN]",
		},
		{
			name:     "google api key",
			input:    "key AIzaSyD1234567890abcdefghijklmnopqrstuv",
			expected: "key [REDACTED_TOKEN]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeLog(tt.input)
			if got != tt.expected {
				t.Errorf("SanitizeLog() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestBuildGitEnv(t *testing.T) {
	env := buildGitEnv("my_test_pat")
	foundAuth := false
	for _, e := range env {
		if strings.HasPrefix(e, "GIT_CONFIG_VALUE_0=") {
			foundAuth = true
			if !strings.Contains(e, "AUTHORIZATION: basic ") {
				t.Errorf("expected basic auth header, got %s", e)
			}
		}
	}
	if !foundAuth {
		t.Errorf("buildGitEnv() did not produce GIT_CONFIG_VALUE_0 auth header")
	}

	emptyEnv := buildGitEnv("")
	for _, e := range emptyEnv {
		if strings.HasPrefix(e, "GIT_CONFIG") {
			t.Errorf("unexpected GIT_CONFIG in empty PAT env: %s", e)
		}
	}
}

func TestHasComposeChanges(t *testing.T) {
	tempDir := t.TempDir()

	// Initialize git repo
	cmdInit := exec.Command("git", "init", "-b", "main", tempDir)
	if out, err := cmdInit.CombinedOutput(); err != nil {
		t.Fatalf("git init failed: %s (%v)", out, err)
	}

	_ = exec.Command("git", "-C", tempDir, "config", "user.name", "Test").Run()
	_ = exec.Command("git", "-C", tempDir, "config", "user.email", "test@example.com").Run()

	// Initial commit with non-compose file
	readmePath := filepath.Join(tempDir, "README.md")
	if err := os.WriteFile(readmePath, []byte("# Test"), 0644); err != nil {
		t.Fatal(err)
	}
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "initial").Run()

	c1Out, _ := exec.Command("git", "-C", tempDir, "rev-parse", "HEAD").Output()
	c1 := string(c1Out)

	daemon := &SyncDaemon{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Same head -> no changes
	changed, err := daemon.HasComposeChanges(ctx, tempDir, c1, c1)
	if err != nil || changed {
		t.Errorf("expected false, got changed=%v, err=%v", changed, err)
	}

	// 2. Commit modifying markdown -> no compose changes
	_ = os.WriteFile(readmePath, []byte("# Updated README"), 0644)
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "update readme").Run()
	c2Out, _ := exec.Command("git", "-C", tempDir, "rev-parse", "HEAD").Output()
	c2 := string(c2Out)

	changed, err = daemon.HasComposeChanges(ctx, tempDir, c1, c2)
	if err != nil || changed {
		t.Errorf("expected false for markdown change, got changed=%v, err=%v", changed, err)
	}

	// 3. Commit modifying docker-compose.yml -> compose changes detected
	composePath := filepath.Join(tempDir, "docker-compose.yml")
	if err := os.WriteFile(composePath, []byte("services: {}"), 0644); err != nil {
		t.Fatal(err)
	}
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "add compose").Run()
	c3Out, _ := exec.Command("git", "-C", tempDir, "rev-parse", "HEAD").Output()
	c3 := string(c3Out)

	changed, err = daemon.HasComposeChanges(ctx, tempDir, c2, c3)
	if err != nil || !changed {
		t.Errorf("expected true for docker-compose.yml change, got changed=%v, err=%v", changed, err)
	}

	// 4. Commit modifying .env -> compose changes detected
	envPath := filepath.Join(tempDir, ".env")
	if err := os.WriteFile(envPath, []byte("FOO=BAR"), 0644); err != nil {
		t.Fatal(err)
	}
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "add env").Run()
	c4Out, _ := exec.Command("git", "-C", tempDir, "rev-parse", "HEAD").Output()
	c4 := string(c4Out)

	changed, err = daemon.HasComposeChanges(ctx, tempDir, c3, c4)
	if err != nil || !changed {
		t.Errorf("expected true for .env change, got changed=%v, err=%v", changed, err)
	}
}
