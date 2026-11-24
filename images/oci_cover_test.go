package images_test

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func findRunfile(relPath string) (string, error) {
	if srcdir := os.Getenv("TEST_SRCDIR"); srcdir != "" {
		workspace := os.Getenv("TEST_WORKSPACE")
		if workspace == "" {
			workspace = "_main"
		}
		candidate := filepath.Join(srcdir, workspace, relPath)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	candidates := []string{
		relPath,
		filepath.Join("images", filepath.Base(relPath)),
		filepath.Join("..", relPath),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
	}
	return "", fmt.Errorf("runfile %q not found", relPath)
}

func TestTarLayers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		tarRelPath      string
		requiredEntries []string
	}{
		{
			name:            "coverage ingest tools layer",
			tarRelPath:      "images/coverage_ingest_tools_layer.tar",
			requiredEntries: []string{"usr/local/bin/go", "usr/local/bin/covdata"},
		},
		{
			name:       "eri restate scripts layer preserves paths",
			tarRelPath: "images/eri_restate_scripts_layer.tar",
			requiredEntries: []string{
				"usr/local/bin/aerial-pr.sh",
				"usr/local/bin/lib/hermetic-go-env.sh",
			},
		},
		{
			name:       "github runner bins layer",
			tarRelPath: "images/github_runner_bins_layer.tar",
			requiredEntries: []string{
				"usr/local/bin/roborev",
				"entrypoint.sh",
			},
		},
		{
			name:       "github runner configs layer",
			tarRelPath: "images/github_runner_configs_layer.tar",
			requiredEntries: []string{
				"etc/roborev/config.toml",
				"etc/claude-settings.json",
			},
		},
		{
			name:       "infisical mcp entrypoint layer",
			tarRelPath: "images/infisical_mcp_entrypoint_layer.tar",
			requiredEntries: []string{
				"usr/local/bin/entrypoint.sh",
			},
		},
		{
			name:       "infisical mcp standby layer",
			tarRelPath: "images/infisical_mcp_standby_layer.tar",
			requiredEntries: []string{
				"usr/local/bin/standby.js",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tarPath, err := findRunfile(tc.tarRelPath)
			if err != nil {
				t.Fatalf("resolve %s: %v", tc.tarRelPath, err)
			}
			tarFile, err := os.Open(tarPath)
			if err != nil {
				t.Fatalf("open %s: %v", tc.tarRelPath, err)
			}
			defer tarFile.Close()

			tr := tar.NewReader(tarFile)
			tarEntries := make(map[string]bool)
			for {
				hdr, err := tr.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("read tar entry: %v", err)
				}
				tarEntries[strings.TrimLeft(hdr.Name, "./")] = true
			}

			for _, required := range tc.requiredEntries {
				if !tarEntries[required] {
					t.Fatalf("expected %s to package %q, got entries: %v", tc.tarRelPath, required, tarEntries)
				}
			}
		})
	}
}

func TestProdCoverBinaryFlush(t *testing.T) {
	t.Parallel()

	sampleBin, err := findRunfile("images/cmd/testsample/testsample_/testsample")
	if err != nil {
		t.Fatalf("resolve sample binary: %v", err)
	}
	covdataBin, err := findRunfile("images/covdata")
	if err != nil {
		t.Fatalf("resolve covdata binary: %v", err)
	}
	goWrapperBin, err := findRunfile("images/go_wrapper_bin/go")
	if err != nil {
		t.Fatalf("resolve go wrapper binary: %v", err)
	}

	flushDir := t.TempDir()

	cmd := exec.Command(sampleBin)
	cmd.Env = append(os.Environ(), "FLUSH_DIR="+flushDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run sample binary: %v\noutput: %s", err, string(output))
	}

	entries, err := os.ReadDir(flushDir)
	if err != nil {
		t.Fatalf("read flush dir: %v", err)
	}

	var hasMeta, hasCounters bool
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "covmeta.") {
			hasMeta = true
		}
		if strings.HasPrefix(name, "covcounters.") {
			hasCounters = true
		}
	}

	if !hasMeta {
		t.Fatalf("expected covmeta.* file in %s, got entries: %v", flushDir, entries)
	}
	if !hasCounters {
		t.Fatalf("expected covcounters.* file in %s, got entries: %v", flushDir, entries)
	}

	covCmd := exec.Command(goWrapperBin, "tool", "covdata", "func", "-i="+flushDir)
	covCmd.Env = append(os.Environ(), "COVDATA_BIN="+covdataBin)
	var covOut, covErr bytes.Buffer
	covCmd.Stdout = &covOut
	covCmd.Stderr = &covErr
	if err := covCmd.Run(); err != nil {
		t.Fatalf("run go tool covdata func via wrapper: %v\nstderr: %s", err, covErr.String())
	}

	report := covOut.String()
	matched, err := regexp.MatchString(`Add\s+100\.0%`, report)
	if err != nil || !matched {
		t.Fatalf("expected coverage report to show Add function at 100.0%% coverage, got:\n%s", report)
	}
}
