package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	ddlRegex        = regexp.MustCompile(`(?i)\b(CREATE|ALTER|DROP)\s+TABLE\b`)
	mainCallRegex   = regexp.MustCompile(`\bmain\(\)`)
	depDirLineRegex = regexp.MustCompile(`(?m)^[ \t]*(?:-[ \t]*)?directory:[ \t]*["']?(/[^"'\s#]*)["']?[ \t]*(?:#.*)?$`)
)

func isCandidateDDLFile(rel string) bool {
	if strings.HasSuffix(rel, "_test.go") || strings.HasSuffix(rel, ".test.ts") || strings.HasPrefix(filepath.Base(rel), "test_") ||
		strings.HasPrefix(rel, "scripts/") {
		return false
	}
	return strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, ".py") || strings.HasSuffix(rel, ".ts")
}

func checkNoImperativeDDL(absRoot string, targets []string) []string {
	var violations []string
	for _, rel := range targets {
		if !isCandidateDDLFile(rel) {
			continue
		}
		full := filepath.Join(absRoot, filepath.FromSlash(rel))
		data, err := os.ReadFile(full)
		if err != nil {
			continue
		}
		lines := bytes.Split(data, []byte("\n"))
		for i, line := range lines {
			trimmed := bytes.TrimSpace(line)
			if bytes.HasPrefix(trimmed, []byte("//")) || bytes.HasPrefix(trimmed, []byte("/*")) || bytes.HasPrefix(trimmed, []byte("*")) || bytes.HasPrefix(trimmed, []byte("#")) {
				continue
			}
			if ddlRegex.Match(trimmed) {
				violations = append(violations, fmt.Sprintf("%s:%d: imperative DDL (CREATE/ALTER/DROP TABLE) detected in application code; schema changes must be declared in schema.sql", rel, i+1))
			}
		}
	}
	return violations
}

func isCandidateAnyFile(rel string) bool {
	for seg := range strings.SplitSeq(rel, "/") {
		if isExcludedDir(seg) || seg == ".." {
			return false
		}
	}
	return true
}

func checkSynapseZeroVanadis(absRoot string, targets []string) []string {
	var violations []string
	for _, rel := range targets {
		if !strings.HasPrefix(rel, "services/synapse/") {
			continue
		}
		full := filepath.Join(absRoot, filepath.FromSlash(rel))
		data, err := os.ReadFile(full)
		if err != nil {
			continue
		}
		if bytes.Contains(bytes.ToLower(data), []byte("vanadis")) {
			violations = append(violations, rel+": reference to 'vanadis' detected; synapse must have zero Vanadis knowledge")
		}
	}
	return violations
}

func checkUTF8BOM(absRoot string, targets []string) []string {
	var violations []string
	bom := []byte{0xef, 0xbb, 0xbf}
	for _, rel := range targets {
		full := filepath.Join(absRoot, filepath.FromSlash(rel))
		data, err := os.ReadFile(full)
		if err != nil {
			continue
		}
		if bytes.HasPrefix(data, bom) {
			violations = append(violations, rel+": invalid UTF-8 BOM detected")
		}
	}
	return violations
}

func checkNoMainInTests(absRoot string, testTargets []string) []string {
	var violations []string
	for _, rel := range testTargets {
		if !strings.HasPrefix(rel, "services/eri/") {
			continue
		}
		full := filepath.Join(absRoot, filepath.FromSlash(rel))
		data, err := os.ReadFile(full)
		if err != nil {
			continue
		}
		for line := range bytes.SplitSeq(data, []byte("\n")) {
			trimmed := bytes.TrimSpace(line)
			if bytes.HasPrefix(trimmed, []byte("//")) || bytes.HasPrefix(trimmed, []byte("/*")) || bytes.HasPrefix(trimmed, []byte("*")) {
				continue
			}
			if mainCallRegex.Match(trimmed) {
				violations = append(violations, rel+": direct execution of main() in test file detected")
				break
			}
		}
	}
	return violations
}

func discoverManifestDirs(absRoot string) (map[string]bool, error) {
	discoveredDirs := make(map[string]bool)
	err := filepath.WalkDir(absRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == absRoot {
			return nil
		}
		rel, relErr := filepath.Rel(absRoot, path)
		if relErr != nil {
			return relErr
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) > 4 {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if d.IsDir() {
			if strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules" || name == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if name == "go.mod" || name == "Cargo.toml" || name == "pyproject.toml" || name == "package.json" {
			dir := filepath.Dir(rel)
			targetDir := "/"
			if dir != "." {
				targetDir = "/" + filepath.ToSlash(dir)
			}
			discoveredDirs[targetDir] = true
		}
		return nil
	})
	return discoveredDirs, err
}

func checkDependabotCoverage(absRoot string) []string {
	discoveredDirs, err := discoverManifestDirs(absRoot)
	if err != nil {
		return []string{err.Error()}
	}
	if len(discoveredDirs) == 0 {
		return nil
	}

	depPath := filepath.Join(absRoot, ".github", "dependabot.yml")
	depBytes, err := os.ReadFile(depPath)
	if os.IsNotExist(err) {
		return []string{".github/dependabot.yml: file not found"}
	}
	if err != nil {
		return []string{err.Error()}
	}
	depContent := string(depBytes)
	configuredDirs := make(map[string]bool)
	for _, m := range depDirLineRegex.FindAllStringSubmatch(depContent, -1) {
		configuredDirs[m[1]] = true
	}

	var violations []string
	for targetDir := range discoveredDirs {
		if !configuredDirs[targetDir] {
			violations = append(violations, ".github/dependabot.yml: missing package manifest coverage for directory "+targetDir)
		}
	}

	ecoCount := strings.Count(depContent, "package-ecosystem:")
	verCount := strings.Count(depContent, "applies-to: version-updates")
	secCount := strings.Count(depContent, "applies-to: security-updates")

	if ecoCount == 0 {
		violations = append(violations, ".github/dependabot.yml: no package-ecosystem entries defined")
	} else if ecoCount != verCount || ecoCount != secCount {
		violations = append(violations, fmt.Sprintf(".github/dependabot.yml: grouping mismatch (package-ecosystems=%d, version-updates=%d, security-updates=%d)", ecoCount, verCount, secCount))
	}

	return violations
}
