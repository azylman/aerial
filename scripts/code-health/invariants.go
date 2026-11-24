package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var ddlRegex = regexp.MustCompile(`(?i)\b(CREATE|ALTER|DROP)\s+TABLE\b`)

func checkNoImperativeDDL(absRoot string, targets []string) []string {
	var violations []string
	for _, rel := range targets {
		if strings.HasSuffix(rel, "_test.go") || strings.HasPrefix(rel, "services/eri/pkg/db/") || strings.HasPrefix(rel, "scripts/") {
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
			if bytes.HasPrefix(trimmed, []byte("//")) || bytes.HasPrefix(trimmed, []byte("/*")) || bytes.HasPrefix(trimmed, []byte("*")) {
				continue
			}
			if ddlRegex.Match(trimmed) {
				violations = append(violations, fmt.Sprintf("%s:%d: imperative DDL (CREATE/ALTER/DROP TABLE) detected in application code; schema changes must be declared in schema.sql", rel, i+1))
			}
		}
	}
	return violations
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
		if bytes.Contains(data, []byte("main()")) {
			violations = append(violations, rel+": direct execution of main() in test file detected")
		}
	}
	return violations
}

func checkDependabotCoverage(absRoot string) []string {
	depPath := filepath.Join(absRoot, ".github", "dependabot.yml")
	depBytes, err := os.ReadFile(depPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return []string{err.Error()}
	}
	depContent := string(depBytes)
	candidateManifests := []string{"go.mod", "services/console/ui/package.json"}
	var violations []string
	for _, m := range candidateManifests {
		if _, statErr := os.Stat(filepath.Join(absRoot, m)); statErr != nil {
			continue
		}
		dir := filepath.Dir(m)
		targetDir := "/"
		if dir != "." {
			targetDir = "/" + filepath.ToSlash(dir)
		}
		quotedDouble := "\"" + targetDir + "\""
		quotedSingle := "'" + targetDir + "'"
		if !strings.Contains(depContent, quotedDouble) && !strings.Contains(depContent, quotedSingle) && !strings.Contains(depContent, targetDir) {
			violations = append(violations, ".github/dependabot.yml: missing package manifest coverage for directory "+targetDir)
		}
	}
	return violations
}
