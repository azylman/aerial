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

type communitySkillCheck struct {
	repoName  string
	dockerArg string
	bazelName string
	skills    []string
}

var communitySkillsToCheck = []communitySkillCheck{
	{
		repoName:  "android_skills",
		dockerArg: "ANDROID_SKILLS",
		bazelName: "android_skills",
		skills: []string{
			"wear-compose-m3",
			"wear-widgets",
			"jetpack-compose-performance",
			"android-intent-security",
			"android-permissions-security",
			"anr-audit-and-fix",
			"testing-setup",
		},
	},
	{
		repoName:  "cc_skills_golang",
		dockerArg: "CC_SKILLS_GOLANG",
		bazelName: "cc_skills_golang",
		skills: []string{
			"golang-code-style",
			"golang-concurrency",
			"golang-context",
			"golang-error-handling",
			"golang-modernize",
			"golang-performance",
			"golang-safety",
			"golang-structs-interfaces",
			"golang-testing",
		},
	},
	{
		repoName:  "beautiful_rust",
		dockerArg: "BEAUTIFUL_RUST",
		bazelName: "beautiful_rust",
		skills: []string{
			"rust-best-practices",
			"rust-port",
			"rust-review",
		},
	},
	{
		repoName:  "kotlin_agent_skills",
		dockerArg: "KOTLIN_AGENT_SKILLS",
		bazelName: "kotlin_agent_skills",
		skills: []string{
			"kotlin-tooling-agp9-migration",
			"kotlin-tooling-kotlin-toolchain",
		},
	},
	{
		repoName:  "kotlin_footguns",
		dockerArg: "KOTLIN_FOOTGUNS",
		bazelName: "kotlin_footguns",
		skills: []string{
			"compose-screens-footguns",
			"footgun-scan",
			"remote-api-footguns",
			"state-and-background-footguns",
		},
	},
}

func readParityFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func findBazelArchiveBlock(moduleBazel, bazelName string) string {
	const target = "http_archive("
	idx := 0
	for {
		start := strings.Index(moduleBazel[idx:], target)
		if start == -1 {
			return ""
		}
		archiveStart := idx + start
		idx = archiveStart + len(target)

		nextArchive := strings.Index(moduleBazel[idx:], target)
		var block string
		if nextArchive == -1 {
			block = moduleBazel[archiveStart:]
		} else {
			block = moduleBazel[archiveStart : idx+nextArchive]
		}

		if strings.Contains(block, `name = "`+bazelName+`"`) {
			return block
		}
	}
}

func checkRepoSkillsParity(chk communitySkillCheck, dockerContent, moduleBazel string) []string {
	var violations []string

	argRevRegex := regexp.MustCompile(`ARG\s+` + chk.dockerArg + `_REV=([^\s]+)`)
	argShaRegex := regexp.MustCompile(`ARG\s+` + chk.dockerArg + `_SHA256=([^\s]+)`)

	dockerRevMatch := argRevRegex.FindStringSubmatch(dockerContent)
	if len(dockerRevMatch) < 2 {
		violations = append(violations, "services/eri-restate/Dockerfile: missing ARG "+chk.dockerArg+"_REV")
	}
	dockerShaMatch := argShaRegex.FindStringSubmatch(dockerContent)
	if len(dockerShaMatch) < 2 {
		violations = append(violations, "services/eri-restate/Dockerfile: missing ARG "+chk.dockerArg+"_SHA256")
	}

	repoBlockMatch := findBazelArchiveBlock(moduleBazel, chk.bazelName)
	if repoBlockMatch == "" {
		violations = append(violations, "MODULE.bazel: missing http_archive for "+chk.bazelName)
		return violations
	}

	bazelShaRegex := regexp.MustCompile(`sha256\s*=\s*"([^"]+)"`)
	bazelRevRegex := regexp.MustCompile(`urls\s*=\s*\[\s*"https://github\.com/[^/]+/[^/]+/archive/([^"/]+)\.tar\.gz"`)

	bazelShaMatch := bazelShaRegex.FindStringSubmatch(repoBlockMatch)
	if len(bazelShaMatch) < 2 {
		violations = append(violations, "MODULE.bazel: missing or invalid sha256 for "+chk.bazelName)
	}
	bazelRevMatch := bazelRevRegex.FindStringSubmatch(repoBlockMatch)
	if len(bazelRevMatch) < 2 {
		violations = append(violations, "MODULE.bazel: missing or invalid url revision for "+chk.bazelName)
	}

	if len(dockerRevMatch) >= 2 && len(bazelRevMatch) >= 2 && dockerRevMatch[1] != bazelRevMatch[1] {
		violations = append(violations, "community skills parity: revision mismatch for "+chk.repoName+
			" (Dockerfile="+dockerRevMatch[1]+" vs MODULE.bazel="+bazelRevMatch[1]+")")
	}
	if len(dockerShaMatch) >= 2 && len(bazelShaMatch) >= 2 && dockerShaMatch[1] != bazelShaMatch[1] {
		violations = append(violations, "community skills parity: sha256 mismatch for "+chk.repoName+
			" (Dockerfile="+dockerShaMatch[1]+" vs MODULE.bazel="+bazelShaMatch[1]+")")
	}

	for _, skill := range chk.skills {
		if !strings.Contains(dockerContent, skill) {
			violations = append(violations, "services/eri-restate/Dockerfile: missing community skill "+skill+" for "+chk.repoName)
		}
		if !strings.Contains(repoBlockMatch, skill) {
			violations = append(violations, "MODULE.bazel: missing community skill "+skill+" for "+chk.repoName)
		}
	}

	return violations
}

func checkCommunitySkillsParity(root string) []string {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return []string{err.Error()}
	}

	dockerfilePath := filepath.Join(absRoot, "services", "eri-restate", "Dockerfile")
	moduleBazelPath := filepath.Join(absRoot, "MODULE.bazel")

	dockerContent, err := readParityFile(dockerfilePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return []string{err.Error()}
	}

	bazelContent, err := readParityFile(moduleBazelPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return []string{err.Error()}
	}

	var violations []string
	for _, chk := range communitySkillsToCheck {
		violations = append(violations, checkRepoSkillsParity(chk, dockerContent, bazelContent)...)
	}

	return violations
}
