package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const defaultShareRoot = "/share"

var identByteTable = func() [256]bool {
	var table [256]bool
	for c := 'a'; c <= 'z'; c++ {
		table[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		table[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		table[c] = true
	}
	table['_'] = true
	return table
}()

var nonDeclPrefixes = []string{" ", "\t", "//", "/*"}

var topLevelPrefixes = []string{
	"func ", "type ", "var ", "const ", "export ", "function ", "class ",
}

var declStopPrefixes = []string{"func ", "type ", "var ", "const "}

func cleanRepoDirName(repo string) string {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return ""
	}
	parts := strings.Split(repo, "/")
	return parts[len(parts)-1]
}

func normalizeSymbolName(symbol string) string {
	sym := strings.TrimSpace(symbol)
	sym = strings.TrimLeft(sym, "*&()")
	sym = strings.TrimRight(sym, "()")
	if idx := strings.LastIndex(sym, "."); idx >= 0 {
		sym = sym[idx+1:]
	}
	sym = strings.TrimLeft(sym, "*&()")
	sym = strings.TrimRight(sym, "()")
	return strings.TrimSpace(sym)
}

func isBoundary(s string, idx int) bool {
	if idx < 0 {
		return true
	}
	if idx >= len(s) {
		return true
	}
	return !identByteTable[s[idx]]
}

func containsIdent(line, sym string) bool {
	if sym == "" {
		return false
	}
	start := 0
	for {
		idx := strings.Index(line[start:], sym)
		if idx < 0 {
			return false
		}
		matchIdx := start + idx
		endIdx := matchIdx + len(sym)
		if isBoundary(line, matchIdx-1) {
			if isBoundary(line, endIdx) {
				return true
			}
		}
		start = matchIdx + 1
	}
}

func stripLineComment(line string) string {
	if idx := strings.Index(line, "//"); idx >= 0 {
		line = line[:idx]
	}
	for {
		start := strings.Index(line, "/*")
		if start < 0 {
			break
		}
		end := strings.Index(line[start+2:], "*/")
		if end < 0 {
			line = line[:start]
			break
		}
		line = line[:start] + line[start+2+end+2:]
	}
	return line
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func updateRawStrState(line string, inRawStr bool) bool {
	count := strings.Count(line, "`")
	if count%2 == 1 {
		return !inRawStr
	}
	return inRawStr
}

func isColumn0Decl(line string) bool {
	if line == "" {
		return false
	}
	return !hasAnyPrefix(line, nonDeclPrefixes)
}

func isTopLevelDeclLine(line, sym string) bool {
	if !isColumn0Decl(line) {
		return false
	}
	if hasAnyPrefix(line, topLevelPrefixes) {
		return true
	}
	return strings.HasPrefix(line, sym+"(")
}

func isSingleLineClosed(decl string) bool {
	if strings.HasSuffix(decl, "}") {
		return strings.Contains(decl, "{")
	}
	return false
}

func shouldScanBlock(decl string) bool {
	if isSingleLineClosed(decl) {
		return false
	}
	if strings.HasSuffix(decl, "{") {
		return true
	}
	if strings.HasSuffix(decl, "(") {
		return true
	}
	return strings.HasSuffix(decl, ",")
}

func isClosingParenEnd(rawLine, cleanLine string) bool {
	if !strings.HasPrefix(rawLine, ")") {
		return false
	}
	return !strings.HasSuffix(cleanLine, "{")
}

func scanBlockEnd(lines []string, startIdx int) int {
	inRawStr := updateRawStrState(lines[startIdx], false)
	decl := strings.TrimSpace(stripLineComment(lines[startIdx]))
	if !shouldScanBlock(decl) {
		return startIdx + 1
	}
	for j := startIdx + 1; j < len(lines); j++ {
		if !inRawStr {
			clean := strings.TrimSpace(stripLineComment(lines[j]))
			if strings.HasPrefix(lines[j], "}") {
				return j + 1
			}
			if isClosingParenEnd(lines[j], clean) {
				return j + 1
			}
			if hasAnyPrefix(lines[j], declStopPrefixes) {
				return j
			}
		}
		inRawStr = updateRawStrState(lines[j], inRawStr)
	}
	return len(lines)
}

func findTopLevelSymbolSpan(lines []string, sym string) (int, int, bool) {
	for idx, line := range lines {
		if isTopLevelDeclLine(line, sym) {
			clean := stripLineComment(line)
			if containsIdent(clean, sym) {
				return idx + 1, scanBlockEnd(lines, idx), true
			}
		}
	}
	return 0, 0, false
}

func findFallbackSymbolLine(lines []string, sym string) (int, int, bool) {
	for idx, line := range lines {
		clean := strings.TrimSpace(stripLineComment(line))
		if clean != "" {
			if containsIdent(clean, sym) {
				return idx + 1, idx + 1, true
			}
		}
	}
	return 0, 0, false
}

// FindSymbolSpan locates the 1-based start and end line for symbol within content.
func FindSymbolSpan(content, symbol string) (int, int, bool) {
	sym := normalizeSymbolName(symbol)
	if sym == "" {
		return 0, 0, false
	}
	if content == "" {
		return 0, 0, false
	}
	normalizedContent := strings.ReplaceAll(content, "\r\n", "\n")
	lines := strings.Split(normalizedContent, "\n")
	if start, end, ok := findTopLevelSymbolSpan(lines, sym); ok {
		return start, end, true
	}
	if start, end, ok := findFallbackSymbolLine(lines, sym); ok {
		return start, end, true
	}
	return 0, 0, false
}

type fileContent struct {
	relPath string
	content string
	lines   []string
}

type symSpan struct {
	symbol    string
	startLine int
	endLine   int
}

func cleanSymbolList(symbols []string) []string {
	var clean []string
	seen := make(map[string]bool)
	for _, s := range symbols {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			if !seen[trimmed] {
				seen[trimmed] = true
				clean = append(clean, trimmed)
			}
		}
	}
	return clean
}

func cleanRelPath(p string) string {
	return strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(p), "./"), "/")
}

func safeJoinPath(baseDir, relPath string) (string, bool) {
	cleanRel := filepath.Clean(relPath)
	if strings.HasPrefix(cleanRel, "..") {
		return "", false
	}
	if filepath.IsAbs(cleanRel) {
		return "", false
	}
	full := filepath.Join(baseDir, cleanRel)
	rel, err := filepath.Rel(baseDir, full)
	if err != nil {
		return "", false
	}
	if strings.HasPrefix(rel, "..") {
		return "", false
	}
	return full, true
}

func readTargetFiles(repoDir string, targetFiles []string) []fileContent {
	var files []fileContent
	seen := make(map[string]bool)
	for _, f := range targetFiles {
		rel := cleanRelPath(f)
		if rel == "" {
			continue
		}
		if seen[rel] {
			continue
		}
		seen[rel] = true
		fullPath, ok := safeJoinPath(repoDir, rel)
		if !ok {
			continue
		}
		data, err := os.ReadFile(fullPath)
		if err != nil {
			continue
		}
		normalized := strings.ReplaceAll(string(data), "\r\n", "\n")
		files = append(files, fileContent{
			relPath: rel,
			content: normalized,
			lines:   strings.Split(normalized, "\n"),
		})
	}
	return files
}

func resolveSingleFile(f fileContent, cleanSymbols []string, fileMatches map[string][]symSpan, resolved map[string]bool) {
	for _, sym := range cleanSymbols {
		norm := normalizeSymbolName(sym)
		if norm == "" {
			continue
		}
		if start, end, ok := FindSymbolSpan(f.content, sym); ok {
			fileMatches[f.relPath] = append(fileMatches[f.relPath], symSpan{symbol: sym, startLine: start, endLine: end})
			resolved[sym] = true
		}
	}
}

func resolvePass1(files []fileContent, cleanSymbols []string, fileMatches map[string][]symSpan, resolved map[string]bool) {
	for _, sym := range cleanSymbols {
		norm := normalizeSymbolName(sym)
		if norm == "" {
			continue
		}
		for _, f := range files {
			if start, end, ok := findTopLevelSymbolSpan(f.lines, norm); ok {
				fileMatches[f.relPath] = append(fileMatches[f.relPath], symSpan{symbol: sym, startLine: start, endLine: end})
				resolved[sym] = true
				break
			}
		}
	}
}

func resolvePass2(files []fileContent, cleanSymbols []string, fileMatches map[string][]symSpan, resolved map[string]bool) {
	for _, sym := range cleanSymbols {
		if resolved[sym] {
			continue
		}
		norm := normalizeSymbolName(sym)
		if norm == "" {
			continue
		}
		for _, f := range files {
			if start, end, ok := findFallbackSymbolLine(f.lines, norm); ok {
				fileMatches[f.relPath] = append(fileMatches[f.relPath], symSpan{symbol: sym, startLine: start, endLine: end})
				resolved[sym] = true
				break
			}
		}
	}
}

func formatFileSpans(targetFiles []string, fileMatches map[string][]symSpan) []string {
	var spanLines []string
	seen := make(map[string]bool)
	for _, f := range targetFiles {
		rel := cleanRelPath(f)
		if rel == "" {
			continue
		}
		if seen[rel] {
			continue
		}
		seen[rel] = true
		matches := fileMatches[rel]
		if len(matches) == 0 {
			continue
		}
		var parts []string
		for _, m := range matches {
			parts = append(parts, fmt.Sprintf("%s (L%d-L%d)", m.symbol, m.startLine, m.endLine))
		}
		spanLines = append(spanLines, fmt.Sprintf("- %s: %s", rel, strings.Join(parts, ", ")))
	}
	return spanLines
}

// ResolveSymbolSpans pre-resolves deterministic StartLine-EndLine spans for targetSymbols across targetFiles on local disk.
func ResolveSymbolSpans(shareRoot, repo string, targetFiles, targetSymbols []string) ([]string, bool) {
	cleanSymbols := cleanSymbolList(targetSymbols)
	if len(cleanSymbols) == 0 {
		return nil, true
	}
	if len(targetFiles) == 0 {
		return nil, true
	}
	repoDir := filepath.Join(strings.TrimSpace(shareRoot), cleanRepoDirName(repo))
	files := readTargetFiles(repoDir, targetFiles)
	if len(files) == 0 {
		return nil, false
	}
	fileMatches := make(map[string][]symSpan)
	resolved := make(map[string]bool)
	if len(files) == 1 {
		resolveSingleFile(files[0], cleanSymbols, fileMatches, resolved)
	} else {
		resolvePass1(files, cleanSymbols, fileMatches, resolved)
		resolvePass2(files, cleanSymbols, fileMatches, resolved)
	}
	spanLines := formatFileSpans(targetFiles, fileMatches)
	return spanLines, len(resolved) == len(cleanSymbols)
}
