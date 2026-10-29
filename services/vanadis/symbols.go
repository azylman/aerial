package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const defaultShareRoot = "/share"

var (
	topDeclPrefixes     = []string{"func ", "type ", "var ", "const "}
	declKeywordPrefixes = []string{"func ", "type ", "var ", "const ", "export ", "function ", "class "}
)

func cleanRepoDirName(repo string) string {
	repo = strings.TrimSpace(repo)
	if idx := strings.LastIndex(repo, "/"); idx >= 0 {
		return repo[idx+1:]
	}
	return repo
}

func normalizeSymbolName(symbol string) string {
	sym := strings.TrimSpace(symbol)
	sym = strings.TrimSuffix(sym, "()")
	if idx := strings.LastIndex(sym, "."); idx >= 0 {
		sym = sym[idx+1:]
	}
	return strings.TrimSpace(sym)
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func isIdentByte(b byte) bool {
	switch {
	case b == '_':
		return true
	case b >= 'a' && b <= 'z':
		return true
	case b >= 'A' && b <= 'Z':
		return true
	case b >= '0' && b <= '9':
		return true
	default:
		return false
	}
}

func isBoundedIdent(line string, start, end int) bool {
	if start > 0 && isIdentByte(line[start-1]) {
		return false
	}
	if end < len(line) && isIdentByte(line[end]) {
		return false
	}
	return true
}

func containsIdent(line, ident string) bool {
	if ident == "" {
		return false
	}
	idx := 0
	for {
		pos := strings.Index(line[idx:], ident)
		if pos < 0 {
			return false
		}
		start := idx + pos
		end := start + len(ident)
		if isBoundedIdent(line, start, end) {
			return true
		}
		idx = start + 1
	}
}

func findCommentSlashIndex(line string) int {
	inQuotes, inBacktick, escaped := false, false, false
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case escaped:
			escaped = false
		case c == '\\' && inQuotes:
			escaped = true
		case c == '"' && !inBacktick:
			inQuotes = !inQuotes
		case c == '`' && !inQuotes:
			inBacktick = !inBacktick
		case c == '/' && !inQuotes && !inBacktick:
			if i+1 < len(line) && line[i+1] == '/' {
				return i
			}
		}
	}
	return -1
}

func stripLineComment(line string) string {
	if idx := strings.Index(line, "//"); idx >= 0 {
		if actualIdx := findCommentSlashIndex(line); actualIdx >= 0 {
			return line[:actualIdx]
		}
	}
	return line
}

func updateRawStrState(line string, inRawStr bool) bool {
	for i := 0; i < len(line); i++ {
		if line[i] == '`' {
			inRawStr = !inRawStr
		}
	}
	return inRawStr
}

func isBlockTerminalLine(line, clean string) (int, bool) {
	if strings.HasPrefix(line, "}") {
		return 1, true
	}
	if strings.HasPrefix(line, ")") && !strings.HasSuffix(clean, "{") {
		return 1, true
	}
	if hasAnyPrefix(line, topDeclPrefixes) {
		return 0, true
	}
	return 0, false
}

func scanBlockEnd(lines []string, startIdx int) int {
	inRawStr := updateRawStrState(lines[startIdx], false)
	cleanStart := strings.TrimSpace(stripLineComment(lines[startIdx]))
	if !strings.HasSuffix(cleanStart, "{") && !strings.HasSuffix(cleanStart, "(") {
		return startIdx + 1
	}
	for j := startIdx + 1; j < len(lines); j++ {
		if !inRawStr {
			clean := strings.TrimSpace(stripLineComment(lines[j]))
			if offset, ok := isBlockTerminalLine(lines[j], clean); ok {
				return j + offset
			}
		}
		inRawStr = updateRawStrState(lines[j], inRawStr)
	}
	return len(lines)
}

func isColumn0Line(line string) bool {
	if len(line) == 0 {
		return false
	}
	b := line[0]
	if b == ' ' || b == '\t' {
		return false
	}
	if strings.HasPrefix(line, "//") || strings.HasPrefix(line, "/*") {
		return false
	}
	return true
}

func isTopLevelDeclaration(line, sym string) bool {
	if !isColumn0Line(line) {
		return false
	}
	matchesPrefix := hasAnyPrefix(line, declKeywordPrefixes) || strings.HasPrefix(line, sym+"(")
	if !matchesPrefix {
		return false
	}
	return containsIdent(stripLineComment(line), sym)
}

func findTopLevelSymbolSpan(lines []string, sym string) (int, int, bool) {
	for idx, line := range lines {
		if isTopLevelDeclaration(line, sym) {
			return idx + 1, scanBlockEnd(lines, idx), true
		}
	}
	return 0, 0, false
}

func findFallbackSymbolLine(lines []string, sym string) (int, int, bool) {
	for idx, line := range lines {
		clean := strings.TrimSpace(stripLineComment(line))
		if clean == "" || strings.HasPrefix(clean, "/*") || strings.HasPrefix(clean, "*") {
			continue
		}
		if containsIdent(clean, sym) {
			return idx + 1, idx + 1, true
		}
	}
	return 0, 0, false
}

// FindSymbolSpan locates the 1-based start and end line for symbol in content.
func FindSymbolSpan(content, symbol string) (int, int, bool) {
	sym := normalizeSymbolName(symbol)
	if sym == "" || content == "" {
		return 0, 0, false
	}
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	if start, end, ok := findTopLevelSymbolSpan(lines, sym); ok {
		return start, end, true
	}
	return findFallbackSymbolLine(lines, sym)
}

type targetFileInfo struct {
	relFile string
	lines   []string
}

type targetSymSpan struct {
	sym   string
	start int
	end   int
}

func loadTargetFiles(shareRoot, repo string, targetFiles []string) []targetFileInfo {
	repoDir := filepath.Join(strings.TrimSpace(shareRoot), cleanRepoDirName(repo))
	var files []targetFileInfo
	seen := make(map[string]bool)
	for _, tf := range targetFiles {
		rel := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(tf), "./"), "/")
		if rel == "" || seen[rel] {
			continue
		}
		fullPath := filepath.Join(repoDir, rel)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			continue
		}
		seen[rel] = true
		lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
		files = append(files, targetFileInfo{relFile: rel, lines: lines})
	}
	return files
}

func expandTargetFilesWithTests(targetFiles []string) []string {
	seen := make(map[string]bool, len(targetFiles))
	for _, tf := range targetFiles {
		rel := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(tf), "./"), "/")
		if rel != "" {
			seen[rel] = true
		}
	}
	expanded := make([]string, 0, len(targetFiles)*2)
	for _, tf := range targetFiles {
		expanded = append(expanded, tf)
		rel := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(tf), "./"), "/")
		if strings.HasSuffix(rel, ".go") && !strings.HasSuffix(rel, "_test.go") {
			testRel := strings.TrimSuffix(rel, ".go") + "_test.go"
			if !seen[testRel] {
				seen[testRel] = true
				expanded = append(expanded, testRel)
			}
		}
	}
	return expanded
}

type testFuncBlock struct {
	name  string
	start int
	end   int
	lines []string
}

func parseTopLevelTestFuncName(line string) string {
	if !strings.HasPrefix(line, "func Test") {
		return ""
	}
	rest := line[len("func "):]
	end := 0
	for end < len(rest) && isIdentByte(rest[end]) {
		end++
	}
	if end > len("Test") && end < len(rest) && (rest[end] == '(' || rest[end] == '[') {
		return rest[:end]
	}
	return ""
}

func collectTestFuncBlocks(lines []string) []testFuncBlock {
	var blocks []testFuncBlock
	for idx, line := range lines {
		name := parseTopLevelTestFuncName(line)
		if name == "" {
			continue
		}
		end := scanBlockEnd(lines, idx)
		blocks = append(blocks, testFuncBlock{
			name:  name,
			start: idx + 1,
			end:   end,
			lines: lines[idx:end],
		})
	}
	return blocks
}

func testNameMatchesSymbol(tf testFuncBlock, sym string) bool {
	lowerName, lowerSym := strings.ToLower(tf.name), strings.ToLower(sym)
	if lowerName == "test"+lowerSym || strings.HasPrefix(lowerName, "test"+lowerSym+"_") {
		return true
	}
	_, _, inBody := findFallbackSymbolLine(tf.lines, sym)
	return strings.Contains(lowerName, lowerSym) && inBody
}

func matchTestBlocksForSymbol(blocks []testFuncBlock, sym string) []testFuncBlock {
	var matched []testFuncBlock
	for _, tf := range blocks {
		if testNameMatchesSymbol(tf, sym) {
			matched = append(matched, tf)
		}
	}
	if len(matched) > 0 {
		return matched
	}
	for _, tf := range blocks {
		if _, _, ok := findFallbackSymbolLine(tf.lines, sym); ok {
			matched = append(matched, tf)
		}
	}
	return matched
}

func appendUniqueSpan(spans []targetSymSpan, cand targetSymSpan) []targetSymSpan {
	for _, existing := range spans {
		if existing.sym == cand.sym && existing.start == cand.start {
			return spans
		}
	}
	return append(spans, cand)
}

func runSymbolPass1(cleanSymbols []string, files []targetFileInfo, fileSpans map[string][]targetSymSpan, resolved map[string]bool) {
	for _, sym := range cleanSymbols {
		for _, f := range files {
			if start, end, ok := findTopLevelSymbolSpan(f.lines, sym); ok {
				fileSpans[f.relFile] = appendUniqueSpan(fileSpans[f.relFile], targetSymSpan{sym: sym, start: start, end: end})
				resolved[sym] = true
			}
		}
	}
}

func runSymbolPass2(cleanSymbols []string, files []targetFileInfo, fileSpans map[string][]targetSymSpan, resolved map[string]bool) {
	for _, sym := range cleanSymbols {
		if resolved[sym] {
			continue
		}
		for _, f := range files {
			if start, end, ok := findFallbackSymbolLine(f.lines, sym); ok {
				fileSpans[f.relFile] = appendUniqueSpan(fileSpans[f.relFile], targetSymSpan{sym: sym, start: start, end: end})
				resolved[sym] = true
			}
		}
	}
}

func runTestSymbolPass(cleanSymbols []string, testFiles []targetFileInfo, fileSpans map[string][]targetSymSpan, resolved map[string]bool) {
	for _, f := range testFiles {
		blocks := collectTestFuncBlocks(f.lines)
		for _, sym := range cleanSymbols {
			for _, tf := range matchTestBlocksForSymbol(blocks, sym) {
				fileSpans[f.relFile] = appendUniqueSpan(fileSpans[f.relFile], targetSymSpan{sym: tf.name, start: tf.start, end: tf.end})
				resolved[sym] = true
			}
		}
	}
}

func resolveSingleFileSpans(cleanSymbols []string, f targetFileInfo, fileSpans map[string][]targetSymSpan, resolved map[string]bool) {
	content := strings.Join(f.lines, "\n")
	for _, sym := range cleanSymbols {
		if start, end, ok := FindSymbolSpan(content, sym); ok {
			fileSpans[f.relFile] = appendUniqueSpan(fileSpans[f.relFile], targetSymSpan{sym: sym, start: start, end: end})
			resolved[sym] = true
		}
	}
}

func formatSpanLines(targetFiles []string, fileSpans map[string][]targetSymSpan) []string {
	var spanLines []string
	seenFileOrder := make(map[string]bool)
	for _, tf := range targetFiles {
		rel := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(tf), "./"), "/")
		if rel == "" || seenFileOrder[rel] {
			continue
		}
		seenFileOrder[rel] = true
		spans := fileSpans[rel]
		if len(spans) == 0 {
			continue
		}
		var symStrs []string
		for _, sp := range spans {
			symStrs = append(symStrs, fmt.Sprintf("%s (L%d-L%d)", sp.sym, sp.start, sp.end))
		}
		spanLines = append(spanLines, fmt.Sprintf("- %s: %s", rel, strings.Join(symStrs, ", ")))
	}
	return spanLines
}

// ResolveSymbolSpans resolves deterministic StartLine-EndLine spans across targetFiles under shareRoot/repo.
func ResolveSymbolSpans(shareRoot, repo string, targetFiles, targetSymbols []string) ([]string, bool) {
	var cleanSymbols []string
	seenSyms := make(map[string]bool)
	for _, s := range targetSymbols {
		norm := normalizeSymbolName(s)
		if norm != "" && !seenSyms[norm] {
			seenSyms[norm] = true
			cleanSymbols = append(cleanSymbols, norm)
		}
	}
	if len(cleanSymbols) == 0 {
		files := loadTargetFiles(shareRoot, repo, targetFiles)
		spanLines := make([]string, 0, len(files))
		for _, f := range files {
			spanLines = append(spanLines, fmt.Sprintf("- %s (L1-L%d)", f.relFile, len(f.lines)))
		}
		return spanLines, len(spanLines) > 0
	}

	expandedFiles := expandTargetFilesWithTests(targetFiles)
	files := loadTargetFiles(shareRoot, repo, expandedFiles)
	if len(files) == 0 {
		return nil, false
	}

	var nonTestFiles, testFiles []targetFileInfo
	for _, f := range files {
		if strings.HasSuffix(f.relFile, "_test.go") {
			testFiles = append(testFiles, f)
		} else {
			nonTestFiles = append(nonTestFiles, f)
		}
	}

	fileSpans := make(map[string][]targetSymSpan)
	resolved := make(map[string]bool)

	if len(files) == 1 && len(testFiles) == 0 {
		resolveSingleFileSpans(cleanSymbols, files[0], fileSpans, resolved)
	} else {
		runSymbolPass1(cleanSymbols, files, fileSpans, resolved)
		runSymbolPass2(cleanSymbols, nonTestFiles, fileSpans, resolved)
		runTestSymbolPass(cleanSymbols, testFiles, fileSpans, resolved)
		runSymbolPass2(cleanSymbols, testFiles, fileSpans, resolved)
	}

	spanLines := formatSpanLines(expandedFiles, fileSpans)
	return spanLines, len(resolved) == len(cleanSymbols)
}
