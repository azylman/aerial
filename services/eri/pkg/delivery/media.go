package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
)

const (
	MaxAttachmentsPerMessage = 10
	MaxAttachmentSizeBytes   = 8 * 1024 * 1024 // 8 MB
)

var (
	internalMediaHTTPClient = &http.Client{Timeout: 5 * time.Second}
	allowedInternalHosts    = map[string]bool{}
)

func isInternalAerialHTTPURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return false
	}
	return strings.HasSuffix(host, ".aerial") || allowedInternalHosts[host]
}

// AllowedAttachmentRoots defines the whitelisted root directories for local file attachments.
var AllowedAttachmentRoots = []string{
	"/root/.gemini/antigravity-cli/brain",
	"/root/.gemini/antigravity-cli/scratch",
	"/tmp",
	"/dev/shm",
}

func init() {
	if tmp := os.TempDir(); tmp != "" {
		AllowedAttachmentRoots = append(AllowedAttachmentRoots, tmp)
	}
}

// Attachment represents a validated, memory-safe in-memory file payload for Discord delivery.
type Attachment struct {
	Filename    string
	ContentType string
	Data        []byte
}

// ToDiscordFile converts the memory-safe attachment into a fresh, rewindable *discordgo.File.
func (a *Attachment) ToDiscordFile() *discordgo.File {
	return &discordgo.File{
		Name:        a.Filename,
		ContentType: a.ContentType,
		Reader:      bytes.NewReader(a.Data),
	}
}

var mdImageRegex = regexp.MustCompile(`!\[([^\]]*)\]\(([^)]+)\)`)

func formatUnavailableFallback(altText, baseName string) string {
	if altText != "" {
		return fmt.Sprintf("*(Image attachment unavailable: %s - %s)*", altText, baseName)
	}
	return fmt.Sprintf("*(Image attachment unavailable: %s)*", baseName)
}

func sanitizeImageURLFallbackBaseName(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "image.jpg"
	}
	b := filepath.Base(u.Path)
	if b == "" || b == "." || b == "/" {
		return "image.jpg"
	}
	return b
}

func processInternalImageMatch(rawPath, altText string, attachments *[]*Attachment) string {
	if len(*attachments) >= MaxAttachmentsPerMessage {
		return ""
	}
	att, err := ResolveAndFetchInternalImage(rawPath)
	if err != nil {
		return formatUnavailableFallback(altText, sanitizeImageURLFallbackBaseName(rawPath))
	}
	*attachments = append(*attachments, att)
	if altText != "" {
		return fmt.Sprintf("**%s**", altText)
	}
	return ""
}

func processLocalImageMatch(rawPath, baseDir, altText string, attachments *[]*Attachment) string {
	if len(*attachments) >= MaxAttachmentsPerMessage {
		return ""
	}
	att, err := ResolveAndValidateLocalImage(rawPath, baseDir)
	if err != nil {
		baseName := filepath.Base(rawPath)
		if baseName == "" || baseName == "." {
			baseName = "image"
		}
		return formatUnavailableFallback(altText, baseName)
	}
	*attachments = append(*attachments, att)
	if altText != "" {
		return fmt.Sprintf("**%s**", altText)
	}
	return ""
}

// ExtractAndSanitizeMedia parses markdown image references, resolves and validates local files,
// extracts valid attachments up to MaxAttachmentsPerMessage, and returns sanitized text and attachments.
func ExtractAndSanitizeMedia(text string, baseDir string) (string, []*Attachment) {
	if strings.TrimSpace(text) == "" {
		return text, nil
	}

	var attachments []*Attachment
	lines := strings.Split(text, "\n")
	var processedLines []string
	var activeFence string
	var activeFenceLen int

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		fenceMarker, fenceLen, isSingleLineSpan := parseCodeFence(trimmed)
		if fenceMarker != "" && !isSingleLineSpan {
			if activeFence == "" {
				activeFence = fenceMarker
				activeFenceLen = fenceLen
			} else if activeFence == fenceMarker && fenceLen >= activeFenceLen {
				activeFence = ""
				activeFenceLen = 0
			}
			processedLines = append(processedLines, line)
			continue
		}

		if activeFence != "" || isSingleLineSpan {
			processedLines = append(processedLines, line)
			continue
		}

		newLine := mdImageRegex.ReplaceAllStringFunc(line, func(match string) string {
			sub := mdImageRegex.FindStringSubmatch(match)
			if len(sub) < 3 {
				return match
			}
			altText := strings.TrimSpace(sub[1])
			rawPath := strings.TrimSpace(sub[2])

			// Remote URLs: check if internal .aerial URL to attach, otherwise leave untouched for Discord client unfurling
			if strings.HasPrefix(rawPath, "http://") || strings.HasPrefix(rawPath, "https://") {
				if !isInternalAerialHTTPURL(rawPath) {
					return match
				}
				if len(attachments) >= MaxAttachmentsPerMessage {
					return match
				}
				return processInternalImageMatch(rawPath, altText, &attachments)
			}

			if len(attachments) >= MaxAttachmentsPerMessage {
				return match
			}
			return processLocalImageMatch(rawPath, baseDir, altText, &attachments)
		})

		processedLines = append(processedLines, newLine)
	}

	cleanedText := strings.Join(processedLines, "\n")
	cleanedText = SanitizeIntermediateStatus(cleanedText)
	cleanedText = FormatDiscordLatex(cleanedText)
	cleanedText = cleanDuplicateBlankLines(cleanedText)

	return cleanedText, attachments
}

var validMediaExts = map[string]bool{
	".png":  true,
	".jpg":  true,
	".jpeg": true,
	".gif":  true,
	".webp": true,
	".wav":  true,
	".mp3":  true,
	".ogg":  true,
	".m4a":  true,
	".flac": true,
}

func tryAutoAttachEntry(dir string, entry os.DirEntry, baseDir string, cutoff time.Time, existingCanonical map[string]bool) *Attachment {
	if !entry.Type().IsRegular() || strings.HasPrefix(entry.Name(), ".") {
		return nil
	}

	ext := strings.ToLower(filepath.Ext(entry.Name()))
	if !validMediaExts[ext] {
		return nil
	}

	fullPath := filepath.Join(dir, entry.Name())
	info, err := entry.Info()
	if err != nil || info.Size() <= 0 || info.Size() > MaxAttachmentSizeBytes {
		return nil
	}

	if info.ModTime().Before(cutoff) {
		return nil
	}

	canonicalPath, err := filepath.EvalSymlinks(fullPath)
	if err != nil {
		canonicalPath = fullPath
	}
	baseName := filepath.Base(canonicalPath)
	if existingCanonical[baseName] || existingCanonical[entry.Name()] {
		return nil
	}

	att, err := ResolveAndValidateLocalImage(fullPath, baseDir)
	if err != nil {
		return nil
	}

	existingCanonical[att.Filename] = true
	existingCanonical[baseName] = true
	existingCanonical[entry.Name()] = true
	return att
}

// AutoAttachNewMedia scans top-level baseDir and baseDir/scratch for newly created/modified
// image files since turnStartTime (with a 2-second safety buffer for OS timestamp truncation)
// and appends resolved attachments up to MaxAttachmentsPerMessage.
func AutoAttachNewMedia(baseDir string, sinceTime time.Time, existing []*Attachment) []*Attachment {
	if baseDir == "" || sinceTime.IsZero() || len(existing) >= MaxAttachmentsPerMessage {
		return existing
	}

	existingCanonical := make(map[string]bool)
	for _, att := range existing {
		if att != nil && att.Filename != "" {
			existingCanonical[att.Filename] = true
		}
	}

	cutoff := sinceTime.Add(-2 * time.Second)
	dirsToScan := []string{
		baseDir,
		filepath.Join(baseDir, "scratch"),
	}

	result := append([]*Attachment(nil), existing...)

	for _, dir := range dirsToScan {
		if len(result) >= MaxAttachmentsPerMessage {
			break
		}

		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}

		for _, entry := range entries {
			if len(result) >= MaxAttachmentsPerMessage {
				break
			}
			att := tryAutoAttachEntry(dir, entry, baseDir, cutoff, existingCanonical)
			if att != nil {
				result = append(result, att)
			}
		}
	}

	return result
}

var intermediateStatusPhrases = []string{
	"Everything is running smoothly! I'll keep working on this and check in shortly.",
	"Everything is running smoothly! I'll keep working on this and check in shortly!",
	"Everything is running smoothly! I'll keep working on this and check in shortly",
}

var intermediateStatusRegexes = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^waiting (for|on) (unit test|docker image|ci checks|ci to complete|github actions|builds|validation|run).*\.?$`),
	regexp.MustCompile(`(?i)^wait for (background task|subagent|remaining subagent|the final subagent).*\.?$`),
}

func isSubstantiveContent(s string) bool {
	clean := strings.Trim(strings.TrimSpace(s), " \t\r\n.!?*-_~`")
	return len(clean) > 0
}

func stripLeadingStatusPhrases(s string) string {
	working := s
	for {
		stripped := false
		for _, phrase := range intermediateStatusPhrases {
			trimmedWorking := strings.TrimSpace(working)
			if strings.HasPrefix(trimmedWorking, phrase) {
				remainder := strings.TrimSpace(dedentLeadingListChunk(strings.Trim(strings.TrimPrefix(trimmedWorking, phrase), "\r\n")))
				// Only strip if there is substantive remainder (not just punctuation or empty)
				if isSubstantiveContent(remainder) {
					working = remainder
					stripped = true
					break
				}
			}
		}
		if !stripped {
			break
		}
	}
	return working
}

func matchesHarnessStatusChatter(trimmedLine string) bool {
	if trimmedLine == "" {
		return false
	}
	for _, re := range intermediateStatusRegexes {
		if re.MatchString(trimmedLine) {
			return true
		}
	}
	return false
}

func updateFenceState(trimmedLine, activeFence string, activeFenceLen int) (string, int) {
	fenceMarker, fenceLen, isSingleLineSpan := parseCodeFence(trimmedLine)
	if fenceMarker != "" && !isSingleLineSpan {
		if activeFence == "" {
			return fenceMarker, fenceLen
		} else if activeFence == fenceMarker && fenceLen >= activeFenceLen {
			return "", 0
		}
	}
	return activeFence, activeFenceLen
}

// SanitizeIntermediateStatus strips repetitive intermediate task status preambles
// and harness wait lines when followed by substantive response content, and deduplicates consecutive identical lines.
func SanitizeIntermediateStatus(s string) string {
	trimmed := strings.TrimSpace(dedentLeadingListChunk(strings.Trim(s, "\r\n")))
	if trimmed == "" {
		return ""
	}

	// 1. If the message starts with one or more intermediate status phrases followed by other content,
	// strip them off so the final substantive response is not cluttered with status updates.
	working := stripLeadingStatusPhrases(trimmed)

	// 2. Filter intermediate harness status chatter and deduplicate consecutive identical lines outside code blocks.
	// Track code fences accurately to prevent inverting on single-line spans, blockquotes, or tildes.
	lines := strings.Split(working, "\n")
	var deduped []string
	var prevLine string
	var activeFence string
	var activeFenceLen int

	for _, l := range lines {
		trimmedLine := strings.TrimSpace(l)
		activeFence, activeFenceLen = updateFenceState(trimmedLine, activeFence, activeFenceLen)

		inCodeBlock := activeFence != ""
		if !inCodeBlock && matchesHarnessStatusChatter(trimmedLine) {
			continue
		}

		if !inCodeBlock && trimmedLine != "" && trimmedLine == prevLine {
			continue
		}
		deduped = append(deduped, l)
		if !inCodeBlock && trimmedLine != "" {
			prevLine = trimmedLine
		} else if inCodeBlock {
			prevLine = ""
		}
	}

	result := strings.TrimSpace(dedentLeadingListChunk(strings.Join(deduped, "\n")))
	if result == "" && trimmed != "" {
		return trimmed
	}
	return result
}

// parseCodeFence detects markdown code fence delimiters (``` or ~~~), measuring marker length
// and detecting single-line code spans. Leading blockquote markers ('> ') are stripped.
func parseCodeFence(line string) (marker string, length int, isSingleLineSpan bool) {
	trimmed := strings.TrimSpace(line)
	for strings.HasPrefix(trimmed, ">") {
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, ">"))
	}

	if strings.HasPrefix(trimmed, "```") {
		marker = "```"
	} else if strings.HasPrefix(trimmed, "~~~") {
		marker = "~~~"
	} else {
		return "", 0, false
	}

	char := rune(marker[0])
	count := 0
	for _, r := range trimmed {
		if r == char {
			count++
		} else {
			break
		}
	}

	// Check if this line is a single-line code block: e.g. ```bash echo 1``` or ~~~python code~~~
	rest := trimmed[count:]
	if strings.Contains(rest, strings.Repeat(string(char), count)) {
		return marker, count, true
	}

	return marker, count, false
}

func cleanDuplicateBlankLines(s string) string {
	for strings.Contains(s, "\n\n\n") {
		s = strings.ReplaceAll(s, "\n\n\n", "\n\n")
	}
	return strings.TrimSpace(s)
}

func resolveSandboxedImagePath(rawPath, baseDir string) (string, error) {
	cleanRaw := strings.TrimPrefix(rawPath, "file://")

	var targetPath string
	if filepath.IsAbs(cleanRaw) {
		targetPath = filepath.Clean(cleanRaw)
	} else if baseDir != "" {
		targetPath = filepath.Clean(filepath.Join(baseDir, cleanRaw))
	} else {
		targetPath = filepath.Clean(cleanRaw)
	}

	// 1. Evaluate symlinks and get canonical path
	canonicalPath, err := filepath.EvalSymlinks(targetPath)
	if err != nil {
		return "", fmt.Errorf("file resolution error: %w", err)
	}

	// 2. Validate against allowed root directories
	var isAllowed bool
	for _, root := range AllowedAttachmentRoots {
		canonicalRoot, err := filepath.EvalSymlinks(filepath.Clean(root))
		if err != nil {
			canonicalRoot = filepath.Clean(root)
		}
		if canonicalPath == canonicalRoot || strings.HasPrefix(canonicalPath, canonicalRoot+string(filepath.Separator)) {
			isAllowed = true
			break
		}
	}

	if !isAllowed {
		return "", fmt.Errorf("access denied: path %q is outside allowed sandbox roots", rawPath)
	}
	return canonicalPath, nil
}

var extMIMEMap = map[string]string{
	".png":  "image/png",
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".gif":  "image/gif",
	".webp": "image/webp",
	".wav":  "audio/wav",
	".mp3":  "audio/mpeg",
	".ogg":  "audio/ogg",
	".m4a":  "audio/mp4",
	".flac": "audio/flac",
}

func detectMediaMIMETypeBytes(data []byte, ext string) (string, error) {
	mimeType := http.DetectContentType(data)
	ext = strings.ToLower(ext)

	if !strings.HasPrefix(mimeType, "image/") && !strings.HasPrefix(mimeType, "audio/") {
		if mimeType == "application/octet-stream" {
			if mapped, ok := extMIMEMap[ext]; ok {
				mimeType = mapped
			}
		} else if mimeType == "application/ogg" && ext == ".ogg" {
			// http.DetectContentType sniffs standard Ogg headers (OggS) as application/ogg
			mimeType = "audio/ogg"
		} else if mimeType == "video/mp4" && ext == ".m4a" {
			// http.DetectContentType sniffs MP4/M4A ftyp containers as video/mp4
			mimeType = "audio/mp4"
		}
	} else if strings.HasPrefix(mimeType, "audio/") {
		// Normalize wav audio types (http.DetectContentType often yields audio/wave)
		if ext == ".wav" && (mimeType == "audio/wave" || mimeType == "audio/x-wav") {
			mimeType = "audio/wav"
		}
	}

	if !strings.HasPrefix(mimeType, "image/") && !strings.HasPrefix(mimeType, "audio/") {
		return "", fmt.Errorf("unsupported MIME type %q: only image/* and audio/* allowed", mimeType)
	}
	return mimeType, nil
}

func detectMediaMIMEType(f *os.File, canonicalPath string) (string, error) {
	header := make([]byte, 512)
	n, err := f.Read(header)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("failed to read file header: %w", err)
	}
	return detectMediaMIMETypeBytes(header[:n], filepath.Ext(canonicalPath))
}

// ResolveAndFetchInternalImage fetches and validates an image from an internal .aerial endpoint.
func ResolveAndFetchInternalImage(rawURL string) (*Attachment, error) {
	if !isInternalAerialHTTPURL(rawURL) {
		return nil, fmt.Errorf("url %q is not an allowed internal .aerial endpoint", rawURL)
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	resp, err := internalMediaHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch internal media: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected http status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxAttachmentSizeBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("response body is empty (0 bytes)")
	}
	if int64(len(data)) > MaxAttachmentSizeBytes {
		return nil, fmt.Errorf("response size %d exceeds limit of %d bytes", len(data), MaxAttachmentSizeBytes)
	}

	baseName := filepath.Base(u.Path)
	ext := filepath.Ext(u.Path)
	if ext == "" || baseName == "" || baseName == "." || baseName == "/" {
		baseName = "image.jpg"
		ext = ".jpg"
	}

	mimeType, err := detectMediaMIMETypeBytes(data, ext)
	if err != nil {
		return nil, err
	}

	return &Attachment{
		Filename:    baseName,
		ContentType: mimeType,
		Data:        data,
	}, nil
}

// ResolveAndValidateLocalImage validates path sandboxing, symlinks, file type, size, and MIME header.
func ResolveAndValidateLocalImage(rawPath string, baseDir string) (*Attachment, error) {
	canonicalPath, err := resolveSandboxedImagePath(rawPath, baseDir)
	if err != nil {
		return nil, err
	}

	// 3. Open file
	f, err := os.Open(canonicalPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("[Delivery] Warning: failed to close media file %s: %v", canonicalPath, closeErr)
		}
	}()

	// 4. Verify regular file (reject sockets, pipes, devices)
	stat, err := f.Stat()
	if err != nil || !stat.Mode().IsRegular() {
		return nil, fmt.Errorf("file is not a regular file")
	}

	// 5. Size check
	if stat.Size() <= 0 {
		return nil, fmt.Errorf("file is empty (0 bytes)")
	}
	if stat.Size() > MaxAttachmentSizeBytes {
		return nil, fmt.Errorf("file size %d exceeds limit of %d bytes", stat.Size(), MaxAttachmentSizeBytes)
	}

	// 6. Sniff MIME type using first 512 bytes
	mimeType, err := detectMediaMIMEType(f, canonicalPath)
	if err != nil {
		return nil, err
	}

	// 7. Read full file data into memory buffer
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to rewind file: %w", err)
	}

	data, err := io.ReadAll(io.LimitReader(f, MaxAttachmentSizeBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read file content: %w", err)
	}
	if int64(len(data)) > MaxAttachmentSizeBytes {
		return nil, fmt.Errorf("file exceeded size limit during read")
	}

	return &Attachment{
		Filename:    filepath.Base(canonicalPath),
		ContentType: mimeType,
		Data:        data,
	}, nil
}
