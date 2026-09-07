package delivery

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/bwmarrin/discordgo"
)

const (
	MaxAttachmentsPerMessage = 10
	MaxAttachmentSizeBytes   = 8 * 1024 * 1024 // 8 MB
)

// AllowedAttachmentRoots defines the whitelisted root directories for local file attachments.
var AllowedAttachmentRoots = []string{
	"/root/.gemini/antigravity-cli/brain",
	"/root/.gemini/antigravity-cli/scratch",
	"/tmp",
	"/dev/shm",
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

			// Remote URLs: leave untouched for Discord client unfurling
			if strings.HasPrefix(rawPath, "http://") || strings.HasPrefix(rawPath, "https://") {
				return match
			}

			if len(attachments) >= MaxAttachmentsPerMessage {
				return match
			}

			att, err := ResolveAndValidateLocalImage(rawPath, baseDir)
			if err != nil {
				baseName := filepath.Base(rawPath)
				if baseName == "" || baseName == "." {
					baseName = "image"
				}
				if altText != "" {
					return fmt.Sprintf("*(Image attachment unavailable: %s - %s)*", altText, baseName)
				}
				return fmt.Sprintf("*(Image attachment unavailable: %s)*", baseName)
			}

			attachments = append(attachments, att)
			if altText != "" {
				return fmt.Sprintf("**%s**", altText)
			}
			return ""
		})

		processedLines = append(processedLines, newLine)
	}

	cleanedText := strings.Join(processedLines, "\n")
	cleanedText = SanitizeIntermediateStatus(cleanedText)
	cleanedText = cleanDuplicateBlankLines(cleanedText)

	return cleanedText, attachments
}

var intermediateStatusPhrases = []string{
	"Everything is running smoothly! I'll keep working on this and check in shortly.",
	"Everything is running smoothly! I'll keep working on this and check in shortly!",
	"Everything is running smoothly! I'll keep working on this and check in shortly",
}

func isSubstantiveContent(s string) bool {
	clean := strings.Trim(strings.TrimSpace(s), " \t\r\n.!?*-_~`")
	return len(clean) > 0
}

// SanitizeIntermediateStatus strips repetitive intermediate task status preambles
// when followed by substantive response content, and deduplicates consecutive identical lines.
func SanitizeIntermediateStatus(s string) string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return ""
	}

	// 1. If the message starts with one or more intermediate status phrases followed by other content,
	// strip them off so the final substantive response is not cluttered with status updates.
	working := trimmed
	for {
		stripped := false
		for _, phrase := range intermediateStatusPhrases {
			trimmedWorking := strings.TrimSpace(working)
			if strings.HasPrefix(trimmedWorking, phrase) {
				remainder := strings.TrimSpace(strings.TrimPrefix(trimmedWorking, phrase))
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

	// 2. Deduplicate consecutive identical lines outside code blocks.
	// Track code fences accurately to prevent inverting on single-line spans, blockquotes, or tildes.
	lines := strings.Split(working, "\n")
	var deduped []string
	var prevLine string
	var activeFence string
	var activeFenceLen int

	for _, l := range lines {
		trimmedLine := strings.TrimSpace(l)
		fenceMarker, fenceLen, isSingleLineSpan := parseCodeFence(trimmedLine)
		if fenceMarker != "" && !isSingleLineSpan {
			if activeFence == "" {
				activeFence = fenceMarker
				activeFenceLen = fenceLen
			} else if activeFence == fenceMarker && fenceLen >= activeFenceLen {
				activeFence = ""
				activeFenceLen = 0
			}
		}

		inCodeBlock := activeFence != ""
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

	return strings.TrimSpace(strings.Join(deduped, "\n"))
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

// ResolveAndValidateLocalImage validates path sandboxing, symlinks, file type, size, and MIME header.
func ResolveAndValidateLocalImage(rawPath string, baseDir string) (*Attachment, error) {
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
		return nil, fmt.Errorf("file resolution error: %w", err)
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
		return nil, fmt.Errorf("access denied: path %q is outside allowed sandbox roots", rawPath)
	}

	// 3. Open file
	f, err := os.Open(canonicalPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

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
	header := make([]byte, 512)
	n, err := f.Read(header)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read file header: %w", err)
	}
	mimeType := http.DetectContentType(header[:n])
	if !strings.HasPrefix(mimeType, "image/") {
		return nil, fmt.Errorf("unsupported MIME type %q: only image/* allowed", mimeType)
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
