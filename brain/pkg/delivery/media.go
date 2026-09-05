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
	inCodeBlock := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			inCodeBlock = !inCodeBlock
			processedLines = append(processedLines, line)
			continue
		}

		if inCodeBlock {
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
	cleanedText = cleanDuplicateBlankLines(cleanedText)

	return cleanedText, attachments
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
