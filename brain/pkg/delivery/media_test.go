package delivery

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func createTestPNG(t *testing.T, dir, filename string) string {
	t.Helper()
	imgPath := filepath.Join(dir, filename)
	img := image.NewRGBA(image.Rect(0, 0, 10, 10))
	for x := 0; x < 10; x++ {
		for y := 0; y < 10; y++ {
			img.Set(x, y, color.RGBA{R: 255, G: 0, B: 0, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("failed to encode test png: %v", err)
	}
	if err := os.WriteFile(imgPath, buf.Bytes(), 0600); err != nil {
		t.Fatalf("failed to write test png: %v", err)
	}
	return imgPath
}

func TestExtractAndSanitizeMedia(t *testing.T) {
	tempDir := t.TempDir()
	img1 := createTestPNG(t, tempDir, "chart.png")
	img2 := createTestPNG(t, tempDir, "arch.png")

	input := "Here is the telemetry analysis:\n\n![Telemetry Chart](" + img1 + ")\n\nAnd the system architecture:\n![Architecture Diagram](" + img2 + ")\n\nAlso check this remote diagram:\n![Remote Architecture](https://example.com/remote.png)\n\n```markdown\n![Inside Code Block](not_an_image.png)\n```"

	cleanText, attachments := ExtractAndSanitizeMedia(input, tempDir)

	if len(attachments) != 2 {
		t.Fatalf("Expected 2 attachments, got %d", len(attachments))
	}

	if attachments[0].Filename != "chart.png" {
		t.Errorf("Expected attachment 0 filename chart.png, got %s", attachments[0].Filename)
	}
	if attachments[1].Filename != "arch.png" {
		t.Errorf("Expected attachment 1 filename arch.png, got %s", attachments[1].Filename)
	}
	if attachments[0].ContentType != "image/png" {
		t.Errorf("Expected image/png MIME, got %s", attachments[0].ContentType)
	}

	// Verify clean text
	if strings.Contains(cleanText, img1) || strings.Contains(cleanText, img2) {
		t.Errorf("Cleaned text should not contain raw local filepaths")
	}
	if !strings.Contains(cleanText, "**Telemetry Chart**") {
		t.Errorf("Expected caption **Telemetry Chart** in clean text")
	}
	if !strings.Contains(cleanText, "https://example.com/remote.png") {
		t.Errorf("Remote URLs should be preserved in text")
	}
	if !strings.Contains(cleanText, "![Inside Code Block](not_an_image.png)") {
		t.Errorf("Code block content should be untouched")
	}
}

func TestExtractMediaMissingFileGracefulDegradation(t *testing.T) {
	tempDir := t.TempDir()
	input := "Check this out:\n\n![Missing Chart](" + filepath.Join(tempDir, "missing.png") + ")\n\nDone."

	cleanText, attachments := ExtractAndSanitizeMedia(input, tempDir)

	if len(attachments) != 0 {
		t.Fatalf("Expected 0 attachments for missing file, got %d", len(attachments))
	}

	if !strings.Contains(cleanText, "*(Image attachment unavailable: Missing Chart - missing.png)*") {
		t.Errorf("Expected fallback indicator for missing image, got:\n%s", cleanText)
	}
}

func TestResolveAndValidateLocalImage_SecuritySandboxing(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Non-image file masquerading as png
	fakeImg := filepath.Join(tempDir, "fake.png")
	_ = os.WriteFile(fakeImg, []byte("NOT_A_PNG_HEADER_DATA_SECRET_KEY=12345"), 0600)

	_, err := ResolveAndValidateLocalImage(fakeImg, tempDir)
	if err == nil {
		t.Errorf("Expected error for non-image file, got nil")
	}

	// 2. Disallowed root path escape
	disallowedFile := "/etc/hosts"
	_, err = ResolveAndValidateLocalImage(disallowedFile, tempDir)
	if err == nil {
		t.Errorf("Expected access denied error for /etc/hosts, got nil")
	}

	// 3. Traversal attempt escaping allowed root
	traversal := filepath.Join(tempDir, "../../etc/passwd")
	_, err = ResolveAndValidateLocalImage(traversal, tempDir)
	if err == nil {
		t.Errorf("Expected error for directory traversal attempt, got nil")
	}

	// 4. Zero byte file
	zeroFile := filepath.Join(tempDir, "zero.png")
	_ = os.WriteFile(zeroFile, []byte{}, 0600)
	_, err = ResolveAndValidateLocalImage(zeroFile, tempDir)
	if err == nil {
		t.Errorf("Expected error for 0-byte file, got nil")
	}

	// 5. Directory passed instead of regular file
	_, err = ResolveAndValidateLocalImage(tempDir, "")
	if err == nil {
		t.Errorf("Expected error when target is a directory, got nil")
	}

	// 6. file:// URI prefix resolution
	validImg := createTestPNG(t, tempDir, "uri_test.png")
	att, err := ResolveAndValidateLocalImage("file://"+validImg, "")
	if err != nil || att == nil || att.Filename != "uri_test.png" {
		t.Errorf("failed to resolve file:// URI: %v, %+v", err, att)
	}

	// 7. Relative path with baseDir
	attRel, err := ResolveAndValidateLocalImage("uri_test.png", tempDir)
	if err != nil || attRel == nil || attRel.Filename != "uri_test.png" {
		t.Errorf("failed to resolve relative path with baseDir: %v, %+v", err, attRel)
	}
}

func TestToDiscordFile(t *testing.T) {
	att := &Attachment{
		Filename:    "chart.png",
		ContentType: "image/png",
		Data:        []byte("fake png bytes"),
	}
	df := att.ToDiscordFile()
	if df.Name != "chart.png" || df.ContentType != "image/png" || df.Reader == nil {
		t.Errorf("unexpected discordgo.File: %+v", df)
	}
}

func TestExtractAndSanitizeMedia_EdgeCases(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Empty string
	if text, atts := ExtractAndSanitizeMedia("", tempDir); text != "" || len(atts) != 0 {
		t.Errorf("expected empty text and nil atts for empty input")
	}

	// 2. Image without alt text
	img1 := createTestPNG(t, tempDir, "no_alt.png")
	text1, atts1 := ExtractAndSanitizeMedia("Check this:\n![]("+img1+")", tempDir)
	if len(atts1) != 1 || strings.Contains(text1, "no_alt.png") {
		t.Errorf("unexpected output for image without alt text: %q", text1)
	}

	// 3. Missing image without alt text
	text2, atts2 := ExtractAndSanitizeMedia("Check this:\n![]("+filepath.Join(tempDir, "missing_no_alt.png")+")", tempDir)
	if len(atts2) != 0 || !strings.Contains(text2, "Image attachment unavailable: missing_no_alt.png") {
		t.Errorf("unexpected output for missing image without alt: %q", text2)
	}

	// 4. Multiple duplicate blank lines
	cleaned := cleanDuplicateBlankLines("line1\n\n\n\n\nline2")
	if cleaned != "line1\n\nline2" {
		t.Errorf("expected collapsed blank lines, got: %q", cleaned)
	}

	// 5. Exceed MaxAttachmentsPerMessage (10 attachments limit)
	var sb strings.Builder
	for i := 0; i < 12; i++ {
		imgPath := createTestPNG(t, tempDir, fmt.Sprintf("img_%d.png", i))
		sb.WriteString("![img](" + imgPath + ")\n")
	}
	_, attsMany := ExtractAndSanitizeMedia(sb.String(), tempDir)
	if len(attsMany) != MaxAttachmentsPerMessage {
		t.Errorf("expected max %d attachments, got %d", MaxAttachmentsPerMessage, len(attsMany))
	}
}
