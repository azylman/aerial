package delivery

import (
	"bytes"
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
}
