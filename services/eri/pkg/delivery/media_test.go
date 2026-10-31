package delivery

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	if err := os.WriteFile(imgPath, buf.Bytes(), 0o600); err != nil {
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
	_ = os.WriteFile(fakeImg, []byte("NOT_A_PNG_HEADER_DATA_SECRET_KEY=12345"), 0o600)

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
	_ = os.WriteFile(zeroFile, []byte{}, 0o600)
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

func TestSanitizeIntermediateStatus(t *testing.T) {
	// 1. Multiple repetitive intermediate status preambles before actual response
	input := "Everything is running smoothly! I'll keep working on this and check in shortly.\nEverything is running smoothly! I'll keep working on this and check in shortly.\nEverything is running smoothly! I'll keep working on this and check in shortly.\nThe girl gang ran a full 4-expert architectural audit."
	got := SanitizeIntermediateStatus(input)
	want := "The girl gang ran a full 4-expert architectural audit."
	if got != want {
		t.Errorf("Expected %q, got %q", want, got)
	}

	// 2. Single intermediate status with no other content is preserved
	single := "Everything is running smoothly! I'll keep working on this and check in shortly."
	if gotSingle := SanitizeIntermediateStatus(single); gotSingle != single {
		t.Errorf("Expected single status preserved %q, got %q", single, gotSingle)
	}

	// 3. Consecutive identical lines deduplicated
	dupLines := "Header line\nSame detail line\nSame detail line\nFooter line"
	gotDup := SanitizeIntermediateStatus(dupLines)
	wantDup := "Header line\nSame detail line\nFooter line"
	if gotDup != wantDup {
		t.Errorf("Expected %q, got %q", wantDup, gotDup)
	}
}

func TestSanitizeIntermediateStatus_ComplexCodeFences(t *testing.T) {
	// 1. Repeated braces and identical lines inside code fences MUST be preserved!
	codeBlockInput := "Here is some code:\n```go\nfunc test() {\n\tif true {\n\t}\n\tif false {\n\t}\n}\n```\nOutside line\nOutside line\nFinal line"
	wantCodeBlock := "Here is some code:\n```go\nfunc test() {\n\tif true {\n\t}\n\tif false {\n\t}\n}\n```\nOutside line\nFinal line"
	gotCodeBlock := SanitizeIntermediateStatus(codeBlockInput)
	if gotCodeBlock != wantCodeBlock {
		t.Errorf("Expected code block with duplicate braces preserved, got:\n%s\nwant:\n%s", gotCodeBlock, wantCodeBlock)
	}

	// 2. Single-line code span must not invert state
	singleLineSpan := "Intro\n```bash echo \"hello\"```\nDuplicate outside\nDuplicate outside\nDone"
	wantSingleLine := "Intro\n```bash echo \"hello\"```\nDuplicate outside\nDone"
	gotSingleLine := SanitizeIntermediateStatus(singleLineSpan)
	if gotSingleLine != wantSingleLine {
		t.Errorf("Expected single-line span to not prevent deduplication outside, got:\n%s\nwant:\n%s", gotSingleLine, wantSingleLine)
	}

	// 3. 4-backticks fence and tildes
	tildeBlock := "Tilde test:\n~~~python\ndef foo():\n    return 1\n    return 1\n~~~\nDuplicate outside\nDuplicate outside"
	wantTilde := "Tilde test:\n~~~python\ndef foo():\n    return 1\n    return 1\n~~~\nDuplicate outside"
	gotTilde := SanitizeIntermediateStatus(tildeBlock)
	if gotTilde != wantTilde {
		t.Errorf("Expected tilde block content preserved, got:\n%s\nwant:\n%s", gotTilde, wantTilde)
	}

	// 4. Blockquoted code fence
	blockquoteBlock := "> ```json\n> {\n> }\n> }\n> ```\nOut\nOut"
	wantBlockquote := "> ```json\n> {\n> }\n> }\n> ```\nOut"
	gotBlockquote := SanitizeIntermediateStatus(blockquoteBlock)
	if gotBlockquote != wantBlockquote {
		t.Errorf("Expected blockquoted fence handled, got:\n%s\nwant:\n%s", gotBlockquote, wantBlockquote)
	}
}

func TestAutoAttachNewMedia(t *testing.T) {
	tempDir := t.TempDir()
	scratchDir := filepath.Join(tempDir, "scratch")
	if err := os.MkdirAll(scratchDir, 0o755); err != nil {
		t.Fatalf("failed to create scratch dir: %v", err)
	}

	sinceTime := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Create test images in baseDir and scratchDir
	img1 := createTestPNG(t, tempDir, "gen_1.png")
	img2 := createTestPNG(t, scratchDir, "gen_2.png")
	_ = img2

	// Pre-existing attachment from text
	preExistingAtt, err := ResolveAndValidateLocalImage(img1, tempDir)
	if err != nil {
		t.Fatalf("failed to resolve pre-existing image: %v", err)
	}
	existing := []*Attachment{preExistingAtt}

	// Run AutoAttachNewMedia
	result := AutoAttachNewMedia(tempDir, sinceTime, existing)

	// Expect img2 to be auto-attached, while img1 is deduplicated
	if len(result) != 2 {
		t.Fatalf("Expected 2 attachments (1 existing + 1 auto-attached), got %d", len(result))
	}

	if result[0].Filename != "gen_1.png" {
		t.Errorf("Expected attachment 0 filename gen_1.png, got %s", result[0].Filename)
	}
	if result[1].Filename != "gen_2.png" {
		t.Errorf("Expected attachment 1 filename gen_2.png, got %s", result[1].Filename)
	}
}

func TestResolveAndValidateLocalImage_MIMETypesAndEdges(t *testing.T) {
	tmpDir := t.TempDir()

	// Empty file
	emptyFile := filepath.Join(tmpDir, "empty.png")
	_ = os.WriteFile(emptyFile, []byte(""), 0o644)
	if _, err := ResolveAndValidateLocalImage(emptyFile, tmpDir); err == nil {
		t.Error("expected error for empty file")
	}

	// Non-regular file (directory)
	subDir := filepath.Join(tmpDir, "dir.png")
	_ = os.MkdirAll(subDir, 0o755)
	if _, err := ResolveAndValidateLocalImage(subDir, tmpDir); err == nil {
		t.Error("expected error for directory")
	}

	// Octet stream extensions (.png, .jpg, .gif, .webp, audio files)
	for _, ext := range []string{".png", ".jpg", ".jpeg", ".gif", ".webp", ".wav", ".mp3", ".ogg", ".m4a", ".flac"} {
		f := filepath.Join(tmpDir, "sample"+ext)
		_ = os.WriteFile(f, []byte{0x00, 0x01, 0x02, 0x03, 0x04}, 0o644)
		att, err := ResolveAndValidateLocalImage(f, tmpDir)
		if err != nil {
			t.Errorf("unexpected error for %s: %v", ext, err)
		}
		if att == nil || (!strings.HasPrefix(att.ContentType, "image/") && !strings.HasPrefix(att.ContentType, "audio/")) {
			t.Errorf("expected image/ or audio/ contentType for %s, got %+v", ext, att)
		}
	}

	// Octet stream unsupported extension (.bin)
	unsupportedFile := filepath.Join(tmpDir, "sample.bin")
	_ = os.WriteFile(unsupportedFile, []byte{0x00, 0x01, 0x02, 0x03, 0x04}, 0o644)
	if _, err := ResolveAndValidateLocalImage(unsupportedFile, tmpDir); err == nil {
		t.Error("expected error for unsupported bin MIME type")
	}

	// Oversized file > MaxAttachmentSizeBytes
	bigFile := filepath.Join(tmpDir, "big.png")
	bf, _ := os.Create(bigFile)
	_ = bf.Truncate(9 * 1024 * 1024)
	_ = bf.Close()
	if _, err := ResolveAndValidateLocalImage(bigFile, tmpDir); err == nil {
		t.Error("expected error for file exceeding max attachment size")
	}

	// Relative path without baseDir
	_, _ = ResolveAndValidateLocalImage("relative/path.png", "")
}

func TestResolveAndValidateLocalImage_AudioAndHeaderSniffing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filename   string
		headerData []byte
		wantMIME   string
		wantErr    bool
	}{
		{
			name:       "valid ogg with OggS header",
			filename:   "audio.ogg",
			headerData: []byte("OggS\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00"),
			wantMIME:   "audio/ogg",
			wantErr:    false,
		},
		{
			name:       "valid m4a with ftyp box",
			filename:   "audio.m4a",
			headerData: []byte("\x00\x00\x00\x1cftypM4A \x00\x00\x02\x00isomiso2mp41"),
			wantMIME:   "audio/mp4",
			wantErr:    false,
		},
		{
			name:       "valid wav with RIFF WAVE header",
			filename:   "audio.wav",
			headerData: []byte("RIFF\x24\x00\x00\x00WAVEfmt \x10\x00\x00\x00\x01\x00\x02\x00"),
			wantMIME:   "audio/wav",
			wantErr:    false,
		},
		{
			name:       "valid flac with fLaC header",
			filename:   "audio.flac",
			headerData: []byte("fLaC\x00\x00\x00\x22\x10\x00\x10\x00"),
			wantMIME:   "audio/flac",
			wantErr:    false,
		},
		{
			name:       "valid mp3 with ID3 header",
			filename:   "audio.mp3",
			headerData: []byte("ID3\x03\x00\x00\x00\x00\x00\x20"),
			wantMIME:   "audio/mpeg",
			wantErr:    false,
		},
		{
			name:       "plaintext masquerading as ogg rejected",
			filename:   "fake.ogg",
			headerData: []byte("Hello, this is just plain text content masquerading as ogg audio file."),
			wantMIME:   "",
			wantErr:    true,
		},
		{
			name:       "plaintext masquerading as m4a rejected",
			filename:   "fake.m4a",
			headerData: []byte("This is plain text not m4a."),
			wantMIME:   "",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			filePath := filepath.Join(tmpDir, tc.filename)
			if err := os.WriteFile(filePath, tc.headerData, 0o600); err != nil {
				t.Fatalf("failed to write test file: %v", err)
			}

			att, err := ResolveAndValidateLocalImage(filePath, tmpDir)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if att.ContentType != tc.wantMIME {
				t.Errorf("got ContentType %q, want %q", att.ContentType, tc.wantMIME)
			}
		})
	}
}

func TestAutoAttachNewMedia_EdgeCases(t *testing.T) {
	// 1. Empty args
	if res := AutoAttachNewMedia("", time.Time{}, nil); res != nil {
		t.Errorf("expected nil for empty baseDir")
	}

	// 2. Already at limit
	var maxAtts []*Attachment
	for i := 0; i < MaxAttachmentsPerMessage; i++ {
		maxAtts = append(maxAtts, &Attachment{Filename: fmt.Sprintf("file%d.png", i)})
	}
	res := AutoAttachNewMedia("/tmp", time.Now(), maxAtts)
	if len(res) != MaxAttachmentsPerMessage {
		t.Errorf("expected existing attachments returned when at limit")
	}

	// 3. Scan directory with hidden file and non-image file
	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, ".hidden.png"), []byte("data"), 0o644)
	_ = os.WriteFile(filepath.Join(tmpDir, "notes.txt"), []byte("data"), 0o644)
	_ = os.WriteFile(filepath.Join(tmpDir, "zero.png"), []byte(""), 0o644)
	res2 := AutoAttachNewMedia(tmpDir, time.Now().Add(-1*time.Minute), nil)
	if len(res2) != 0 {
		t.Errorf("expected 0 attachments for hidden, txt, and empty files, got %d", len(res2))
	}
}

func TestMedia_SanitizeIntermediateStatus_Empty(t *testing.T) {
	if res := SanitizeIntermediateStatus("   "); res != "" {
		t.Errorf("expected empty string for whitespace input, got %q", res)
	}
}

func TestAutoAttachNewMedia_BreakLimitsAndErrors(t *testing.T) {
	tmpDir := t.TempDir()
	now := time.Now()

	// 1. Create 12 valid images to trigger MaxAttachmentsPerMessage break
	for i := 0; i < 12; i++ {
		createTestPNG(t, tmpDir, fmt.Sprintf("pic_%02d.png", i))
	}

	// 2. Old image with ModTime before cutoff
	oldFile := filepath.Join(tmpDir, "old.png")
	_ = os.WriteFile(oldFile, []byte("pngdata"), 0o644)
	oldTime := now.Add(-10 * time.Minute)
	_ = os.Chtimes(oldFile, oldTime, oldTime)

	// 3. Corrupt image that fails ResolveAndValidateLocalImage
	badImg := filepath.Join(tmpDir, "bad.png")
	_ = os.WriteFile(badImg, []byte("not really a png"), 0o644)

	res := AutoAttachNewMedia(tmpDir, now.Add(-5*time.Second), nil)
	if len(res) != MaxAttachmentsPerMessage {
		t.Errorf("expected %d attachments, got %d", MaxAttachmentsPerMessage, len(res))
	}
}

func TestExtractAndSanitizeMedia_EdgePaths(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Dot path: ![Alt](.)
	out, _ := ExtractAndSanitizeMedia("Here is: ![My Dot](.)", tmpDir)
	if !strings.Contains(out, "*(Image attachment unavailable: My Dot - image)*") {
		t.Errorf("expected fallback with 'image' basename, got %s", out)
	}

	// 2. Missing image without alt text: ![](/does/not/exist.png)
	out2, _ := ExtractAndSanitizeMedia("Here is: ![](/does/not/exist.png)", tmpDir)
	if !strings.Contains(out2, "*(Image attachment unavailable: exist.png)*") {
		t.Errorf("expected fallback without alt text, got %s", out2)
	}
}

func TestExtractAndSanitizeMedia_InternalAerialImages(t *testing.T) {
	// Generate valid test PNG data
	img := image.NewRGBA(image.Rect(0, 0, 5, 5))
	var pngBuf bytes.Buffer
	if err := png.Encode(&pngBuf, img); err != nil {
		t.Fatalf("failed to encode png: %v", err)
	}
	validPNGData := pngBuf.Bytes()

	// Setup mock internal server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/media/storyboard.png":
			w.Header().Set("Content-Type", "image/png")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(validPNGData)
		case "/media/noext":
			w.Header().Set("Content-Type", "image/png")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(validPNGData)
		case "/media/notfound.png":
			http.NotFound(w, r)
		case "/media/error.png":
			w.WriteHeader(http.StatusInternalServerError)
		case "/media/empty.png":
			w.WriteHeader(http.StatusOK)
		case "/media/oversized.png":
			w.WriteHeader(http.StatusOK)
			chunk := make([]byte, 1024*1024)
			for i := 0; i < 9; i++ {
				_, _ = w.Write(chunk)
			}
		case "/media/corrupt.png":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not a valid image or audio format"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse server url: %v", err)
	}

	// Register test server host in allowedInternalHosts
	serverHost := strings.ToLower(serverURL.Hostname())
	allowedInternalHosts[serverHost] = true
	defer delete(allowedInternalHosts, serverHost)

	tests := []struct {
		name               string
		input              string
		wantAttachments    int
		wantAttachmentName string
		wantContentType    string
		wantTextContain    string
		wantTextNotContain string
	}{
		{
			name:               "valid internal image attached and replaced with alt text",
			input:              fmt.Sprintf("Episode ready:\n![Garage Storyboard](%s/media/storyboard.png)", server.URL),
			wantAttachments:    1,
			wantAttachmentName: "storyboard.png",
			wantContentType:    "image/png",
			wantTextContain:    "**Garage Storyboard**",
			wantTextNotContain: server.URL,
		},
		{
			name:               "valid internal image without extension defaults to image.jpg",
			input:              fmt.Sprintf("Episode ready:\n![Snapshot](%s/media/noext)", server.URL),
			wantAttachments:    1,
			wantAttachmentName: "image.jpg",
			wantContentType:    "image/png",
			wantTextContain:    "**Snapshot**",
			wantTextNotContain: server.URL,
		},
		{
			name:               "404 internal image fallback indicator",
			input:              fmt.Sprintf("Check this:\n![Missing](%s/media/notfound.png)", server.URL),
			wantAttachments:    0,
			wantTextContain:    "*(Image attachment unavailable: Missing - notfound.png)*",
			wantTextNotContain: server.URL,
		},
		{
			name:               "500 internal image fallback indicator",
			input:              fmt.Sprintf("Check this:\n![Broken](%s/media/error.png)", server.URL),
			wantAttachments:    0,
			wantTextContain:    "*(Image attachment unavailable: Broken - error.png)*",
			wantTextNotContain: server.URL,
		},
		{
			name:               "empty internal image fallback indicator",
			input:              fmt.Sprintf("Check this:\n![Empty](%s/media/empty.png)", server.URL),
			wantAttachments:    0,
			wantTextContain:    "*(Image attachment unavailable: Empty - empty.png)*",
			wantTextNotContain: server.URL,
		},
		{
			name:               "oversized internal image fallback indicator",
			input:              fmt.Sprintf("Check this:\n![Big](%s/media/oversized.png)", server.URL),
			wantAttachments:    0,
			wantTextContain:    "*(Image attachment unavailable: Big - oversized.png)*",
			wantTextNotContain: server.URL,
		},
		{
			name:               "corrupt non-image internal payload fallback indicator",
			input:              fmt.Sprintf("Check this:\n![Corrupt](%s/media/corrupt.png)", server.URL),
			wantAttachments:    0,
			wantTextContain:    "*(Image attachment unavailable: Corrupt - corrupt.png)*",
			wantTextNotContain: server.URL,
		},
		{
			name:               "external public url is untouched for discord unfurling",
			input:              "Here is an external diagram:\n![External Diagram](https://example.com/remote.png)",
			wantAttachments:    0,
			wantTextContain:    "![External Diagram](https://example.com/remote.png)",
			wantTextNotContain: "**External Diagram**",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cleanText, attachments := ExtractAndSanitizeMedia(tc.input, "")
			if len(attachments) != tc.wantAttachments {
				t.Fatalf("expected %d attachments, got %d", tc.wantAttachments, len(attachments))
			}
			if tc.wantAttachments > 0 {
				if attachments[0].Filename != tc.wantAttachmentName {
					t.Errorf("expected attachment name %q, got %q", tc.wantAttachmentName, attachments[0].Filename)
				}
				if attachments[0].ContentType != tc.wantContentType {
					t.Errorf("expected content type %q, got %q", tc.wantContentType, attachments[0].ContentType)
				}
			}
			if tc.wantTextContain != "" && !strings.Contains(cleanText, tc.wantTextContain) {
				t.Errorf("expected cleanText to contain %q, got %q", tc.wantTextContain, cleanText)
			}
			if tc.wantTextNotContain != "" && strings.Contains(cleanText, tc.wantTextNotContain) {
				t.Errorf("expected cleanText to not contain %q, got %q", tc.wantTextNotContain, cleanText)
			}
		})
	}
}

func TestResolveAndFetchInternalImage_Validation(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr bool
	}{
		{
			name:    "disallowed non-internal domain",
			rawURL:  "https://example.com/pic.png",
			wantErr: true,
		},
		{
			name:    "invalid url format",
			rawURL:  "://invalid-url",
			wantErr: true,
		},
		{
			name:    "unsupported scheme file",
			rawURL:  "file:///vision.aerial/media/test.png",
			wantErr: true,
		},
		{
			name:    "internal .aerial host with connection refused",
			rawURL:  "http://vision.aerial:59999/nonexistent.jpg",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ResolveAndFetchInternalImage(tc.rawURL)
			if tc.wantErr && err == nil {
				t.Errorf("expected error for %s, got nil", tc.rawURL)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error for %s: %v", tc.rawURL, err)
			}
		})
	}
}

func TestSanitizeIntermediateStatus_HarnessChatter(t *testing.T) {
	input := `Waiting for unit test suite completion on PR #225.
Waiting on Docker image builds for dashboard on PR #225.
Handled, boss—PR #225 is merged and synced! 🍵`
	got := SanitizeIntermediateStatus(input)
	want := "Handled, boss—PR #225 is merged and synced! 🍵"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}

	// Conversational sentences preserved
	convInput := "Waiting on your decision between Option A and Option B.\nPlease let me know."
	gotConv := SanitizeIntermediateStatus(convInput)
	if gotConv != convInput {
		t.Errorf("expected conversational text preserved, got %q", gotConv)
	}

	// Lines inside code block preserved
	codeBlock := "Here is a script:\n```bash\nwaiting for unit test suite completion\n```\nDone."
	gotCode := SanitizeIntermediateStatus(codeBlock)
	if gotCode != codeBlock {
		t.Errorf("expected code block line preserved, got %q", gotCode)
	}

	// Indented list following status preamble or harness chatter is dedented uniformly
	listAfterChatter := "Everything is running smoothly! I'll keep working on this and check in shortly.\nWaiting for unit test suite completion.\n  - **coven**: cpu = 100\n  - **hangar**: cpu = 100"
	wantList := "- **coven**: cpu = 100\n- **hangar**: cpu = 100"
	if gotList := SanitizeIntermediateStatus(listAfterChatter); gotList != wantList {
		t.Errorf("expected dedented list %q, got %q", wantList, gotList)
	}
}
