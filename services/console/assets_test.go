package main

import (
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

type errorFS struct{}

func (errorFS) Open(name string) (fs.File, error) {
	return nil, errors.New("simulated filesystem failure")
}

func TestStaticAssetsIntegrity(t *testing.T) {
	requiredFiles := []string{
		"static/favicon.ico",
		"static/favicon.png",
		"static/favicon-32x32.png",
		"static/favicon-16x16.png",
		"static/apple-touch-icon.png",
	}

	for _, reqFile := range requiredFiles {
		data, err := os.ReadFile(reqFile)
		if err != nil {
			t.Errorf("failed to read required file %s: %v", reqFile, err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("file %s is empty", reqFile)
		}
	}
}

// builtConsoleFS mirrors the image's /app/static: the favicons from static/ plus the Vite dist
// (SPA shell and content-hashed assets/) copied over them by the Dockerfile.
var builtConsoleFS = fstest.MapFS{
	"index.html":                {Data: []byte(`<div id="root"></div><script type="module" src="/console/assets/index-Ab12Cd34.js"></script>`)},
	"assets/index-Ab12Cd34.js":  {Data: []byte(`console.log("console")`)},
	"assets/index-Ef56Gh78.css": {Data: []byte(`:root{color-scheme:dark}`)},
	"favicon-32x32.png":         {Data: []byte{0, 0, 1, 0}},
	"apple-touch-icon.png":      {Data: []byte("\x89PNG")},
}

func TestAssetRegistry_ServeHTTP(t *testing.T) {
	t.Parallel()
	builtReg, err := NewAssetRegistry(builtConsoleFS)
	if err != nil {
		t.Fatalf("failed to create AssetRegistry: %v", err)
	}
	emptyReg, err := NewAssetRegistry(fstest.MapFS{})
	if err != nil {
		t.Fatalf("failed to create empty AssetRegistry: %v", err)
	}

	tests := []struct {
		name            string
		reg             *AssetRegistry
		method          string
		path            string
		ifNoneMatch     string
		wantCode        int
		wantContentType string
		wantCache       string
		wantETag        string
		wantBody        string
	}{
		{name: "root_shell", reg: builtReg, method: "GET", path: "/", wantCode: http.StatusOK, wantContentType: "text/html; charset=utf-8", wantCache: "no-cache, must-revalidate", wantETag: `"06c6880aaf6bbd18"`, wantBody: `<div id="root"></div><script type="module" src="/console/assets/index-Ab12Cd34.js"></script>`},
		{name: "console_prefix_shell", reg: builtReg, method: "GET", path: "/console/", wantCode: http.StatusOK, wantContentType: "text/html; charset=utf-8", wantCache: "no-cache, must-revalidate", wantETag: `"06c6880aaf6bbd18"`, wantBody: `<div id="root"></div><script type="module" src="/console/assets/index-Ab12Cd34.js"></script>`},
		{name: "explicit_index_html", reg: builtReg, method: "GET", path: "/console/index.html", wantCode: http.StatusOK, wantContentType: "text/html; charset=utf-8", wantCache: "no-cache, must-revalidate", wantETag: `"06c6880aaf6bbd18"`, wantBody: `<div id="root"></div><script type="module" src="/console/assets/index-Ab12Cd34.js"></script>`},
		{name: "hashed_js_immutable", reg: builtReg, method: "GET", path: "/console/assets/index-Ab12Cd34.js", wantCode: http.StatusOK, wantContentType: "text/javascript; charset=utf-8", wantCache: "public, max-age=31536000, immutable", wantETag: `"7ef459c7e6b1475d"`, wantBody: `console.log("console")`},
		{name: "hashed_css_immutable_root", reg: builtReg, method: "GET", path: "/assets/index-Ef56Gh78.css", wantCode: http.StatusOK, wantContentType: "text/css; charset=utf-8", wantCache: "public, max-age=31536000, immutable", wantETag: `"df0a81bf0e43dd5d"`, wantBody: `:root{color-scheme:dark}`},
		{name: "favicon_revalidates", reg: builtReg, method: "GET", path: "/console/favicon-32x32.png", wantCode: http.StatusOK, wantContentType: "image/png", wantCache: "no-cache, must-revalidate", wantETag: `"6b1e73a0094b7b81"`, wantBody: "\x00\x00\x01\x00"},
		{name: "hashed_asset_304", reg: builtReg, method: "GET", path: "/assets/index-Ab12Cd34.js", ifNoneMatch: `"7ef459c7e6b1475d"`, wantCode: http.StatusNotModified, wantContentType: "text/javascript; charset=utf-8", wantCache: "public, max-age=31536000, immutable", wantETag: `"7ef459c7e6b1475d"`},
		{name: "shell_304_weak_etag", reg: builtReg, method: "GET", path: "/console/", ifNoneMatch: `W/"06c6880aaf6bbd18"`, wantCode: http.StatusNotModified, wantContentType: "text/html; charset=utf-8", wantCache: "no-cache, must-revalidate", wantETag: `"06c6880aaf6bbd18"`},
		{name: "head_headers_without_body", reg: builtReg, method: "HEAD", path: "/apple-touch-icon.png", wantCode: http.StatusOK, wantContentType: "image/png", wantCache: "no-cache, must-revalidate", wantETag: `"0f4636c78f65d363"`},
		{name: "spa_deep_link_console", reg: builtReg, method: "GET", path: "/console/memory", wantCode: http.StatusOK, wantContentType: "text/html; charset=utf-8", wantCache: "no-cache, must-revalidate", wantETag: `"06c6880aaf6bbd18"`, wantBody: `<div id="root"></div><script type="module" src="/console/assets/index-Ab12Cd34.js"></script>`},
		{name: "spa_deep_link_nested_root", reg: builtReg, method: "GET", path: "/builds/abc?tab=chunks", wantCode: http.StatusOK, wantContentType: "text/html; charset=utf-8", wantCache: "no-cache, must-revalidate", wantETag: `"06c6880aaf6bbd18"`, wantBody: `<div id="root"></div><script type="module" src="/console/assets/index-Ab12Cd34.js"></script>`},
		{name: "stale_hashed_asset_404", reg: builtReg, method: "GET", path: "/console/assets/index-Stale999", wantCode: http.StatusNotFound, wantContentType: "text/plain; charset=utf-8", wantBody: "404 page not found\n"},
		{name: "missing_file_with_extension_404", reg: builtReg, method: "GET", path: "/missing.js", wantCode: http.StatusNotFound, wantContentType: "text/plain; charset=utf-8", wantBody: "404 page not found\n"},
		{name: "post_rejected", reg: builtReg, method: "POST", path: "/console/", wantCode: http.StatusMethodNotAllowed, wantContentType: "text/plain; charset=utf-8", wantBody: "Method Not Allowed\n"},
		{name: "unbuilt_ui_shell_404", reg: emptyReg, method: "GET", path: "/console/", wantCode: http.StatusNotFound, wantContentType: "text/plain; charset=utf-8", wantBody: "404 page not found\n"},
		{name: "unbuilt_ui_deep_link_404", reg: emptyReg, method: "GET", path: "/telemetry", wantCode: http.StatusNotFound, wantContentType: "text/plain; charset=utf-8", wantBody: "404 page not found\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.ifNoneMatch != "" {
				req.Header.Set("If-None-Match", tc.ifNoneMatch)
			}
			rec := httptest.NewRecorder()
			tc.reg.ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantCode)
			}
			if got := rec.Header().Get("Content-Type"); got != tc.wantContentType {
				t.Errorf("Content-Type = %q, want %q", got, tc.wantContentType)
			}
			if got := rec.Header().Get("Cache-Control"); got != tc.wantCache {
				t.Errorf("Cache-Control = %q, want %q", got, tc.wantCache)
			}
			if got := rec.Header().Get("ETag"); got != tc.wantETag {
				t.Errorf("ETag = %q, want %q", got, tc.wantETag)
			}
			if got := rec.Body.String(); got != tc.wantBody {
				t.Errorf("body = %q, want %q", got, tc.wantBody)
			}
		})
	}
}

func TestAssetRegistry_Reload(t *testing.T) {
	initialDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(initialDir, "index.html"), []byte("<h1>Initial</h1>"), 0o644); err != nil {
		t.Fatalf("failed to create initial index.html: %v", err)
	}

	reg, err := NewAssetRegistry(os.DirFS(initialDir))
	if err != nil {
		t.Fatalf("NewAssetRegistry failed: %v", err)
	}

	updatedDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(updatedDir, "index.html"), []byte("<h1>Reloaded</h1>"), 0o644); err != nil {
		t.Fatalf("failed to create updated index.html: %v", err)
	}

	tests := []struct {
		name          string
		fsys          fs.FS
		wantErr       bool
		wantBodyMatch string
	}{
		{
			name:          "successful_reload_updates_content",
			fsys:          os.DirFS(updatedDir),
			wantErr:       false,
			wantBodyMatch: "<h1>Reloaded</h1>",
		},
		{
			name:          "unreadable_fs_retains_existing_content",
			fsys:          errorFS{},
			wantErr:       true,
			wantBodyMatch: "<h1>Reloaded</h1>",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := reg.Reload(tc.fsys)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Reload() error = %v, wantErr %v", err, tc.wantErr)
			}

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			reg.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("ServeHTTP status = %d, want %d", rec.Code, http.StatusOK)
			}
			if !strings.Contains(rec.Body.String(), tc.wantBodyMatch) {
				t.Errorf("expected body to contain %q, got: %s", tc.wantBodyMatch, rec.Body.String())
			}
		})
	}
}
