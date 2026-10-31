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
)

type errorFS struct{}

func (errorFS) Open(name string) (fs.File, error) {
	return nil, errors.New("simulated filesystem failure")
}

func TestStaticAssetsIntegrity(t *testing.T) {
	requiredFiles := []string{
		"static/index.html",
		"static/style.css",
		"static/app.js",
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

func TestAppJSDeclaredFunctions(t *testing.T) {
	data, err := os.ReadFile("static/app.js")
	if err != nil {
		t.Fatalf("failed to read static/app.js: %v", err)
	}
	contentStr := string(data)

	requiredFunctions := []string{
		"function formatAgentsviewSessionUrl",
		"function parseValidTimestampMs",
		"function formatElapsedTicker",
		"function escapeHtml",
		"function formatUptime",
		"function getTriggerBadge",
		"function renderActiveTasks",
		"function renderDeployments",
		"function renderServicesGrid",
		"async function fetchStatus",
	}

	for _, fn := range requiredFunctions {
		if !strings.Contains(contentStr, fn) {
			t.Errorf("critical function definition missing in app.js: %q", fn)
		}
	}
}

func TestIndexHTMLRequiredDOMBindings(t *testing.T) {
	data, err := os.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("failed to read static/index.html: %v", err)
	}
	htmlStr := string(data)

	requiredIDs := []string{
		`id="quick-launch-dock"`,
		`id="tasks-count-badge"`,
		`id="active-tasks-container"`,
		`id="deployments-container"`,
		`id="deploy-count-badge"`,
		`id="gitsync-badge"`,
		`id="services-grid"`,
		`id="active-count"`,
		`id="permet-score-val"`,
		`id="permet-bar-fill"`,
		`id="tab-telemetry-btn"`,
		`id="tab-tasks-btn"`,
		`id="tab-schedules-btn"`,
		`id="tab-memory-btn"`,
	}

	for _, idAttr := range requiredIDs {
		if !strings.Contains(htmlStr, idAttr) {
			t.Errorf("required DOM ID binding missing in index.html: %q", idAttr)
		}
	}

	requiredHeadElements := []string{
		`href="favicon-32x32.png"`,
		`href="favicon-16x16.png"`,
		`href="favicon.ico"`,
		`href="apple-touch-icon.png"`,
		`name="theme-color" content="#06050a"`,
		`name="apple-mobile-web-app-title" content="Aerial HUD"`,
	}
	for _, el := range requiredHeadElements {
		if !strings.Contains(htmlStr, el) {
			t.Errorf("required head element missing in index.html: %q", el)
		}
	}
}

func TestZeroPersonalDataAndHardcodedIPs(t *testing.T) {
	files := []string{"static/app.js", "static/index.html", "static/style.css"}

	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("failed to read %s: %v", f, err)
		}
		str := string(data)

		// Assert zero private LAN IP leaks
		if strings.Contains(str, "192.168.") {
			t.Errorf("found private LAN IP (192.168.x.x) in %s", f)
		}
		if strings.Contains(str, "10.0.") {
			t.Errorf("found private LAN IP (10.0.x.x) in %s", f)
		}
	}
}

func TestAssetRegistry_ServeHTTP(t *testing.T) {
	staticFS := os.DirFS("static")

	defaultReg, err := NewAssetRegistry(staticFS, "testcommit123")
	if err != nil {
		t.Fatalf("failed to create AssetRegistry: %v", err)
	}

	fallbackReg, err := NewAssetRegistry(staticFS, "")
	if err != nil {
		t.Fatalf("failed to create AssetRegistry with fallback: %v", err)
	}

	tests := []struct {
		name                  string
		versionToken          string
		method                string
		path                  string
		matchETagPath         string
		weakETag              string
		emptyFS               bool
		wantCode              int
		wantContentTypeSubstr string
		wantCacheSubstr       string
		wantBodySubstr        []string
		wantEmptyBody         bool
	}{
		{
			name:            "serves_index_html_root",
			method:          "GET",
			path:            "/",
			wantCode:        http.StatusOK,
			wantCacheSubstr: "no-cache",
			wantBodySubstr:  []string{"app.js?v=testcommit123", "style.css?v=testcommit123"},
		},
		{
			name:           "empty_version_token_fallback",
			versionToken:   "EMPTY",
			method:         "GET",
			path:           "/",
			wantCode:       http.StatusOK,
			wantBodySubstr: []string{"app.js?v=", "style.css?v="},
		},
		{
			name:          "304_not_modified_matching_etag",
			method:        "GET",
			path:          "/app.js",
			matchETagPath: "/app.js",
			wantCode:      http.StatusNotModified,
			wantEmptyBody: true,
		},
		{
			name:          "304_not_modified_weak_etag",
			method:        "GET",
			path:          "/style.css",
			matchETagPath: "/style.css",
			weakETag:      "W/",
			wantCode:      http.StatusNotModified,
		},
		{
			name:            "immutable_cache_control_version_param",
			method:          "GET",
			path:            "/app.js?v=testcommit123",
			wantCode:        http.StatusOK,
			wantCacheSubstr: "immutable",
		},
		{
			name:                  "head_request_headers_empty_body",
			method:                "HEAD",
			path:                  "/app.js",
			wantCode:              http.StatusOK,
			wantContentTypeSubstr: "javascript",
			wantEmptyBody:         true,
		},
		{
			name:     "returns_404_nonexistent_asset",
			method:   "GET",
			path:     "/nonexistent.js",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "returns_405_post_request",
			method:   "POST",
			path:     "/app.js",
			wantCode: http.StatusMethodNotAllowed,
		},
		{
			name:                  "serves_favicon_ico",
			method:                "GET",
			path:                  "/favicon.ico",
			wantCode:              http.StatusOK,
			wantContentTypeSubstr: "icon",
		},
		{
			name:          "serves_favicon_ico_304",
			method:        "GET",
			path:          "/favicon.ico",
			matchETagPath: "/favicon.ico",
			wantCode:      http.StatusNotModified,
			wantEmptyBody: true,
		},
		{
			name:                  "serves_favicon_png",
			method:                "GET",
			path:                  "/favicon.png",
			wantCode:              http.StatusOK,
			wantContentTypeSubstr: "image/png",
		},
		{
			name:                  "serves_favicon_32x32_png",
			method:                "GET",
			path:                  "/favicon-32x32.png",
			wantCode:              http.StatusOK,
			wantContentTypeSubstr: "image/png",
		},
		{
			name:                  "serves_favicon_16x16_png",
			method:                "GET",
			path:                  "/favicon-16x16.png",
			wantCode:              http.StatusOK,
			wantContentTypeSubstr: "image/png",
		},
		{
			name:                  "serves_apple_touch_icon_png",
			method:                "GET",
			path:                  "/apple-touch-icon.png",
			wantCode:              http.StatusOK,
			wantContentTypeSubstr: "image/png",
		},
		{
			name:                  "serves_favicon_dashboard_prefix",
			method:                "GET",
			path:                  "/dashboard/favicon.ico",
			wantCode:              http.StatusOK,
			wantContentTypeSubstr: "icon",
		},
		{
			name:         "fallback_on_empty_dir_fs",
			emptyFS:      true,
			versionToken: "dev",
			method:       "GET",
			path:         "/",
			wantCode:     http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := defaultReg
			if tc.versionToken == "EMPTY" {
				reg = fallbackReg
			} else if tc.emptyFS {
				emptyReg, err := NewAssetRegistry(os.DirFS(t.TempDir()), tc.versionToken)
				if err != nil {
					t.Fatalf("NewAssetRegistry failed on empty dir: %v", err)
				}
				if emptyReg == nil {
					t.Fatalf("expected non-nil asset registry")
				}
				reg = emptyReg
			}

			req := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.matchETagPath != "" {
				probeReq := httptest.NewRequest(http.MethodGet, tc.matchETagPath, nil)
				probeRec := httptest.NewRecorder()
				reg.ServeHTTP(probeRec, probeReq)
				etag := probeRec.Header().Get("ETag")
				if etag == "" {
					t.Fatalf("expected non-empty ETag for probe on %s", tc.matchETagPath)
				}
				if tc.weakETag != "" {
					etag = tc.weakETag + etag
				}
				req.Header.Set("If-None-Match", etag)
			}

			rec := httptest.NewRecorder()
			reg.ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("expected status %d, got %d", tc.wantCode, rec.Code)
			}
			if tc.wantCode == http.StatusOK && tc.path == "/" {
				if rec.Header().Get("ETag") == "" {
					t.Errorf("expected ETag header to be set")
				}
			}
			if tc.wantCacheSubstr != "" {
				cc := rec.Header().Get("Cache-Control")
				if !strings.Contains(cc, tc.wantCacheSubstr) {
					t.Errorf("expected Cache-Control to contain %q, got %q", tc.wantCacheSubstr, cc)
				}
			}
			if tc.wantContentTypeSubstr != "" {
				ct := rec.Header().Get("Content-Type")
				if !strings.Contains(ct, tc.wantContentTypeSubstr) {
					t.Errorf("expected Content-Type to contain %q, got %q", tc.wantContentTypeSubstr, ct)
				}
			}
			if tc.wantEmptyBody && rec.Body.Len() != 0 {
				t.Errorf("expected empty body, got %d bytes", rec.Body.Len())
			}
			bodyStr := rec.Body.String()
			for _, sub := range tc.wantBodySubstr {
				if !strings.Contains(bodyStr, sub) {
					t.Errorf("expected body to contain %q, got: %s", sub, bodyStr)
				}
			}
		})
	}
}

func TestAssetRegistry_Reload(t *testing.T) {
	initialDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(initialDir, "index.html"), []byte("<h1>Initial</h1>"), 0o644); err != nil {
		t.Fatalf("failed to create initial index.html: %v", err)
	}

	reg, err := NewAssetRegistry(os.DirFS(initialDir), "v1")
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
		versionToken  string
		wantErr       bool
		wantBodyMatch string
	}{
		{
			name:          "successful_reload_updates_content",
			fsys:          os.DirFS(updatedDir),
			versionToken:  "v2",
			wantErr:       false,
			wantBodyMatch: "<h1>Reloaded</h1>",
		},
		{
			name:          "unreadable_fs_retains_existing_content",
			fsys:          errorFS{},
			versionToken:  "v3",
			wantErr:       true,
			wantBodyMatch: "<h1>Reloaded</h1>",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := reg.Reload(tc.fsys, tc.versionToken)
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
