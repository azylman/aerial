package httpkit

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

type mockCloser struct {
	err error
}

func (m *mockCloser) Close() error {
	return m.err
}

type errResponseWriter struct {
	header http.Header
	status int
	err    error
}

func (w *errResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *errResponseWriter) WriteHeader(status int) {
	w.status = status
}

func (w *errResponseWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func TestIsClientDisconnect_TableDriven(t *testing.T) {
	t.Parallel()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	reqWithCanceledCtx := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(canceledCtx)
	reqNormal := httptest.NewRequest(http.MethodGet, "/", nil)

	tests := []struct {
		name     string
		req      *http.Request
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			req:      reqNormal,
			err:      nil,
			expected: false,
		},
		{
			name:     "canceled request context",
			req:      reqWithCanceledCtx,
			err:      errors.New("arbitrary error"),
			expected: true,
		},
		{
			name:     "broken pipe syscall EPIPE",
			req:      reqNormal,
			err:      syscall.EPIPE,
			expected: true,
		},
		{
			name:     "connection reset syscall ECONNRESET",
			req:      reqNormal,
			err:      syscall.ECONNRESET,
			expected: true,
		},
		{
			name:     "net ErrClosed",
			req:      reqNormal,
			err:      net.ErrClosed,
			expected: true,
		},
		{
			name:     "generic unrelated error",
			req:      reqNormal,
			err:      errors.New("unrelated disk error"),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := IsClientDisconnect(tc.req, tc.err)
			if got != tc.expected {
				t.Errorf("IsClientDisconnect() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestWriteResponse_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		errWriter  bool
		errType    error
		disconnect bool
	}{
		{
			name:      "normal successful write",
			errWriter: false,
		},
		{
			name:       "write error with client disconnect",
			errWriter:  true,
			errType:    syscall.EPIPE,
			disconnect: true,
		},
		{
			name:       "write error non-disconnect",
			errWriter:  true,
			errType:    errors.New("socket timeout"),
			disconnect: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			var w http.ResponseWriter
			if tc.errWriter {
				w = &errResponseWriter{err: tc.errType}
			} else {
				w = httptest.NewRecorder()
			}

			WriteResponse(w, req, "test-svc", []byte("payload"))
			if !tc.errWriter {
				rec := w.(*httptest.ResponseRecorder)
				if rec.Body.String() != "payload" {
					t.Errorf("WriteResponse body = %q, want 'payload'", rec.Body.String())
				}
			}
		})
	}
}

func TestWriteJSON_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		status         int
		data           any
		existingCT     string
		wantStatus     int
		wantBodySubstr string
	}{
		{
			name:           "valid map with default content-type",
			status:         http.StatusOK,
			data:           map[string]string{"foo": "bar"},
			existingCT:     "",
			wantStatus:     http.StatusOK,
			wantBodySubstr: `{"foo":"bar"}`,
		},
		{
			name:           "valid map with custom content-type preserved",
			status:         http.StatusCreated,
			data:           map[string]int{"count": 42},
			existingCT:     "application/vnd.api+json",
			wantStatus:     http.StatusCreated,
			wantBodySubstr: `{"count":42}`,
		},
		{
			name:           "unmarshalable type returns 500 internal error",
			status:         http.StatusOK,
			data:           make(chan int),
			existingCT:     "",
			wantStatus:     http.StatusInternalServerError,
			wantBodySubstr: "Internal error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			if tc.existingCT != "" {
				rec.Header().Set("Content-Type", tc.existingCT)
			}

			WriteJSON(rec, tc.status, tc.data)
			if rec.Code != tc.wantStatus {
				t.Errorf("WriteJSON code = %d, want %d", rec.Code, tc.wantStatus)
			}
			if !strings.Contains(rec.Body.String(), tc.wantBodySubstr) {
				t.Errorf("WriteJSON body = %q, want substring %q", rec.Body.String(), tc.wantBodySubstr)
			}
		})
	}
}

func TestCloseWarn_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		closer io.Closer
	}{
		{
			name:   "nil closer",
			closer: nil,
		},
		{
			name:   "closer without error",
			closer: &mockCloser{err: nil},
		},
		{
			name:   "closer with error",
			closer: &mockCloser{err: errors.New("close failure")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			CloseWarn(tc.closer, "test-resource")
		})
	}
}

func TestNormalizeMCPToolPrefix_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      string
		oldPrefix string
		newPrefix string
		wantBody  string
	}{
		{
			name:      "empty old prefix returns untouched",
			body:      `{"method":"tools/call","params":{"name":"scheduler_run"}}`,
			oldPrefix: "",
			newPrefix: "",
			wantBody:  `{"method":"tools/call","params":{"name":"scheduler_run"}}`,
		},
		{
			name:      "body does not contain prefix",
			body:      `{"method":"tools/call","params":{"name":"unrelated_tool"}}`,
			oldPrefix: "scheduler_",
			newPrefix: "",
			wantBody:  `{"method":"tools/call","params":{"name":"unrelated_tool"}}`,
		},
		{
			name:      "compact name without space",
			body:      `{"method":"tools/call","params":{"name":"memory_memory_search"}}`,
			oldPrefix: "memory_memory_",
			newPrefix: "memory_",
			wantBody:  `{"method":"tools/call","params":{"name":"memory_search"}}`,
		},
		{
			name:      "single space after colon",
			body:      `{"method":"tools/call","params":{"name": "scheduler_schedule_once"}}`,
			oldPrefix: "scheduler_",
			newPrefix: "",
			wantBody:  `{"method":"tools/call","params":{"name": "schedule_once"}}`,
		},
		{
			name:      "double space after colon",
			body:      `{"method":"tools/call","params":{"name":  "scheduler_schedule_once"}}`,
			oldPrefix: "scheduler_",
			newPrefix: "",
			wantBody:  `{"method":"tools/call","params":{"name": "schedule_once"}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := NormalizeMCPToolPrefix([]byte(tc.body), tc.oldPrefix, tc.newPrefix)
			if string(got) != tc.wantBody {
				t.Errorf("NormalizeMCPToolPrefix() = %s, want %s", string(got), tc.wantBody)
			}
		})
	}
}

func TestServeMCPStream_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		method         string
		body           string
		readyBanner    string
		oldToolPrefix  string
		newToolPrefix  string
		wantStatus     int
		wantBodySubstr string
		wantHeaderCT   string
	}{
		{
			name:           "get readiness banner",
			method:         http.MethodGet,
			readyBanner:    "Memory Service Ready\n",
			wantStatus:     http.StatusOK,
			wantBodySubstr: "Memory Service Ready\n",
			wantHeaderCT:   "text/plain; charset=utf-8",
		},
		{
			name:           "put disallowed method",
			method:         http.MethodPut,
			wantStatus:     http.StatusMethodNotAllowed,
			wantBodySubstr: "Method not allowed",
		},
		{
			name:           "post empty body rejected",
			method:         http.MethodPost,
			body:           "   ",
			wantStatus:     http.StatusBadRequest,
			wantBodySubstr: "Empty request body",
		},
		{
			name:           "post valid body with tool normalization",
			method:         http.MethodPost,
			body:           `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"scheduler_list"}}`,
			oldToolPrefix:  "scheduler_",
			newToolPrefix:  "",
			wantStatus:     http.StatusOK,
			wantBodySubstr: "invoked:list",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			streamHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				if strings.Contains(string(b), `"name":"list"`) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("invoked:list"))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("stream-handled"))
			})

			req := httptest.NewRequest(tc.method, "/mcp", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()

			ServeMCPStream(rec, req, streamHandler, tc.readyBanner, tc.oldToolPrefix, tc.newToolPrefix)

			if rec.Code != tc.wantStatus {
				t.Errorf("ServeMCPStream status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if !strings.Contains(rec.Body.String(), tc.wantBodySubstr) {
				t.Errorf("ServeMCPStream body = %q, want substring %q", rec.Body.String(), tc.wantBodySubstr)
			}
			if tc.wantHeaderCT != "" && rec.Header().Get("Content-Type") != tc.wantHeaderCT {
				t.Errorf("ServeMCPStream Content-Type = %q, want %q", rec.Header().Get("Content-Type"), tc.wantHeaderCT)
			}
		})
	}
}

func TestResolveConfigPathWithLocal_TableDriven(t *testing.T) {
	// Not t.Parallel() because it modifies environment variables
	tempDir := t.TempDir()
	validLocal := filepath.Join(tempDir, "local_valid.yaml")
	_ = os.WriteFile(validLocal, []byte("key: val\n"), 0o644)

	emptyLocal := filepath.Join(tempDir, "local_empty.yaml")
	_ = os.WriteFile(emptyLocal, []byte(""), 0o644)

	fallback1 := filepath.Join(tempDir, "fallback1.yaml")
	_ = os.WriteFile(fallback1, []byte("fb1"), 0o644)

	nonExistent := filepath.Join(tempDir, "does_not_exist.yaml")

	tests := []struct {
		name          string
		envKey        string
		envVal        string
		localPath     string
		fallbackPaths []string
		wantResult    string
	}{
		{
			name:       "env variable explicit key set",
			envKey:     "MY_CONFIG",
			envVal:     "/etc/config/my_app.yaml",
			wantResult: "/etc/config/my_app.yaml",
		},
		{
			name:       "default env key CONFIG_PATH set",
			envKey:     "",
			envVal:     "/etc/default/config.yaml",
			wantResult: "/etc/default/config.yaml",
		},
		{
			name:       "local path exists with positive size",
			envKey:     "UNSET_ENV",
			localPath:  validLocal,
			wantResult: validLocal,
		},
		{
			name:          "local path empty size falls back to existing fallback",
			envKey:        "UNSET_ENV",
			localPath:     emptyLocal,
			fallbackPaths: []string{fallback1},
			wantResult:    fallback1,
		},
		{
			name:          "none exist returns fallbackPaths[0]",
			envKey:        "UNSET_ENV",
			localPath:     nonExistent,
			fallbackPaths: []string{nonExistent},
			wantResult:    nonExistent,
		},
		{
			name:          "none exist and fallbackPaths empty returns empty string",
			envKey:        "UNSET_ENV",
			localPath:     nonExistent,
			fallbackPaths: nil,
			wantResult:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envKey != "" {
				t.Setenv(tc.envKey, tc.envVal)
			} else if tc.envVal != "" {
				t.Setenv("CONFIG_PATH", tc.envVal)
			}

			got := ResolveConfigPathWithLocal(tc.envKey, tc.localPath, tc.fallbackPaths...)
			if got != tc.wantResult {
				t.Errorf("ResolveConfigPathWithLocal() = %q, want %q", got, tc.wantResult)
			}
		})
	}
}

type failingWriter struct {
	err error
}

func (f *failingWriter) Write([]byte) (int, error) {
	return 0, f.err
}

func TestArchiveCoverageDir_TableDriven(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "file1.txt"), []byte("sample-data"), 0o600)

	subDirParent := t.TempDir()
	_ = os.Mkdir(filepath.Join(subDirParent, "nested"), 0o700)

	tests := []struct {
		name       string
		writer     io.Writer
		dir        string
		wantErrMsg string
	}{
		{
			name:   "successful archive",
			writer: io.Discard,
			dir:    tmpDir,
		},
		{
			name:       "nonexistent directory error",
			writer:     io.Discard,
			dir:        filepath.Join(tmpDir, "nonexistent"),
			wantErrMsg: "no such file or directory",
		},
		{
			name:       "unreadable subdirectory entry error",
			writer:     io.Discard,
			dir:        subDirParent,
			wantErrMsg: "is a directory",
		},
		{
			name:       "failing writer error",
			writer:     &failingWriter{err: errors.New("write failure")},
			dir:        tmpDir,
			wantErrMsg: "write failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := archiveCoverageDir(tc.writer, tc.dir)
			if tc.wantErrMsg == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErrMsg) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErrMsg, err)
			}
		})
	}
}

func TestHandleCoverageFlush_TableDriven(t *testing.T) {
	// Not t.Parallel() because it mutates package variables WriteCountersDirFn/WriteMetaDirFn and TMPDIR
	origCounters := WriteCountersDirFn
	origMeta := WriteMetaDirFn
	origArchive := archiveCoverageDir
	defer func() {
		WriteCountersDirFn = origCounters
		WriteMetaDirFn = origMeta
		archiveCoverageDir = origArchive
	}()

	tests := []struct {
		name           string
		invalidTmpDir  bool
		writeDummyFile bool
		archiveErr     error
		metaErr        error
		countersErr    error
		wantStatus     int
		wantCT         string
		wantBodySubstr string
	}{
		{
			name:           "successful flush streams tar archive",
			writeDummyFile: true,
			wantStatus:     http.StatusOK,
			wantCT:         "application/x-tar",
			wantBodySubstr: "covmeta.test",
		},
		{
			name:       "meta error still succeeds when counters succeed",
			metaErr:    errors.New("meta failure"),
			wantStatus: http.StatusOK,
			wantCT:     "application/x-tar",
		},
		{
			name:           "counters error returns 500",
			countersErr:    errors.New("counters write failed"),
			wantStatus:     http.StatusInternalServerError,
			wantBodySubstr: "counters write failed",
		},
		{
			name:          "mkdirtemp error returns 500",
			invalidTmpDir: true,
			wantStatus:    http.StatusInternalServerError,
		},
		{
			name:           "archive error returns 500",
			archiveErr:     errors.New("tar archive failed"),
			wantStatus:     http.StatusInternalServerError,
			wantBodySubstr: "tar archive failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.invalidTmpDir {
				t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "nonexistent", "subdir"))
			}

			WriteMetaDirFn = func(dir string) error {
				if tc.writeDummyFile {
					_ = os.WriteFile(filepath.Join(dir, "covmeta.test"), []byte("meta-bytes"), 0o600)
				}
				return tc.metaErr
			}
			WriteCountersDirFn = func(string) error {
				return tc.countersErr
			}
			archiveCoverageDir = func(w io.Writer, dir string) error {
				if tc.archiveErr != nil {
					return tc.archiveErr
				}
				return origArchive(w, dir)
			}

			handler := HandleCoverageFlush("test-svc")
			req := httptest.NewRequest(http.MethodPost, "/debug/coverage/flush", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("HandleCoverageFlush status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if tc.wantCT != "" && rec.Header().Get("Content-Type") != tc.wantCT {
				t.Errorf("HandleCoverageFlush Content-Type = %q, want %q", rec.Header().Get("Content-Type"), tc.wantCT)
			}
			if tc.wantBodySubstr != "" && !strings.Contains(rec.Body.String(), tc.wantBodySubstr) {
				t.Errorf("HandleCoverageFlush body = %q, want substring %q", rec.Body.String(), tc.wantBodySubstr)
			}
		})
	}
}
