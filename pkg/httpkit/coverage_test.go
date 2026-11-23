package httpkit

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type failingWriter struct {
	err error
}

func (f *failingWriter) Write([]byte) (int, error) {
	return 0, f.err
}

func TestArchiveCoverageDir_TableDriven(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "file1.txt"), []byte("sample-data"), 0o600); err != nil {
		t.Fatal(err)
	}

	subDirParent := t.TempDir()
	if err := os.Mkdir(filepath.Join(subDirParent, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		writer     io.Writer
		dir        string
		wantErrMsg string
	}{
		{"successful archive", io.Discard, tmpDir, ""},
		{"nonexistent directory error", io.Discard, filepath.Join(tmpDir, "nonexistent"), "no such file or directory"},
		{"unreadable subdirectory entry error", io.Discard, subDirParent, "is a directory"},
		{"failing writer error", &failingWriter{err: errors.New("write failure")}, tmpDir, "write failure"},
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
		{name: "successful flush streams tar archive", writeDummyFile: true, wantStatus: http.StatusOK, wantCT: "application/x-tar", wantBodySubstr: "covmeta.test"},
		{name: "meta error still succeeds when counters succeed", metaErr: errors.New("meta failure"), wantStatus: http.StatusOK, wantCT: "application/x-tar"},
		{
			name: "counters error returns 500", countersErr: errors.New("counters write failed"), wantStatus: http.StatusInternalServerError,
			wantBodySubstr: "counters write failed",
		},
		{name: "mkdirtemp error returns 500", invalidTmpDir: true, wantStatus: http.StatusInternalServerError},
		{name: "archive error returns 500", archiveErr: errors.New("tar archive failed"), wantStatus: http.StatusInternalServerError, wantBodySubstr: "tar archive failed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.invalidTmpDir {
				t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "nonexistent", "subdir"))
			}

			WriteMetaDirFn = func(dir string) error {
				if tc.writeDummyFile {
					if err := os.WriteFile(filepath.Join(dir, "covmeta.test"), []byte("meta-bytes"), 0o600); err != nil {
						return err
					}
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
			req := httptest.NewRequest(http.MethodPost, "/debug/coverage/flush", http.NoBody)
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

// writeCoverageFile returns a coverage writer stub that drops name into the
// flush directory (when name is set) and returns err.
func writeCoverageFile(name string, err error) func(string) error {
	return func(dir string) error {
		if name != "" {
			if wErr := os.WriteFile(filepath.Join(dir, name), []byte(name+"-bytes"), 0o600); wErr != nil {
				return wErr
			}
		}
		return err
	}
}

// archiveEntries returns the entry names of a coverage tar archive, failing
// when an entry's content or mode differs from what writeCoverageFile wrote.
func archiveEntries(t *testing.T, r io.Reader) []string {
	t.Helper()
	var got []string
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return got
		}
		if err != nil {
			t.Fatalf("read archive: %v", err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read archive entry: %v", err)
		}
		if string(data) != hdr.Name+"-bytes" || hdr.Mode != 0o600 {
			t.Errorf("entry %s = (%q, %o), want (%q, 600)", hdr.Name, data, hdr.Mode, hdr.Name+"-bytes")
		}
		got = append(got, hdr.Name)
	}
}

func TestArchiveCoverage(t *testing.T) {
	// Not t.Parallel(): the mkdir row overrides TMPDIR via t.Setenv.
	// A writer is passed only when its row sets a file or an error; otherwise it is nil.
	tests := []struct {
		name          string
		invalidTmpDir bool
		metaFile      string
		metaErr       error
		countersFile  string
		countersErr   error
		wantErr       string
		wantFiles     []string
	}{
		{name: "archives meta and counters files", metaFile: "covmeta.1", countersFile: "covcounters.1", wantFiles: []string{"covcounters.1", "covmeta.1"}},
		{name: "nil writers produce empty archive"},
		{
			name: "meta error is tolerated", metaFile: "covmeta.1", metaErr: errors.New("meta failure"), countersFile: "covcounters.1",
			wantFiles: []string{"covcounters.1", "covmeta.1"},
		},
		{name: "counters error is returned", countersErr: errors.New("counters write failed"), wantErr: "counters write failed"},
		{name: "mkdirtemp error is returned", invalidTmpDir: true, wantErr: "no such file or directory"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.invalidTmpDir {
				t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "nonexistent", "subdir"))
			}

			var writeMeta, writeCounters func(string) error
			if tc.metaFile != "" || tc.metaErr != nil {
				writeMeta = writeCoverageFile(tc.metaFile, tc.metaErr)
			}
			if tc.countersFile != "" || tc.countersErr != nil {
				writeCounters = writeCoverageFile(tc.countersFile, tc.countersErr)
			}

			var buf bytes.Buffer
			err := ArchiveCoverage(&buf, writeMeta, writeCounters)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ArchiveCoverage error = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ArchiveCoverage unexpected error: %v", err)
			}

			got := archiveEntries(t, &buf)
			if !reflect.DeepEqual(got, tc.wantFiles) {
				t.Errorf("archive files = %v, want %v", got, tc.wantFiles)
			}
		})
	}
}
