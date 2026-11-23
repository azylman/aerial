package httpkit

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/coverage"
)

var (
	WriteCountersDirFn = coverage.WriteCountersDir
	WriteMetaDirFn     = coverage.WriteMetaDir
)

var archiveCoverageDir = func(w io.Writer, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(w)
	for _, e := range entries {
		data, rErr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rErr != nil {
			return rErr
		}
		hErr := tw.WriteHeader(&tar.Header{Name: e.Name(), Mode: 0o600, Size: int64(len(data))})
		_, wErr := tw.Write(data)
		if err := errors.Join(hErr, wErr); err != nil {
			return err
		}
	}
	return tw.Close()
}

// HandleCoverageFlush returns an HTTP handler for /debug/coverage/flush that writes
// runtime coverage metadata and counters to an ephemeral directory and streams them
// back as an application/x-tar archive.
func HandleCoverageFlush(service string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		if err := ArchiveCoverage(&buf, WriteMetaDirFn, WriteCountersDirFn); err != nil {
			log.Printf("[%s] coverage flush error: %v", service, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(http.StatusOK)
		WriteResponse(w, r, service, buf.Bytes())
	}
}

// ArchiveCoverage writes coverage metadata and counters into an ephemeral
// directory and streams them to w as a tar archive. A writeMeta error is
// logged and tolerated (metadata may already have been written); mkdir,
// writeCounters and archive errors are returned unwrapped. Nil funcs are
// skipped.
func ArchiveCoverage(w io.Writer, writeMeta, writeCounters func(dir string) error) error {
	tmpDir, err := os.MkdirTemp("", "covflush-*")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			log.Printf("coverage flush cleanup error: %v", err)
		}
	}()

	if writeMeta != nil {
		if err := writeMeta(tmpDir); err != nil {
			log.Printf("coverage flush meta error: %v", err)
		}
	}
	if writeCounters != nil {
		if err := writeCounters(tmpDir); err != nil {
			return err
		}
	}
	return archiveCoverageDir(w, tmpDir)
}
