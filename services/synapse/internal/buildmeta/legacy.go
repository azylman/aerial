package buildmeta

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

// CheckoutRevisionFromArchive scans a workflow log zip archive for git checkout log output
// and returns the resolved SHA.
func CheckoutRevisionFromArchive(raw []byte) (string, error) {
	archive, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return "", err
	}
	revision := ""
	var total int64
	for _, file := range archive.File {
		if file.FileInfo().IsDir() {
			continue
		}
		reader, openErr := file.Open()
		if openErr != nil {
			return "", openErr
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, 8<<20+1))
		closeErr := reader.Close()
		if readErr != nil {
			return "", readErr
		}
		if closeErr != nil {
			return "", closeErr
		}
		total += int64(len(data))
		if len(data) > 8<<20 || total > 64<<20 {
			return "", fmt.Errorf("historical logs exceed limit")
		}
		found, parseErr := CheckoutRevisionFromLog(string(data))
		if parseErr != nil {
			return "", parseErr
		}
		if found == "" {
			continue
		}
		if revision != "" && revision != found {
			return "", fmt.Errorf("conflicting checkout revisions")
		}
		revision = found
	}
	if revision == "" {
		return "", fmt.Errorf("historical checkout revision missing")
	}
	return revision, nil
}

// CheckoutRevisionFromLog parses log text for a git log -1 format=%H command output.
func CheckoutRevisionFromLog(log string) (string, error) {
	revision := ""
	expectSHA := false
	for line := range strings.SplitSeq(log, "\n") {
		fields := strings.Fields(line)
		if expectSHA && len(fields) == 2 && model.MetadataSHA.MatchString(fields[1]) {
			if revision != "" && revision != fields[1] {
				return "", fmt.Errorf("conflicting checkout revisions")
			}
			revision = fields[1]
		}
		expectSHA = strings.Contains(line, "[command]/") && strings.HasSuffix(strings.TrimSpace(line), "/git log -1 --format=%H")
	}
	return revision, nil
}
