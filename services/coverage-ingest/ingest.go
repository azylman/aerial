package main

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

// defaultFlushTimeout bounds a single flush request when the config omits flush_timeout.
const defaultFlushTimeout = 60 * time.Second

// defaultConnectRetryWindow bounds how long a refused flush dial is retried when the
// config omits connect_retry_window. A new pod's IP reaches the NetworkPolicy allow sets
// a few seconds after start, and until then protected targets REJECT the connection.
const defaultConnectRetryWindow = 30 * time.Second

// defaultConnectRetryInterval is the wait between refused dials when the config omits
// connect_retry_interval.
const defaultConnectRetryInterval = time.Second

// maxArchiveEntryBytes caps a single extracted coverage file so a misbehaving target
// cannot fill the collector's scratch disk.
const maxArchiveEntryBytes = 256 << 20

// Target is one coverage flush endpoint. Several targets may share a Service name;
// their counters are merged before ingest.
type Target struct {
	Service string `yaml:"service"`
	URL     string `yaml:"url"`
}

// Spool is a directory where short-lived jobs leave finished coverage runs for the
// collector. Each visible subdirectory is one run holding covmeta.* and covcounters.*
// files; writers build runs in hidden ".tmp-*" siblings and rename them into place.
type Spool struct {
	Service string `yaml:"service"`
	Dir     string `yaml:"dir"`
}

// Config is the collector's YAML configuration.
type Config struct {
	Targets      []Target      `yaml:"targets"`
	Spools       []Spool       `yaml:"spools"`
	FlushTimeout time.Duration `yaml:"flush_timeout"`
	// ConnectRetryWindow bounds retries of a flush dial refused with ECONNREFUSED. It
	// runs inside the per-target FlushTimeout. Like flush_timeout, an omitted or zero
	// value takes the default; the window is a safety bound, not an opt-in feature.
	ConnectRetryWindow time.Duration `yaml:"connect_retry_window"`
	// ConnectRetryInterval is the wait between refused dials; omitted or zero takes the
	// default.
	ConnectRetryInterval time.Duration `yaml:"connect_retry_interval"`
}

// CoverageRow is one function's coverage within a service.
type CoverageRow struct {
	File     string
	Function string
	Percent  float64
}

// ParseConfig decodes and validates collector YAML, applying default durations.
func ParseConfig(data []byte) (Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if len(cfg.Targets) == 0 && len(cfg.Spools) == 0 {
		return Config{}, errors.New("config: no targets or spools")
	}
	targetServices := map[string]bool{}
	for i, t := range cfg.Targets {
		if strings.TrimSpace(t.Service) == "" || strings.TrimSpace(t.URL) == "" {
			return Config{}, fmt.Errorf("config: target %d needs service and url", i)
		}
		targetServices[t.Service] = true
	}
	spoolServices := map[string]bool{}
	for i, sp := range cfg.Spools {
		if strings.TrimSpace(sp.Service) == "" || !filepath.IsAbs(sp.Dir) {
			return Config{}, fmt.Errorf("config: spool %d needs service and absolute dir", i)
		}
		// A service fed by two sources would have each ingest prune the other's
		// function set.
		if targetServices[sp.Service] {
			return Config{}, fmt.Errorf("config: spool service %q is also a target", sp.Service)
		}
		if spoolServices[sp.Service] {
			return Config{}, fmt.Errorf("config: spool service %q appears twice", sp.Service)
		}
		spoolServices[sp.Service] = true
	}
	durations := []struct {
		name  string
		value *time.Duration
		def   time.Duration
	}{
		{"flush_timeout", &cfg.FlushTimeout, defaultFlushTimeout},
		{"connect_retry_window", &cfg.ConnectRetryWindow, defaultConnectRetryWindow},
		{"connect_retry_interval", &cfg.ConnectRetryInterval, defaultConnectRetryInterval},
	}
	for _, d := range durations {
		if *d.value < 0 {
			return Config{}, fmt.Errorf("config: %s must not be negative", d.name)
		}
		if *d.value == 0 {
			*d.value = d.def
		}
	}
	return cfg, nil
}

// ParseFuncReport converts `go tool covdata func` output into rows keyed by
// (file, function). Receiver prefixes are stripped and duplicate keys keep the max percent.
func ParseFuncReport(out []byte) []CoverageRow {
	var rows []CoverageRow
	index := map[[2]string]int{}
	for line := range strings.SplitSeq(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] == "total" || strings.HasPrefix(fields[0], "total:") {
			continue
		}
		file := stripLineSuffix(fields[0])
		fn := fields[1]
		if i := strings.LastIndex(fn, "."); i >= 0 {
			fn = fn[i+1:]
		}
		pct, err := strconv.ParseFloat(strings.TrimSuffix(fields[2], "%"), 64)
		if err != nil || file == "" || fn == "" {
			continue
		}
		key := [2]string{file, fn}
		if i, ok := index[key]; ok {
			rows[i].Percent = max(rows[i].Percent, pct)
			continue
		}
		index[key] = len(rows)
		rows = append(rows, CoverageRow{File: file, Function: fn, Percent: pct})
	}
	return rows
}

// stripLineSuffix removes a trailing ":<line>:" (or ":<line>") from a covdata location.
func stripLineSuffix(loc string) string {
	loc = strings.TrimSuffix(loc, ":")
	if i := strings.LastIndex(loc, ":"); i >= 0 {
		if _, err := strconv.Atoi(loc[i+1:]); err == nil {
			return loc[:i]
		}
	}
	return loc
}

// ExtractFlushArchive writes the regular files of a coverage flush tar into dir and
// returns how many covcounters files it contained. Entry names with path separators or
// ".." are rejected.
func ExtractFlushArchive(r io.Reader, dir string) (int, error) {
	tr := tar.NewReader(r)
	counters := 0
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return counters, nil
		}
		if err != nil {
			return counters, fmt.Errorf("read archive: %w", err)
		}
		if strings.ContainsAny(hdr.Name, `/\`) || strings.Contains(hdr.Name, "..") || hdr.Name == "" {
			return counters, fmt.Errorf("unsafe archive entry %q", hdr.Name)
		}
		if hdr.Typeflag != tar.TypeReg {
			return counters, fmt.Errorf("archive entry %q is not a regular file", hdr.Name)
		}
		if hdr.Size > maxArchiveEntryBytes {
			return counters, fmt.Errorf("archive entry %q exceeds %d bytes", hdr.Name, maxArchiveEntryBytes)
		}
		if err := writeEntry(dir, hdr.Name, tr); err != nil {
			return counters, err
		}
		if strings.HasPrefix(hdr.Name, "covcounters.") {
			counters++
		}
	}
}

// writeEntry creates a new file name in dir. Callers must pass a name already validated
// to contain no path separators or "..", so the joined path cannot leave dir.
func writeEntry(dir, name string, r io.Reader) error {
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	_, cErr := io.Copy(f, io.LimitReader(r, maxArchiveEntryBytes))
	return errors.Join(cErr, f.Close())
}

// CovdataRunner produces a `go tool covdata func` report over merged coverage dirs.
type CovdataRunner interface {
	Func(ctx context.Context, dirs []string) ([]byte, error)
}

// ExecCovdata runs `go tool covdata func` using the Go binary at GoBin.
type ExecCovdata struct {
	GoBin string
}

// covdataArgs returns the argv (after the go binary) for a merged func report.
func covdataArgs(dirs []string) []string {
	return []string{"tool", "covdata", "func", "-i=" + strings.Join(dirs, ",")}
}

// Func implements CovdataRunner.
func (e ExecCovdata) Func(ctx context.Context, dirs []string) ([]byte, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, e.GoBin, covdataArgs(dirs)...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("covdata func: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// CoverageStore persists per-service coverage rows.
type CoverageStore interface {
	UpsertService(ctx context.Context, service string, rows []CoverageRow) error
}

// TxBeginner is the subset of a pgx pool the store needs.
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// PostgresStore writes production_code_coverage.
type PostgresStore struct {
	DB TxBeginner
}

// upsertCoverageSQL matches the retired collector: a 0% sample never lowers a recorded
// percent nor clears an older last_executed_at.
const upsertCoverageSQL = `INSERT INTO production_code_coverage
    (service, file, function_name, coverage_percent, last_executed_at, first_seen_at, updated_at)
SELECT $1, t.file, t.function_name, t.pct,
       CASE WHEN t.pct > 0 THEN now() ELSE NULL END, now(), now()
FROM unnest($2::text[], $3::text[], $4::real[]) AS t(file, function_name, pct)
ON CONFLICT (service, file, function_name) DO UPDATE SET
    coverage_percent = CASE WHEN excluded.coverage_percent > 0
        THEN excluded.coverage_percent ELSE production_code_coverage.coverage_percent END,
    last_executed_at = CASE WHEN excluded.coverage_percent > 0
        THEN now() ELSE production_code_coverage.last_executed_at END,
    updated_at = now()`

// deleteAbsentSQL removes a service's rows for functions no longer present in its binaries.
const deleteAbsentSQL = `DELETE FROM production_code_coverage p
WHERE p.service = $1
  AND NOT EXISTS (
    SELECT 1 FROM unnest($2::text[], $3::text[]) AS k(file, function_name)
    WHERE k.file = p.file AND k.function_name = p.function_name)`

// UpsertService applies rows for service in one transaction and prunes functions absent
// from rows. An empty rows slice is a no-op so a broken report never wipes a service.
func (s PostgresStore) UpsertService(ctx context.Context, service string, rows []CoverageRow) (err error) {
	if len(rows) == 0 {
		return nil
	}
	files := make([]string, len(rows))
	funcs := make([]string, len(rows))
	pcts := make([]float32, len(rows))
	for i, r := range rows {
		files[i], funcs[i], pcts[i] = r.File, r.Function, float32(r.Percent)
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s: %w", service, err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback(ctx))
		}
	}()
	if _, err = tx.Exec(ctx, upsertCoverageSQL, service, files, funcs, pcts); err != nil {
		return fmt.Errorf("upsert %s: %w", service, err)
	}
	if _, err = tx.Exec(ctx, deleteAbsentSQL, service, files, funcs); err != nil {
		return fmt.Errorf("prune %s: %w", service, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", service, err)
	}
	return nil
}

// groupTargets returns service names in first-seen order and each service's targets.
func groupTargets(targets []Target) ([]string, map[string][]Target) {
	var order []string
	groups := map[string][]Target{}
	for _, t := range targets {
		if _, ok := groups[t.Service]; !ok {
			order = append(order, t.Service)
		}
		groups[t.Service] = append(groups[t.Service], t)
	}
	return order, groups
}

// fetchTarget creates dir, POSTs a flush request, and extracts the archive into it.
// The whole fetch is bounded by cfg.FlushTimeout.
func fetchTarget(ctx context.Context, client *http.Client, cfg Config, t Target, dir string) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.FlushTimeout)
	defer cancel()
	if err := os.Mkdir(dir, 0o700); err != nil {
		return fmt.Errorf("create target dir: %w", err)
	}
	resp, err := postFlush(ctx, client, cfg, t)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, rErr := io.ReadAll(io.LimitReader(resp.Body, 512))
		return errors.Join(fmt.Errorf("flush: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body))), rErr)
	}
	n, err := ExtractFlushArchive(resp.Body, dir)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("flush archive has no covcounters files")
	}
	return nil
}

// postFlush POSTs the flush request, retrying dials refused with ECONNREFUSED every
// cfg.ConnectRetryInterval while the next attempt still fits in cfg.ConnectRetryWindow
// and ctx is live. Any other error fails immediately; giving up returns the last error.
func postFlush(ctx context.Context, client *http.Client, cfg Config, t Target) (*http.Response, error) {
	start := time.Now()
	for attempt := 1; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.URL, http.NoBody)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			if attempt > 1 {
				log.Printf("[coverage-ingest] target %s (%s): connected after %d attempts", t.Service, t.URL, attempt)
			}
			return resp, nil
		}
		if !errors.Is(err, syscall.ECONNREFUSED) || !waitRetry(ctx, cfg, time.Since(start)) {
			if attempt > 1 {
				log.Printf("[coverage-ingest] target %s (%s): gave up after %d refused attempts", t.Service, t.URL, attempt)
			}
			return nil, fmt.Errorf("flush: %w", err)
		}
	}
}

// waitRetry waits one retry interval and reports whether another attempt may run. It
// returns false without waiting when retries are disabled or the wait would overrun the
// retry window, and false early when ctx ends.
func waitRetry(ctx context.Context, cfg Config, elapsed time.Duration) bool {
	if cfg.ConnectRetryInterval <= 0 || elapsed+cfg.ConnectRetryInterval > cfg.ConnectRetryWindow {
		return false
	}
	timer := time.NewTimer(cfg.ConnectRetryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Ingest flushes every target, merges counters per service, and upserts each service.
// Failing targets are logged and skipped; the returned error names every failure.
func Ingest(ctx context.Context, cfg Config, client *http.Client, runner CovdataRunner, store CoverageStore) error {
	workDir, err := os.MkdirTemp("", "coverage-ingest-*")
	if err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(workDir); err != nil {
			log.Printf("[coverage-ingest] cleanup %s: %v", workDir, err)
		}
	}()

	order, groups := groupTargets(cfg.Targets)
	var errs []error
	for si, service := range order {
		var dirs []string
		for ti, t := range groups[service] {
			dir := filepath.Join(workDir, fmt.Sprintf("%d-%d", si, ti))
			if err := fetchTarget(ctx, client, cfg, t, dir); err != nil {
				log.Printf("[coverage-ingest] skipping target %s (%s): %v", t.Service, t.URL, err)
				errs = append(errs, fmt.Errorf("target %s (%s): %w", t.Service, t.URL, err))
				continue
			}
			dirs = append(dirs, dir)
		}
		if len(dirs) == 0 {
			continue
		}
		if err := ingestService(ctx, service, dirs, runner, store); err != nil {
			log.Printf("[coverage-ingest] service %s failed: %v", service, err)
			errs = append(errs, fmt.Errorf("service %s: %w", service, err))
		}
	}
	return errors.Join(errs...)
}

func ingestService(ctx context.Context, service string, dirs []string, runner CovdataRunner, store CoverageStore) error {
	out, err := runner.Func(ctx, dirs)
	if err != nil {
		return err
	}
	rows := ParseFuncReport(out)
	if len(rows) == 0 {
		return errors.New("covdata report has no functions")
	}
	if err := store.UpsertService(ctx, service, rows); err != nil {
		return err
	}
	log.Printf("[coverage-ingest] %s: upserted %d functions from %d sources", service, len(rows), len(dirs))
	return nil
}

// staleSpoolTempAge is how old a hidden ".tmp-*" spool entry must be before the
// collector treats its writer as dead and removes it.
const staleSpoolTempAge = 24 * time.Hour

// spoolTempPrefix starts the name of a run a writer is still building.
const spoolTempPrefix = ".tmp-"

// spoolGroup is a set of spooled runs written by the same binary version (identical
// covmeta files), so one covdata call can merge them.
type spoolGroup struct {
	newest string   // greatest run name in the group
	dirs   []string // run dir paths, ascending by name
}

// IngestSpools ingests every finished run in each spool under the spool's service and
// deletes runs only after their rows commit. Runs are grouped by covmeta file set and
// applied oldest group first so the newest binary's function set prunes last. A failed
// group keeps its runs and stops later groups of that spool, preserving that order on
// the next tick; so does a group whose runs cannot all be deleted. Incomplete runs are reported and kept; abandoned temp runs older than
// staleSpoolTempAge are removed.
func IngestSpools(ctx context.Context, spools []Spool, runner CovdataRunner, store CoverageStore) error {
	var errs []error
	for _, sp := range spools {
		if err := ingestSpool(ctx, sp, runner, store); err != nil {
			log.Printf("[coverage-ingest] spool %s (%s): %v", sp.Service, sp.Dir, err)
			errs = append(errs, fmt.Errorf("spool %s (%s): %w", sp.Service, sp.Dir, err))
		}
	}
	return errors.Join(errs...)
}

func ingestSpool(ctx context.Context, sp Spool, runner CovdataRunner, store CoverageStore) error {
	entries, err := os.ReadDir(sp.Dir)
	if err != nil {
		return fmt.Errorf("read spool: %w", err)
	}
	var errs []error
	groups := map[string]*spoolGroup{}
	for _, e := range entries { // os.ReadDir sorts by name
		name := e.Name()
		path := filepath.Join(sp.Dir, name)
		if strings.HasPrefix(name, ".") {
			if strings.HasPrefix(name, spoolTempPrefix) {
				errs = append(errs, removeStaleTemp(e, path))
			}
			continue
		}
		key, err := spoolRunKey(e, path)
		if err != nil {
			errs = append(errs, fmt.Errorf("run %s: %w", name, err))
			continue
		}
		g := groups[key]
		if g == nil {
			g = &spoolGroup{}
			groups[key] = g
		}
		g.dirs = append(g.dirs, path)
		g.newest = name
	}
	ordered := make([]*spoolGroup, 0, len(groups))
	for _, g := range groups {
		ordered = append(ordered, g)
	}
	slices.SortFunc(ordered, func(a, b *spoolGroup) int { return strings.Compare(a.newest, b.newest) })
	for _, g := range ordered {
		if err := ingestService(ctx, sp.Service, g.dirs, runner, store); err != nil {
			errs = append(errs, fmt.Errorf("runs through %s: %w", g.newest, err))
			break
		}
		// A run left behind would be re-ingested alone on the next tick. Stop here so
		// no newer group commits ahead of it and gets pruned back to its function set.
		var rmErrs []error
		for _, dir := range g.dirs {
			if err := os.RemoveAll(dir); err != nil {
				rmErrs = append(rmErrs, fmt.Errorf("remove ingested run: %w", err))
			}
		}
		if len(rmErrs) > 0 {
			errs = append(errs, rmErrs...)
			break
		}
	}
	return errors.Join(errs...)
}

// spoolRunKey validates a visible spool entry as a complete run and returns its sorted
// covmeta file names joined, which identifies the binary version that wrote it.
func spoolRunKey(e os.DirEntry, path string) (string, error) {
	if !e.IsDir() {
		return "", errors.New("not a directory")
	}
	files, err := os.ReadDir(path)
	if err != nil {
		return "", fmt.Errorf("read run: %w", err)
	}
	var metas []string
	counters := 0
	for _, f := range files {
		switch {
		case strings.HasPrefix(f.Name(), "covmeta."):
			metas = append(metas, f.Name())
		case strings.HasPrefix(f.Name(), "covcounters."):
			counters++
		}
	}
	if len(metas) == 0 || counters == 0 {
		return "", fmt.Errorf("incomplete run: %d covmeta and %d covcounters files", len(metas), counters)
	}
	return strings.Join(metas, "\x00"), nil
}

// removeStaleTemp deletes an abandoned writer's temp run once it is older than
// staleSpoolTempAge; younger entries may belong to a live writer and are left alone.
func removeStaleTemp(e os.DirEntry, path string) error {
	info, err := e.Info()
	if err != nil {
		return fmt.Errorf("stat %s: %w", e.Name(), err)
	}
	if time.Since(info.ModTime()) < staleSpoolTempAge {
		return nil
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove stale temp run: %w", err)
	}
	log.Printf("[coverage-ingest] removed abandoned spool temp %s", path)
	return nil
}
