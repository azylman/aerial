package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/azylman/aerial/services/coverage-ingest/internal/config"
)

// groupTargets returns service names in first-seen order and each service's targets.
func groupTargets(targets []config.Target) ([]string, map[string][]config.Target) {
	var order []string
	groups := map[string][]config.Target{}
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
func fetchTarget(ctx context.Context, client *http.Client, cfg config.Config, t config.Target, dir string) error {
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
	n, err := config.ExtractFlushArchive(resp.Body, dir)
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
func postFlush(ctx context.Context, client *http.Client, cfg config.Config, t config.Target) (*http.Response, error) {
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
func waitRetry(ctx context.Context, cfg config.Config, elapsed time.Duration) bool {
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

// Ingest flushes every target in cfg, merges counters per service, and upserts each
// service. Failing targets are logged and skipped; the returned error names every failure.
func (s *Server) Ingest(ctx context.Context, cfg config.Config) error {
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
			if err := fetchTarget(ctx, s.client, cfg, t, dir); err != nil {
				log.Printf("[coverage-ingest] skipping target %s (%s): %v", t.Service, t.URL, err)
				errs = append(errs, fmt.Errorf("target %s (%s): %w", t.Service, t.URL, err))
				continue
			}
			dirs = append(dirs, dir)
		}
		if len(dirs) == 0 {
			continue
		}
		if err := s.ingestService(ctx, service, dirs); err != nil {
			log.Printf("[coverage-ingest] service %s failed: %v", service, err)
			errs = append(errs, fmt.Errorf("service %s: %w", service, err))
		}
	}
	return errors.Join(errs...)
}

// ingestService converts dirs into one service's rows and upserts them. s.sem serializes
// every call so pull rounds and pushes never run covdata or upserts concurrently; a
// caller whose ctx ends while waiting gives up with ctx's error.
func (s *Server) ingestService(ctx context.Context, service string, dirs []string) error {
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("wait for ingest slot: %w", ctx.Err())
	}
	defer func() { <-s.sem }()
	out, err := s.runner.Func(ctx, dirs)
	if err != nil {
		return err
	}
	rows := config.ParseFuncReport(out)
	if len(rows) == 0 {
		return errors.New("covdata report has no functions")
	}
	if err := s.store.UpsertService(ctx, service, rows); err != nil {
		return err
	}
	log.Printf("[coverage-ingest] %s: upserted %d functions from %d sources", service, len(rows), len(dirs))
	return nil
}
