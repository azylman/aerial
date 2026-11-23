package config

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultFlushTimeout bounds a single flush request when the config omits flush_timeout.
const DefaultFlushTimeout = 60 * time.Second

// DefaultConnectRetryWindow bounds how long a refused flush dial is retried when the
// config omits connect_retry_window. A new pod's IP reaches the NetworkPolicy allow sets
// a few seconds after start, and until then protected targets REJECT the connection.
const DefaultConnectRetryWindow = 30 * time.Second

// DefaultConnectRetryInterval is the wait between refused dials when the config omits
// connect_retry_interval.
const DefaultConnectRetryInterval = time.Second

// DefaultPullInterval is the wait between pull rounds when the config omits pull_interval.
const DefaultPullInterval = 15 * time.Minute

// DefaultListen is the push server's listen address when the config omits listen.
const DefaultListen = ":8080"

// DefaultMaxPushBytes caps a pushed coverage archive when the config omits max_push_bytes.
const DefaultMaxPushBytes int64 = 64 << 20

// MaxArchiveEntryBytes caps a single extracted coverage file so a misbehaving target
// cannot fill the collector's scratch disk.
const MaxArchiveEntryBytes int64 = 256 << 20

// Target is one coverage flush endpoint. Several targets may share a Service name;
// their counters are merged before ingest.
type Target struct {
	Service string `yaml:"service"`
	URL     string `yaml:"url"`
}

// Config is the collector's YAML configuration.
type Config struct {
	Targets      []Target      `yaml:"targets"`
	FlushTimeout time.Duration `yaml:"flush_timeout"`
	// ConnectRetryWindow bounds retries of a flush dial refused with ECONNREFUSED. It
	// runs inside the per-target FlushTimeout. Like flush_timeout, an omitted or zero
	// value takes the default; the window is a safety bound, not an opt-in feature.
	ConnectRetryWindow time.Duration `yaml:"connect_retry_window"`
	// ConnectRetryInterval is the wait between refused dials; omitted or zero takes the
	// default.
	ConnectRetryInterval time.Duration `yaml:"connect_retry_interval"`
	// PushServices lists the services allowed to POST coverage archives. A push service
	// may not also be a pull target: both paths prune the same service's rows.
	PushServices []string `yaml:"push_services"`
	// PullInterval is the wait between pull rounds; omitted or zero takes the default.
	PullInterval time.Duration `yaml:"pull_interval"`
	// Listen is the push server's listen address, read once at startup.
	Listen string `yaml:"listen"`
	// MaxPushBytes caps a pushed archive body; omitted or zero takes the default.
	MaxPushBytes int64 `yaml:"max_push_bytes"`
}

// CoverageRow is one function's coverage within a service.
type CoverageRow struct {
	File     string
	Function string
	Percent  float64
}

// ParseConfig decodes and validates collector YAML, applying defaults.
func ParseConfig(data []byte) (Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if len(cfg.Targets) == 0 && len(cfg.PushServices) == 0 {
		return Config{}, errors.New("config: no targets or push_services")
	}
	pulled := map[string]bool{}
	for i, t := range cfg.Targets {
		if strings.TrimSpace(t.Service) == "" || strings.TrimSpace(t.URL) == "" {
			return Config{}, fmt.Errorf("config: target %d needs service and url", i)
		}
		pulled[t.Service] = true
	}
	pushed := map[string]bool{}
	for i, name := range cfg.PushServices {
		switch {
		case strings.TrimSpace(name) == "":
			return Config{}, fmt.Errorf("config: push_services %d is empty", i)
		case pushed[name]:
			return Config{}, fmt.Errorf("config: push service %q is listed twice", name)
		case pulled[name]:
			return Config{}, fmt.Errorf("config: push service %q is also a pull target", name)
		}
		pushed[name] = true
	}
	if cfg.MaxPushBytes < 0 {
		return Config{}, errors.New("config: max_push_bytes must not be negative")
	}
	if cfg.MaxPushBytes == 0 {
		cfg.MaxPushBytes = DefaultMaxPushBytes
	}
	if cfg.Listen == "" {
		cfg.Listen = DefaultListen
	}
	durations := []struct {
		name  string
		value *time.Duration
		def   time.Duration
	}{
		{"flush_timeout", &cfg.FlushTimeout, DefaultFlushTimeout},
		{"connect_retry_window", &cfg.ConnectRetryWindow, DefaultConnectRetryWindow},
		{"connect_retry_interval", &cfg.ConnectRetryInterval, DefaultConnectRetryInterval},
		{"pull_interval", &cfg.PullInterval, DefaultPullInterval},
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
		if hdr.Size > MaxArchiveEntryBytes {
			return counters, fmt.Errorf("archive entry %q exceeds %d bytes", hdr.Name, MaxArchiveEntryBytes)
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
	_, cErr := io.Copy(f, io.LimitReader(r, MaxArchiveEntryBytes))
	return errors.Join(cErr, f.Close())
}

// PruneKeepSet returns the sorted services whose rows survive a prune: every pull
// target and push service in cfg. An empty configured set is an
// error so a broken config can never wipe the table.
func PruneKeepSet(cfg Config) ([]string, error) {
	var keep []string
	for _, t := range cfg.Targets {
		if name := strings.TrimSpace(t.Service); name != "" {
			keep = append(keep, name)
		}
	}
	for _, name := range cfg.PushServices {
		if name = strings.TrimSpace(name); name != "" {
			keep = append(keep, name)
		}
	}
	if len(keep) == 0 {
		return nil, errors.New("prune keep set is empty")
	}
	slices.Sort(keep)
	return slices.Compact(keep), nil
}
