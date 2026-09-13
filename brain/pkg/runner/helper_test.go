package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	if os.Getenv("GO_WANT_HELPER_PROCESS") == "1" || isTestHelperArgs() {
		runHelperProcess()
	}
}

func isTestHelperArgs() bool {
	for _, arg := range os.Args[1:] {
		if arg == "--dangerously-skip-permissions" {
			return true
		}
	}
	return false
}

// TestHelperProcess serves as an in-process mock executable for cross-platform subprocess tests.
// It is invoked by running the compiled test binary with -test.run=^TestHelperProcess$.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	runHelperProcess()
}

func runHelperProcess() {
	defer func() {
		_ = os.Stdout.Sync()
		_ = os.Stderr.Sync()
	}()

	mode := os.Getenv("MOCK_MODE")

	switch mode {
	case "echo":
		// Find arguments after "--"
		args := os.Args
		for i, arg := range args {
			if arg == "--" && i+1 < len(args) {
				args = args[i+1:]
				break
			}
		}
		// Echo arguments or prompt
		out := strings.Join(args, " ")
		for i, arg := range args {
			if arg == "-p" && i+1 < len(args) {
				out = args[i+1]
				break
			}
		}
		fmt.Println(out)
		os.Exit(0)

	case "model_err":
		_, _ = fmt.Fprintln(os.Stderr, `modelprovider is set to "gemini"`)
		os.Exit(1)

	case "hang":
		time.Sleep(10 * time.Minute)
		os.Exit(0)

	case "exit":
		os.Exit(0)

	case "bad_result":
		convID := "11111111-2222-3333-4444-555555555555"
		fmt.Printf("{\"event\":\"init\",\"conversation_id\":\"%s\"}\n", convID)
		_ = os.Stdout.Sync()
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			fmt.Println(`{"event":"result","result":INVALID_JSON}`)
		}
		os.Exit(0)

	case "stream-json":
		convID := os.Getenv("MOCK_CONV_ID")
		if convID == "" {
			convID = "11111111-2222-3333-4444-555555555555"
		}

		fmt.Printf("{\"event\":\"init\",\"conversation_id\":\"%s\"}\n", convID)
		_ = os.Stdout.Sync()

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			if strings.Contains(line, "CRASH_ALWAYS") || strings.Contains(line, "CRASH_NOW") {
				os.Exit(1)
			}

			if strings.Contains(line, "CRASH_ONCE") {
				markerPath := os.Getenv("MOCK_MARKER")
				if markerPath == "" {
					markerPath = filepath.Join(os.TempDir(), "mock_crash_once.marker")
				}
				if _, err := os.Stat(markerPath); os.IsNotExist(err) {
					_ = os.WriteFile(markerPath, []byte("1"), 0600)
					os.Exit(1)
				}
			}

			if strings.Contains(line, "SLEEP") {
				time.Sleep(5 * time.Second)
			}

			content := line
			var req struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			}
			if err := json.Unmarshal([]byte(line), &req); err == nil && req.Message.Content != "" {
				content = req.Message.Content
			}

			res := map[string]any{
				"event": "result",
				"result": map[string]any{
					"status":          "SUCCESS",
					"response":        "ECHO:" + content,
					"conversation_id": convID,
				},
			}
			data, _ := json.Marshal(res)
			fmt.Println(string(data))
			_ = os.Stdout.Sync()
		}
		os.Exit(0)

	case "target":
		targetID := os.Getenv("AERIAL_TARGET_ID")
		fmt.Printf("TARGET=%s\n{\"status\":\"SUCCESS\",\"response\":\"target ok\"}\n", targetID)
		os.Exit(0)

	case "pulse_stderr":
		pulses := 5
		if pStr := os.Getenv("MOCK_PULSES"); pStr != "" {
			if p, err := strconv.Atoi(pStr); err == nil && p > 0 {
				pulses = p
			}
		}
		sleepMs := 15
		if sStr := os.Getenv("MOCK_PULSE_SLEEP_MS"); sStr != "" {
			if s, err := strconv.Atoi(sStr); err == nil && s > 0 {
				sleepMs = s
			}
		}
		for i := 1; i <= pulses; i++ {
			fmt.Fprintf(os.Stderr, "pulse %d\n", i)
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		}
		fmt.Println(`{"status":"SUCCESS","response":"done"}`)
		os.Exit(0)

	case "pulse_stdout":
		pulses := 5
		if pStr := os.Getenv("MOCK_PULSES"); pStr != "" {
			if p, err := strconv.Atoi(pStr); err == nil && p > 0 {
				pulses = p
			}
		}
		sleepMs := 15
		if sStr := os.Getenv("MOCK_PULSE_SLEEP_MS"); sStr != "" {
			if s, err := strconv.Atoi(sStr); err == nil && s > 0 {
				sleepMs = s
			}
		}
		for i := 1; i <= pulses; i++ {
			fmt.Printf("stdout pulse %d\n", i)
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		}
		fmt.Println(`{"status":"SUCCESS","response":"done"}`)
		os.Exit(0)

	case "pulse_file":
		logFile := os.Getenv("MOCK_LOG_FILE")
		pulses := 5
		if pStr := os.Getenv("MOCK_PULSES"); pStr != "" {
			if p, err := strconv.Atoi(pStr); err == nil && p > 0 {
				pulses = p
			}
		}
		sleepMs := 15
		if sStr := os.Getenv("MOCK_PULSE_SLEEP_MS"); sStr != "" {
			if s, err := strconv.Atoi(sStr); err == nil && s > 0 {
				sleepMs = s
			}
		}
		for i := 1; i <= pulses; i++ {
			if logFile != "" {
				f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err == nil {
					_, _ = fmt.Fprintf(f, "{\"step\": %d}\n", i)
					_ = f.Sync()
					_ = f.Close()
				}
			}
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		}
		fmt.Println(`{"status":"SUCCESS","response":"done"}`)
		os.Exit(0)

	case "stall_file":
		logFile := os.Getenv("MOCK_LOG_FILE")
		stallMs := 150
		if sStr := os.Getenv("MOCK_STALL_MS"); sStr != "" {
			if s, err := strconv.Atoi(sStr); err == nil && s > 0 {
				stallMs = s
			}
		}
		if logFile != "" {
			f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err == nil {
				_, _ = fmt.Fprintln(f, "started build")
				_ = f.Sync()
				_ = f.Close()
			}
		}
		time.Sleep(time.Duration(stallMs) * time.Millisecond)
		fmt.Println(`{"status":"SUCCESS","response":"done"}`)
		os.Exit(0)

	default:
		fmt.Printf("{\"status\":\"SUCCESS\",\"response\":\"default helper ok\"}\n")
		os.Exit(0)
	}
}

// getHelperProcessBin returns the resolved absolute path to the compiled test binary.
func getHelperProcessBin(t *testing.T) string {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		bin, err = filepath.Abs(os.Args[0])
		if err != nil {
			t.Fatalf("failed to resolve test binary path: %v", err)
		}
	}
	return bin
}

// MockStreamWorkerConfig controls mock stream behavior for testing WorkerInstance and UtilityDaemon in memory.
type MockStreamWorkerConfig struct {
	ConvID     string
	CrashOnce  bool
	CrashNow   bool
	SleepDelay time.Duration
	BadResult  bool
	NoInit     bool
	HangInit   bool
	RSSBytes    uint64
	Roots       []string
	TurnStarted chan struct{}
	BlockUntil  chan struct{}
}

// MockStreamWorkerHarness manages the background goroutine and pipes for in-memory stream testing.
type MockStreamWorkerHarness struct {
	StdinWriter  *io.PipeWriter
	StdoutReader *io.PipeReader
	Cancel       context.CancelFunc
	done         chan struct{}
}

// Close gracefully terminates the mock worker pipes and waits for the handler routine.
func (h *MockStreamWorkerHarness) Close() {
	if h.Cancel != nil {
		h.Cancel()
	}
	if h.StdinWriter != nil {
		_ = h.StdinWriter.Close()
	}
	if h.StdoutReader != nil {
		_ = h.StdoutReader.Close()
	}
	select {
	case <-h.done:
	case <-time.After(500 * time.Millisecond):
	}
}

var mockConvSeq atomic.Int64

// NewMockStreamWorker creates an in-memory stream pair backed by an active goroutine event loop.
func NewMockStreamWorker(t *testing.T, cfg MockStreamWorkerConfig) (*WorkerInstance, *MockStreamWorkerHarness) {
	t.Helper()

	convID := cfg.ConvID
	if convID == "" {
		seq := mockConvSeq.Add(1)
		convID = fmt.Sprintf("11111111-2222-3333-4444-%012d", seq)
	}

	workerInR, userInW := io.Pipe()   // user writes to userInW, worker reads from workerInR
	userOutR, workerOutW := io.Pipe() // worker writes to workerOutW, user reads from userOutR

	ctx, cancel := context.WithCancel(context.Background())
	harness := &MockStreamWorkerHarness{
		StdinWriter:  userInW,
		StdoutReader: userOutR,
		Cancel:       cancel,
		done:         make(chan struct{}),
	}

	go func() {
		defer close(harness.done)
		defer workerInR.Close()
		defer workerOutW.Close()

		if cfg.HangInit {
			<-ctx.Done()
			return
		}

		if !cfg.NoInit {
			initPayload := fmt.Sprintf("{\"event\":\"init\",\"conversation_id\":\"%s\"}\n", convID)
			if _, err := workerOutW.Write([]byte(initPayload)); err != nil {
				return
			}
		} else {
			return
		}

		scanner := bufio.NewScanner(workerInR)
		crashOnceDone := false

		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			if cfg.TurnStarted != nil {
				select {
				case cfg.TurnStarted <- struct{}{}:
				default:
				}
			}

			if cfg.BlockUntil != nil {
				select {
				case <-cfg.BlockUntil:
				case <-ctx.Done():
					return
				}
			}

			if cfg.CrashNow || strings.Contains(line, "CRASH_ALWAYS") || strings.Contains(line, "CRASH_NOW") {
				_ = workerOutW.CloseWithError(errors.New("worker crashed"))
				return
			}

			if cfg.CrashOnce && !crashOnceDone {
				crashOnceDone = true
				_ = workerOutW.CloseWithError(errors.New("worker crashed once"))
				return
			}

			if cfg.SleepDelay > 0 || strings.Contains(line, "SLEEP") {
				delay := cfg.SleepDelay
				if delay <= 0 {
					delay = 5 * time.Second
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
				}
			}

			if cfg.BadResult {
				_, _ = workerOutW.Write([]byte("{\"event\":\"result\",\"result\":INVALID_JSON}\n"))
				return
			}

			content := line
			var req struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			}
			if err := json.Unmarshal([]byte(line), &req); err == nil && req.Message.Content != "" {
				content = req.Message.Content
			}

			res := map[string]any{
				"event": "result",
				"result": map[string]any{
					"status":          "SUCCESS",
					"response":        "ECHO:" + content,
					"conversation_id": convID,
				},
			}
			data, _ := json.Marshal(res)
			if _, err := workerOutW.Write(append(data, '\n')); err != nil {
				return
			}
		}
	}()

	var rssFn func() uint64
	if cfg.RSSBytes > 0 {
		rssFn = func() uint64 { return cfg.RSSBytes }
	}

	worker, err := NewWorkerInstanceFromStreams(ctx, userInW, userOutR, cancel, cfg.Roots, 2*time.Second, rssFn)
	if err != nil {
		harness.Close()
		t.Fatalf("NewWorkerInstanceFromStreams failed: %v", err)
	}

	return worker, harness
}
