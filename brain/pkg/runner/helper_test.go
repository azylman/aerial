package runner

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") == "1" {
		runHelperProcess()
	}
}

func readPulseParams() (int, time.Duration) {
	pulses, sleepMs := 5, 15
	if p, err := strconv.Atoi(os.Getenv("MOCK_PULSES")); err == nil && p > 0 {
		pulses = p
	}
	if s, err := strconv.Atoi(os.Getenv("MOCK_PULSE_SLEEP_MS")); err == nil && s > 0 {
		sleepMs = s
	}
	return pulses, time.Duration(sleepMs) * time.Millisecond
}

func runHelperProcess() {
	defer func() {
		_ = os.Stdout.Sync()
		_ = os.Stderr.Sync()
	}()

	switch mode := os.Getenv("MOCK_MODE"); mode {
	case "echo":
		out, args := "", os.Args
		for i, arg := range args {
			if arg == "--" && i+1 < len(args) {
				args = args[i+1:]
				break
			}
		}
		out = strings.Join(args, " ")
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
		fmt.Println(`{"event":"init","conversation_id":"11111111-2222-3333-4444-555555555555"}`)
		_ = os.Stdout.Sync()
		if scanner := bufio.NewScanner(os.Stdin); scanner.Scan() {
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
			fmt.Printf("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":%q,\"conversation_id\":%q}}\n", "ECHO:"+content, convID)
			_ = os.Stdout.Sync()
		}
		os.Exit(0)
	case "target":
		fmt.Printf("TARGET=%s\n{\"status\":\"SUCCESS\",\"response\":\"target ok\"}\n", os.Getenv("AERIAL_TARGET_ID"))
		os.Exit(0)
	case "pulse_stderr", "pulse_stdout", "pulse_file":
		logFile := os.Getenv("MOCK_LOG_FILE")
		pulses, interval := readPulseParams()
		for i := 1; i <= pulses; i++ {
			if mode == "pulse_stderr" {
				fmt.Fprintf(os.Stderr, "pulse %d\n", i)
			} else if mode == "pulse_stdout" {
				fmt.Printf("stdout pulse %d\n", i)
			} else if logFile != "" {
				if f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
					_, _ = fmt.Fprintf(f, "{\"step\": %d}\n", i)
					_ = f.Close()
				}
			}
			time.Sleep(interval)
		}
		fmt.Println(`{"status":"SUCCESS","response":"done"}`)
		os.Exit(0)
	case "stall_file":
		if lf := os.Getenv("MOCK_LOG_FILE"); lf != "" {
			_ = os.WriteFile(lf, []byte("started build\n"), 0644)
		}
		stallMs := 150
		if s, err := strconv.Atoi(os.Getenv("MOCK_STALL_MS")); err == nil && s > 0 {
			stallMs = s
		}
		time.Sleep(time.Duration(stallMs) * time.Millisecond)
		fmt.Println(`{"status":"SUCCESS","response":"done"}`)
		os.Exit(0)
	default:
		fmt.Println(`{"status":"SUCCESS","response":"default helper ok"}`)
		os.Exit(0)
	}
}
