package main

import (
	"context"
	"io"
	"os"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestMainEntrypoint(t *testing.T) {
	tests := []struct {
		name     string
		runCode  int
		wantExit int
	}{
		{name: "success exit code", runCode: 0, wantExit: 0},
		{name: "failure exit code", runCode: 1, wantExit: 1},
		{name: "usage exit code", runCode: 2, wantExit: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origRun := runFn
			origExit := exitFn
			defer func() {
				runFn = origRun
				exitFn = origExit
			}()

			var ranArgs []string
			var ranStderr io.Writer
			runFn = func(_ context.Context, args []string, stderr io.Writer) int {
				ranArgs = args
				ranStderr = stderr
				return tc.runCode
			}

			var exitCode int
			exitFn = func(code int) {
				exitCode = code
			}

			origArgs := os.Args
			os.Args = []string{"coverage-ingest", "-config", "test.yaml"}
			defer func() { os.Args = origArgs }()

			main()

			if exitCode != tc.wantExit {
				t.Fatalf("exitCode = %d, want %d", exitCode, tc.wantExit)
			}
			if len(ranArgs) != 2 || ranArgs[0] != "-config" || ranArgs[1] != "test.yaml" {
				t.Fatalf("ranArgs = %v, want [-config test.yaml]", ranArgs)
			}
			if ranStderr != os.Stderr {
				t.Fatalf("ranStderr = %v, want os.Stderr", ranStderr)
			}
		})
	}
}
