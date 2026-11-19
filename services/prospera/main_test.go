package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/prospera/internal/config"
)

type runAppMainSignalTestCase struct {
	name         string
	loadErr      error
	runErr       error
	reloadCfg    *config.Config
	wantExit     bool
	wantStatus   httpkit.ReloadStatus
	wantCanceled bool
}

func TestRunApp_MainSignal(t *testing.T) {
	t.Cleanup(installTestTracing) // main() runs tracing.Init, which replaces the test tracer
	origLoad := loadConfigFn
	origRun := runAppFn
	origExit := exitFn
	defer func() {
		loadConfigFn = origLoad
		runAppFn = origRun
		exitFn = origExit
	}()

	tests := []runAppMainSignalTestCase{
		{
			name:     "config error triggers exit",
			loadErr:  errors.New("mock config error"),
			wantExit: true,
		},
		{
			name:     "run app error triggers exit",
			runErr:   errors.New("mock run error"),
			wantExit: true,
		},
		{
			name:     "server closed error does not trigger exit",
			runErr:   http.ErrServerClosed,
			wantExit: false,
		},
		{
			name:     "clean execution",
			wantExit: false,
		},
		{
			name:         "port change requests graceful restart",
			reloadCfg:    &config.Config{Port: "9090", DatabaseURL: "sqlite://:memory:"},
			wantStatus:   "restarting",
			wantCanceled: true,
		},
		{
			name:       "live field change keeps running",
			reloadCfg:  &config.Config{Port: "4019", DatabaseURL: "sqlite://:memory:", DefaultDiscordChannel: "new-chan"},
			wantStatus: "reloaded",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runMainSignalCase(t, tc)
		})
	}
}

func runMainSignalCase(t *testing.T, tc runAppMainSignalTestCase) {
	t.Helper()
	exited := false
	exitFn = func(format string, v ...any) {
		exited = true
	}
	loads := 0
	loadConfigFn = func(_ string) (*config.Config, error) {
		loads++
		if tc.loadErr != nil {
			return nil, tc.loadErr
		}
		if loads > 1 && tc.reloadCfg != nil {
			return tc.reloadCfg, nil
		}
		return &config.Config{Port: "4019", DatabaseURL: "sqlite://:memory:"}, nil
	}
	var gotStatus httpkit.ReloadStatus
	canceled := false
	runAppFn = func(ctx context.Context, cfgSrc *httpkit.Reloadable[config.Config]) error {
		if tc.reloadCfg == nil {
			return tc.runErr
		}
		gotStatus, _ = cfgSrc.Reload()
		if !tc.wantCanceled {
			canceled = ctx.Err() != nil
			return tc.runErr
		}
		select {
		case <-ctx.Done():
			canceled = true
		case <-time.After(5 * time.Second):
		}
		return tc.runErr
	}

	main()

	if exited != tc.wantExit {
		t.Errorf("main() exited = %v, want %v", exited, tc.wantExit)
	}
	if gotStatus != tc.wantStatus {
		t.Errorf("reload status = %q, want %q", gotStatus, tc.wantStatus)
	}
	if canceled != tc.wantCanceled {
		t.Errorf("run context canceled = %v, want %v", canceled, tc.wantCanceled)
	}
}

func TestProsperaRestartRequired(t *testing.T) {
	t.Parallel()
	base := config.Config{
		Port:             "8080",
		DatabaseURL:      "postgres://db/aerial",
		TwilioAccountSID: "AC1",
		TwilioAuthToken:  "tok",
		GoogleMCPURL:     "http://google-mcp",
		CovenURL:         "http://coven",
		GeminiLiveModel:  "live-1",
	}
	tests := []struct {
		name string
		next config.Config
		want bool
	}{
		{name: "identical", next: config.Config{Port: "8080", DatabaseURL: "postgres://db/aerial", TwilioAccountSID: "AC1", TwilioAuthToken: "tok", GoogleMCPURL: "http://google-mcp", CovenURL: "http://coven", GeminiLiveModel: "live-1"}, want: false},
		{name: "live model change", next: config.Config{Port: "8080", DatabaseURL: "postgres://db/aerial", TwilioAccountSID: "AC1", TwilioAuthToken: "tok", GoogleMCPURL: "http://google-mcp", CovenURL: "http://coven", GeminiLiveModel: "live-2", DefaultDiscordChannel: "chan", MaxConcurrentCalls: 9}, want: false},
		{name: "port change", next: config.Config{Port: "9090", DatabaseURL: "postgres://db/aerial", TwilioAccountSID: "AC1", TwilioAuthToken: "tok", GoogleMCPURL: "http://google-mcp", CovenURL: "http://coven", GeminiLiveModel: "live-1"}, want: true},
		{name: "database change", next: config.Config{Port: "8080", DatabaseURL: "postgres://db2/aerial", TwilioAccountSID: "AC1", TwilioAuthToken: "tok", GoogleMCPURL: "http://google-mcp", CovenURL: "http://coven", GeminiLiveModel: "live-1"}, want: true},
		{name: "twilio account change", next: config.Config{Port: "8080", DatabaseURL: "postgres://db/aerial", TwilioAccountSID: "AC2", TwilioAuthToken: "tok", GoogleMCPURL: "http://google-mcp", CovenURL: "http://coven", GeminiLiveModel: "live-1"}, want: true},
		{name: "twilio token change", next: config.Config{Port: "8080", DatabaseURL: "postgres://db/aerial", TwilioAccountSID: "AC1", TwilioAuthToken: "tok2", GoogleMCPURL: "http://google-mcp", CovenURL: "http://coven", GeminiLiveModel: "live-1"}, want: true},
		{name: "google mcp change", next: config.Config{Port: "8080", DatabaseURL: "postgres://db/aerial", TwilioAccountSID: "AC1", TwilioAuthToken: "tok", GoogleMCPURL: "http://google-mcp-2", CovenURL: "http://coven", GeminiLiveModel: "live-1"}, want: true},
		{name: "coven change", next: config.Config{Port: "8080", DatabaseURL: "postgres://db/aerial", TwilioAccountSID: "AC1", TwilioAuthToken: "tok", GoogleMCPURL: "http://google-mcp", CovenURL: "http://coven-2", GeminiLiveModel: "live-1"}, want: true},
		{name: "nats url change", next: config.Config{Port: "8080", DatabaseURL: "postgres://db/aerial", TwilioAccountSID: "AC1", TwilioAuthToken: "tok", GoogleMCPURL: "http://google-mcp", CovenURL: "http://coven", GeminiLiveModel: "live-1", NATSURL: "nats://nats.aerial:4222"}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			old := base
			next := tc.next
			if got := prosperaRestartRequired(&old, &next); got != tc.want {
				t.Fatalf("prosperaRestartRequired() = %v, want %v", got, tc.want)
			}
		})
	}
}
