package main

import (
	"log"
	"os"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/server"
)

var (
	configPath = config.DefaultConfigPath
	runAppFn   = server.RunApp
	exitFn     = log.Fatalf
)

func main() {
	runMain(os.Getenv, tracing.DefaultConfigPath)
}

// runMain loads configuration through getenv, initializes tracing from
// tracingConfigPath and runs the app until a signal arrives.
func runMain(getenv func(string) string, tracingConfigPath string) {
	httpkit.RunService(httpkit.ServiceSpec[*config.Config]{
		Name:              "coven",
		ConfigPath:        config.ResolveConfigPath(configPath),
		TracingConfigPath: tracingConfigPath,
		Load:              func(path string) (*config.Config, error) { return config.LoadConfig(path, getenv) },
		Run:               runAppFn,
		Exit:              exitFn,
	})
}
