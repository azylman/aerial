package main

import (
	"log"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/transcripts/internal/config"
	"github.com/azylman/aerial/services/transcripts/internal/server"
)

var (
	configPath   = config.DefaultConfigPath
	loadConfigFn = config.LoadConfig
	runAppFn     = server.RunApp
	exitFn       = log.Fatalf
)

func main() {
	httpkit.RunService(httpkit.ServiceSpec[*config.Config]{
		Name:              "transcripts",
		ConfigPath:        config.ResolveConfigPath(configPath),
		TracingConfigPath: tracing.DefaultConfigPath,
		Load:              loadConfigFn,
		Run:               runAppFn,
		Exit:              exitFn,
	})
}
