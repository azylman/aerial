package main

import (
	"log"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/console/internal/config"
	"github.com/azylman/aerial/services/console/internal/server"
)

var (
	defaultConfigPath = config.DefaultConfigPath
	loadConfigFn      = config.LoadConfig
	runConsoleFn      = server.RunConsoleServer
	exitFn            = log.Fatalf
)

func main() {
	httpkit.RunService(httpkit.ServiceSpec[config.ConsoleConfig]{
		Name:              "console",
		ConfigPath:        config.ResolveConfigPath(defaultConfigPath),
		TracingConfigPath: tracing.DefaultConfigPath,
		Load:              loadConfigFn,
		Run:               runConsoleFn,
		Exit:              exitFn,
	})
}
