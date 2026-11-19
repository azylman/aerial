package main

// Root compatibility shims for the Layer-4 server package.
//
// The HTTP/MCP server (Server, NewServer, Routes, InvalidateSchedulesCache,
// HandleOneShotJob, HandleExternalEvent, ConfigureEventBus) now lives in
// internal/server (Layer 4). This file keeps zero-logic aliases only so
// main.go and the remaining root tests compile unchanged until the final
// purge (haro-07b). Shims contain no logic and carry no coverage-bearing
// statements. Methods (Routes, InvalidateSchedulesCache, HandleExternalEvent,
// ConfigureEventBus) are available automatically through the type alias; only
// the free functions are wrapped here.

import (
	"github.com/azylman/aerial/services/haro/internal/server"
)

// Server serves the haro MCP endpoint and schedule inspection HTTP routes.
type Server = server.Server
