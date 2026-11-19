package main

import (
	"github.com/azylman/aerial/services/prospera/internal/mediastream"
)

// Manager registry types and constructors live in internal/mediastream.
// The aliases below keep existing callers compiling until the final
// cutover chunk removes this shim file.
type (
	SessionManagerOption = mediastream.SessionManagerOption
	CallSessionManager   = mediastream.CallSessionManager
)

var (
	NewCallSessionManager = mediastream.NewCallSessionManager
	WithPacerInterval     = mediastream.WithPacerInterval
	WithGeminiDialer      = mediastream.WithGeminiDialer
	WithSessionFactory    = mediastream.WithSessionFactory

	// ActiveCount exposes the registry size query as a package-level
	// function value to keep the transition API reachable for deadcode
	// until post-cutover callers (e.g. Drain) land.
	ActiveCount = (*CallSessionManager).ActiveCount
	// WaitForSession exposes the registry wait primitive as a
	// package-level function value to keep the transition API reachable
	// for deadcode until post-cutover callers land.
	WaitForSession = (*CallSessionManager).WaitForSession
)

// MediaStream session bridge types and constructors live in
// internal/mediastream. The aliases below keep remaining callers compiling
// until the final cutover chunk removes this shim file.
type (
	MediaStreamSessionConfig = mediastream.MediaStreamSessionConfig
	MediaStreamSession       = mediastream.MediaStreamSession
)

var NewMediaStreamSession = mediastream.NewMediaStreamSession
