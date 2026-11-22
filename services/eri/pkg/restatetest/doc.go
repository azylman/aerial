// Package restatetest runs handlers against a real restate-server binary.
//
// Its code compiles only under the restateintegration build tag and is driven by
// scripts/restate-integration-test.sh, which fetches a pinned restate-server
// release and passes it via -restate-server-bin. Agent pods have no docker, so
// the server runs as a plain child process with a temp base dir and free ports.
//
// This file is the only one without the build tag: it holds no code and only
// keeps the package visible to default-tag tooling (go test/vet on the
// package directory).
package restatetest
