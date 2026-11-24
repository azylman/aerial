# Bazel 9.3.3 Monorepo Build System, Ephemeral Gazelle & `rules_oci` Rollout

**Date:** 2026-11-23  
**Task:** 271 (Monorepo Build, Test & Container Image Pipeline Modernization)  
**Status:** Implemented  
**Scope:** Unified Bazel 9.3.3 (Bzlmod) build, test, and container image pipeline: LAN `bazel-remote` action/repository cache on `quiet-zero`, single-module `go.mod` consolidation in `azylman/aerial`, ephemeral `gazelle` `BUILD.bazel` generation, and two-phase `rules_oci` daemonless container image publishing across all 20 images in `azylman/aerial` (20 `rules_oci` images with base `Dockerfile` fallback + 6 non-Go `sh_test` targets), `azylman/aerial-config` (Bazel 9.3.3 + ephemeral `gazelle` for `//scripts/validate/...`), and all 11 images in `azylman/aerial-sidecars` (11 `rules_oci` images with base `Dockerfile` fallback unified under `scripts/bazel-select-images.sh` + 3 non-Go `sh_test` targets), with `azylman/mirrormere` intentionally left on its native open-source `Dockerfile` and `go test` workflow per user directive.

---

## 1. Intent and Why

Today, `azylman/aerial`, `azylman/mirrormere`, and `azylman/aerial-sidecars` rely on three separate mechanisms for change detection, testing, and container image builds:

1. **Fragmented Multi-Module Go Topology in `aerial`:** `aerial` maintains 17 separate production `go.mod` files (`pkg/go.mod`, 12 `services/*/go.mod`, 2 `mcp/*/go.mod`, and 2 `scripts/*/go.mod`) wired together via relative `replace github.com/azylman/aerial/pkg => ../../pkg` directives. Dependency upgrades require updating 17 `go.mod`/`go.sum` pairs in lockstep, and `scripts/check-coverage.sh` must loop over 17 module directories spawning separate `go test` invocations.
2. **Bespoke Change-Detection Plumbing (`scripts/affected` & `dorny/paths-filter`):** `aerial` maintains a custom Go import-graph scanner (`scripts/affected`) paired with a manual 20-entry `scripts/affected/catalog.json`, while `mirrormere` and `aerial-sidecars` rely on coarse `dorny/paths-filter` YAML globs and repo-wide `HEAD^{tree}` cache tags that force unnecessary image rebuilds whenever unrelated files change.
3. **Redundant Compilation & Slow `docker build` Loops:** In CI, the `test` job compiles every Go package to run unit tests and coverage gates, and then the `build-and-push` job spins up isolated Docker BuildKit containers for each changed service, re-downloading modules and re-compiling the exact same Go packages from scratch inside `golang:1.27.3-alpine` builder stages before snapshotting and pushing layers.

Standardizing on **Bazel 9.3.3 (LTS)** with **Bzlmod**, **ephemeral `gazelle` generation**, a cluster-hosted **`bazel-remote` cache on `quiet-zero`**, and **`rules_oci`** eliminates all three bottlenecks while keeping day-to-day agent and open-source developer workflows completely friction-free:

- **Zero Manual `BUILD.bazel` Maintenance for Agents:** Generated per-package `BUILD.bazel` files are `.gitignore`d and synthesized on the fly in ~150–250ms by `gazelle` inside `scripts/verify.sh`, `scripts/check-coverage.sh`, and CI. Because Bazel's action cache keys strictly on SHA-256 content hashes rather than file `mtime`s or Git tracking state, ephemeral `BUILD.bazel` files achieve 100% cache hit rates against `bazel-remote`.
- **Instant Ephemeral Scratch & CI Builds via Bazel 9.3.3 Repo Contents Cache:** Bazel 9's `--repo_contents_cache` and `--experimental_remote_repo_contents_cache` cache the *already unpacked and evaluated* external repository trees locally across all `aerial-pr.sh init` scratch clones in `eri-restate` and remotely on `bazel-remote` (`quiet-zero`). Cold scale-to-zero ARC runner pods skip downloading, unpacking, and running `gazelle` over third-party Go modules.
- **Compile Once, Assemble OCI Layers in <2 Seconds:** `rules_oci` reuses the compiled binary artifacts from the Bazel cache and streams deterministic, reproducible OCI layers directly to `ghcr.io` without invoking `docker build` or a BuildKit daemon, allowing `aerial` to delete 16 boilerplate `Dockerfile`s while preserving all 3 `Dockerfile`s in open-source `mirrormere`.

---

## 2. Goals, Non-Goals, and Anti-Goals

### Goals
- **Pin Bazel 9.3.3 LTS Everywhere:** Standardize on Bazel `9.3.3` via `.bazelversion` and pre-baked `bazelisk` (`/usr/local/bin/bazel`) + `gazelle` (`/usr/local/bin/gazelle`) binaries in `aerial-github-runner` and `aerial-eri-restate`.
- **Deploy LAN `bazel-remote` Cache on `quiet-zero`:** Run `buchgr/bazel-remote` on `quiet-zero` backed by a 55 GiB `local-path` NVMe PVC (`--max_size=50` GiB with automatic LRU eviction), exposed inside the cluster mesh at `http://bazel-remote.aerial:9090` (HTTP/REST) and `grpc://bazel-remote.aerial:9092` (gRPC).
- **Collapse `aerial` into a Single Root `go.mod`:** Merge `aerial`'s 17 production Go modules into one root `go.mod` (`module github.com/azylman/aerial`) and `go.sum`, preserving all existing import paths (`github.com/azylman/aerial/...`) and `L0 -> L3` package boundaries while deleting all sub-module `go.mod`/`go.sum` files and `replace` directives.
- **Ephemeral `gazelle` with Zero Checked-In Per-Package `BUILD.bazel` Files:** Keep only root `MODULE.bazel`, `MODULE.bazel.lock`, `.bazelrc`, `.bazelversion`, root `BUILD.bazel`, `images/BUILD.bazel`, and `build/*.bzl` macros in Git; ignore generated per-package `BUILD.bazel` files in `.gitignore` and auto-generate them before `bazel` invocations.
- **Replace 16 Boilerplate `Dockerfile`s in `aerial` with `rules_oci` (`oci_image`, `oci_load`, `oci_push`):** Use a Bazel 9 symbolic macro (`aerial_go_image`) plus static layer targets (`oci_image` + `oci_load` + `oci_push`) in `images/BUILD.bazel` to build, locally load (`bazel run //images:load_<service>`), and push all 16 pure-Go and static-asset service images in `aerial`, including live production coverage instrumentation (`-cover -covermode=atomic` for `coverage-ingest`).
- **First-Class External Contributor Ergonomics:** Keep default `.bazelrc` flags 100% portable for external macOS/Linux machines (scoping cluster-internal `bazel-remote.aerial` and `/data/cache` flags under `build:cluster` enabled via `/etc/bazel.bazelrc` on cluster pods), support `bazel run //:gazelle` when a standalone `gazelle` binary is absent (or native `go test` fallback in `scripts/verify.sh` when `bazel` is not installed locally), and provide a per-service `oci_load` target (`//images:load_<service>`) for every `rules_oci` image.
- **Preserve Open-Source `Dockerfile` Usability in `mirrormere`:** Keep all 3 `Dockerfile`s (`./Dockerfile`, `client/remote/Dockerfile`, `client/cast-watcher/Dockerfile`) in `mirrormere` and keep generated `BUILD.bazel` files `.gitignore`d so external contributors can clone `mirrormere` and run `docker build`, `docker compose`, or `go test ./...` without installing Bazel.
- **Retire `scripts/affected` & `catalog.json` While Preserving `build-metadata` Check-Run Contract:** Replace `scripts/affected` and `dorny/paths-filter` with Bazel's content-addressed dependency graph and OCI digest comparisons against `:latest`, while continuing to emit `build-metadata` and `build-metadata-result` GitHub Check Runs via `scripts/build-metadata.cjs` so `synapse` and `vanadis` promote images without change.

### Non-Goals
- **Converting Heavy OS / CUDA / Upstream-Patch `Dockerfile`s to `rules_oci`:**
  - In `aerial`: `services/github-runner/Dockerfile`, `services/eri-restate/Dockerfile`, `mcp/discord-mcp/Dockerfile` (clones and patches upstream `vianaz/mcp-discord` TypeScript), and `mcp/infisical-mcp/Dockerfile` (bundles global `@infisical/mcp` npm CLI + `tbxark/mcp-proxy`) remain on standard `Dockerfile` builds.
  - In `aerial-sidecars`: `senses` (Rust + CUDA 12 + ONNX Runtime C++) and `pjev` (Rust + CUDA 12 + `llama.cpp` C++) remain on their native `Dockerfile` builds.
- **Remote Build Execution (RBE) Worker Fleet:** We use `bazel-remote` strictly as a shared LAN Action Cache (AC), Content-Addressable Storage (CAS), and Remote Repository Contents Cache. Compilation and test execution run locally on `calibarn`'s ARC runner pods and `eri-restate`.

### Anti-Goals (Failure Conditions Even If Technically Working)
- **Manual `gazelle` or `MODULE.bazel` Steps Required from Agents:** If an agent authoring a Go PR via `scripts/aerial-pr.sh` has to manually run `gazelle` or resolve merge conflicts in generated `BUILD.bazel` files, the design has failed.
- **Regression in Live Production Coverage (`coverage-ingest`) or Unit Test Coverage Gating (`>= 95.0%`):** Production Go service images built by `rules_oci` must continue emitting `runtime/coverage` metadata (`covmeta.*` and `covcounters.*` on `POST /debug/coverage/flush`), `coverage-ingest` must continue executing `go tool covdata func` inside its read-only root filesystem container, and per-package unit test coverage enforcement (`>= 95.0%` statement floor) must remain intact.
- **Breaking Non-Bazel Users of `mirrormere`:** Any change that deletes `mirrormere`'s `Dockerfile`s or requires external users to install Bazel to build `mirrormere` violates the open-source sharing constraint.

---

## 3. Constraints & Bazel 9.3.3 Standards Alignment

1. **Bazel 9.3.3 Pure Bzlmod Invariant:**
   - Bazel 9 completely removes legacy `WORKSPACE` support. All external dependencies (`rules_go`, `gazelle`, `rules_oci`, `rules_pkg`, `aspect_rules_js`, `rules_python`, Go toolchains, `go_deps`, and OCI base images) are declared in `MODULE.bazel` with deterministic lockfiles (`MODULE.bazel.lock`).
2. **Bazel 9.3.3 Performance Flags & External Contributor Portability in `.bazelrc`:**
   - Default `.bazelrc` settings remain 100% portable on external macOS/Linux machines: `test --test_env=PATH` passes through the host `PATH` (without hardcoding Linux container paths), and cluster-only flags are scoped under a `cluster` config profile (`common:cluster` / `build:cluster`) activated automatically inside `aerial-eri-restate` and `aerial-github-runner` via `/etc/bazel.bazelrc` (`common --config=cluster`).
   - `build:cluster --remote_cache=grpc://bazel-remote.aerial:9092` (with `--remote_upload_local_results=true` and `--remote_timeout=5s` fallback so builds never fail if `bazel-remote` is unreachable).
   - `common:cluster --repo_contents_cache=/data/cache/bazel-repo-contents` and `common:cluster --repository_cache=/data/cache/bazel-repo-cache` (shared across all `aerial-pr.sh init` scratch workspaces inside `eri-restate` and mounted via `ci-cache` hostPath on ARC runners).
   - `build:cluster --experimental_remote_repo_contents_cache` (shares unpacked, evaluated external repositories across runner pods via `bazel-remote`).
   - `--remote_download_minimal` paired with Bazel 9.3's default `--rewind_lost_inputs` (avoids downloading intermediate `.a` files on cache hits while automatically rewinding and rebuilding any action whose intermediate blob was LRU-evicted by `bazel-remote`).
   - `--test_summary=short_uncached` (new in Bazel 9.1+; suppresses cached test targets from stdout so agent and CI logs display only tests that actually ran or failed).
3. **Hermetic `pkg/pgtest` Sandbox Invariant:**
   - Bazel's Linux sandbox mounts a deeply nested `$TEST_TMPDIR` (>120 bytes) that exceeds the 107-byte POSIX `AF_UNIX` socket path limit when PostgreSQL creates `.s.PGSQL.5432`. `pkg/pgtest` must allocate its socket/data directory via `os.MkdirTemp("/tmp", "pg")` (which remains isolated inside the per-test `/tmp` mount of Bazel's `linux-sandbox`).
   - `pkg/pgtest`'s `findSchema(start string)` parent-directory walk remains **unmodified**: under `bazel test`, `rules_go` sets the process working directory (`os.Getwd()`) of every `go_test` to `$TEST_SRCDIR/_main/<package_dir>`. Declaring `//services/eri/pkg/db:schema.sql` in the `go_test` `data` attribute (via `# gazelle:map_kind go_test` in `//build:go_test.bzl`) places `schema.sql` at `$TEST_SRCDIR/_main/services/eri/pkg/db/schema.sql`, where `findSchema`'s existing parent walk finds it automatically while preserving unit tests (`TestFindSchema` and `TestRun`) that pass an isolated `t.TempDir()` as `start`.
4. **Physical Immutability & Ephemeral Scratch Workspaces:**
   - `/share/aerial` and `/share/aerial-config` remain mounted read-only (`:ro`). Ephemeral `gazelle` generation executes strictly inside writable scratch clones (`/data/scratch/*`) initialized by `scripts/aerial-pr.sh init` or CI checkout workspaces.

---

## 4. Approaches Considered

### Approach 1: Bazel 9.3.3 (Bzlmod) + Single Root `go.mod` + Ephemeral `gazelle` + `bazel-remote` + `rules_oci` (Selected)
Collapse `aerial` into a single root `go.mod`, deploy `bazel-remote` on `quiet-zero`, `.gitignore` generated per-package `BUILD.bazel` files (synthesizing them ephemerally via `gazelle` in a ~200ms pre-step), run `bazel test //...` against the LAN remote cache, and replace `aerial`'s 16 Go/static `Dockerfile`s with `rules_oci` while keeping `mirrormere`'s 3 `Dockerfile`s intact for open-source users.
- **Pros:** Zero `BUILD.bazel` git noise for agents; single `go.mod` eliminates 17-module `replace` drift; `bazel-remote` + Bazel 9 Repo Contents Cache makes both cold CI runs and scratch workspace tests complete in 1–2s for unchanged packages; `rules_oci` reuses compiled test binaries to push container images in <2s without Docker BuildKit.
- **Cons:** Requires a one-time consolidation of `aerial`'s 17 `go.mod` files and a custom Starlark configuration transition on `aerial_go_image` to compile production binaries with Go coverage instrumentation.

### Approach 2: Checked-In `BUILD.bazel` Files Auto-Updated by `scripts/aerial-pr.sh submit` (Rejected)
Check all generated `BUILD.bazel` files into Git across `aerial`, `mirrormere`, and `aerial-sidecars`, relying on a pre-commit hook in `scripts/aerial-pr.sh submit` to run `gazelle` and `git add '**/BUILD.bazel'`.
- **Pros:** Allows running `bazel` directly on a raw checkout without a wrapper script.
- **Cons:** Adds ~120 generated `BUILD.bazel` files to the repositories, causes frequent merge conflicts when parallel Vanadis chunks add or rename files in neighboring packages, and clutters open-source `mirrormere` with Bazel boilerplate.

### Approach 3: Buck2 or Pants Polyglot Build System (Rejected)
Use Buck2 or Pants 2.31 instead of Bazel 9.3.3.
- **Pros:** Pants uses smaller `BUILD` files; Buck2 has a fast Rust daemon.
- **Cons:** Pants' Go backend (`pants.backend.experimental.go`) remains experimental and lacks `rules_oci` parity; Buck2 lacks Bzlmod `go.mod` integration, mature `gazelle` generation, and first-party daemonless OCI image rules.

**Decision:** **Approach 1** was selected.

---

## 5. Detailed Architecture

### 5.1 LAN Remote Cache Infrastructure (`bazel-remote` on `quiet-zero`)
- **Workload Placement:** `Deployment/bazel-remote` (1 replica, `Recreate` strategy) pinned to `quiet-zero` (`nodeSelector: { kubernetes.io/hostname: quiet-zero }`) where local NVMe storage lives.
- **Storage & Eviction:** Backed by `PersistentVolumeClaim/bazel-remote-data` (`storageClassName: local-path`, `55Gi` request) with `--max_size=50` (50 GiB cap; `bazel-remote` enforces automatic LRU eviction and garbage collection on disk).
- **Endpoints & Protocol:**
  - `Service/bazel-remote` in namespace `default` (resolvable cluster-wide as `bazel-remote.aerial` via CoreDNS rewrite):
    - Port `9090` (HTTP REST cache + `/status` and `/metrics` Prometheus scrape endpoint).
    - Port `9092` (gRPC Action Cache, CAS, and ByteStream protocol used by Bazel `--remote_cache=grpc://bazel-remote.aerial:9092`).
- **Compression:** Zstd compression enabled natively over gRPC (`--remote_cache_compression=true` in `.bazelrc`).

### 5.2 Toolchain Baking (`aerial-github-runner` & `aerial-eri-restate`)
To prevent any dynamic binary downloads at build time, both `services/github-runner/Dockerfile` and `services/eri-restate/Dockerfile` pre-bake:
- **`bazelisk` v1.28.1** installed at `/usr/local/bin/bazel` and `/usr/local/bin/bazelisk` (SHA-256 verified), pre-warmed during `docker build` via:
  ```dockerfile
  ENV BAZELISK_HOME=/opt/bazel-cache
  RUN BAZELISK_HOME=/opt/bazel-cache USE_BAZEL_VERSION=9.3.3 bazelisk version && \
      chmod -R a+rX /opt/bazel-cache
  ```
  so `bazelisk` populates its exact metadata and `9.3.3` binary cache layout at image build time and never fetches Bazel over the network at runtime.
- **`gazelle` v0.47.0** (matching the BCR `gazelle` module version) and **`buildifier` v8.5.1** installed at `/usr/local/bin/gazelle` and `/usr/local/bin/buildifier`.
- **`crane` v0.21.5** (`go-containerregistry`) installed at `/usr/local/bin/crane` for fast OCI manifest inspection (`crane digest`) and registry-side tagging (`crane tag`) without spinning up `docker buildx imagetools`.
- **`/etc/bazel.bazelrc` Cluster Profile Activation:** Both images write `common --config=cluster` to `/etc/bazel.bazelrc` so cluster pods automatically use `bazel-remote.aerial:9092` and `/data/cache` while external contributors running on their own machines use the portable default `.bazelrc`.

### 5.3 Collapsing `aerial` into One Root `go.mod`
1. **Single Root Module (`module github.com/azylman/aerial`):**
   - Create root `/go.mod` (`go 1.27.3`) uniting the direct and indirect dependencies of `pkg/go.mod`, all 12 Go services under `services/`, `mcp/docket-mcp`, `mcp/kernel-mcp`, and `scripts/code-health`.
   - Delete the 17 sub-module `go.mod` and `go.sum` files (leaving only the synthetic test fixtures under `scripts/affected/testdata/` until `scripts/affected` itself is deleted in Phase 4).
   - Run `go mod tidy` at the repository root to produce a single canonical `/go.sum`.
   - Update `.github/dependabot.yml` to replace the 17 per-service `gomod` entries with a single root `directory: "/"` entry (satisfying `check_dependabot_coverage` in `scripts/verify.sh`).
2. **Zero Import Path Churn, Scoped `deadcode` & Preserved `L0 -> L3` Architecture:**
   - Because every sub-module already used `github.com/azylman/aerial/<subpath>` as its module name, collapsing to root `module github.com/azylman/aerial` requires **zero Go import path changes** across the entire monorepo.
   - Each service's `internal/*` directory remains enforced by the Go compiler (a service `services/haro` cannot import `services/coven/internal/...`), and `scripts/code-health` continues enforcing the 3-file `package main` ceiling and `L0 -> L3` layer ratchet.
   - In `scripts/verify.sh` (`run_deadcode`), pass `-filter="^github\\.com/azylman/aerial/${svc}($|/)"` when checking a single service `svc` so `deadcode` scopes reachability analysis to that service's packages rather than the entire root module.
3. **`pkg/pgtest` Bazel Sandbox Hardening:**
   - In `pkg/pgtest/pgtest.go`, change line 121 from `os.MkdirTemp("", "pg")` to `os.MkdirTemp("/tmp", "pg")` so the Unix domain socket path (`/tmp/pgXXXXXXXXXX/.s.PGSQL.5432`) is always ~33 bytes, staying well below the 107-byte kernel `sun_path` limit even when Bazel sets `$TMPDIR` to a deep sandbox path.
   - Leave `findSchema(start string)` completely untouched; `//build:go_test.bzl` attaches `//services/eri/pkg/db:schema.sql` to `go_test` `data` runfiles so `$TEST_SRCDIR/_main/services/eri/pkg/db/schema.sql` is present as an ancestor of `os.Getwd()` inside the sandbox.

### 5.4 Ephemeral `gazelle` & Checked-In Root Bazel Files
Only lightweight root/macro configuration files are checked into Git in `aerial` (and `mirrormere`):
1. **`.bazelversion`**:
   ```text
   9.3.3
   ```
2. **`.bazelrc`** (portable defaults + `cluster` config profile for `eri-restate` & ARC runners):
   ```ini
   common --enable_bzlmod

   build --remote_download_minimal
   build --rewind_lost_inputs
   build --incompatible_strict_action_env

   # Pass host PATH through into sandboxed test actions so pkg/pgtest finds initdb/pg_ctl on any OS
   test --test_env=PATH
   test --test_output=errors
   test --test_summary=short_uncached
   coverage --@rules_go//go/config:cover_format=go_cover

   # Cluster profile (enabled automatically on eri-restate and ARC runners via /etc/bazel.bazelrc)
   common:cluster --repo_contents_cache=/data/cache/bazel-repo-contents
   common:cluster --repository_cache=/data/cache/bazel-repo-cache
   build:cluster --remote_cache=grpc://bazel-remote.aerial:9092
   build:cluster --experimental_remote_repo_contents_cache
   build:cluster --remote_cache_compression
   build:cluster --remote_upload_local_results
   build:cluster --remote_timeout=5s
   ```
3. **`MODULE.bazel`** (and `MODULE.bazel.lock`):
   - Declares `rules_go` (`0.65.0`), `gazelle` (`0.47.0`), `rules_oci` (`2.3.0`), `rules_pkg` (`1.1.0`), `aspect_rules_js` (`2.4.0`).
   - Configures `go_sdk.from_file(go_mod = "//:go.mod")` and `go_deps.from_file(go_mod = "//:go.mod")`.
   - Declares pinned OCI base images via `oci.pull`:
     - `@distroless_static` (`gcr.io/distroless/static-debian12:nonroot`, which pre-bundles `ca-certificates`, `tzdata`, and non-root user `65532:65532` / `10001:10001`).
     - `@nginx_base` (`nginx:1.30.7-alpine-slim` for `services/docs-service`).
     - `@atlas_base` (`arigaio/atlas:1.3.4` for `services/migrator`).
4. **Root `BUILD.bazel`**:
   - Directives and `//:gazelle` runner target:
     ```python
     load("@gazelle//:def.bzl", "gazelle")

     # gazelle:prefix github.com/azylman/aerial
     # gazelle:exclude vendor
     # gazelle:exclude node_modules
     # gazelle:exclude .git
     # gazelle:exclude scripts/affected/testdata
     # gazelle:map_kind go_test go_test //build:go_test.bzl
     gazelle(name = "gazelle")
     ```
   - Exports shared root test fixtures (`filegroup(name = "testdata", srcs = glob(["testdata/**"]), visibility = ["//visibility:public"])`).
5. **`build/go_test.bzl` & `build/oci.bzl`**:
   - `build/go_test.bzl`: Wraps `@rules_go//go:def.bzl` `go_test` to automatically append `//services/eri/pkg/db:schema.sql`, `//:testdata`, and `//services/synapse:testdata` to `data` so every sandboxed test has access to the production schema and cross-package test fixtures.
   - `build/oci.bzl`: Defines the Bazel 9 symbolic macro `aerial_go_image` and coverage transition (Section 5.5).
6. **`ensure_bazel_build_files()` Helper in `scripts/lib/hermetic-go-env.sh` & External Contributor Fallback**:
   - Automatically invoked by `scripts/verify.sh`, `scripts/check-coverage.sh`, and CI before any `bazel` command:
     1. If `bazel` is not installed locally (and not running in CI), `scripts/verify.sh` and `scripts/check-coverage.sh` fall back transparently to native `go test` against the single root `go.mod` so external contributors never have to install Bazel to run verification checks.
     2. If `go.mod` is modified or newer than `MODULE.bazel.lock`, runs `bazel mod tidy` so `use_repo(go_deps, ...)` in `MODULE.bazel` updates automatically when an agent or contributor adds a new external Go dependency.
     3. Runs pre-baked `/usr/local/bin/gazelle` (~180ms) if present, or falls back to `bazel run //:gazelle` if a contributor only installed `bazelisk`.
     4. Appends deterministic `exports_files` / `filegroup` declarations (`visibility = ["//visibility:public"]`) to the 5 packages with cross-package static or test assets (`services/eri/pkg/db/BUILD.bazel` for `schema.sql` and `migrations/**`, `services/console/BUILD.bazel` for `static/**`, `services/docs-service/BUILD.bazel` for `nginx.conf` and `app/**`, `web/theme/BUILD.bazel` for `aerial-tokens.css` and `aerial-theme.js`, and `services/synapse/BUILD.bazel` for `testdata/**`).
7. **`.gitignore` Entries**:
   - Ignore all generated `BUILD.bazel` files (`**/BUILD.bazel`) while un-ignoring `!/BUILD.bazel`, `!/build/BUILD.bazel`, and `!/images/BUILD.bazel`.

### 5.5 `rules_oci` & Production Coverage Transition (`build/oci.bzl` & `images/BUILD.bazel`)
Every production Go service in `aerial` (except `redact-pattern` and `coverage-ingest` itself) serves `POST /debug/coverage/flush` so `coverage-ingest` can collect live production statement coverage via Go's `runtime/coverage` package.

To guarantee `rules_oci` binaries include Go's `-cover` instrumentation (supported in `rules_go` v0.65.0 via `#4372`) while preserving exact container paths and working directories:
1. **Coverage Configuration Transition (`_go_prod_cover_transition`)**:
   - In `build/oci.bzl`, define an incoming Starlark transition rule `go_cover_binary` wrapping `go_binary` with:
     - `//command_line_option:collect_code_coverage = True`
     - `//command_line_option:instrumentation_filter = "^//(pkg|services|mcp|images)[/:]"`
     - `//command_line_option:strip = "always"` (`-ldflags="-s -w" -trimpath`)
     - `@rules_go//go/config:cover_format = "go_cover"`
     - `@rules_go//go/config:pure = True` (`CGO_ENABLED=0`)
     - `@rules_go//go/config:tags = ["timetzdata"]` (embeds IANA timezone database directly in the binary as a defense-in-depth backup to `@distroless_static`'s `/usr/share/zoneinfo`).
2. **Bazel 9 Symbolic Macro (`aerial_go_image`) in `images/BUILD.bazel`**:
   - All 16 OCI image targets live in a single checked-in `images/BUILD.bazel` file, keeping per-service `services/*/BUILD.bazel` files 100% pure `gazelle` output.
   - Distroless `rules_oci` images intentionally drop legacy Dockerfile `HEALTHCHECK` (`wget`/`curl`) directives in favor of native Kubernetes HTTP `livenessProbe` and `readinessProbe` checks in `k8s/*.yaml`.
   - For each service `<name>`, `aerial_go_image` (as well as the `docs` and `migrator` static targets in `images/BUILD.bazel`) emits three sibling targets:
     - `//images:<name>` (`oci_image`): Deterministic OCI image directory.
     - `//images:load_<name>` (`oci_load` with `repo_tags = ["ghcr.io/azylman/aerial-" + name + ":local"]`): Loads the container image directly into a local Docker or Podman daemon via `bazel run //images:load_<name>` so external contributors can run any service container locally without per-service `Dockerfile`s.
     - `//images:push_<name>` (`oci_push` to `ghcr.io/azylman/aerial-<name>`): Daemonless registry push used by CI.
   - `aerial_go_image` accepts:
     - `name`: Service identifier (e.g., `"haro"`, `"console"`, `"redact-pattern"`, `"coverage-ingest"`).
     - `binary`: Label of the `go_binary` target (e.g., `"//services/haro:haro"`).
     - `bin_path`: Destination path inside the container (defaults to `"/app/" + name`; `"/app/aerial-console"` for `console`; `"/usr/local/bin/redact-pattern"` for `redact-pattern`).
     - `workdir`: Container working directory (defaults to `"/app"`, which ensures `services/console` resolves relative `"static/index.html"`).
     - `cover`: Boolean (defaults to `True`; set to `False` for `redact-pattern` and `coverage-ingest`).
     - `extra_tars`: Optional list of static asset tar layers:
       - `console`: `static/` + Vite `ui/dist/` + `web/theme/aerial-tokens.css` -> `/app/static`.
       - `coverage-ingest`: includes a Bazel-built static `cmd/covdata` binary at `/usr/local/bin/covdata` and a 2-line `/usr/local/bin/go` wrapper (`#!/bin/sh` / `shift; shift; exec /usr/local/bin/covdata "$@"`) plus `@alpine_base` `/bin/sh` so `ExecCovdata` (`go tool covdata func -i=...`) runs in `readOnlyRootFilesystem: true` without bundling a 300 MB Go SDK.

### 5.6 Image Build Matrix Across the Repositories

#### 1. `azylman/aerial` (20 Images Total + 6 Non-Go `sh_test` Targets)
- **All 20 Images Unified on `rules_oci` in Checked-In `images/BUILD.bazel` (Two-Phase `oci.pull` Base + `rules_oci` Application Layers):**
  - **Pure Go Services with `cover = True` (12):** `eri`, `haro`, `coven`, `docket-mcp`, `kernel-mcp`, `synapse`, `transcripts`, `vanadis`, `vanadis-restate`, `vanadis-nats`, `prospera`, plus `console` (Go binary + Vite UI dist tar + static/theme tars).
  - **Pure Go Services with `cover = False` (2):** `redact-pattern` (`bin_path = "/usr/local/bin/redact-pattern"`), `coverage-ingest` (`cover = False` + bundled `covdata` binary).
  - **Static / Migration Images (2):**
    - `docs` (`@nginx_base` + `services/docs-service` nginx config & Docsify app tar + `//web/theme` tar -> `/usr/share/nginx/html/assets`).
    - `migrator` (`@atlas_base` + `//services/eri/pkg/db:migrations_tar` -> `/migrations`).
  - **Two-Phase Base Images with `rules_oci` Application Layers (4):**
    - `eri-restate` (`@eri_restate_base` + Go covered binary + aux binaries layer `hook-guard`/`transcript-search` + scripts layer + rules layer + agents layer). Retained `services/eri-restate/Dockerfile` serves as base fallback on `Dockerfile` changes.
    - `github-runner` (`@github_runner_base` + `github_runner_bins_layer` `0755` + `github_runner_configs_layer` `0644`). Retained `services/github-runner/Dockerfile` serves as base fallback on `Dockerfile` changes.
    - `discord-mcp` (`@discord_mcp_base`). Retained `mcp/discord-mcp/Dockerfile` serves as base fallback on `Dockerfile` changes.
    - `infisical-mcp` (`@infisical_mcp_base` + `entrypoint.sh` `0755` + `standby.js` `0644`). Retained `mcp/infisical-mcp/Dockerfile` serves as base fallback on `Dockerfile` changes.
- **6 Non-Go `sh_test` Targets:** Repo-level test targets covering shell scripts, verification invariants, and schema validation.

#### 2. `azylman/aerial-config` (Validation Pipeline)
- Runs Bazel 9.3.3 + ephemeral `gazelle` for `//scripts/validate/...` (validating rules, channels, services, crons, and YAML configurations against cluster schemas).

#### 3. `azylman/aerial-sidecars` (11 Images Total + 3 Non-Go `sh_test` Targets)
- Add root `MODULE.bazel` (`rules_go` + `rules_python` + `rules_oci`), `.bazelrc` pointing to `bazel-remote.aerial:9092`, and ephemeral `gazelle`.
- **All 11 Images Unified on Two-Phase `rules_oci`:** All sidecars build via `rules_oci` with base `Dockerfile` fallback when base Dockerfile or heavy source files are modified.
- Unified under `scripts/bazel-select-images.sh` for change detection and image selection alongside 3 non-Go `sh_test` targets.

#### 4. `azylman/mirrormere` (Native Open-Source Preservation)
- Intentionally left on its native open-source `Dockerfile` and `go test` workflow per user directive, preserving frictionless upstream Docker and Go development without requiring Bazel.

### 5.7 Change Selection & `scripts/build-metadata.cjs` / `synapse` Continuity
To ensure `synapse` (`ResolveRunMetadata` and `RepoObject.HandleImagePromotion`) and `vanadis` continue tracking image builds and updating `ConfigMap/aerial-image-tags` in `flux-system` without modification:
1. **Digest Comparison Against Promoted `:latest` (`scripts/bazel-select-images.sh`):**
   - In the `changes` job, `bazel build //images:all` builds the 16 `oci_image` descriptors in **~1–2 seconds** from `bazel-remote`.
   - For each of the 16 `rules_oci` services, `scripts/bazel-select-images.sh` reads the built image's deterministic OCI manifest digest (`bazel-bin/images/<service>/index.json`) and compares it against the currently promoted `:latest` digest in GHCR (`crane digest ghcr.io/azylman/aerial-<service>:latest`).
   - Because PR builds push `:tree-<sha>` / `:bazel-<digest>` tags and **do not** mutate `:latest`, an image modified in a PR still differs from `:latest` when that PR merges to `main`! Thus, both the PR run and the `main` push run select the exact set of changed services into `MATRIX_JSON` (`[{"service": "...", "image_name": "..."}]`) and publish the `build-metadata` (`manifest`) check run via `node scripts/build-metadata.cjs manifest`.
   - For the 4 retained `Dockerfile` images (`eri-restate`, `github-runner`, `discord-mcp`, `infisical-mcp`), `scripts/bazel-select-images.sh` computes a deterministic Bazel target/file hash over their inputs and compares it against a `org.opencontainers.image.source.digest` annotation (or git diff against `origin/main` / `HEAD~1`).
2. **Pushing & Check-Run Reporting (`build-and-push` & `promote-release`):**
   - For each selected `rules_oci` service, `build-and-push` publishes `node scripts/build-metadata.cjs running`, runs `bazel run //images:push_<service>` (or `crane tag` when promoting a cached PR digest on `main`), and publishes `node scripts/build-metadata.cjs result` with `IMAGE_DIGEST`.
   - On `main`, `promote-release` publishes `node scripts/build-metadata.cjs final` and promotes all non-`eri` images to `:latest` via `crane tag` in parallel **before** promoting `aerial-eri-restate:latest` and `aerial-eri:latest` sequentially (preserving zero-RPC-skew rollout ordering).

---

## 6. Five-Phase Rollout Roadmap

Each phase is independently deployable, verifiable, and leaves the cluster and CI green before the next phase begins:

### Phase 1: LAN Remote Cache (`bazel-remote` on `quiet-zero`) & Runner Toolchains
1. **Cluster Manifest (`k8s/observability-and-mcp.yaml` or `k8s/bazel-remote.yaml` in `aerial`):**
   - Deploy `PersistentVolumeClaim/bazel-remote-data` (`55Gi`, `local-path`), `Deployment/bazel-remote` (`buchgr/bazel-remote:v2.6.1`, `--max_size=50`, pinned to `quiet-zero`), `Service/bazel-remote` (`:9090` HTTP, `:9092` gRPC), and `VMServiceScrape/bazel-remote`.
2. **Pre-Bake Bazel 9.3.3 Toolchain (`services/github-runner/Dockerfile` & `services/eri-restate/Dockerfile`):**
   - Bake `bazelisk` (`/usr/local/bin/bazel`), pre-warmed `/opt/bazel-cache` (`Bazel 9.3.3`), `gazelle`, `buildifier`, and `crane` into both images.
   - Verify `curl -fsS http://bazel-remote.aerial:9090/status` from inside `eri-restate` and CI runner pods.

### Phase 2: Collapse `aerial` into One Root `go.mod` & Harden `pkg/pgtest`
1. **Sandbox-Ready `pkg/pgtest`:**
   - Update `pkg/pgtest/pgtest.go` line 121 to use `os.MkdirTemp("/tmp", "pg")` (leaving `findSchema` untouched).
2. **Single Root `go.mod` Unification:**
   - Create root `go.mod` (`module github.com/azylman/aerial`) and `go.sum`, delete the 17 sub-module `go.mod`/`go.sum` files, collapse `.github/dependabot.yml` to a single `directory: "/"` `gomod` entry, scope `deadcode -filter` per service in `scripts/verify.sh`, and update `scripts/check-coverage.sh`, `scripts/code-health`, `scripts/affected`, and service `Dockerfile`s to use the root `go.mod`.

### Phase 3: Bazel 9.3.3 + Ephemeral `gazelle` for Tests (`aerial` & `mirrormere`)
1. **Root Bazel Configuration:**
   - Add `.bazelversion` (`9.3.3`), `.bazelrc`, `MODULE.bazel`, `MODULE.bazel.lock`, root `BUILD.bazel`, `build/go_test.bzl`, and `ensure_bazel_build_files()` in `scripts/lib/hermetic-go-env.sh`, while adding generated `BUILD.bazel` paths to `.gitignore`.
2. **Verification & Coverage Integration:**
   - Update `scripts/verify.sh` and `scripts/check-coverage.sh` to run `ensure_bazel_build_files` (~200ms) followed by `bazel coverage //...` (`--@rules_go//go/config:cover_format=go_cover`) and enforce the 95.0% per-package and global statement coverage floor.
3. **Apply Identical Setup to `mirrormere`:**
   - Add root `MODULE.bazel`, `.bazelrc`, `.bazelversion`, root `BUILD.bazel`, and `.gitignore` rules to `mirrormere` so `mirrormere` tests run via `gazelle && bazel test //...` against `bazel-remote`.

### Phase 4: `rules_oci` Container Builds & Retirement of `scripts/affected`
1. **`aerial` `rules_oci` Migration (`build/oci.bzl` & `images/BUILD.bazel`):**
   - Implement `aerial_go_image` with `_go_prod_cover_transition` in `build/oci.bzl` and declare all 16 OCI images in `images/BUILD.bazel`.
   - Verify via an automated test that a `go_cover_binary` target writes valid `covmeta` and `covcounters` archives on `POST /debug/coverage/flush` and that `coverage-ingest`'s bundled `covdata` binary parses them.
   - Update `.github/workflows/docker-publish.yml` to select changed images via `scripts/bazel-select-images.sh` (preserving `scripts/build-metadata.cjs` check runs and `eri-restate`/`eri` promotion ordering), delete the 16 replaced `Dockerfile`s, and delete `scripts/affected` + `catalog.json`.
2. **`mirrormere` Digest-Gated `Dockerfile` Builds:**
   - Keep all 3 `Dockerfile`s in `mirrormere`; update `mirrormere`'s workflow to gate `Dockerfile` builds on each target's transitive Bazel dependency digest against `:latest`.

### Phase 5: `aerial-sidecars` Bazel 9.3.3 & `rules_oci` Rollout
1. Add root `MODULE.bazel` (`rules_go` + `rules_python` + `rules_oci`), `.bazelrc`, `.bazelversion`, and `.gitignore` to `aerial-sidecars`.
2. Migrate the 9 Go and Python sidecars to `bazel test //...` and `rules_oci` while keeping `senses` and `pjev` on path-filtered `Dockerfile` builds.

---

## 7. What Is Left to the Builder

The following implementation details are intentionally left to the builder during execution of each phase:
- Exact Kubernetes resource requests/limits for `Deployment/bazel-remote` on `quiet-zero` (within standard node budgets: e.g., `250m`–`2000m` CPU, `1Gi`–`4Gi` RAM).
- Whether `services/console/ui` Vite bundling inside `images/BUILD.bazel` uses `aspect_rules_js` (`js_run_binary`) or a hermitized `npm ci && npm run build` genrule cached by Bazel over `services/console/ui/**` and `web/theme/**`.
- Exact Starlark attribute signature of `aerial_go_image` in `build/oci.bzl` (as long as it supports `cover = True/False`, custom `bin_path`/`workdir`, extra static tar layers, and non-root/root user selection).
