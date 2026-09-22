# Implementation Plan: Decouple Ollama num_ctx into Client Request & Eliminate Custom Container Image

## Context & Motivation
Currently, Aerial builds and maintains a custom container image `ghcr.io/azylman/aerial-ollama:latest` built from `docker/ollama/Dockerfile`. This Dockerfile:
1. Installs `curl` solely for container healthchecks and local polling.
2. Starts a background Ollama daemon to pull `all-minilm`.
3. Generates a custom Modelfile (`PARAMETER num_ctx 512`) and runs `ollama create all-minilm` to increase context length from 256 to 512.

Because `docker/ollama/Dockerfile` relies on floating `FROM ollama/ollama:latest`, any edit in `docker/ollama/` triggers CI to pull the latest upstream release from Docker Hub, generating a 5.5GB image rebuild and invalidating host layer caches. Furthermore, Ollama's `/api/embeddings` endpoint natively supports request-level `options: {"num_ctx": 512}`, making image-level Modelfile baking completely redundant.

## Objectives
1. **Request-Level Options**: Add `NumCtx int` to `OllamaConfig` and pass `options: {"num_ctx": 512}` in `brain/pkg/memory/ollama.go` on every embedding request.
2. **Eliminate Custom Image**: Delete `docker/ollama/` completely from the repository.
3. **Use Upstream Image**: Update `docker-compose.yml` to use `image: ollama/ollama:latest` with native `/bin/ollama list` healthcheck and automated startup bootstrap for `all-minilm`.
4. **CI Pruning**: Remove `ollama` from `.github/workflows/docker-publish.yml` paths filter and build catalog.
5. **Unit Tests**: Update memory tests to verify `options: {"num_ctx": 512}` is serialized in requests.

## Component Diffs
### 1. `brain/pkg/config/config.go`
- Add `NumCtx int` to `OllamaConfig` with yaml/json tags.
- In `DefaultConfigData()`, set `NumCtx: 512`.

### 2. `brain/pkg/memory/ollama.go`
- Add `Options map[string]any` to `EmbeddingRequest`.
- In `GenerateEmbedding`, populate `Options: map[string]any{"num_ctx": numCtx}` (falling back to 512 if <= 0).

### 3. `brain/pkg/memory/memory_test.go`
- In `TestMockOllamaClient`, assert `r.Options["num_ctx"] == float64(512)`.
- Add test verifying custom `NumCtx` configuration propagates to request options.

### 4. `docker-compose.yml`
- Remove `build: ./docker/ollama`.
- Set `image: ollama/ollama:latest`.
- Update healthcheck to `test: ["CMD", "/bin/ollama", "list"]`.
- Add `entrypoint` to start server, pull `all-minilm` if missing, and wait for process.

### 5. `.github/workflows/docker-publish.yml`
- Remove `ollama` paths-filter and build catalog entry.

### 6. `docker/ollama/`
- Delete directory and Dockerfile.

## Verification Gates
1. `(cd brain && go test -v ./pkg/memory/...)`
2. `(cd brain && go test -v ./pkg/config/...)`
3. `docker compose -f docker-compose.yml config --quiet`
4. `./scripts/verify.sh --staged`
