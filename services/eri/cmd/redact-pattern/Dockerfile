# syntax=docker/dockerfile:1
# redact-pattern: OTel Collector initContainer that renders the trace-redaction
# statements from shared-secrets. Its own image so eri releases never roll the collector.
# Only this package and pkg/ are copied; the build fetches just the modules it imports.
# Static, CGO off, no -cover (it runs once per collector pod start).
FROM golang:1.27.3-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY pkg/ ./pkg/
COPY services/eri/cmd/redact-pattern/ ./services/eri/cmd/redact-pattern/
WORKDIR /src/services/eri
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/redact-pattern ./cmd/redact-pattern

FROM scratch

COPY --link --from=builder /out/redact-pattern /usr/local/bin/redact-pattern

USER 10001:10001

ENTRYPOINT ["/usr/local/bin/redact-pattern"]
