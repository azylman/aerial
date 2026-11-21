# syntax=docker/dockerfile:1
# Standalone Eri HTTP ingress and API server image.
FROM golang:1.27.3-alpine AS builder

WORKDIR /src
COPY pkg/go.mod pkg/go.sum* ./pkg/
COPY services/eri/go.mod services/eri/go.sum* ./services/eri/
WORKDIR /src/services/eri
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY pkg/ /src/pkg/
COPY services/eri/ ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /app/eri .
# redact-pattern runs once per OTel Collector pod start (initContainer); built without -cover.
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /app/redact-pattern ./cmd/redact-pattern

FROM alpine:3.24

RUN apk add --no-cache ca-certificates curl tzdata && \
    rm -rf /var/cache/apk/* && \
    addgroup -g 10001 -S appgroup && \
    adduser -u 10001 -S appuser -G appgroup

COPY --link --from=builder /app/eri /app/eri
COPY --link --from=builder /app/redact-pattern /usr/local/bin/redact-pattern

USER 10001:10001

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --start-interval=2s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/app/eri"]
