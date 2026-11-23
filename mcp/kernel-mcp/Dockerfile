# syntax=docker/dockerfile:1
FROM golang:1.27.3-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

WORKDIR /src/mcp/kernel-mcp
COPY pkg/ /src/pkg/
COPY mcp/kernel-mcp/ ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /app/kernel-mcp .

FROM alpine:3.24

RUN apk add --no-cache tzdata ca-certificates

WORKDIR /app
COPY --from=builder --link /app/kernel-mcp /app/kernel-mcp

ENV PORT=4018
EXPOSE 4018

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --start-interval=2s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:4018/health || exit 1

ENTRYPOINT ["/app/kernel-mcp"]
