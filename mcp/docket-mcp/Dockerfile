# syntax=docker/dockerfile:1
FROM golang:1.27.3-alpine AS builder

WORKDIR /src
COPY pkg/go.mod pkg/go.sum* ./pkg/
COPY mcp/docket-mcp/go.mod mcp/docket-mcp/go.sum* ./mcp/docket-mcp/
WORKDIR /src/mcp/docket-mcp
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY pkg/ /src/pkg/
COPY mcp/docket-mcp/ ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /app/docket-mcp .

FROM alpine:3.24

RUN apk add --no-cache tzdata ca-certificates

WORKDIR /app
COPY --from=builder --link /app/docket-mcp /app/docket-mcp

ENV PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --start-interval=2s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["/app/docket-mcp"]
