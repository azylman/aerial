# syntax=docker/dockerfile:1
FROM golang:1.27.1-alpine AS builder

WORKDIR /src

COPY go.mod go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /app/memory-mcp .

FROM alpine:3.24

RUN apk add --no-cache tzdata

WORKDIR /app
COPY --from=builder --link /app/memory-mcp /app/memory-mcp

ENV PORT=4008
EXPOSE 4008

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --start-interval=2s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:4008/health || exit 1

CMD ["/app/memory-mcp"]
