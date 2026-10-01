# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS builder

WORKDIR /src

COPY go.mod go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /app/scheduler-mcp .

FROM alpine:3.20

RUN apk add --no-cache tzdata

WORKDIR /app
COPY --from=builder --link /app/scheduler-mcp /app/scheduler-mcp

ENV PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:8080/health || exit 1

CMD ["/app/scheduler-mcp"]
