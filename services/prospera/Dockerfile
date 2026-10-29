# syntax=docker/dockerfile:1
FROM golang:1.27.3-alpine AS builder

WORKDIR /src
COPY pkg/go.mod pkg/go.sum* ./pkg/
COPY services/prospera/go.mod services/prospera/go.sum* ./services/prospera/
WORKDIR /src/services/prospera
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY pkg/ /src/pkg/
COPY services/prospera/ ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /app/prospera .

FROM alpine:3.24

LABEL org.opencontainers.image.title="aerial-prospera"

RUN apk add --no-cache tzdata

WORKDIR /app
COPY --from=builder --link /app/prospera /app/prospera

ENV PORT=4019
EXPOSE 4019

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --start-interval=2s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:4019/health || exit 1

ENTRYPOINT ["/app/prospera"]
