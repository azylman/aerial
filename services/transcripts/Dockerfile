# syntax=docker/dockerfile:1
FROM golang:1.27.3-alpine AS builder

WORKDIR /src
COPY pkg/go.mod pkg/go.sum* ./pkg/
COPY services/transcripts/go.mod services/transcripts/go.sum* ./services/transcripts/
WORKDIR /src/services/transcripts
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY pkg/ /src/pkg/
COPY services/transcripts/ ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /app/transcripts .

FROM alpine:3.24

LABEL org.opencontainers.image.title="aerial-transcripts"

RUN apk add --no-cache tzdata

WORKDIR /app
COPY --from=builder --link /app/transcripts /app/transcripts

ENV PORT=4030
EXPOSE 4030

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --start-interval=2s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:4030/health || exit 1

ENTRYPOINT ["/app/transcripts"]
