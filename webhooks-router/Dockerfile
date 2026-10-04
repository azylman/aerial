# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build -cover -trimpath -ldflags="-s -w" -o /bin/webhooks-router .

FROM alpine:3.21

RUN apk add --no-cache ca-certificates curl tzdata && \
    rm -rf /var/cache/apk/* && \
    addgroup -g 10001 -S appgroup && \
    adduser -u 10001 -S appuser -G appgroup

COPY --link --from=builder /bin/webhooks-router /bin/webhooks-router

USER 10001:10001

EXPOSE 4020

HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --start-interval=2s --retries=3 \
    CMD curl -fsS http://127.0.0.1:4020/healthz || exit 1

ENTRYPOINT ["/bin/webhooks-router"]
