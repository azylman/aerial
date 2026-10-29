# syntax=docker/dockerfile:1
FROM golang:1.27.3-alpine AS builder

WORKDIR /src
COPY pkg/go.mod pkg/go.sum* ./pkg/
COPY services/webhooks-router/go.mod services/webhooks-router/go.sum* ./services/webhooks-router/
WORKDIR /src/services/webhooks-router
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY pkg/ /src/pkg/
COPY services/webhooks-router/ ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /bin/webhooks-router .

FROM alpine:3.24

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
