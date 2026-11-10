# syntax=docker/dockerfile:1
# React console (/console/): built here so the final image carries only static files, no node.
FROM node:26.13.0-alpine3.24 AS ui
WORKDIR /src/services/console/ui
COPY services/console/ui/package.json services/console/ui/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY services/console/ui/ ./
RUN npm run build

FROM golang:1.27.3-alpine AS builder
WORKDIR /src
COPY pkg/go.mod pkg/go.sum* ./pkg/
COPY services/console/go.mod services/console/go.sum* ./services/console/
WORKDIR /src/services/console
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY pkg/ /src/pkg/
COPY services/console/ ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /app/aerial-console .

FROM alpine:3.24
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder --link /app/aerial-console .
COPY services/console/static /app/static
COPY --from=ui --link /src/services/console/ui/dist /app/static
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --start-interval=2s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:8080/health || exit 1
CMD ["./aerial-console"]
