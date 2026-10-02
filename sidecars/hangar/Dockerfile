# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS builder

WORKDIR /build
COPY sidecars/hangar/go.mod sidecars/hangar/go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY sidecars/hangar/ ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /hangar .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates git bash tzdata docker-cli && \
    mkdir -p /mnt/data/supervisor/share && \
    ln -s /share/aerial /mnt/data/supervisor/share/aerial && \
    ln -s /share/aerial-config /mnt/data/supervisor/share/aerial-config

COPY --link --from=docker/compose-bin:v2.32.4 /docker-compose /usr/local/lib/docker/cli-plugins/docker-compose
RUN ln -s /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose

COPY --link --from=hashicorp/nomad:1.8.4 /bin/nomad /usr/local/bin/nomad
COPY --link --from=hashicorp/nomad:1.8.4 /lib/ld-linux-x86-64.so.2 /lib/libc.so.6 /lib/libresolv.so.2 /lib/libpthread.so.0 /lib/libm.so.6 /lib/
RUN mkdir -p /lib64 && ln -sf /lib/ld-linux-x86-64.so.2 /lib64/ld-linux-x86-64.so.2

COPY --link --from=builder /hangar /usr/local/bin/hangar
RUN ln -s /usr/local/bin/hangar /usr/local/bin/gitsync

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --start-interval=2s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:8080/health || exit 1

CMD ["/usr/local/bin/hangar"]
