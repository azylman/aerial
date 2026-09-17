FROM golang:1.24-alpine AS builder

WORKDIR /build
COPY sidecars/hangar/go.mod sidecars/hangar/go.sum* ./
RUN go mod download

COPY sidecars/hangar/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /hangar .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates git bash curl tzdata docker-cli && \
    mkdir -p /mnt/data/supervisor/share && \
    ln -s /share/aerial /mnt/data/supervisor/share/aerial && \
    ln -s /share/aerial-config /mnt/data/supervisor/share/aerial-config

COPY --from=docker/compose-bin:v2.32.4 /docker-compose /usr/local/lib/docker/cli-plugins/docker-compose
RUN ln -s /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose

COPY --from=builder /hangar /usr/local/bin/hangar
RUN ln -s /usr/local/bin/hangar /usr/local/bin/gitsync

EXPOSE 8080

CMD ["/usr/local/bin/hangar"]
