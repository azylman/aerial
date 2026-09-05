FROM golang:1.24-alpine AS builder

WORKDIR /build
COPY sidecars/gitsync/go.mod sidecars/gitsync/go.sum* ./
RUN go mod download

COPY sidecars/gitsync/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /gitsync .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates git bash curl tzdata docker-cli && \
    mkdir -p /mnt/data/supervisor/share && \
    ln -s /share/aerial /mnt/data/supervisor/share/aerial && \
    ln -s /share/aerial-config /mnt/data/supervisor/share/aerial-config

COPY --from=docker/compose-bin:v2.32.4 /docker-compose /usr/local/lib/docker/cli-plugins/docker-compose
RUN ln -s /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose

COPY --from=builder /gitsync /usr/local/bin/gitsync

EXPOSE 8080

CMD ["/usr/local/bin/gitsync"]
