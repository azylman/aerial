# syntax=docker/dockerfile:1
# Always-on production coverage collector (a Deployment): pulls flush targets on an
# interval and accepts pushed archives on :8080. The runtime image keeps the Go toolchain
# because ingest shells out to `go tool covdata func`. The collector itself is
# deliberately NOT built with -cover.
FROM golang:1.27.3-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

WORKDIR /src/services/coverage-ingest
COPY pkg/ /src/pkg/
COPY services/coverage-ingest/ ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/coverage-ingest .

FROM golang:1.27.3-alpine

# Go distributions no longer ship a prebuilt covdata; install it into GOTOOLDIR so
# `go tool covdata` runs it directly instead of compiling it on every ingest.
RUN CGO_ENABLED=0 go install -trimpath cmd/covdata && \
    test -x "$(go env GOTOOLDIR)/covdata" && \
    go clean -cache && rm -rf /root/.cache && \
    apk add --no-cache ca-certificates tzdata && \
    rm -rf /var/cache/apk/* && \
    addgroup -g 10001 -S appgroup && \
    adduser -u 10001 -S appuser -G appgroup

COPY --link --from=builder /bin/coverage-ingest /bin/coverage-ingest

ENV GOCACHE=/tmp/go-build GOTOOLCHAIN=local
USER 10001:10001
EXPOSE 8080

ENTRYPOINT ["/bin/coverage-ingest"]
