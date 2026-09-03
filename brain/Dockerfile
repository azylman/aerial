# --- Build stage --------------------------------------------------------
FROM golang:1.22-bookworm AS builder

WORKDIR /build

COPY brain/go.mod brain/go.sum* ./
RUN go mod download

COPY brain/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /aerial-brain .

# Install golangci-lint in builder stage for runtime export
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b /usr/local/bin v1.59.1

# --- Runtime stage -------------------------------------------------------
FROM ubuntu:24.04

# Install base utilities, build tools, certificates, Docker CLI, and CLI prerequisites
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    bash \
    git \
    make \
    jq \
    tar \
    gzip \
    tzdata \
    docker.io \
    docker-compose-v2 \
    python3 \
    python3-yaml \
    && rm -rf /var/lib/apt/lists/*

# Copy Go SDK and golangci-lint from builder to provide local compiler and linting parity
COPY --from=builder /usr/local/go /usr/local/go
COPY --from=builder /usr/local/bin/golangci-lint /usr/local/bin/golangci-lint
ENV PATH="/usr/local/go/bin:/root/go/bin:${PATH}"

# Install official Antigravity CLI binary
RUN curl -fsSL https://antigravity.google/cli/install.sh | bash -s -- -d /usr/local/bin

WORKDIR /app

COPY --from=builder /aerial-brain /usr/local/bin/aerial-brain
COPY scripts/ /usr/local/bin/
RUN chmod +x /usr/local/bin/*.sh || true
COPY GEMINI.md /app/GEMINI.md
COPY .agents /app/.agents

ENV GIT_TERMINAL_PROMPT=0

EXPOSE 8080

CMD ["/usr/local/bin/aerial-brain"]

