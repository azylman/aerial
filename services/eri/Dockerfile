# syntax=docker/dockerfile:1
# --- Build stage --------------------------------------------------------
FROM golang:1.27.3-bookworm AS builder

WORKDIR /src

COPY pkg/go.mod pkg/go.sum* ./pkg/
COPY services/eri/go.mod services/eri/go.sum* ./services/eri/
WORKDIR /src/services/eri
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY pkg/ /src/pkg/
COPY services/eri/ ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /app/eri .

# Exclude CLI helpers (transcript-search, hook-guard) from -cover to avoid per-hook counter file floods.
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /transcript-search ./cmd/transcript-search

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /hook-guard ./cmd/hook-guard

# Prune Go SDK test, doc, and api directories before exporting to runtime stage
RUN rm -rf /usr/local/go/test /usr/local/go/doc /usr/local/go/api

# --- Superpowers stage ---------------------------------------------------
FROM alpine:3.24 AS superpowers

RUN mkdir -p /tmp/superpowers /opt/skills /opt/superpowers && \
    wget -qO- https://github.com/obra/superpowers/archive/refs/tags/v7.0.1.tar.gz | tar -xz --strip-components=1 -C /tmp/superpowers && \
    for skill in \
        systematic-debugging \
        test-driven-development \
        verification-before-completion \
        brainstorming \
        writing-plans \
        dispatching-parallel-agents \
        receiving-code-review \
        writing-skills; do \
        cp -a "/tmp/superpowers/skills/$skill" /opt/skills/; \
    done && \
    rm -rf /tmp/superpowers && \
    ln -s /opt/skills /opt/superpowers/skills

# --- Runtime stage -------------------------------------------------------
FROM ubuntu:24.04

# Install base utilities, build tools, certificates, Python, and CLI prerequisites
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    wget \
    bash \
    git \
    make \
    jq \
    ripgrep \
    tar \
    gzip \
    tzdata \
    python3 \
    openssh-client \
    postgresql-client \
    gcc \
    libc6-dev \
    libatomic1 \
    && rm -rf /var/lib/apt/lists/*

# Configure default SSH client with include for aerial-config
RUN mkdir -p -m 0700 /root/.ssh && \
    echo "Include /share/aerial-config/ssh/config" > /root/.ssh/config && \
    chmod 0600 /root/.ssh/config

# Copy Go SDK, golangci-lint, and Node.js 22 LTS runtime from builders/images
COPY --link --from=builder /usr/local/go /usr/local/go
COPY --link --from=golangci/golangci-lint:v2.15.0 /usr/bin/golangci-lint /usr/local/bin/golangci-lint
COPY --link --from=node:22-bookworm-slim /usr/local/bin/node /usr/local/bin/node
COPY --link --from=arigaio/atlas:1.3.4 /atlas /usr/local/bin/atlas
COPY --link --from=1password/op:2 /usr/local/bin/op /usr/local/bin/op
ENV PATH="/usr/local/go/bin:/root/go/bin:${PATH}"

# Install official Antigravity CLI binary
RUN curl -fsSL --compressed https://antigravity.google/cli/install.sh | bash -s -- -d /usr/local/bin

# Install Stripe Link CLI (@stripe/link-cli v0.29.0) standalone binary
RUN curl -fsSL https://github.com/stripe/link-cli/releases/download/%40stripe/link-cli%401.0.0/link-cli-linux-x64 -o /usr/local/bin/link-cli && \
    echo "40fa5fbade72cc9fde4014312fa5b7a3eeb6b7b151e0a97539991100f3e9943f  /usr/local/bin/link-cli" | sha256sum -c - && \
    chmod 0755 /usr/local/bin/link-cli
ENV LINK_AUTH_FILE="/data/runtimes/discord/.config/link-cli-nodejs/config.json"

# Copy decoupled superpowers skills
COPY --link --from=superpowers /opt/skills /opt/skills
COPY --link --from=superpowers /opt/superpowers /opt/superpowers

WORKDIR /app

COPY --link --from=builder /app/eri /app/eri
RUN ln -s /app/eri /usr/local/bin/aerial-brain && \
    ln -s /app/eri /usr/local/bin/aerial-eri
COPY --link --from=builder /transcript-search /usr/local/bin/transcript-search
COPY --link --from=builder /hook-guard /usr/local/bin/hook-guard
RUN ln -s /usr/local/bin/hook-guard /usr/local/bin/share-guard && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/schedule-guard && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/batch-guard && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/search-guard
COPY --link scripts/ /usr/local/bin/
RUN chmod +x /usr/local/bin/*.sh 2>/dev/null || true
COPY --link rules /app/rules
COPY --link .agents /app/.agents

ENV GIT_TERMINAL_PROMPT=0

EXPOSE 8080

ENTRYPOINT ["/app/eri"]
