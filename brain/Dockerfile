# syntax=docker/dockerfile:1
# --- Build stage --------------------------------------------------------
FROM golang:1.24-bookworm AS builder

WORKDIR /build

COPY brain/go.mod brain/go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY brain/ ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /aerial-brain .

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /transcript-search ./cmd/transcript-search

# Prune Go SDK test, doc, and api directories before exporting to runtime stage
RUN rm -rf /usr/local/go/test /usr/local/go/doc /usr/local/go/api

# --- Superpowers stage ---------------------------------------------------
FROM alpine:3.20 AS superpowers

RUN mkdir -p /tmp/superpowers /opt/skills /opt/superpowers && \
    wget -qO- https://github.com/obra/superpowers/archive/refs/tags/v6.3.0.tar.gz | tar -xz --strip-components=1 -C /tmp/superpowers && \
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
    tar \
    gzip \
    tzdata \
    python3 \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Copy statically linked Docker CLI and Compose plugin (avoids heavy docker.io/containerd apt packages)
COPY --link --from=docker:cli /usr/local/bin/docker /usr/local/bin/docker
COPY --link --from=docker/compose-bin:v2.32.4 /docker-compose /usr/local/lib/docker/cli-plugins/docker-compose
RUN ln -s /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose

# Configure default SSH client with include for aerial-config
RUN mkdir -p -m 0700 /root/.ssh && \
    echo "Include /share/aerial-config/ssh/config" > /root/.ssh/config && \
    chmod 0600 /root/.ssh/config

# Copy Go SDK, golangci-lint, and Node.js 20 LTS runtime from builders/images
COPY --link --from=builder /usr/local/go /usr/local/go
COPY --link --from=golangci/golangci-lint:v1.64.5 /usr/bin/golangci-lint /usr/local/bin/golangci-lint
COPY --link --from=node:20-bookworm-slim /usr/local/bin/node /usr/local/bin/node
ENV PATH="/usr/local/go/bin:/root/go/bin:${PATH}"

# Install official Antigravity CLI binary
RUN curl -fsSL https://antigravity.google/cli/install.sh | bash -s -- -d /usr/local/bin

# Copy decoupled superpowers skills
COPY --link --from=superpowers /opt/skills /opt/skills
COPY --link --from=superpowers /opt/superpowers /opt/superpowers

WORKDIR /app

COPY --link --from=builder /aerial-brain /usr/local/bin/aerial-brain
COPY --link --from=builder /transcript-search /usr/local/bin/transcript-search
COPY --link scripts/ /usr/local/bin/
RUN chmod +x /usr/local/bin/*.sh || true
COPY --link rules /app/rules
COPY --link .agents /app/.agents

ENV GIT_TERMINAL_PROMPT=0

EXPOSE 8080

CMD ["/usr/local/bin/aerial-brain"]
