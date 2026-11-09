# syntax=docker/dockerfile:1
# --- Build stage --------------------------------------------------------
FROM golang:1.27.3-bookworm AS builder

WORKDIR /src

# Prune Go SDK test, doc, and api directories before source copy to keep /usr/local/go layer static
RUN rm -rf /usr/local/go/test /usr/local/go/doc /usr/local/go/api

COPY pkg/go.mod pkg/go.sum* ./pkg/
COPY services/eri/go.mod services/eri/go.sum* ./services/eri/
WORKDIR /src/services/eri
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY pkg/ /src/pkg/

# Exclude CLI helpers (transcript-search, hook-guard) from -cover to avoid per-hook counter file floods.
# Build CLI helpers before copying full services/eri tree so service edits do not invalidate their layers.
COPY services/eri/pkg/guard/ ./pkg/guard/
COPY services/eri/cmd/hook-guard/ ./cmd/hook-guard/
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /hook-guard ./cmd/hook-guard

COPY services/eri/pkg/config/ ./pkg/config/
COPY services/eri/pkg/metrics/ ./pkg/metrics/
COPY services/eri/pkg/session/ ./pkg/session/
COPY services/eri/pkg/db/ ./pkg/db/
COPY services/eri/pkg/transcript/ ./pkg/transcript/
COPY services/eri/cmd/transcript-search/ ./cmd/transcript-search/
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /transcript-search ./cmd/transcript-search

COPY services/eri/ ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /app/eri .

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

# --- Rust toolchain builder stage -----------------------------------------
FROM ubuntu:24.04 AS rust-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:${PATH}

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    --default-toolchain stable \
    --profile minimal \
    --component rustfmt,clippy && \
    rm -rf /usr/local/rustup/toolchains/*/share/doc && \
    chmod -R a+rX /usr/local/rustup /usr/local/cargo

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
    g++ \
    libc6-dev \
    libatomic1 \
    pkg-config \
    libespeak-ng-dev \
    && rm -rf /var/lib/apt/lists/*

# Playwright Chromium runtime libs + CI-matching fonts for the console parity suite (turns run without root, so they can't apt-get at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libasound2t64 \
    libatk-bridge2.0-0t64 \
    libatk1.0-0t64 \
    libatspi2.0-0t64 \
    libcairo2 \
    libcups2t64 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0t64 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    libfontconfig1 \
    libfreetype6 \
    fonts-liberation \
    fonts-noto-color-emoji \
    fonts-ipafont-gothic \
    fonts-wqy-zenhei \
    fonts-tlwg-loma-otf \
    fonts-freefont-ttf \
    xfonts-scalable \
    && rm -rf /var/lib/apt/lists/*

# Configure default SSH client with include for aerial-config
RUN mkdir -p -m 0700 /root/.ssh && \
    echo "Include /share/aerial-config/ssh/config" > /root/.ssh/config && \
    chmod 0600 /root/.ssh/config && \
    mkdir -p -m 0755 /etc/ssh/ssh_config.d && \
    echo "Include /share/aerial-config/ssh/config" > /etc/ssh/ssh_config.d/aerial.conf && \
    chmod 0644 /etc/ssh/ssh_config.d/aerial.conf

# Copy Go SDK, Rust toolchain, golangci-lint, and Node.js 22 LTS runtime from builders/images
COPY --link --from=builder /usr/local/go /usr/local/go
COPY --link --from=rust-builder /usr/local/rustup /usr/local/rustup
COPY --link --from=rust-builder /usr/local/cargo /usr/local/cargo
COPY --link --from=golangci/golangci-lint:v2.15.0 /usr/bin/golangci-lint /usr/local/bin/golangci-lint
COPY --link --from=node:22-bookworm-slim /usr/local/bin/node /usr/local/bin/node
COPY --link --from=node:22-bookworm-slim /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm && \
    ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx && \
    ln -s ../lib/node_modules/corepack/dist/corepack.js /usr/local/bin/corepack && \
    node --version && \
    npm --version && \
    npm install -g @anthropic-ai/claude-code && \
    claude --version
COPY --link --from=arigaio/atlas:1.3.4 /atlas /usr/local/bin/atlas
COPY --link --from=1password/op:2 /usr/local/bin/op /usr/local/bin/op
RUN curl -fsSL https://dl.k8s.io/release/v1.32.2/bin/linux/amd64/kubectl -o /usr/local/bin/kubectl && \
    echo "4f6a959dcc5b702135f8354cc7109b542a2933c46b808b248a214c1f69f817ea  /usr/local/bin/kubectl" | sha256sum -c - && \
    chmod 0755 /usr/local/bin/kubectl && \
    curl -fsSL https://get.helm.sh/helm-v3.17.1-linux-amd64.tar.gz -o /tmp/helm.tar.gz && \
    echo "3b66f3cd28409f29832b1b35b43d9922959a32d795003149707fea84cbcd4469  /tmp/helm.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/helm.tar.gz --strip-components=1 -C /usr/local/bin linux-amd64/helm && \
    rm -f /tmp/helm.tar.gz && \
    chmod 0755 /usr/local/bin/helm && \
    curl -fsSL https://github.com/fluxcd/flux2/releases/download/v2.5.1/flux_2.5.1_linux_amd64.tar.gz -o /tmp/flux.tar.gz && \
    echo "f64c85db4b94aefcdf6e0f2825c32573fc2bd234e5489ff332fee62776973ec3  /tmp/flux.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/flux.tar.gz -C /usr/local/bin flux && \
    rm -f /tmp/flux.tar.gz && \
    chmod 0755 /usr/local/bin/flux && \
    kubectl version --client && helm version --short && flux --version
ENV RUSTUP_HOME="/usr/local/rustup" \
    CARGO_HOME="/usr/local/cargo" \
    PATH="/usr/local/cargo/bin:/usr/local/go/bin:/root/go/bin:${PATH}"

# Install official Antigravity CLI binary
RUN curl -fsSL --compressed https://antigravity.google/cli/install.sh | bash -s -- -d /usr/local/bin

# Install Stripe Link CLI (@stripe/link-cli v0.29.0) standalone binary
RUN curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 https://github.com/stripe/link-cli/releases/download/%40stripe/link-cli%401.0.0/link-cli-linux-x64 -o /usr/local/bin/link-cli && \
    echo "40fa5fbade72cc9fde4014312fa5b7a3eeb6b7b151e0a97539991100f3e9943f  /usr/local/bin/link-cli" | sha256sum -c - && \
    chmod 0755 /usr/local/bin/link-cli
ENV LINK_AUTH_FILE="/data/runtimes/discord/.config/link-cli-nodejs/config.json"

# Copy decoupled superpowers skills
COPY --link --from=superpowers /opt/skills /opt/skills
COPY --link --from=superpowers /opt/superpowers /opt/superpowers

WORKDIR /app

RUN ln -s /app/eri /usr/local/bin/aerial-brain && \
    ln -s /app/eri /usr/local/bin/aerial-eri && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/share-guard && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/schedule-guard && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/batch-guard && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/search-guard && \
    ln -sf /usr/local/bin/hook-guard /usr/local/bin/commit-guard

COPY --link scripts/ /usr/local/bin/
RUN chmod +x /usr/local/bin/*.sh 2>/dev/null || true
COPY --link rules /app/rules
COPY --link .agents /app/.agents
COPY --link --from=builder /hook-guard /usr/local/bin/hook-guard
COPY --link --from=builder /transcript-search /usr/local/bin/transcript-search
COPY --link --from=builder /app/eri /app/eri

ENV GIT_TERMINAL_PROMPT=0

EXPOSE 8080

ENTRYPOINT ["/app/eri"]
