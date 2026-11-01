# syntax=docker/dockerfile:1
FROM golang:1.27.3-bookworm AS go-builder

RUN mkdir -p /out/bin && \
    GOBIN=/out/bin CGO_ENABLED=0 go install golang.org/x/tools/cmd/deadcode@v0.52.0 && \
    GOBIN=/out/bin CGO_ENABLED=0 go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.4.1 && \
    rm -rf /usr/local/go/test /usr/local/go/doc /usr/local/go/api

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

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    GIT_TERMINAL_PROMPT=0 \
    RUNNER_ALLOW_RUNASROOT="1"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    wget \
    bash \
    git \
    jq \
    ripgrep \
    tar \
    gzip \
    tzdata \
    libicu74 \
    libkrb5-3 \
    zlib1g \
    libssl3t64 \
    gcc \
    g++ \
    libc6-dev \
    libatomic1 \
    pkg-config \
    libespeak-ng-dev \
    make \
    procps \
    python3 \
    python3-venv \
    python3-pip \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

# Install GitHub CLI (gh)
RUN mkdir -p -m 755 /etc/apt/keyrings && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg -o /etc/apt/keyrings/githubcli-archive-keyring.gpg && \
    chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list && \
    apt-get update && apt-get install -y --no-install-recommends gh && \
    rm -rf /var/lib/apt/lists/*

# Install Google Chrome Stable
RUN mkdir -p -m 755 /etc/apt/keyrings && \
    curl -fsSL https://dl.google.com/linux/linux_signing_key.pub -o /etc/apt/keyrings/google-chrome.asc && \
    chmod go+r /etc/apt/keyrings/google-chrome.asc && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/google-chrome.asc] https://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && apt-get install -y --no-install-recommends google-chrome-stable && \
    rm -rf /var/lib/apt/lists/* && \
    google-chrome --version

# Install official Antigravity CLI (agy)
RUN curl -fsSL --compressed https://antigravity.google/cli/install.sh | bash -s -- -d /usr/local/bin

# Install kenn-io/roborev v0.74.0 with SHA256 verification
ARG ROBOREV_VERSION=0.74.0
ARG ROBOREV_SHA256=9f8b72b7bb42b1572b11b6fa52f59255a6862ce714d945b64e776f24c2192a87
RUN curl -fsSL "https://github.com/kenn-io/roborev/releases/download/v${ROBOREV_VERSION}/roborev_${ROBOREV_VERSION}_linux_amd64.tar.gz" -o /tmp/roborev.tar.gz && \
    echo "${ROBOREV_SHA256}  /tmp/roborev.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/roborev.tar.gz -C /usr/local/bin roborev && \
    chmod 0755 /usr/local/bin/roborev && \
    rm -f /tmp/roborev.tar.gz

# Pre-baked toolchain: Go 1.27.3, Rust stable, deadcode, oapi-codegen, golangci-lint, Node 22, Docker CLI + plugins
COPY --from=go-builder /usr/local/go /usr/local/go
COPY --from=go-builder /out/bin/* /usr/local/bin/
COPY --from=rust-builder /usr/local/rustup /usr/local/rustup
COPY --from=rust-builder /usr/local/cargo /usr/local/cargo
COPY --from=golangci/golangci-lint:v2.15.0 /usr/bin/golangci-lint /usr/local/bin/golangci-lint
COPY --from=node:22-bookworm-slim /usr/local/bin/node /usr/local/bin/node
COPY --from=docker:27-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker:27-cli /usr/local/libexec/docker/cli-plugins/ /usr/local/libexec/docker/cli-plugins/

# Pre-baked Python virtual environment
RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir \
        "pytest>=8.3.0" \
        "pytest-asyncio>=0.24.0" \
        "ruff>=0.6.0" \
        "pyyaml>=6.0.2" \
        "httpx>=0.27.2" \
        "fastapi>=0.115.0" \
        "uvicorn[standard]>=0.30.6" \
        "pydantic>=2.6.0" \
        "aiohttp>=3.9.0" \
        "numpy>=2.1.0" \
        "scipy>=1.14.0" \
        "soundfile>=0.12.1" \
        "prometheus-client>=0.20.0" \
        "git+https://github.com/brockventures/banana-protocol.git@v0.6.1"

ENV RUSTUP_HOME="/usr/local/rustup" \
    CARGO_HOME="/usr/local/cargo" \
    PATH="/usr/local/cargo/bin:/opt/venv/bin:/usr/local/go/bin:/root/go/bin:${PATH}"

# Install GitHub Actions Runner v2.338.2 with SHA256 verification
ARG RUNNER_VERSION=2.338.2
ARG RUNNER_SHA256=baebf204307bac6193480852732f0aba2ae97b6c5597c244f4d5558390147c40
WORKDIR /actions-runner
RUN curl -fsSL "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz" -o /tmp/runner.tar.gz && \
    echo "${RUNNER_SHA256}  /tmp/runner.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/runner.tar.gz -C /actions-runner && \
    rm -f /tmp/runner.tar.gz

COPY services/github-runner/entrypoint.sh /entrypoint.sh
RUN chmod 0755 /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
