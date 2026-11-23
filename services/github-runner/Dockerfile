# syntax=docker/dockerfile:1
FROM golang:1.27.4-bookworm AS go-builder

RUN mkdir -p /out/bin && \
    GOBIN=/out/bin CGO_ENABLED=0 go install golang.org/x/tools/cmd/deadcode@v0.52.0 && \
    GOBIN=/out/bin CGO_ENABLED=0 go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.4.1 && \
    git clone https://github.com/azylman/roborev.git /tmp/roborev && \
    (cd /tmp/roborev && git checkout dd816f9c3f87489334c45a165ac569842594f9ba && GOBIN=/out/bin CGO_ENABLED=0 go install ./cmd/roborev) && \
    mv /out/bin/roborev /out/bin/roborev.real && \
    rm -rf /tmp/roborev && \
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

FROM node:22-bookworm-slim AS npm-tools

ENV NPM_CONFIG_PREFIX=/opt/npm-global

RUN npm install -g @anthropic-ai/claude-code && npm cache clean --force

FROM ubuntu:24.04 AS cli-tools

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

ARG MUSE_VERSION=1.4.23-R8977.1
ARG MUSE_SHA256=7ca9e16659f759eab3ae6c4b61c6f1e70f5f3fea1ca961ebd49044a1146956f4
ARG RUNNER_VERSION=2.338.2
ARG RUNNER_SHA256=baebf204307bac6193480852732f0aba2ae97b6c5597c244f4d5558390147c40

RUN mkdir -p /out/usr/local/bin /out/actions-runner && \
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 "https://lookaside.facebook.com/lookaside/muse/download/?channel=muse&version=${MUSE_VERSION}&file=muse-x86-linux" -o /out/usr/local/bin/muse && \
    echo "${MUSE_SHA256}  /out/usr/local/bin/muse" | sha256sum -c - && \
    chmod 0755 /out/usr/local/bin/muse && \
    curl -fsSL "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz" -o /tmp/runner.tar.gz && \
    echo "${RUNNER_SHA256}  /tmp/runner.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/runner.tar.gz -C /out/actions-runner && \
    rm -f /tmp/runner.tar.gz

FROM ubuntu:24.04 AS python-venv

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir \
        "pip" \
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

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    GIT_TERMINAL_PROMPT=0

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
    postgresql-client \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

# Install GitHub CLI (gh)
RUN mkdir -p -m 755 /etc/apt/keyrings && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg -o /etc/apt/keyrings/githubcli-archive-keyring.gpg && \
    chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list && \
    apt-get update && apt-get install -y --no-install-recommends gh && \
    rm -rf /var/lib/apt/lists/*

# Postgres 19 server + pgvector for hermetic Go store tests (pkg/pgtest finds /usr/lib/postgresql/19/bin).
# createcluster.d stops the Debian postinst from creating a main cluster, so the image ships no data dir or service.
# The hold keeps the existing postgresql-client metapackage on the distro client instead of PGDG's newest major.
RUN apt-mark hold postgresql-client && \
    mkdir -p -m 755 /etc/apt/keyrings /etc/postgresql-common/createcluster.d && \
    echo "create_main_cluster = false" > /etc/postgresql-common/createcluster.d/00-no-main-cluster.conf && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /etc/apt/keyrings/pgdg.asc && \
    chmod go+r /etc/apt/keyrings/pgdg.asc && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/pgdg.asc] https://apt.postgresql.org/pub/repos/apt noble-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && apt-get install -y --no-install-recommends postgresql-19 postgresql-19-pgvector && \
    apt-mark unhold postgresql-client && \
    rm -rf /var/lib/apt/lists/* /var/lib/postgresql/19 /etc/postgresql/19 && \
    /usr/lib/postgresql/19/bin/initdb --version && \
    psql --version

# Install Google Chrome Stable
RUN mkdir -p -m 755 /etc/apt/keyrings && \
    curl -fsSL https://dl.google.com/linux/linux_signing_key.pub -o /etc/apt/keyrings/google-chrome.asc && \
    chmod go+r /etc/apt/keyrings/google-chrome.asc && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/google-chrome.asc] https://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && apt-get install -y --no-install-recommends google-chrome-stable && \
    rm -rf /var/lib/apt/lists/* && \
    google-chrome --version

# Pre-baked toolchain: Go 1.27.4, Rust stable, deadcode, oapi-codegen, roborev, golangci-lint, Node 22, Docker CLI + plugins
COPY --link --from=go-builder /usr/local/go /usr/local/go
COPY --link --from=go-builder /out/bin/* /usr/local/bin/
COPY --link --chmod=0755 services/github-runner/roborev-wrapper.sh /usr/local/bin/roborev
COPY --link --chmod=0644 services/github-runner/roborev-config.toml /etc/roborev/config.toml
COPY --link --from=rust-builder /usr/local/rustup /usr/local/rustup
COPY --link --chown=1000:1000 --from=rust-builder /usr/local/cargo /usr/local/cargo
COPY --link --from=golangci/golangci-lint:v2.15.0 /usr/bin/golangci-lint /usr/local/bin/golangci-lint
COPY --link --from=node:22-bookworm-slim /usr/local/bin/node /usr/local/bin/node
COPY --link --from=node:22-bookworm-slim /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm && \
    ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx && \
    ln -s ../lib/node_modules/corepack/dist/corepack.js /usr/local/bin/corepack && \
    node --version && \
    npm --version
COPY --link --from=npm-tools /opt/npm-global /opt/npm-global
COPY --link --from=docker:27-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --link --from=docker:27-cli /usr/local/libexec/docker/cli-plugins/ /usr/local/libexec/docker/cli-plugins/
COPY --link --from=cli-tools /out/usr/local/bin/muse /usr/local/bin/muse
COPY --link --chown=1000:1000 --from=cli-tools /out/actions-runner /actions-runner
COPY --link --from=python-venv /opt/venv /opt/venv

ENV RUSTUP_HOME="/usr/local/rustup" \
    CARGO_HOME="/usr/local/cargo" \
    PATH="/opt/npm-global/bin:/usr/local/cargo/bin:/opt/venv/bin:/usr/local/go/bin:/data/cache/go/bin:/home/ubuntu/go/bin:/root/go/bin:${PATH}"

RUN (useradd -m -u 1000 runner 2>/dev/null || true) && \
    mkdir -p /data && \
    chown -R 1000:1000 /data

COPY services/github-runner/entrypoint.sh /entrypoint.sh
RUN chmod 0755 /entrypoint.sh

WORKDIR /actions-runner

ENTRYPOINT ["/entrypoint.sh"]
