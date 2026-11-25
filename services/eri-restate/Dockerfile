# syntax=docker/dockerfile:1
# --- Go SDK and gazelle builder stage -----------------------------------
FROM golang:1.27.3-bookworm AS builder

# Prune Go SDK test, doc, and api directories before source copy to keep /usr/local/go layer static
RUN rm -rf /usr/local/go/test /usr/local/go/doc /usr/local/go/api

RUN git clone --depth 1 --branch v0.54.0 https://github.com/bazelbuild/bazel-gazelle.git /tmp/gazelle && \
    (cd /tmp/gazelle && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bin/gazelle ./cmd/gazelle) && \
    rm -rf /tmp/gazelle

# --- Superpowers stage ---------------------------------------------------
FROM alpine:3.24 AS superpowers

ARG SUPERPOWERS_VERSION=v7.0.1
ARG SUPERPOWERS_SHA256=2295dd270d929ddbe3f82e43ab5cc6e3dfb4afdbfb56225347394491d34d747d

RUN mkdir -p /tmp/superpowers /opt/skills /opt/superpowers && \
    wget -qO /tmp/superpowers.tar.gz "https://github.com/obra/superpowers/archive/refs/tags/${SUPERPOWERS_VERSION}.tar.gz" && \
    echo "${SUPERPOWERS_SHA256}  /tmp/superpowers.tar.gz" | sha256sum -c - && \
    tar -xz --strip-components=1 -C /tmp/superpowers -f /tmp/superpowers.tar.gz && \
    rm -f /tmp/superpowers.tar.gz && \
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

# --- Node / npm global tools stage ----------------------------------------
FROM node:22-bookworm-slim AS npm-tools

ENV NPM_CONFIG_PREFIX=/opt/npm-global

RUN npm install -g @anthropic-ai/claude-code @openai/codex && npm cache clean --force

# --- CLI tools builder stage ----------------------------------------------
FROM ubuntu:24.04 AS cli-tools

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

ARG KUBECTL_VERSION=v1.32.2
ARG KUBECTL_SHA256=4f6a959dcc5b702135f8354cc7109b542a2933c46b808b248a214c1f69f817ea
ARG HELM_VERSION=v3.17.1
ARG HELM_SHA256=3b66f3cd28409f29832b1b35b43d9922959a32d795003149707fea84cbcd4469
ARG FLUX_VERSION=2.5.1
ARG FLUX_SHA256=f64c85db4b94aefcdf6e0f2825c32573fc2bd234e5489ff332fee62776973ec3
ARG AGY_VERSION=1.5.9
ARG AGY_SHA256=8b1f5e56779f2434a0af36f1b715a8392a084de550f8c68bc79821f95fabf3fa
ARG MUSE_VERSION=1.4.23-R8977.1
ARG MUSE_SHA256=7ca9e16659f759eab3ae6c4b61c6f1e70f5f3fea1ca961ebd49044a1146956f4
ARG LINK_CLI_VERSION=1.0.0
ARG LINK_CLI_SHA256=40fa5fbade72cc9fde4014312fa5b7a3eeb6b7b151e0a97539991100f3e9943f
ARG BAZELISK_VERSION=v1.30.0
ARG BAZELISK_SHA256=a248ae2b2b62b5baebb748f6fba7203bff1a27e5d01b55399a8406744cb539e8
ARG BUILDIFIER_VERSION=v10.2.0
ARG BUILDIFIER_SHA256=470844069d710d0b7740e55007071c63e4d00be89a833dce5d1a91790e84dc28
ARG CRANE_VERSION=v0.22.1
ARG CRANE_SHA256=0ab7a1d6932a213aed964ce97666c3077fe691c8606413674a8b3e0b9ec4cda0

RUN mkdir -p /out/usr/local/bin && \
    curl -fsSL https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl -o /out/usr/local/bin/kubectl && \
    echo "${KUBECTL_SHA256}  /out/usr/local/bin/kubectl" | sha256sum -c - && \
    chmod 0755 /out/usr/local/bin/kubectl && \
    curl -fsSL https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz -o /tmp/helm.tar.gz && \
    echo "${HELM_SHA256}  /tmp/helm.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/helm.tar.gz --strip-components=1 -C /out/usr/local/bin linux-amd64/helm && \
    rm -f /tmp/helm.tar.gz && \
    chmod 0755 /out/usr/local/bin/helm && \
    curl -fsSL https://github.com/fluxcd/flux2/releases/download/v${FLUX_VERSION}/flux_${FLUX_VERSION}_linux_amd64.tar.gz -o /tmp/flux.tar.gz && \
    echo "${FLUX_SHA256}  /tmp/flux.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/flux.tar.gz -C /out/usr/local/bin flux && \
    rm -f /tmp/flux.tar.gz && \
    chmod 0755 /out/usr/local/bin/flux && \
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 "https://github.com/google-antigravity/antigravity-cli/releases/download/${AGY_VERSION}/agy_cli_linux_x64.tar.gz" -o /tmp/agy.tar.gz && \
    echo "${AGY_SHA256}  /tmp/agy.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/agy.tar.gz -O antigravity > /out/usr/local/bin/agy && \
    rm -f /tmp/agy.tar.gz && \
    chmod 0755 /out/usr/local/bin/agy && \
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 "https://lookaside.facebook.com/lookaside/muse/download/?channel=muse&version=${MUSE_VERSION}&file=muse-x86-linux" -o /out/usr/local/bin/muse && \
    echo "${MUSE_SHA256}  /out/usr/local/bin/muse" | sha256sum -c - && \
    chmod 0755 /out/usr/local/bin/muse && \
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 "https://github.com/stripe/link-cli/releases/download/%40stripe/link-cli%40${LINK_CLI_VERSION}/link-cli-linux-x64" -o /out/usr/local/bin/link-cli && \
    echo "${LINK_CLI_SHA256}  /out/usr/local/bin/link-cli" | sha256sum -c - && \
    chmod 0755 /out/usr/local/bin/link-cli && \
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 "https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-linux-amd64" -o /out/usr/local/bin/bazelisk && \
    echo "${BAZELISK_SHA256}  /out/usr/local/bin/bazelisk" | sha256sum -c - && \
    chmod 0755 /out/usr/local/bin/bazelisk && \
    ln -s bazelisk /out/usr/local/bin/bazel && \
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 "https://github.com/bazelbuild/buildtools/releases/download/${BUILDIFIER_VERSION}/buildifier-linux-amd64" -o /out/usr/local/bin/buildifier && \
    echo "${BUILDIFIER_SHA256}  /out/usr/local/bin/buildifier" | sha256sum -c - && \
    chmod 0755 /out/usr/local/bin/buildifier && \
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 "https://github.com/google/go-containerregistry/releases/download/${CRANE_VERSION}/go-containerregistry_Linux_x86_64.tar.gz" -o /tmp/crane.tar.gz && \
    echo "${CRANE_SHA256}  /tmp/crane.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/crane.tar.gz -C /out/usr/local/bin crane && \
    chmod 0755 /out/usr/local/bin/crane && \
    rm -f /tmp/crane.tar.gz

# --- Runtime stage -------------------------------------------------------
FROM ubuntu:24.04

# Install base utilities, build tools, certificates, Python, and CLI prerequisites
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    wget \
    bash \
    bubblewrap \
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

# Playwright Chromium runtime libs + CI-matching fonts for the console e2e suite (turns run without root, so they can't apt-get at runtime)
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

# Configure default SSH client to include the config copied to /data/ssh by the ssh-bootstrap initContainer
RUN mkdir -p -m 0755 /etc/ssh/ssh_config.d && \
    echo "Include /data/ssh/config" > /etc/ssh/ssh_config.d/aerial.conf && \
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
    npm --version
COPY --link --from=npm-tools /opt/npm-global /opt/npm-global
COPY --link --from=arigaio/atlas:1.3.4 /atlas /usr/local/bin/atlas
COPY --link --from=1password/op:2 /usr/local/bin/op /usr/local/bin/op
COPY --link --from=builder /out/bin/* /usr/local/bin/
COPY --link --from=cli-tools /out/usr/local/bin/ /usr/local/bin/

ENV BAZELISK_HOME=/opt/bazel-cache \
    RUSTUP_HOME="/usr/local/rustup" \
    CARGO_HOME="/usr/local/cargo" \
    PATH="/opt/npm-global/bin:/usr/local/cargo/bin:/usr/local/go/bin:/root/go/bin:${PATH}"
ENV AGY_CLI_DISABLE_AUTO_UPDATE=true
ENV LINK_AUTH_FILE="/data/runtimes/discord/.config/link-cli-nodejs/config.json"

RUN BAZELISK_HOME=/opt/bazel-cache USE_BAZEL_VERSION=9.3.3 /usr/local/bin/bazelisk version && \
    chmod -R a+rX /opt/bazel-cache && \
    echo 'common --config=cluster' > /etc/bazel.bazelrc && \
    chmod 0644 /etc/bazel.bazelrc

# Copy decoupled superpowers skills
COPY --link --from=superpowers /opt/skills /opt/skills
COPY --link --from=superpowers /opt/superpowers /opt/superpowers

WORKDIR /app

RUN ln -s /app/eri-restate /usr/local/bin/aerial-brain && \
    ln -s /app/eri-restate /usr/local/bin/aerial-eri && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/share-guard && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/live-state-guard && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/schedule-guard && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/batch-guard && \
    ln -s /usr/local/bin/hook-guard /usr/local/bin/search-guard && \
    ln -sf /usr/local/bin/hook-guard /usr/local/bin/commit-guard

COPY --link scripts/ /usr/local/bin/
RUN chmod +x /usr/local/bin/*.sh 2>/dev/null || true
COPY --link rules /app/rules
COPY --link .agents /app/.agents
COPY --link .bazel-stage/hook-guard /usr/local/bin/hook-guard
COPY --link .bazel-stage/transcript-search /usr/local/bin/transcript-search
COPY --link .bazel-stage/eri-restate /app/eri-restate

ENV GIT_TERMINAL_PROMPT=0

EXPOSE 9080 4028

ENTRYPOINT ["/app/eri-restate"]
