# syntax=docker/dockerfile:1
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
    && rm -rf /var/lib/apt/lists/*

# Install GitHub CLI (gh)
RUN mkdir -p -m 755 /etc/apt/keyrings && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg -o /etc/apt/keyrings/githubcli-archive-keyring.gpg && \
    chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list && \
    apt-get update && apt-get install -y --no-install-recommends gh && \
    rm -rf /var/lib/apt/lists/*

# Install official Antigravity CLI (agy)
RUN curl -fsSL --compressed https://antigravity.google/cli/install.sh | bash -s -- -d /usr/local/bin

# Install kenn-io/roborev v0.72.0 with SHA256 verification
ARG ROBOREV_VERSION=0.74.0
ARG ROBOREV_SHA256=9f8b72b7bb42b1572b11b6fa52f59255a6862ce714d945b64e776f24c2192a87
RUN curl -fsSL "https://github.com/kenn-io/roborev/releases/download/v${ROBOREV_VERSION}/roborev_${ROBOREV_VERSION}_linux_amd64.tar.gz" -o /tmp/roborev.tar.gz && \
    echo "${ROBOREV_SHA256}  /tmp/roborev.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/roborev.tar.gz -C /usr/local/bin roborev && \
    chmod 0755 /usr/local/bin/roborev && \
    rm -f /tmp/roborev.tar.gz

# Install GitHub Actions Runner v2.338.0 with SHA256 verification
ARG RUNNER_VERSION=2.338.2
ARG RUNNER_SHA256=baebf204307bac6193480852732f0aba2ae97b6c5597c244f4d5558390147c40
WORKDIR /actions-runner
RUN curl -fsSL "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz" -o /tmp/runner.tar.gz && \
    echo "${RUNNER_SHA256}  /tmp/runner.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/runner.tar.gz -C /actions-runner && \
    rm -f /tmp/runner.tar.gz

COPY services/roborev-runner/entrypoint.sh /entrypoint.sh
RUN chmod 0755 /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
