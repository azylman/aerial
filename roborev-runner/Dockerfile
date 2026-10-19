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
ARG ROBOREV_VERSION=0.72.0
ARG ROBOREV_SHA256=962a2c7eb4c65c262b31d06dd31fa8014af3ca78405180ceb651d64517d5e56e
RUN curl -fsSL "https://github.com/kenn-io/roborev/releases/download/v${ROBOREV_VERSION}/roborev_${ROBOREV_VERSION}_linux_amd64.tar.gz" -o /tmp/roborev.tar.gz && \
    echo "${ROBOREV_SHA256}  /tmp/roborev.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/roborev.tar.gz -C /usr/local/bin roborev && \
    chmod 0755 /usr/local/bin/roborev && \
    rm -f /tmp/roborev.tar.gz

# Install GitHub Actions Runner v2.338.0 with SHA256 verification
ARG RUNNER_VERSION=2.338.0
ARG RUNNER_SHA256=af4b794c1bc41d73d40535e3fe092a39f9679cd8d965954c2aca25a05ca41d32
WORKDIR /actions-runner
RUN curl -fsSL "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz" -o /tmp/runner.tar.gz && \
    echo "${RUNNER_SHA256}  /tmp/runner.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/runner.tar.gz -C /actions-runner && \
    rm -f /tmp/runner.tar.gz

COPY roborev-runner/entrypoint.sh /entrypoint.sh
RUN chmod 0755 /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
