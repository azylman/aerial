# --- Build stage --------------------------------------------------------
FROM golang:1.22-alpine AS builder

WORKDIR /build

COPY brain/go.mod brain/go.sum* ./
RUN go mod download

COPY brain/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /aerial-brain .

# --- Runtime stage -------------------------------------------------------
FROM ubuntu:24.04

# Install base utilities, certificates, Docker CLI, and CLI prerequisites
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    bash \
    git \
    jq \
    tar \
    gzip \
    tzdata \
    docker.io \
    docker-compose-v2 \
    && rm -rf /var/lib/apt/lists/*

# Install official Antigravity CLI binary
RUN curl -fsSL https://antigravity.google/cli/install.sh | bash -s -- -d /usr/local/bin

WORKDIR /app

COPY --from=builder /aerial-brain /usr/local/bin/aerial-brain
COPY GEMINI.md /app/GEMINI.md
COPY SYSTEM.md /app/SYSTEM.md
COPY .agents /app/.agents

ENV GIT_TERMINAL_PROMPT=0

EXPOSE 8080

CMD ["/usr/local/bin/aerial-brain"]

