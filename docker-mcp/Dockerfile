# syntax=docker/dockerfile:1
FROM python:3.12-alpine

# Install nodejs, npm, tini init, docker-cli, curl (no netcat needed)
RUN apk add --no-cache nodejs npm tini docker-cli curl

# Install universal stdio-to-HTTP MCP translation gateway and purge npm CLI/caches
RUN --mount=type=cache,target=/root/.npm \
    npm install -g supergateway@^4.1.0 && \
    rm -rf /usr/lib/node_modules/npm /usr/bin/npm /usr/bin/npx

# Install official mcp-server-docker using BuildKit pip cache mount
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install mcp-server-docker

# Copy entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/entrypoint.sh"]
