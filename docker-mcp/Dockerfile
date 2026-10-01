# syntax=docker/dockerfile:1
FROM python:3.12-alpine

# Install nodejs, npm, tini init, docker-cli, curl (no netcat needed)
RUN apk add --no-cache nodejs npm tini docker-cli curl

# Install universal stdio-to-HTTP MCP translation gateway and purge npm CLI/caches
RUN --mount=type=cache,target=/root/.npm \
    npm install -g supergateway@^3.4.0 && \
    rm -rf /usr/lib/node_modules/npm /usr/bin/npm /usr/bin/npx

# Install official mcp-server-docker using BuildKit pip cache mount
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install mcp-server-docker

ENV PORT=4002
EXPOSE 4002

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["supergateway", "--host", "0.0.0.0", "--port", "4002", "--outputTransport", "streamableHttp", "--streamableHttpPath", "/mcp", "--stateful", "--sessionTimeout", "60000", "--cors", "--stdio", "mcp-server-docker"]
