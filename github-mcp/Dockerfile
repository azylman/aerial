# syntax=docker/dockerfile:1
FROM ghcr.io/github/github-mcp-server@sha256:fbec75de11c255213fa08d80fb166abe73d851fff631c51c0079872967720699 AS src
FROM node:22-alpine

# Install tini init and curl for OPTIONS healthcheck (no netcat needed)
RUN apk add --no-cache tini curl

# Install universal stdio-to-HTTP MCP translation gateway and purge npm/corepack runtime bloat
RUN --mount=type=cache,target=/root/.npm \
    npm install -g supergateway@^4.1.0 && \
    rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack

# Copy GitHub MCP server static binary
COPY --from=src --link /server/github-mcp-server /usr/local/bin/github-mcp-server

# Copy entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/entrypoint.sh"]
