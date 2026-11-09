# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS mcp-proxy
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go install github.com/tbxark/mcp-proxy@v1.1.0

FROM node:22-alpine

# Install tini init and curl for OPTIONS healthcheck
RUN apk add --no-cache tini curl

# Install @infisical/mcp, symlink binary, and purge npm/corepack bloat
RUN --mount=type=cache,target=/root/.npm \
    npm install -g @infisical/mcp@0.0.24 && \
    ln -s /usr/local/bin/mcp /usr/local/bin/infisical-mcp && \
    rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack

# Stateless streamable-http proxy sharing one long-lived stdio child across all callers
COPY --from=mcp-proxy /go/bin/mcp-proxy /usr/local/bin/mcp-proxy

WORKDIR /app
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENV PORT=4007
EXPOSE 4007

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/usr/local/bin/entrypoint.sh"]
