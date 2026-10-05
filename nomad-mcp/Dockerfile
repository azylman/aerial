# syntax=docker/dockerfile:1
FROM golang:1.27.1-alpine AS builder

RUN apk add --no-cache git
ENV GOTOOLCHAIN=auto
WORKDIR /app
RUN CGO_ENABLED=0 go install github.com/kocierik/mcp-nomad@v0.3.1

FROM node:22-alpine

# Install tini init and curl for OPTIONS healthcheck
RUN apk add --no-cache tini curl

# Install universal stdio-to-HTTP MCP translation gateway and purge npm/corepack runtime bloat
RUN npm install -g supergateway@^4.1.0 && \
    rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack

# Copy compiled mcp-nomad Go binary
COPY --from=builder /go/bin/mcp-nomad /usr/local/bin/mcp-nomad

# Copy entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/entrypoint.sh"]
