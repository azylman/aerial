# syntax=docker/dockerfile:1
FROM ghcr.io/github/github-mcp-server@sha256:fbec75de11c255213fa08d80fb166abe73d851fff631c51c0079872967720699 AS src
FROM node:22-alpine

# Install tini init, curl and netcat (no docker-cli needed)
RUN apk add --no-cache tini curl netcat-openbsd

# Install universal stdio-to-HTTP MCP translation gateway
RUN npm install -g supergateway@^3.4.0

# Copy GitHub MCP server static binary
COPY --from=src /server/github-mcp-server /usr/local/bin/github-mcp-server

ENV PORT=4003
EXPOSE 4003

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["supergateway", "--host", "0.0.0.0", "--port", "4003", "--outputTransport", "streamableHttp", "--streamableHttpPath", "/mcp", "--stateful", "--sessionTimeout", "60000", "--cors", "--stdio", "github-mcp-server stdio"]
