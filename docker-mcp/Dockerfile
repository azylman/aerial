FROM mcp/docker:latest AS src
FROM node:22-alpine

# Install tini init, docker-cli (for host socket communication), curl and netcat
RUN apk add --no-cache tini docker-cli curl netcat-openbsd

# Install universal stdio-to-HTTP MCP translation gateway
RUN npm install -g supergateway@^3.4.0

# Copy Nix environment and entrypoint closure
COPY --from=src /app /app
COPY --from=src /nix /nix

ENV PORT=4002
EXPOSE 4002

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["supergateway", "--host", "0.0.0.0", "--port", "4002", "--outputTransport", "streamableHttp", "--streamableHttpPath", "/mcp", "--stateful", "--sessionTimeout", "60000", "--cors", "--stdio", "/app/result/bin/entrypoint"]
