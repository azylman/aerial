FROM python:3.12-alpine

# Install nodejs, npm, tini init, docker-cli, curl and netcat
RUN apk add --no-cache nodejs npm tini docker-cli curl netcat-openbsd

# Install universal stdio-to-HTTP MCP translation gateway
RUN npm install -g supergateway@^3.4.0

# Install official mcp-server-docker
RUN pip install --no-cache-dir mcp-server-docker

ENV PORT=4002
EXPOSE 4002

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["supergateway", "--host", "0.0.0.0", "--port", "4002", "--outputTransport", "streamableHttp", "--streamableHttpPath", "/mcp", "--stateful", "--sessionTimeout", "60000", "--cors", "--stdio", "mcp-server-docker"]
