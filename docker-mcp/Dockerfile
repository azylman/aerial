FROM node:22-alpine

# Install Docker CLI to talk to host socket, netcat and curl for healthchecks
RUN apk add --no-cache docker-cli curl netcat-openbsd

# Install universal stdio-to-HTTP MCP translation gateway
RUN npm install -g supergateway

ENV PORT=4002
EXPOSE 4002

CMD ["supergateway", "--host", "0.0.0.0", "--port", "4002", "--outputTransport", "sse", "--ssePath", "/sse", "--messagePath", "/message", "--cors", "--stdio", "docker run -i --rm -v /var/run/docker.sock:/var/run/docker.sock mcp/docker"]
