FROM node:22-alpine

# Install Docker CLI to talk to host socket, netcat and curl for healthchecks
RUN apk add --no-cache docker-cli curl netcat-openbsd

# Install universal stdio-to-HTTP MCP translation gateway
RUN npm install -g supergateway

ENV PORT=4002
EXPOSE 4002

CMD ["supergateway", "--port", "4002", "--streamableHttpPath", "/mcp", "--cors", "--stdio", "docker run -i --rm -v /var/run/docker.sock:/var/run/docker.sock mcp/docker"]
