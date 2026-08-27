FROM node:22-alpine

# Install Docker CLI to talk to host socket and curl for healthchecks
RUN apk add --no-cache docker-cli curl

# Install universal stdio-to-HTTP MCP translation gateway
RUN npm install -g supergateway

EXPOSE 4002

CMD ["supergateway", "--port", "4002", "--streamableHttpPath", "/mcp", "--stdio", "docker run -i --rm -v /var/run/docker.sock:/var/run/docker.sock mcp/docker"]
