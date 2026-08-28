FROM node:22-alpine

RUN apk add --no-cache docker-cli curl netcat-openbsd

RUN npm install -g supergateway

ENV PORT=4003
EXPOSE 4003

CMD ["supergateway", "--host", "0.0.0.0", "--port", "4003", "--outputTransport", "sse", "--ssePath", "/sse", "--messagePath", "/message", "--cors", "--stdio", "docker run -i --rm -e GITHUB_PERSONAL_ACCESS_TOKEN ghcr.io/github/github-mcp-server"]
