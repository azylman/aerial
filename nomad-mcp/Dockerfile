# syntax=docker/dockerfile:1
FROM golang:alpine AS builder

RUN apk add --no-cache git
ENV GOTOOLCHAIN=auto
WORKDIR /app
RUN CGO_ENABLED=0 go install github.com/kocierik/mcp-nomad@v0.3.1

FROM node:22-alpine

# Install tini init and curl for OPTIONS healthcheck
RUN apk add --no-cache tini curl

# Install universal stdio-to-HTTP MCP translation gateway and purge npm/corepack runtime bloat
RUN npm install -g supergateway@^3.4.0 && \
    rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack

# Copy compiled mcp-nomad Go binary
COPY --from=builder /go/bin/mcp-nomad /usr/local/bin/mcp-nomad

ENV PORT=4006
EXPOSE 4006

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["supergateway", "--host", "0.0.0.0", "--port", "4006", "--outputTransport", "streamableHttp", "--streamableHttpPath", "/mcp", "--stateful", "--sessionTimeout", "60000", "--cors", "--stdio", "mcp-nomad -transport stdio"]
