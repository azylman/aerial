# syntax=docker/dockerfile:1
FROM node:22-alpine

# Install tini init and curl for OPTIONS healthcheck
RUN apk add --no-cache tini curl

# Install supergateway and @infisical/mcp, symlink binary, and purge npm/corepack bloat
RUN --mount=type=cache,target=/root/.npm \
    npm install -g supergateway@^4.1.0 @infisical/mcp@0.0.24 && \
    ln -s /usr/local/bin/mcp /usr/local/bin/infisical-mcp && \
    rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack

WORKDIR /app
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENV PORT=4007
EXPOSE 4007

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/usr/local/bin/entrypoint.sh"]
