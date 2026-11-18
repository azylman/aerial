# syntax=docker/dockerfile:1
# -----------------------------------------------------------------------------
# Stage 1: Build upstream vianaz/mcp-discord TypeScript project
# -----------------------------------------------------------------------------
FROM node:22-alpine AS node-build
WORKDIR /app

# Download pinned release tarball directly and isolate McpServer protocol instance per HTTP session
RUN wget -qO- https://github.com/vianaz/mcp-discord/archive/f350aac986338cf14d9b210104cb594385347508.tar.gz | tar -xz --strip-components=1 && \
    sed -i 's/await server\.connect(transport);/await createMcpServer(discord).connect(transport);/g' src/app.ts && \
    grep -q 'await createMcpServer(discord).connect(transport);' src/app.ts

# Install dependencies with cache mount and build TypeScript
RUN --mount=type=cache,target=/root/.npm npm install --ignore-scripts
RUN npm run build
RUN npm prune --omit=dev --ignore-scripts

# -----------------------------------------------------------------------------
# Stage 2: Production Runtime
# -----------------------------------------------------------------------------
FROM node:22-alpine
WORKDIR /app

# Purge npm, corepack, and yarn runtime bloat (not needed; node runs build/app.js)
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack /opt/yarn* /root/.npm

# Copy compiled Node upstream files with decoupled layers
COPY --from=node-build --link /app/build ./build
COPY --from=node-build --link /app/package.json ./
COPY --from=node-build --link /app/node_modules ./node_modules

ENV PORT=8080

EXPOSE 8080

CMD ["node", "build/app.js", "--transport", "http", "--port", "8080"]
