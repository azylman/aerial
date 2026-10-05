# syntax=docker/dockerfile:1
# -----------------------------------------------------------------------------
# Stage 1: Build upstream vianaz/mcp-discord TypeScript project
# -----------------------------------------------------------------------------
FROM node:22-alpine AS node-build
WORKDIR /app

# Download pinned release tarball directly (avoids git package and commit history overhead)
RUN wget -qO- https://github.com/vianaz/mcp-discord/archive/74ac920fbc6abb8e1f7d09aea84e530e12ad1d1d.tar.gz | tar -xz --strip-components=1

# Install dependencies with cache mount and build TypeScript
RUN --mount=type=cache,target=/root/.npm npm install --ignore-scripts
RUN npm run build
RUN npm prune --omit=dev --ignore-scripts

# -----------------------------------------------------------------------------
# Stage 2: Build Go proxy wrapper binary
# -----------------------------------------------------------------------------
FROM golang:1.24-alpine AS go-build
WORKDIR /build

COPY go.mod go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY main.go proxy.go pure.go ./

# Build statically linked Go proxy with compiler cache mounts
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /discord-mcp .

# -----------------------------------------------------------------------------
# Stage 3: Production Runtime
# -----------------------------------------------------------------------------
FROM node:22-alpine
WORKDIR /app

# Purge npm, corepack, and yarn runtime bloat (not needed; node runs build/app.js)
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack /opt/yarn* /root/.npm

# Copy compiled Node upstream files with decoupled layers
COPY --from=node-build --link /app/build ./build
COPY --from=node-build --link /app/package.json ./
COPY --from=node-build --link /app/node_modules ./node_modules

# Copy Go wrapper binary
COPY --from=go-build --link /discord-mcp /app/discord-mcp

ENV PORT=4001
ENV UPSTREAM_PORT=4005
ENV NODE_BIN=node
ENV APP_PATH=build/app.js

EXPOSE 4001

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --start-interval=2s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:4001/health || exit 1

CMD ["/app/discord-mcp"]
