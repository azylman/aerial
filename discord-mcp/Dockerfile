# -----------------------------------------------------------------------------
# Stage 1: Build upstream vianaz/mcp-discord TypeScript project
# -----------------------------------------------------------------------------
FROM node:20-alpine AS node-build
WORKDIR /app
RUN apk add --no-cache git

# Clone vianaz/mcp-discord fix branch
RUN git clone --depth=1 --branch fix/http-per-session-mcp-server https://github.com/vianaz/mcp-discord.git .

# Install dependencies and build TypeScript
RUN npm install --ignore-scripts
RUN npm run build
RUN npm prune --omit=dev --ignore-scripts

# -----------------------------------------------------------------------------
# Stage 2: Build Go proxy wrapper binary with automated tests
# -----------------------------------------------------------------------------
FROM golang:1.22-alpine AS go-build
WORKDIR /build

COPY go.mod ./
COPY main.go proxy.go proxy_test.go ./

# Run unit tests during container build
RUN CGO_ENABLED=0 go test -v ./...

# Build statically linked Go proxy
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /discord-mcp .

# -----------------------------------------------------------------------------
# Stage 3: Production Runtime
# -----------------------------------------------------------------------------
FROM node:20-alpine
WORKDIR /app

RUN apk add --no-cache curl

# Copy compiled Node upstream files
COPY --from=node-build /app/build ./build
COPY --from=node-build /app/package.json ./
COPY --from=node-build /app/node_modules ./node_modules

# Copy Go wrapper binary
COPY --from=go-build /discord-mcp /app/discord-mcp

ENV PORT=4001
ENV UPSTREAM_PORT=4005
ENV NODE_BIN=node
ENV APP_PATH=build/app.js

EXPOSE 4001

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
  CMD curl -fsS http://localhost:4001/health || exit 1

CMD ["/app/discord-mcp"]
