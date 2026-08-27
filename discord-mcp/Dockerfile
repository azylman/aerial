# 1. Fetch source code from upstream ha-addon-discord-mcp repository
FROM alpine/git:latest AS source
WORKDIR /src
RUN git clone --depth=1 https://github.com/azylman/ha-addon-discord-mcp.git .

# 2. Build TypeScript project
FROM node:20-alpine AS build
WORKDIR /app
COPY --from=source /src/discord_mcp/package.json /src/discord_mcp/tsconfig.json ./
RUN npm install --ignore-scripts

COPY --from=source /src/discord_mcp/src ./src
RUN npm run build

# 3. Production Runtime Container
FROM node:20-alpine
WORKDIR /app

RUN apk add --no-cache curl

COPY --from=build /app/build ./build
COPY --from=build /app/package.json ./
RUN npm install --omit=dev --ignore-scripts

ENV PORT=4001
EXPOSE 4001

CMD ["node", "build/app.js", "--transport", "http", "--port", "4001"]
