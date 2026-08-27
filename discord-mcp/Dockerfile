# Builds discord-mcp from upstream ha-addon-discord-mcp repository
FROM alpine/git:latest AS source
WORKDIR /src
RUN git clone --depth=1 https://github.com/azylman/ha-addon-discord-mcp.git .

FROM node:lts-alpine AS build
WORKDIR /app
RUN apk add --no-cache git \
  && git clone https://github.com/vianaz/mcp-discord.git . \
  && git checkout 74ac920fbc6abb8e1f7d09aea84e530e12ad1d1d

COPY --from=source /src/discord_mcp/thread_tool.patch /app/thread_tool.patch
RUN git apply /app/thread_tool.patch
RUN npm install --ignore-scripts
RUN npm run build

FROM node:lts-alpine
WORKDIR /app
COPY --from=build /app/build ./build
COPY --from=build /app/package.json /app/package-lock.json ./
RUN npm install --omit=dev --ignore-scripts

EXPOSE 4001
CMD ["node", "build/app.js"]
