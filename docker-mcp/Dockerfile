# Builds docker-mcp from upstream ha-docker-mcp-addon repository
FROM alpine/git:latest AS source
WORKDIR /src
RUN git clone --depth=1 https://github.com/azylman/ha-docker-mcp-addon.git .

FROM node:22-alpine AS build
WORKDIR /app
COPY --from=source /src/package.json /src/tsconfig.json ./
RUN npm install --ignore-scripts
COPY --from=source /src/src/ ./src/
RUN npm run build

FROM node:22-alpine
WORKDIR /app
COPY --from=build /app/dist ./dist
COPY --from=build /app/package.json ./
RUN npm install --omit=dev --ignore-scripts

EXPOSE 4002
CMD ["node", "dist/app.js"]
