# --- Build stage --------------------------------------------------------
FROM node:22-alpine AS build
WORKDIR /app

COPY package.json tsconfig.json ./
RUN npm install --ignore-scripts

COPY src/ ./src/
RUN npm run build

# --- Runtime stage -------------------------------------------------------
FROM node:22-alpine
WORKDIR /app

COPY --from=build /app/dist ./dist
COPY --from=build /app/package.json ./
RUN npm install --omit=dev --ignore-scripts

EXPOSE 4002
CMD ["node", "dist/app.js"]
