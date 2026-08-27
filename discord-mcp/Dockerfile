# --- Build stage --------------------------------------------------------
FROM node:20-alpine AS build
WORKDIR /app

COPY package.json tsconfig.json ./
RUN npm install --ignore-scripts

COPY src/ ./src/
RUN npm run build

# --- Runtime stage -------------------------------------------------------
FROM node:20-alpine
WORKDIR /app

COPY --from=build /app/build ./build
COPY --from=build /app/package.json ./
RUN npm install --omit=dev --ignore-scripts

EXPOSE 4001
CMD ["node", "build/app.js"]
