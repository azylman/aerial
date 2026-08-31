FROM nginx:1.27-alpine

# Install curl for healthchecking and clean default Nginx files
RUN apk add --no-cache curl ca-certificates && \
    rm -rf /usr/share/nginx/html/* /etc/nginx/conf.d/default.conf && \
    mkdir -p /docs

# Copy custom Nginx configuration
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy Docsify SPA shell, vendored assets, and fallback templates
WORKDIR /usr/share/nginx/html
COPY app/index.html ./index.html
COPY app/assets/ ./assets/
COPY app/fallback/ ./fallback/

# Expose internal HTTP port
EXPOSE 80

HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -fsS http://127.0.0.1:80/health || exit 1

CMD ["nginx", "-g", "daemon off;"]
