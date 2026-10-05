# syntax=docker/dockerfile:1
FROM nginx:1.30-alpine-slim

# Clean default Nginx files and prepare /docs mount point (no curl needed; uses BusyBox wget)
RUN rm -rf /usr/share/nginx/html/* /etc/nginx/conf.d/default.conf && \
    mkdir -p /docs

# Copy custom Nginx configuration
COPY --link nginx.conf /etc/nginx/conf.d/default.conf

# Copy Docsify SPA shell, vendored assets, and fallback templates
WORKDIR /usr/share/nginx/html
COPY --link app/index.html ./index.html
COPY --link app/assets/ ./assets/
COPY --link app/fallback/ ./fallback/

# Expose internal HTTP port
EXPOSE 80

HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --start-interval=2s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:80/health || exit 1

CMD ["nginx", "-g", "daemon off;"]
