# syntax=docker/dockerfile:1
FROM nginx:1.30-alpine-slim
COPY --link 40-link-nomad-config.sh /docker-entrypoint.d/40-link-nomad-config.sh
RUN chmod +x /docker-entrypoint.d/40-link-nomad-config.sh
COPY --link default.conf /etc/nginx/conf.d/default.conf
COPY --link grafana-cyberpunk.css /etc/nginx/grafana-cyberpunk.css
COPY --link agentsview-cyberpunk.css /etc/nginx/agentsview-cyberpunk.css
COPY --link openobserve-cyberpunk.css /etc/nginx/openobserve-cyberpunk.css
EXPOSE 80
STOPSIGNAL SIGTERM
