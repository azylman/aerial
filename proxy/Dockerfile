# syntax=docker/dockerfile:1
FROM nginx:1.27-alpine-slim
COPY --link default.conf /etc/nginx/conf.d/default.conf
COPY --link grafana-cyberpunk.css /etc/nginx/grafana-cyberpunk.css
COPY --link agentsview-cyberpunk.css /etc/nginx/agentsview-cyberpunk.css
COPY --link openobserve-cyberpunk.css /etc/nginx/openobserve-cyberpunk.css
EXPOSE 80
STOPSIGNAL SIGTERM
