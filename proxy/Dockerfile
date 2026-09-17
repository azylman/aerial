FROM nginx:alpine
COPY default.conf /etc/nginx/conf.d/default.conf
COPY grafana-cyberpunk.css /etc/nginx/grafana-cyberpunk.css
COPY agentsview-cyberpunk.css /etc/nginx/agentsview-cyberpunk.css
COPY openobserve-cyberpunk.css /etc/nginx/openobserve-cyberpunk.css
EXPOSE 80
STOPSIGNAL SIGTERM
