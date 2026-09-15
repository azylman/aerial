FROM ghcr.io/gethomepage/homepage:latest

USER root
COPY config/ /app/core-config/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
