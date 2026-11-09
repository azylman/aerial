# syntax=docker/dockerfile:1
FROM ghcr.io/github/github-mcp-server@sha256:ffced0d76e77428532a2ced185516992d77fd146d69eec2f244e6237d1a97796 AS src
FROM alpine:3.24

RUN apk add --no-cache tini

COPY --from=src --link /server/github-mcp-server /usr/local/bin/github-mcp-server
COPY --chmod=0755 entrypoint.sh /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/entrypoint.sh"]
