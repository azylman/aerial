# syntax=docker/dockerfile:1
FROM arigaio/atlas:1.3.4

# Copy core migrations into container image
COPY services/eri/pkg/db/migrations /migrations

USER 0:0
ENTRYPOINT ["/atlas"]
