FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
COPY main_test.go ./
COPY static/ ./static/
RUN CGO_ENABLED=0 GOOS=linux go build -o aerial-dashboard .

FROM alpine:latest
RUN apk add --no-cache curl ca-certificates
WORKDIR /app
COPY --from=builder /app/aerial-dashboard .
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s CMD curl -fsS http://localhost:8080/health || exit 1
CMD ["./aerial-dashboard"]
