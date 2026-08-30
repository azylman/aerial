FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY main.go ./
COPY static/ ./static/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o aerial-dashboard .

FROM alpine:3.20
RUN apk add --no-cache curl ca-certificates
WORKDIR /app
COPY --from=builder /app/aerial-dashboard .
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 CMD curl -fsS http://localhost:8080/health || exit 1
CMD ["./aerial-dashboard"]
