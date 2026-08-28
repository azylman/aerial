FROM golang:1.22-alpine AS builder

WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/scheduler-mcp .

FROM alpine:latest

RUN apk add --no-cache curl tzdata

WORKDIR /app
COPY --from=builder /app/scheduler-mcp /app/scheduler-mcp

ENV PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
  CMD curl -fsS http://localhost:8080/health || exit 1

CMD ["/app/scheduler-mcp"]
