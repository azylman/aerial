# --- Build stage --------------------------------------------------------
FROM golang:1.22-alpine AS builder

WORKDIR /build
COPY go.mod ./
COPY main.go ./
RUN go mod tidy
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /discord-funnel .

# --- Runtime stage -------------------------------------------------------
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /discord-funnel /usr/local/bin/discord-funnel

CMD ["/usr/local/bin/discord-funnel"]
