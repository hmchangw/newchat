# syntax=docker/dockerfile:1

# ── Build stage ───────────────────────────────────────────────────────────────
FROM golang:1.25.13-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY tools/cdc-verify/ tools/cdc-verify/

RUN CGO_ENABLED=0 go build -o /cdc-verify ./tools/cdc-verify/

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /cdc-verify /usr/local/bin/cdc-verify

EXPOSE 8091

USER app
ENTRYPOINT ["cdc-verify"]
