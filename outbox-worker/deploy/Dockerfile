FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY outbox-worker/ outbox-worker/
RUN CGO_ENABLED=0 go build -o /outbox-worker ./outbox-worker/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /outbox-worker /outbox-worker
USER app
ENTRYPOINT ["/outbox-worker"]
