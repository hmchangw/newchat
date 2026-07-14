FROM golang:1.25.12-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY notification-worker/ notification-worker/

RUN CGO_ENABLED=0 go build -o /notification-worker ./notification-worker/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /notification-worker /notification-worker

USER app
ENTRYPOINT ["/notification-worker"]
