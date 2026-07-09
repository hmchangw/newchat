FROM golang:1.26.5-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY message-worker/ message-worker/
RUN CGO_ENABLED=0 go build -o /message-worker ./message-worker/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /message-worker /message-worker
USER app
ENTRYPOINT ["/message-worker"]
