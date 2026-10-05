FROM golang:1.25.13-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY archive-worker/ archive-worker/
RUN CGO_ENABLED=0 go build -o /archive-worker ./archive-worker/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /archive-worker /archive-worker
USER app
ENTRYPOINT ["/archive-worker"]
