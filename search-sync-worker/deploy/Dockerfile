FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY search-sync-worker/ search-sync-worker/
RUN CGO_ENABLED=0 go build -o /search-sync-worker ./search-sync-worker/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /search-sync-worker /search-sync-worker
USER app
ENTRYPOINT ["/search-sync-worker"]
