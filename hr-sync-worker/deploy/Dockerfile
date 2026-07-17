FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY hr-sync-worker/ hr-sync-worker/
RUN CGO_ENABLED=0 go build -o /hr-sync-worker ./hr-sync-worker/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /hr-sync-worker /hr-sync-worker
USER app
ENTRYPOINT ["/hr-sync-worker"]
