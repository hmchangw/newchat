FROM golang:1.25.13-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY cache-metrics-worker/ cache-metrics-worker/
RUN CGO_ENABLED=0 go build -o /cache-metrics-worker ./cache-metrics-worker/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /cache-metrics-worker /cache-metrics-worker
USER app
ENTRYPOINT ["/cache-metrics-worker"]
