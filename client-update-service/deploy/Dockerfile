FROM golang:1.25.13-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY client-update-service/ client-update-service/
RUN CGO_ENABLED=0 go build -o /client-update-service ./client-update-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /client-update-service /client-update-service
USER app
ENTRYPOINT ["/client-update-service"]
