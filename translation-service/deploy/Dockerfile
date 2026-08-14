FROM golang:1.25.13-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY translation-service/ translation-service/
RUN CGO_ENABLED=0 go build -o /translation-service ./translation-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /translation-service /translation-service
USER app
ENTRYPOINT ["/translation-service"]
