FROM golang:1.25.11-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY admin-service/ admin-service/
RUN CGO_ENABLED=0 go build -o /admin-service ./admin-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /admin-service /admin-service
USER app
ENTRYPOINT ["/admin-service"]
