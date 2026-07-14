FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY upload-service/ upload-service/
RUN CGO_ENABLED=0 go build -o /upload-service ./upload-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /upload-service /upload-service
USER app
ENTRYPOINT ["/upload-service"]
