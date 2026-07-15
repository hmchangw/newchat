FROM golang:1.26.5-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY search-service/ search-service/
RUN CGO_ENABLED=0 go build -o /search-service ./search-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /search-service /search-service
USER app
ENTRYPOINT ["/search-service"]
