FROM golang:1.26.5-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY media-service/ media-service/
RUN CGO_ENABLED=0 go build -o /media-service ./media-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /media-service /media-service
USER app
ENTRYPOINT ["/media-service"]
