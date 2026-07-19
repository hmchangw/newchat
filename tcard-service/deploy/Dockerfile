FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY tcard-service/ tcard-service/
RUN CGO_ENABLED=0 go build -o /tcard-service ./tcard-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /tcard-service /tcard-service
USER app
ENTRYPOINT ["/tcard-service"]
